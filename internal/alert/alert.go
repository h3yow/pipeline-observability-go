// Package alert implements the per-job alert lifecycle state machine with
// debounced firing/recovery so threshold flapping never repeats alerts.
//
// Lifecycle (independently per (jobID, kind)):
//
//	OK --breachCount >= FireAfterN--> FIRING      (exactly one firing alert)
//	FIRING --passCount >= RecoverAfterN--> RECOVERED (exactly one recovery
//	                                                 alert), state resets OK
//
// While in FIRING, repeated breaches produce NO new alerts; isolated
// passes that do not meet the recovery threshold are absorbed, so flapping
// near the boundary cannot generate duplicate alerts. Recovery and a later
// re-fire are distinct, separately sequenced alerts.
package alert

import (
	"fmt"
	"sync"

	"github.com/h3yow/pipeline-observability-go/internal/model"
)

// Transition is one alert lifecycle transition.
type Transition struct {
	Alert     model.Alert
	FromState model.AlertState
	ToState   model.AlertState
	Evidence  string
}

type obsResult struct {
	transition *Transition
}

type slot struct {
	state        model.AlertState
	breachRun    int // consecutive breaches while OK
	passRun      int // consecutive passes while FIRING
	firedVersion int
	firedSeq     int64
	// applied deduplicates re-delivered observations (e.g. same runID fed
	// twice); the original outcome is returned instead of re-applied.
	applied map[string]obsResult
	// emitted records transitions already emitted for this slot, keyed by
	// (toState, triggering observation) and valued by the assigned seq.
	// Replays (Recompute) never emit the same lifecycle transition twice.
	emitted map[string]int64
}

// Manager owns per (job, kind) alert lifecycle state.
type Manager struct {
	mu     sync.Mutex
	seq    int64
	slots  map[string]*slot
	events []Transition
}

// NewManager creates an alert Manager.
func NewManager() *Manager {
	return &Manager{slots: map[string]*slot{}}
}

func alertKey(jobID model.JobID, kind model.AlertKind) string {
	return string(jobID) + "|" + string(kind)
}

// Observe feeds one breach/pass observation for a (job, kind) pair.
//
// fireAfterN/recoverAfterN come from the versioned rule set bound at fire
// time. ruleVer is stamped on the firing alert. It returns a Transition
// exactly once per lifecycle change; nil while a debounce counter is
// accumulating (including repeated breaches while already FIRING).
func (m *Manager) Observe(obsID string, jobID model.JobID, kind model.AlertKind, breach bool,
	ruleVer, fireAfterN, recoverAfterN int, at int64, evidence string) *Transition {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := alertKey(jobID, kind)
	s, ok := m.slots[k]
	if !ok {
		s = newSlot()
		m.slots[k] = s
	}
	// Idempotent re-delivery: return the original outcome, do not advance.
	if prior, seen := s.applied[obsID]; seen {
		return prior.transition
	}
	var outcome *Transition
	if tr := m.apply(s, jobID, kind, breach, ruleVer, fireAfterN, recoverAfterN, at, evidence); tr != nil {
		emitted, _ := m.emitLocked(s, *tr, obsID)
		if emitted.ToState == model.AlertStateFiring {
			s.firedSeq = emitted.Alert.Seq
		}
		outcome = &emitted
	}
	s.applied[obsID] = obsResult{transition: outcome}
	return outcome
}

// Observation is one breach/pass input for a canonical replay.
type Observation struct {
	ObsID         string // unique per run+kind; identifies the observation
	Breach        bool
	RuleVer       int
	FireAfterN    int
	RecoverAfterN int
	At            int64 // event time of the observation (unix ns)
	Evidence      string
}

// Recompute resets the (job, kind) slot and replays the given observations
// as the complete, canonically ordered history. Transitions already emitted
// (in this or earlier calls) are not emitted again; only genuinely new
// transitions are returned and appended to the event log.
//
// This is how the engine incorporates backfilled runs: inserting the
// resolved run into its canonical position and replaying yields exactly
// the alert lifecycle that in-order delivery would have produced, without
// duplicating alerts that already went out.
func (m *Manager) Recompute(jobID model.JobID, kind model.AlertKind, obs []Observation) []Transition {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := alertKey(jobID, kind)
	s, ok := m.slots[k]
	if !ok {
		s = newSlot()
		m.slots[k] = s
	}
	// Reset runtime state; emitted/applied survive so replays stay
	// idempotent and cross-references (firedSeq) are re-established below.
	s.state = model.AlertStateOK
	s.breachRun, s.passRun = 0, 0
	s.firedVersion, s.firedSeq = 0, 0

	var out []Transition
	for _, o := range obs {
		tr := m.apply(s, jobID, kind, o.Breach, o.RuleVer, o.FireAfterN, o.RecoverAfterN, o.At, o.Evidence)
		if tr == nil {
			continue
		}
		emitted, isNew := m.emitLocked(s, *tr, o.ObsID)
		if emitted.ToState == model.AlertStateFiring {
			s.firedSeq = emitted.Alert.Seq
		}
		if isNew {
			out = append(out, emitted)
		}
	}
	return out
}

func newSlot() *slot {
	return &slot{state: model.AlertStateOK, applied: map[string]obsResult{}, emitted: map[string]int64{}}
}

// emitLocked resolves the transition's identity (toState + triggering
// observation): if this slot already emitted it, the original seq is
// reused and the second return value is false; otherwise a new seq is
// assigned and the transition is appended to the event log. This makes
// replays safe: a recomputed history never duplicates an emitted alert.
func (m *Manager) emitLocked(s *slot, tr Transition, triggerObsID string) (Transition, bool) {
	key := string(tr.ToState) + "|" + triggerObsID
	if seq, ok := s.emitted[key]; ok {
		tr.Alert.Seq = seq
		return tr, false
	}
	m.seq++
	s.emitted[key] = m.seq
	tr.Alert.Seq = m.seq
	m.events = append(m.events, tr)
	return tr, true
}

// apply performs the state transition for one genuinely new observation.
// Observations must be delivered in run order; the engine feeds them sorted
// by event time (out-of-order/late handling happens at intake).
//
// apply is pure with respect to the event log: it only advances the slot
// state machine and returns the transition (if any) without a sequence
// number. Emission (seq assignment, log append, replay dedupe) happens in
// emitLocked, so both Observe and Recompute share one state machine.
func (m *Manager) apply(s *slot, jobID model.JobID, kind model.AlertKind, breach bool,
	ruleVer, fireAfterN, recoverAfterN int, at int64, evidence string) *Transition {
	switch s.state {
	case model.AlertStateOK:
		if !breach {
			if s.breachRun != 0 {
				s.breachRun = 0 // a pass resets the fire debounce
			}
			return nil
		}
		s.breachRun++
		if s.breachRun < fireAfterN {
			return nil
		}
		return m.fire(jobID, kind, s, ruleVer, at, evidence)
	case model.AlertStateFiring:
		if breach {
			// Already firing: repeated breach is a no-op (no duplicate alert).
			s.passRun = 0
			return nil
		}
		s.passRun++
		if s.passRun < recoverAfterN {
			return nil
		}
		return m.recover(jobID, kind, s, at, evidence)
	default:
		s.state = model.AlertStateOK
		s.breachRun, s.passRun = 0, 0
		return nil
	}
}

func (m *Manager) fire(jobID model.JobID, kind model.AlertKind,
	s *slot, ruleVer int, at int64, evidence string) *Transition {
	al := model.Alert{
		JobID:     jobID,
		Kind:      kind,
		State:     model.AlertStateFiring,
		Version:   ruleVer,
		Reason:    fmt.Sprintf("%s breach confirmed after %d consecutive breach run(s)", kind, s.breachRun),
		Timestamp: atTime(at),
	}
	tr := Transition{
		Alert:     al,
		FromState: model.AlertStateOK,
		ToState:   model.AlertStateFiring,
		Evidence:  evidence,
	}
	s.state = model.AlertStateFiring
	s.firedVersion = ruleVer
	s.breachRun, s.passRun = 0, 0
	return &tr
}

func (m *Manager) recover(jobID model.JobID, kind model.AlertKind,
	s *slot, at int64, evidence string) *Transition {
	al := model.Alert{
		JobID:   jobID,
		Kind:    kind,
		State:   model.AlertStateRecover,
		Version: s.firedVersion,
		Reason: fmt.Sprintf("%s recovered after %d consecutive pass run(s); firing alert seq=%d",
			kind, s.passRun, s.firedSeq),
		Timestamp: atTime(at),
	}
	tr := Transition{
		Alert:     al,
		FromState: model.AlertStateFiring,
		ToState:   model.AlertStateRecover,
		Evidence:  evidence,
	}
	// Reset to OK so a later breach re-fires as a NEW, distinguishable alert.
	s.state = model.AlertStateOK
	s.breachRun, s.passRun = 0, 0
	return &tr
}

// State returns the current lifecycle state of one alert.
func (m *Manager) State(jobID model.JobID, kind model.AlertKind) (model.AlertState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.slots[alertKey(jobID, kind)]
	if !ok {
		return "", false
	}
	return s.state, true
}

// Events returns all emitted lifecycle transitions in emission order.
func (m *Manager) Events() []Transition {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Transition, len(m.events))
	copy(out, m.events)
	return out
}

// Snapshot returns a copy of current states keyed by "jobID|kind".
func (m *Manager) Snapshot() map[string]model.AlertState {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]model.AlertState, len(m.slots))
	for k, s := range m.slots {
		out[k] = s.state
	}
	return out
}
