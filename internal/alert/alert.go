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
	outcome := m.apply(s, jobID, kind, breach, ruleVer, fireAfterN, recoverAfterN, at, evidence)
	s.applied[obsID] = obsResult{transition: outcome}
	return outcome
}

func newSlot() *slot {
	return &slot{state: model.AlertStateOK, applied: map[string]obsResult{}}
}

// apply performs the state transition for one genuinely new observation.
// Observations must be delivered in run order; the engine feeds them sorted
// by event time (out-of-order/late handling happens at intake).
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
	m.seq++
	al := model.Alert{
		JobID:     jobID,
		Kind:      kind,
		State:     model.AlertStateFiring,
		Version:   ruleVer,
		Reason:    fmt.Sprintf("%s breach confirmed after %d consecutive breach run(s)", kind, s.breachRun),
		Timestamp: atTime(at),
		Seq:       m.seq,
	}
	tr := Transition{
		Alert:     al,
		FromState: model.AlertStateOK,
		ToState:   model.AlertStateFiring,
		Evidence:  evidence,
	}
	s.state = model.AlertStateFiring
	s.firedVersion, s.firedSeq = ruleVer, m.seq
	s.breachRun, s.passRun = 0, 0
	m.events = append(m.events, tr)
	return &tr
}

func (m *Manager) recover(jobID model.JobID, kind model.AlertKind,
	s *slot, at int64, evidence string) *Transition {
	m.seq++
	al := model.Alert{
		JobID:   jobID,
		Kind:    kind,
		State:   model.AlertStateRecover,
		Version: s.firedVersion,
		Reason: fmt.Sprintf("%s recovered after %d consecutive pass run(s); firing alert seq=%d",
			kind, s.passRun, s.firedSeq),
		Timestamp: atTime(at),
		Seq:       m.seq,
	}
	tr := Transition{
		Alert:     al,
		FromState: model.AlertStateFiring,
		ToState:   model.AlertStateRecover,
		Evidence:  evidence,
	}
	m.events = append(m.events, tr)
	// Reset to OK so a later breach re-fires as a NEW, distinguishable alert.
	s.state = model.AlertStateOK
	s.breachRun, s.passRun = 0, 0
	s.firedSeq = 0
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
