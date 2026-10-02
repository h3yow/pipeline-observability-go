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
	"sort"
	"strings"
	"sync"

	"github.com/h3yow/pipeline-observability-go/internal/model"
)

// Transition is one alert lifecycle transition.
type Transition struct {
	Alert     model.Alert
	FromState model.AlertState
	ToState   model.AlertState
	// RunID is the run whose observation caused the transition. It makes
	// canonical replay (e.g. after backfill) explainable and sortable.
	RunID    model.RunID
	Evidence string
}

// Observation is one determinable run's alert observation, supplied in
// canonical event-time order for a lifecycle rebuild. Unknown (not
// determinable) runs must never be observed at all.
type Observation struct {
	RunID         model.RunID
	JobID         model.JobID
	Kind          model.AlertKind
	Breach        bool
	RuleVer       int
	FireAfterN    int
	RecoverAfterN int
	At            int64
	Evidence      string
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

	// obsID is "RunID:Kind" (built by the engine); recover the run id so the
	// transition records its provenance without changing the public API.
	runID := model.RunID(obsID)
	if i := strings.LastIndex(obsID, ":"); i >= 0 {
		runID = model.RunID(obsID[:i])
	}

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
	outcome := m.apply(s, runID, jobID, kind, breach, ruleVer, fireAfterN, recoverAfterN, at, evidence)
	s.applied[obsID] = obsResult{transition: outcome}
	return outcome
}

func newSlot() *slot {
	return &slot{state: model.AlertStateOK, applied: map[string]obsResult{}}
}

// apply performs the state transition for one genuinely new observation.
// Observations must be delivered in run order; the engine feeds them sorted
// by event time (out-of-order/late handling happens at intake).
func (m *Manager) apply(s *slot, runID model.RunID, jobID model.JobID, kind model.AlertKind, breach bool,
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
		return m.fire(s, runID, jobID, kind, ruleVer, at, evidence)
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
		return m.recover(s, runID, jobID, kind, at, evidence)
	default:
		s.state = model.AlertStateOK
		s.breachRun, s.passRun = 0, 0
		return nil
	}
}

func (m *Manager) fire(s *slot, runID model.RunID, jobID model.JobID, kind model.AlertKind,
	ruleVer int, at int64, evidence string) *Transition {
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
		RunID:     runID,
		Evidence:  evidence,
	}
	s.state = model.AlertStateFiring
	s.firedVersion, s.firedSeq = ruleVer, m.seq
	s.breachRun, s.passRun = 0, 0
	m.events = append(m.events, tr)
	return &tr
}

func (m *Manager) recover(s *slot, runID model.RunID, jobID model.JobID, kind model.AlertKind,
	at int64, evidence string) *Transition {
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
		RunID:     runID,
		Evidence:  evidence,
	}
	m.events = append(m.events, tr)
	// Reset to OK so a later breach re-fires as a NEW, distinguishable alert.
	s.state = model.AlertStateOK
	s.breachRun, s.passRun = 0, 0
	s.firedSeq = 0
	return &tr
}

// Rebuild replaces the alert lifecycle of the kinds appearing in obs with a
// fresh fold over the supplied observations, which must already be in
// canonical order (At, then RunID, then Kind). Every other kind keeps its
// state. Afterwards the complete transition log is re-sorted canonically and
// re-sequenced, so the global AlertEvents() history is byte-identical to the
// history a manager would hold had every observation — including ones
// recovered by backfill — arrived in normal event-time order. It returns the
// transitions that now exist for the rebuilt kinds (in canonical order).
func (m *Manager) Rebuild(obs []Observation) []Transition {
	m.mu.Lock()
	defer m.mu.Unlock()

	kinds := map[string]bool{}
	for _, o := range obs {
		kinds[alertKey(o.JobID, o.Kind)] = true
	}

	// Drop the old slots and log entries for the rebuilt kinds.
	kept := make([]Transition, 0, len(m.events))
	for _, tr := range m.events {
		if !kinds[alertKey(tr.Alert.JobID, tr.Alert.Kind)] {
			kept = append(kept, tr)
		}
	}
	for k := range kinds {
		delete(m.slots, k)
	}

	var rebuilt []Transition
	for _, o := range obs {
		k := alertKey(o.JobID, o.Kind)
		s, ok := m.slots[k]
		if !ok {
			s = newSlot()
			m.slots[k] = s
		}
		obsID := string(o.RunID) + ":" + string(o.Kind)
		if tr := m.apply(s, o.RunID, o.JobID, o.Kind, o.Breach,
			o.RuleVer, o.FireAfterN, o.RecoverAfterN, o.At, o.Evidence); tr != nil {
			s.applied[obsID] = obsResult{transition: tr}
			rebuilt = append(rebuilt, *tr)
		} else {
			s.applied[obsID] = obsResult{}
		}
	}

	// Canonical global log: observation event time, then run, then kind.
	all := kept
	all = append(all, rebuilt...)
	sort.SliceStable(all, func(i, j int) bool {
		ti, tj := all[i].Alert.Timestamp.UnixNano(), all[j].Alert.Timestamp.UnixNano()
		if ti != tj {
			return ti < tj
		}
		if ri, rj := string(all[i].RunID), string(all[j].RunID); ri != rj {
			return ri < rj
		}
		return string(all[i].Alert.Kind) < string(all[j].Alert.Kind)
	})
	// Re-sequence in canonical order so histories compare across paths, and
	// rewrite each RECOVERED reason to point at the global seq of the FIRING
	// it recovers (its fold-local seq may differ from the global position).
	lastFiring := map[string]int64{}
	for i := range all {
		all[i].Alert.Seq = int64(i + 1)
		key := alertKey(all[i].Alert.JobID, all[i].Alert.Kind)
		switch all[i].ToState {
		case model.AlertStateFiring:
			lastFiring[key] = all[i].Alert.Seq
		case model.AlertStateRecover:
			if fs, ok := lastFiring[key]; ok {
				// Preserve the fold-produced wording (incl. the pass-run
				// count) so canonical histories are textually identical to
				// the incremental path; only retarget the firing seq, whose
				// global position may differ from the fold-local one.
				if prefix, _, ok := strings.Cut(all[i].Alert.Reason, "firing alert seq="); ok {
					all[i].Alert.Reason = prefix + fmt.Sprintf("firing alert seq=%d", fs)
				}
			}
		}
	}
	m.seq = int64(len(all))
	m.events = all
	return rebuilt
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
