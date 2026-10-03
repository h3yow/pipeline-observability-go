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
//
// Canonical history & backfill:
//
// Each (jobID, kind) slot owns a CANONICAL alert history: the exact
// transition sequence the canonically ordered observations produce.
// Alert.Seq is the 1-based position of a transition within its slot's
// history (not a global counter). Recompute replays the complete
// observation list and REBUILDS the slot history to match it:
// transitions the replay no longer produces (e.g. a backfilled pass that
// breaks a breach streak, or a firing point that moved) are removed, and
// seqs are renumbered to canonical positions. The rebuilt history is
// therefore identical — count, order, seqs and the fire a recovery refers
// to — to the history the never-lost, in-order path would have produced.
package alert

import (
	"fmt"
	"sort"
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

// historyEntry is one canonical transition together with its identity key
// (target state + triggering observation). The key is what replays match
// against to decide whether a transition is genuinely new.
type historyEntry struct {
	key string
	tr  Transition
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
	// history is the slot's canonical alert history in emission order.
	// Each entry's Alert.Seq equals its 1-based position in this slice.
	history []historyEntry
}

// Manager owns per (job, kind) alert lifecycle state.
type Manager struct {
	mu    sync.Mutex
	slots map[string]*slot
}

// NewManager creates an alert Manager.
func NewManager() *Manager {
	return &Manager{slots: map[string]*slot{}}
}

func alertKey(jobID model.JobID, kind model.AlertKind) string {
	return string(jobID) + "|" + string(kind)
}

// transitionKey is the identity of a lifecycle transition: its target
// state plus the observation that triggered it. Two replays that produce
// the same key refer to the same alert; a replay that drops a key has
// invalidated the corresponding alert.
func transitionKey(toState model.AlertState, triggerObsID string) string {
	return string(toState) + "|" + triggerObsID
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

	s := m.slotLocked(jobID, kind)
	// Idempotent re-delivery: return the original outcome, do not advance.
	if prior, seen := s.applied[obsID]; seen {
		return prior.transition
	}
	var outcome *Transition
	if tr := m.apply(s, jobID, kind, breach, ruleVer, fireAfterN, recoverAfterN, at, evidence); tr != nil {
		tr.Alert.Seq = int64(len(s.history)) + 1
		if tr.ToState == model.AlertStateFiring {
			s.firedSeq = tr.Alert.Seq
		}
		s.history = append(s.history, historyEntry{key: transitionKey(tr.ToState, obsID), tr: *tr})
		outcome = tr
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
// as the complete, canonically ordered history, then REBUILDS the slot's
// alert history to exactly what the replay produced:
//
//   - transitions already present (same target state + triggering
//     observation) are kept and are NOT returned again;
//   - transitions the replay no longer produces are removed from the
//     history — a backfill can move the firing point earlier/later or
//     invalidate a firing entirely, and the stale alert must not linger;
//   - seqs are renumbered to canonical 1-based positions, so the rebuilt
//     history equals the never-lost, in-order history entry by entry
//     (including the fire seq a recovery refers to).
//
// The return value holds only the genuinely new transitions (those whose
// identity was not in the previous history).
func (m *Manager) Recompute(jobID model.JobID, kind model.AlertKind, obs []Observation) []Transition {
	m.mu.Lock()
	defer m.mu.Unlock()

	s := m.slotLocked(jobID, kind)
	prev := make(map[string]bool, len(s.history))
	for _, e := range s.history {
		prev[e.key] = true
	}

	// Reset runtime state; the history is rebuilt below. Cross-references
	// (firedSeq) are re-established in canonical order as we replay.
	s.state = model.AlertStateOK
	s.breachRun, s.passRun = 0, 0
	s.firedVersion, s.firedSeq = 0, 0

	var canon []historyEntry
	var newOnes []Transition
	for _, o := range obs {
		tr := m.apply(s, jobID, kind, o.Breach, o.RuleVer, o.FireAfterN, o.RecoverAfterN, o.At, o.Evidence)
		if tr == nil {
			continue
		}
		tr.Alert.Seq = int64(len(canon)) + 1
		if tr.ToState == model.AlertStateFiring {
			s.firedSeq = tr.Alert.Seq
		}
		key := transitionKey(tr.ToState, o.ObsID)
		canon = append(canon, historyEntry{key: key, tr: *tr})
		if !prev[key] {
			newOnes = append(newOnes, *tr)
		}
	}
	s.history = canon
	return newOnes
}

func (m *Manager) slotLocked(jobID model.JobID, kind model.AlertKind) *slot {
	k := alertKey(jobID, kind)
	s, ok := m.slots[k]
	if !ok {
		s = &slot{state: model.AlertStateOK, applied: map[string]obsResult{}}
		m.slots[k] = s
	}
	return s
}

// apply performs the state transition for one genuinely new observation.
// Observations must be delivered in run order; the engine feeds them sorted
// by event time (out-of-order/late handling happens at intake).
//
// apply is pure with respect to the history: it only advances the slot
// state machine and returns the transition (if any) without a sequence
// number. Sequencing and history bookkeeping happen in the callers
// (Observe / Recompute), so both share one state machine.
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

// Events returns the canonical alert history of every slot. Slots are
// ordered by their (jobID|kind) key so the result is deterministic even
// when slots were created concurrently; within a slot, entries are in
// canonical emission order (Alert.Seq is the 1-based position).
func (m *Manager) Events() []Transition {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.slots))
	for k := range m.slots {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []Transition
	for _, k := range keys {
		for _, e := range m.slots[k].history {
			out = append(out, e.tr)
		}
	}
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
