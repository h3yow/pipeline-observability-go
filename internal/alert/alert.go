// Package alert implements the per-job alert lifecycle state machine with
// debounced firing/recovery so threshold flapping never repeats alerts.
//
// Lifecycle (independently per (jobID, kind)):
//
//	OK --breachCount >= FireAfterN--> FIRING      (exactly one firing alert)
//	FIRING --passCount >= RecoverAfterN--> RECOVERED (exactly one recovery
//	                                                 alert), state resets OK
//
// While in FIRING, repeated breaches produce NO new alerts; isolated passes
// that do not meet the recovery threshold are absorbed, so flapping near the
// boundary cannot generate duplicate alerts. Recovery and a later re-fire are
// distinct, separately sequenced alerts.
//
// Canonical rebuild: each slot keeps the COMPLETE, canonically ordered set of
// its observations. Whenever the set changes (a new run, or a backfilled run
// inserted into its canonical event-time position), the slot's whole
// lifecycle is re-derived from scratch and the global alert log is rebuilt.
// Therefore the final alert history (number of alerts, their order, which
// recovery closes which firing) is a pure function of the accepted
// observation set: it is identical whether a missing event never was lost or
// was supplied later via backfill. In particular an alert whose trigger moved
// (earlier/later) or disappeared leaves no stale duplicate behind.
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

// Observation is one breach/pass input for a canonical replay.
type Observation struct {
	// ObsID uniquely identifies the observation (runID+kind); a repeated
	// ObsID is the same observation and is applied once.
	ObsID         string
	Breach        bool
	RuleVer       int
	FireAfterN    int
	RecoverAfterN int
	At            int64 // event time of the observation (unix ns)
	Evidence      string
}

// descriptor is one fully re-derived lifecycle transition of a slot, before
// global seq assignment. fireOrdinal pairs each recovery with the ordinal of
// the firing it closes (0-based across the slot's lifecycle); triggerID is
// the ObsID of the observation that crossed the debounce threshold.
type descriptor struct {
	jobID       model.JobID
	kind        model.AlertKind
	from        model.AlertState
	to          model.AlertState
	ruleVer     int
	at          int64
	evidence    string
	reason      string
	triggerID   string
	fireOrdinal int // ordinal of the firing (both fire and its recovery)
	passCount   int // consecutive passes that triggered a recovery
	isRecovery  bool
}

type slot struct {
	jobID model.JobID
	kind  model.AlertKind
	// obs is the COMPLETE canonical observation history (ObsID unique).
	obs []Observation
	// descs is the lifecycle re-derived from obs.
	descs []descriptor
	// applied dedupes re-delivered observations in Observe (retry storms).
	applied map[string]bool
	// outcome caches the *Transition each triggering observation returned, so
	// an at-least-once re-delivery returns the identical pointer.
	outcome map[string]*Transition
}

// Manager owns per (job, kind) alert lifecycle state.
type Manager struct {
	mu     sync.Mutex
	slots  map[string]*slot
	events []Transition // canonical, globally rebuilt log
}

// NewManager creates an alert Manager.
func NewManager() *Manager {
	return &Manager{slots: map[string]*slot{}}
}

func alertKey(jobID model.JobID, kind model.AlertKind) string {
	return string(jobID) + "|" + string(kind)
}

// Observe incrementally feeds one breach/pass observation for a (job, kind).
//
// Re-delivering the same obsID (at-least-once retry storm) is a no-op and
// returns the transition the observation originally caused (nil when it only
// advanced a debounce counter). New observations are appended in canonical
// position and the whole lifecycle is re-derived.
func (m *Manager) Observe(obsID string, jobID model.JobID, kind model.AlertKind, breach bool,
	ruleVer, fireAfterN, recoverAfterN int, at int64, evidence string) *Transition {
	m.mu.Lock()
	defer m.mu.Unlock()

	s := m.getOrCreateSlot(jobID, kind)
	o := Observation{
		ObsID: obsID, Breach: breach, RuleVer: ruleVer,
		FireAfterN: fireAfterN, RecoverAfterN: recoverAfterN,
		At: at, Evidence: evidence,
	}
	if s.applied[obsID] {
		// Re-delivery: return the identical transition pointer first caused
		// by this observation (nil when it only advanced a counter).
		if tr, ok := s.outcome[obsID]; ok {
			return tr
		}
		return nil
	}

	prevByID := m.descIndexLocked(s)
	s.obs = mergeObservation(s.obs, o)
	s.applied[obsID] = true
	m.rebuildSlotLocked(s)
	m.rebuildLogLocked()

	// Cache and return the transition caused by this observation if new.
	var caused *Transition
	for i := range m.events {
		d := descriptorForEvent(s, m.events[i])
		if d != nil && d.triggerID == obsID && prevByID[d.identity()] == nil {
			caused = &m.events[i]
			break
		}
	}
	s.outcome[obsID] = caused
	return caused
}

// Recompute replaces the slot's COMPLETE, canonically ordered observation
// history and re-derives the lifecycle from scratch. The global alert log is
// rebuilt so it always equals the history the never-lost, in-order path
// would have produced:
//
//   - a firing whose trigger moved to an earlier/later observation stays one
//     firing (no stale duplicate);
//   - a firing that no longer holds disappears, together with any recovery
//     that belonged to it;
//   - a recovery stays paired with the same firing by within-slot ordinal.
//
// It returns only transitions that did not exist before this call (matched by
// lifecycle identity), so feeding the same growing prefixes incrementally —
// as the engine does after each Evaluate — reports each transition once.
func (m *Manager) Recompute(jobID model.JobID, kind model.AlertKind, obs []Observation) []Transition {
	m.mu.Lock()
	defer m.mu.Unlock()

	s := m.getOrCreateSlot(jobID, kind)
	prevByID := m.descIndexLocked(s)

	// Replace the canonical set; dedupe by ObsID (repeated/concurrent
	// backfills of the same run produce one observation).
	merged := make([]Observation, 0, len(obs))
	seen := map[string]bool{}
	for _, o := range obs {
		if seen[o.ObsID] {
			continue
		}
		seen[o.ObsID] = true
		merged = append(merged, o)
	}
	sort.SliceStable(merged, func(i, j int) bool {
		if merged[i].At != merged[j].At {
			return merged[i].At < merged[j].At
		}
		return merged[i].ObsID < merged[j].ObsID
	})
	s.obs = merged
	s.applied = map[string]bool{}
	for _, o := range merged {
		s.applied[o.ObsID] = true
	}
	m.rebuildSlotLocked(s)
	m.rebuildLogLocked()

	var out []Transition
	for _, d := range s.descs {
		if prevByID[d.identity()] != nil {
			continue // already reported by an earlier call
		}
		if tr, ok := m.materializeLocked(s, d); ok {
			out = append(out, tr)
		}
	}
	return out
}

func (m *Manager) getOrCreateSlot(jobID model.JobID, kind model.AlertKind) *slot {
	k := alertKey(jobID, kind)
	s, ok := m.slots[k]
	if !ok {
		s = &slot{
			jobID:   jobID,
			kind:    kind,
			applied: map[string]bool{},
			outcome: map[string]*Transition{},
		}
		m.slots[k] = s
	}
	return s
}

// identity is the order-independent lifecycle identity of a transition:
// slot + direction + within-slot ordinal. A firing keeps the same identity
// when its trigger moves; a disappeared firing has no matching identity and
// is therefore dropped from the rebuilt log.
func (d descriptor) identity() string {
	return fmt.Sprintf("%s|%d|%s", d.kind, d.fireOrdinal, d.to)
}

// descIndexLocked indexes a slot's current descriptors by identity.
func (m *Manager) descIndexLocked(s *slot) map[string]*descriptor {
	idx := map[string]*descriptor{}
	for i := range s.descs {
		d := s.descs[i]
		idx[d.identity()] = &d
	}
	return idx
}

// rebuildSlotLocked re-derives the slot's lifecycle purely from s.obs.
func (m *Manager) rebuildSlotLocked(s *slot) {
	s.descs = s.descs[:0]
	state := model.AlertStateOK
	var breachRun, passRun int
	var fireOrdinal int
	var firedRuleVer int

	fireReason := func(kind model.AlertKind, n int) string {
		return fmt.Sprintf("%s breach confirmed after %d consecutive breach run(s)", kind, n)
	}

	for _, o := range s.obs {
		switch state {
		case model.AlertStateOK:
			if !o.Breach {
				breachRun = 0 // a pass resets the fire debounce
				continue
			}
			breachRun++
			if breachRun < o.FireAfterN {
				continue
			}
			s.descs = append(s.descs, descriptor{
				jobID: s.jobID, kind: s.kind,
				from: model.AlertStateOK, to: model.AlertStateFiring,
				ruleVer: o.RuleVer, at: o.At, evidence: o.Evidence,
				reason:    fireReason(s.kind, breachRun),
				triggerID: o.ObsID, fireOrdinal: fireOrdinal,
			})
			firedRuleVer = o.RuleVer
			state = model.AlertStateFiring
			breachRun, passRun = 0, 0
		case model.AlertStateFiring:
			if o.Breach {
				passRun = 0 // repeated breach while firing: no new alert
				continue
			}
			passRun++
			if passRun < o.RecoverAfterN {
				continue
			}
			s.descs = append(s.descs, descriptor{
				jobID: s.jobID, kind: s.kind,
				from: model.AlertStateFiring, to: model.AlertStateRecover,
				ruleVer: firedRuleVer, at: o.At, evidence: o.Evidence,
				reason: fmt.Sprintf("%s recovered after %d consecutive pass run(s); closes firing #%d",
					s.kind, passRun, fireOrdinal+1),
				triggerID: o.ObsID, fireOrdinal: fireOrdinal,
				passCount: passRun, isRecovery: true,
			})
			state = model.AlertStateOK
			breachRun, passRun = 0, 0
			fireOrdinal++
		}
	}
}

// rebuildLogLocked merges every slot's re-derived descriptors into one
// canonical global log ordered by (event time, slot, within-slot order) and
// assigns monotonic global seqs. Because the order is content-defined, the
// log is byte-identical for the never-lost path and any backfill path that
// ends with the same observation set.
func (m *Manager) rebuildLogLocked() {
	type item struct {
		s *slot
		d descriptor
	}
	var items []item
	for _, s := range m.slots {
		for i := range s.descs {
			items = append(items, item{s: s, d: s.descs[i]})
		}
	}
	sort.SliceStable(items, func(a, b int) bool {
		da, db := items[a].d, items[b].d
		if da.at != db.at {
			return da.at < db.at
		}
		ka := alertKey(da.jobID, da.kind)
		kb := alertKey(db.jobID, db.kind)
		if ka != kb {
			return ka < kb
		}
		if da.fireOrdinal != db.fireOrdinal {
			return da.fireOrdinal < db.fireOrdinal
		}
		// A firing precedes its own recovery at the same time.
		if da.to != db.to {
			return da.to == model.AlertStateFiring
		}
		return false
	})

	transitions := make([]Transition, len(items))
	for i, it := range items {
		s, d := it.s, it.d
		transitions[i] = Transition{
			Alert: model.Alert{
				JobID:     s.jobID,
				Kind:      s.kind,
				State:     d.to,
				Version:   d.ruleVer,
				Reason:    d.reason, // recovery reason finalized below
				Timestamp: atTime(d.at),
			},
			FromState: d.from, ToState: d.to, Evidence: d.evidence,
		}
	}

	// Global seqs follow canonical order. Remember each firing's seq so its
	// recovery can name it; then finalize recovery reasons.
	firingSeqByKey := map[string]int64{} // slot|#fireOrdinal -> global seq
	for i := range transitions {
		transitions[i].Alert.Seq = int64(i + 1)
	}
	for i, it := range items {
		if !it.d.isRecovery {
			firingSeqByKey[alertKey(it.d.jobID, it.d.kind)+fmt.Sprintf("|#%d", it.d.fireOrdinal)] =
				transitions[i].Alert.Seq
		}
	}
	for i, it := range items {
		if it.d.isRecovery {
			transitions[i].Alert.Reason = fmt.Sprintf(
				"%s recovered after %d consecutive pass run(s); firing alert seq=%d",
				it.d.kind, it.d.passCount,
				firingSeqByKey[alertKey(it.d.jobID, it.d.kind)+fmt.Sprintf("|#%d", it.d.fireOrdinal)])
		}
	}
	m.events = transitions
}

// materializeLocked returns the global-log transition corresponding to a
// slot descriptor. The global log is content-sorted, so filtering it for the
// slot preserves the slot's lifecycle order; the descriptor's slot ordinal
// indexes directly into it.
func (m *Manager) materializeLocked(s *slot, d descriptor) (Transition, bool) {
	ord := descriptorOrdinal(s, d)
	if ord < 0 {
		return Transition{}, false
	}
	var slotTrs []Transition
	for _, tr := range m.events {
		if tr.Alert.JobID == s.jobID && tr.Alert.Kind == s.kind {
			slotTrs = append(slotTrs, tr)
		}
	}
	if ord >= len(slotTrs) {
		return Transition{}, false
	}
	return slotTrs[ord], true
}

func descriptorOrdinal(s *slot, d descriptor) int {
	for i, x := range s.descs {
		if x.identity() == d.identity() && x.at == d.at {
			return i
		}
	}
	return -1
}

// descriptorForEvent maps a global-log transition back to the slot
// descriptor it was materialized from (matched by slot, direction and event
// time), or nil when the event belongs to a different slot.
func descriptorForEvent(s *slot, tr Transition) *descriptor {
	if tr.Alert.JobID != s.jobID || tr.Alert.Kind != s.kind {
		return nil
	}
	for i := range s.descs {
		d := s.descs[i]
		if d.to == tr.ToState && atTime(d.at).Equal(tr.Alert.Timestamp) {
			return &s.descs[i]
		}
	}
	return nil
}

// mergeObservation inserts o into a canonical (At, ObsID)-sorted list,
// replacing an existing entry with the same ObsID.
func mergeObservation(obs []Observation, o Observation) []Observation {
	out := make([]Observation, 0, len(obs)+1)
	inserted := false
	for _, x := range obs {
		if x.ObsID == o.ObsID {
			if !inserted {
				out = append(out, o)
				inserted = true
			}
			continue
		}
		if !inserted && (o.At < x.At || (o.At == x.At && o.ObsID < x.ObsID)) {
			out = append(out, o)
			inserted = true
		}
		out = append(out, x)
	}
	if !inserted {
		out = append(out, o)
	}
	return out
}

// State returns the current lifecycle state of one alert.
func (m *Manager) State(jobID model.JobID, kind model.AlertKind) (model.AlertState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.slots[alertKey(jobID, kind)]
	if !ok {
		return "", false
	}
	if len(s.descs) == 0 {
		return model.AlertStateOK, true
	}
	last := s.descs[len(s.descs)-1]
	if last.to == model.AlertStateFiring {
		return model.AlertStateFiring, true
	}
	return model.AlertStateOK, true
}

// Events returns all emitted lifecycle transitions in canonical order.
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
		if len(s.descs) == 0 {
			out[k] = model.AlertStateOK
			continue
		}
		last := s.descs[len(s.descs)-1]
		if last.to == model.AlertStateFiring {
			out[k] = model.AlertStateFiring
		} else {
			out[k] = model.AlertStateOK
		}
	}
	return out
}
