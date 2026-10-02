// Package ingest accepts raw run events concurrently, deduplicates and
// orders them, and explains why each event was applied, delayed or dropped.
//
// Determinism contract:
//   - All state is guarded by one mutex; each accepted event receives a
//     monotonic intake sequence number.
//   - Per-run aggregates are rebuilt in canonical event-time order
//     ((OccurredAt, EventID)), so the aggregate for an identical input set
//     is the same regardless of arrival order or concurrency scheduling.
//   - Already-applied state is never overwritten: duplicates return the
//     original receipt, conflicting same-phase events are rejected.
package ingest

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/h3yow/pipeline-observability-go/internal/event"
	"github.com/h3yow/pipeline-observability-go/internal/model"
)

// Disposition describes what the intake decided for one event.
type Disposition string

const (
	// DispositionInvalid: envelope failed validation (unknown version/missing field).
	DispositionInvalid Disposition = "INVALID"
	// DispositionDuplicate: event with same EventID already processed.
	DispositionDuplicate Disposition = "DUPLICATE"
	// DispositionConflict: a different EventID for the same run/phase.
	DispositionConflict Disposition = "CONFLICT"
	// DispositionLate: event time is older than the current watermark.
	DispositionLate Disposition = "LATE"
	// DispositionOutOfOrder: arrived after a later event of the same run;
	// buffered and the aggregate rebuilt in event-time order (delayed apply).
	DispositionOutOfOrder Disposition = "OUT_OF_ORDER_BUFFERED"
	// DispositionApplied: event applied immediately to the run aggregate.
	DispositionApplied Disposition = "APPLIED"
	// DispositionBackfilled: event explicitly backfilled (operator action);
	// accepted into its run bypassing the lateness watermark.
	DispositionBackfilled Disposition = "BACKFILLED"
)

// Receipt is the per-event intake result.
type Receipt struct {
	EventID     string
	RunID       string
	Accepted    bool
	Disposition Disposition
	Reason      string
	// AppliedSeq is the monotonic intake sequence when the event was
	// applied (including delayed application); zero when not applied.
	AppliedSeq int64
}

type seenRecord struct {
	disp Disposition
	seq  int64
}

// RejectedEvent records one event that intake refused, kept per run so the
// cause of a missing input (late, invalid, superseded) stays explainable.
type RejectedEvent struct {
	EventID     string
	Type        event.Type
	Disposition Disposition
	Reason      string
}

type acceptedEvent struct {
	ev  *event.DecodedEvent
	seq int64
}

type runAgg struct {
	runID string
	jobID string
	// events holds all accepted events keyed by EventID (a set: duplicate
	// EventIDs can never occur here, they are stopped by the seen index).
	events map[string]acceptedEvent
	state  model.RunState
}

// Store is the concurrent intake aggregator.
type Store struct {
	mu sync.Mutex

	allowLateness int64 // nanoseconds
	seq           int64
	maxOccurredNs int64 // global max accepted event time (watermark source)

	seen map[string]seenRecord
	runs map[string]*runAgg
	// rejected indexes refused events by run id (INVALID/LATE/CONFLICT),
	// so a run with a missing input can explain WHY it is missing.
	rejected map[string][]RejectedEvent
}

// NewStore creates an intake store with the given event-time lateness
// allowance (in nanoseconds). Events older than
// maxObservedOccurredAt - allowLateness are rejected as LATE.
func NewStore(allowLateness int64) *Store {
	return &Store{
		allowLateness: allowLateness,
		seen:          map[string]seenRecord{},
		runs:          map[string]*runAgg{},
		rejected:      map[string][]RejectedEvent{},
	}
}

// recordRejectedLocked notes a refused event against its run (when the run
// id is known). Duplicate deliveries are not recorded: they are replays of
// an already-decided event, not independent gaps.
func (s *Store) recordRejectedLocked(runID string, typ event.Type, eventID string, disp Disposition, reason string) {
	if runID == "" {
		return
	}
	s.rejected[runID] = append(s.rejected[runID], RejectedEvent{
		EventID: eventID, Type: typ, Disposition: disp, Reason: reason,
	})
}

// forgetRejectedLocked drops the rejection record for eventID (used when an
// explicit backfill later accepts an event that was previously refused).
func (s *Store) forgetRejectedLocked(runID, eventID string) {
	recs := s.rejected[runID]
	for i, r := range recs {
		if r.EventID == eventID {
			s.rejected[runID] = append(recs[:i], recs[i+1:]...)
			break
		}
	}
	if len(s.rejected[runID]) == 0 {
		delete(s.rejected, runID)
	}
}

// Ingest validates, deduplicates and applies one envelope.
//
// Safe for concurrent use. The decision for an event depends only on the
// accepted input set, never on goroutine scheduling.
func (s *Store) Ingest(_ context.Context, env event.Envelope) Receipt {
	s.mu.Lock()
	defer s.mu.Unlock()

	dec, err := env.Validate()
	if err != nil {
		s.recordRejectedLocked(env.RunID, env.Type, env.EventID, DispositionInvalid, err.Error())
		return Receipt{
			EventID:     env.EventID,
			RunID:       env.RunID,
			Accepted:    false,
			Disposition: DispositionInvalid,
			Reason:      err.Error(),
		}
	}

	// 1) Duplicate delivery: return the ORIGINAL decision, never re-apply.
	if rec, ok := s.seen[dec.EventID]; ok {
		return Receipt{
			EventID:     dec.EventID,
			RunID:       dec.RunID,
			Accepted:    false,
			Disposition: DispositionDuplicate,
			Reason: fmt.Sprintf("duplicate delivery of %s (first seen as %s, seq=%d)",
				dec.EventID, rec.disp, rec.seq),
		}
	}

	occurredNs := dec.OccurredAt.UnixNano()

	// 2) Watermark / lateness check (before any state is touched).
	wm := s.watermarkLocked()
	if occurredNs < wm {
		s.seen[dec.EventID] = seenRecord{disp: DispositionLate}
		reason := fmt.Sprintf("event time %s older than watermark %s (maxOccurred-lateness)",
			dec.OccurredAt.UTC().Format("15:04:05.000"),
			unixNsToTime(wm).Format("15:04:05.000"))
		s.recordRejectedLocked(dec.RunID, dec.Type, dec.EventID, DispositionLate, reason)
		return Receipt{
			EventID:     dec.EventID,
			RunID:       dec.RunID,
			Accepted:    false,
			Disposition: DispositionLate,
			Reason:      reason,
		}
	}

	agg, ok := s.runs[dec.RunID]
	if !ok {
		agg = &runAgg{
			runID:  dec.RunID,
			jobID:  dec.JobID,
			events: map[string]acceptedEvent{},
		}
		s.runs[dec.RunID] = agg
	} else if agg.jobID != dec.JobID {
		// A run id must never belong to two jobs.
		s.seen[dec.EventID] = seenRecord{disp: DispositionConflict}
		reason := fmt.Sprintf("run %s already belongs to job %s, event claims %s", dec.RunID, agg.jobID, dec.JobID)
		s.recordRejectedLocked(dec.RunID, dec.Type, dec.EventID, DispositionConflict, reason)
		return Receipt{
			EventID:     dec.EventID,
			RunID:       dec.RunID,
			Accepted:    false,
			Disposition: DispositionConflict,
			Reason:      reason,
		}
	}

	// 3) Same-run same-phase conflict with a DIFFERENT event id: existing
	//    applied state wins; the newcomer is rejected, nothing overwritten.
	if reason := phaseConflictLocked(agg, dec); reason != "" {
		s.seen[dec.EventID] = seenRecord{disp: DispositionConflict}
		s.recordRejectedLocked(dec.RunID, dec.Type, dec.EventID, DispositionConflict, reason)
		return Receipt{
			EventID:     dec.EventID,
			RunID:       dec.RunID,
			Accepted:    false,
			Disposition: DispositionConflict,
			Reason:      reason,
		}
	}

	// 4) Out-of-order detection (purely informational about ordering):
	//    did a later-in-time event of this run already arrive?
	outOfOrder := false
	laterEvent := ""
	for _, ae := range agg.events {
		e := ae.ev
		if e.OccurredAt.UnixNano() > occurredNs {
			outOfOrder = true
			laterEvent = e.EventID
			break
		}
	}

	// 5) Accept: allocate sequence, store, rebuild aggregate canonically.
	s.seq++
	seq := s.seq
	agg.events[dec.EventID] = acceptedEvent{ev: dec, seq: seq}
	s.seen[dec.EventID] = seenRecord{
		disp: DispositionApplied,
		seq:  seq,
	}
	if occurredNs > s.maxOccurredNs {
		s.maxOccurredNs = occurredNs
	}
	agg.rebuild()

	disp := DispositionApplied
	reason := fmt.Sprintf("applied in event-time order (seq=%d)", seq)
	if outOfOrder {
		disp = DispositionOutOfOrder
		reason = fmt.Sprintf("arrived after later event %s; buffered and aggregate rebuilt in event-time order (seq=%d)",
			laterEvent, seq)
	}
	return Receipt{
		EventID:     dec.EventID,
		RunID:       dec.RunID,
		Accepted:    true,
		Disposition: disp,
		Reason:      reason,
		AppliedSeq:  seq,
	}
}

// Backfill explicitly inserts a historically missing event into its run
// (operator action, e.g. recovered from an archive). Unlike Ingest it
// bypasses the lateness watermark — the caller vouches for the event — and
// it may accept an event that was previously rejected as LATE.
//
// Guarantees:
//   - Validation still applies: undecodable envelopes stay INVALID.
//   - Same-phase conflicts still apply: a run that already has an event for
//     that phase keeps it; the backfill is rejected, nothing overwritten.
//   - Idempotent: backfilling an already-accepted EventID returns the
//     original receipt as DUPLICATE and changes nothing.
//   - The aggregate is rebuilt canonically, so the result equals the state
//     in which the event had never been lost.
//   - Backfilled events do NOT advance the watermark: historical data must
//     not reopen lateness windows for other runs.
func (s *Store) Backfill(_ context.Context, env event.Envelope) Receipt {
	s.mu.Lock()
	defer s.mu.Unlock()

	dec, err := env.Validate()
	if err != nil {
		s.recordRejectedLocked(env.RunID, env.Type, env.EventID, DispositionInvalid, err.Error())
		return Receipt{
			EventID:     env.EventID,
			RunID:       env.RunID,
			Accepted:    false,
			Disposition: DispositionInvalid,
			Reason:      err.Error(),
		}
	}

	if rec, ok := s.seen[dec.EventID]; ok {
		switch rec.disp {
		case DispositionLate, DispositionConflict:
			// Explicit backfill may override these refusals; re-attempt below.
		default:
			// Already accepted (APPLIED / OUT_OF_ORDER_BUFFERED / BACKFILLED):
			// replay the original decision, never re-apply.
			return Receipt{
				EventID:     dec.EventID,
				RunID:       dec.RunID,
				Accepted:    false,
				Disposition: DispositionDuplicate,
				Reason: fmt.Sprintf("duplicate backfill of %s (first seen as %s, seq=%d)",
					dec.EventID, rec.disp, rec.seq),
			}
		}
	}

	agg, ok := s.runs[dec.RunID]
	if !ok {
		agg = &runAgg{
			runID:  dec.RunID,
			jobID:  dec.JobID,
			events: map[string]acceptedEvent{},
		}
		s.runs[dec.RunID] = agg
	} else if agg.jobID != dec.JobID {
		s.seen[dec.EventID] = seenRecord{disp: DispositionConflict}
		reason := fmt.Sprintf("run %s already belongs to job %s, event claims %s", dec.RunID, agg.jobID, dec.JobID)
		s.recordRejectedLocked(dec.RunID, dec.Type, dec.EventID, DispositionConflict, reason)
		return Receipt{
			EventID:     dec.EventID,
			RunID:       dec.RunID,
			Accepted:    false,
			Disposition: DispositionConflict,
			Reason:      reason,
		}
	}

	// Same-phase conflict is still enforced for backfills: existing applied
	// state wins, nothing is overwritten.
	if reason := phaseConflictLocked(agg, dec); reason != "" {
		s.seen[dec.EventID] = seenRecord{disp: DispositionConflict}
		s.recordRejectedLocked(dec.RunID, dec.Type, dec.EventID, DispositionConflict, reason)
		return Receipt{
			EventID:     dec.EventID,
			RunID:       dec.RunID,
			Accepted:    false,
			Disposition: DispositionConflict,
			Reason:      reason,
		}
	}

	// Accept: allocate sequence, store, rebuild canonically. The watermark
	// (maxOccurredNs) is deliberately left untouched.
	s.seq++
	seq := s.seq
	agg.events[dec.EventID] = acceptedEvent{ev: dec, seq: seq}
	s.seen[dec.EventID] = seenRecord{disp: DispositionBackfilled, seq: seq}
	s.forgetRejectedLocked(dec.RunID, dec.EventID)
	agg.rebuild()

	return Receipt{
		EventID:     dec.EventID,
		RunID:       dec.RunID,
		Accepted:    true,
		Disposition: DispositionBackfilled,
		Reason: fmt.Sprintf("explicitly backfilled into run %s; aggregate rebuilt in event-time order (seq=%d)",
			dec.RunID, seq),
		AppliedSeq: seq,
	}
}

// RejectedFor returns the refused events recorded for a run, ordered by
// (Type, EventID) for a stable, deterministic view.
func (s *Store) RejectedFor(runID string) []RejectedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	recs := s.rejected[runID]
	out := make([]RejectedEvent, len(recs))
	copy(out, recs)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Type != out[j].Type {
			return out[i].Type < out[j].Type
		}
		return out[i].EventID < out[j].EventID
	})
	return out
}

// watermarkLocked returns maxOccurred - allowLateness (unix nanoseconds).
func (s *Store) watermarkLocked() int64 {
	if s.maxOccurredNs == 0 {
		return 0
	}
	return s.maxOccurredNs - s.allowLateness
}

// phaseConflictLocked checks whether dec repeats the started/finished
// phase of its run under a different EventID.
func phaseConflictLocked(agg *runAgg, dec *event.DecodedEvent) string {
	for _, ae := range agg.events {
		e := ae.ev
		if e.Type == dec.Type {
			return fmt.Sprintf("run %s already has %s event %s; rejecting same-phase event %s without overwriting",
				dec.RunID, dec.Type, e.EventID, dec.EventID)
		}
	}
	return ""
}

// Run returns the current aggregated state of one run.
func (s *Store) Run(runID string) (model.RunState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	agg, ok := s.runs[runID]
	if !ok {
		return model.RunState{}, false
	}
	return agg.state, true
}

// Runs returns a snapshot of all aggregated runs ordered by RunID.
func (s *Store) Runs() []model.RunState {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.runs))
	for id := range s.runs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]model.RunState, 0, len(ids))
	for _, id := range ids {
		out = append(out, s.runs[id].state)
	}
	return out
}

// CompleteRuns returns aggregated runs that have a finished event.
func (s *Store) CompleteRuns() []model.RunState {
	all := s.Runs()
	out := make([]model.RunState, 0, len(all))
	for _, r := range all {
		if r.IsComplete() {
			out = append(out, r)
		}
	}
	return out
}
