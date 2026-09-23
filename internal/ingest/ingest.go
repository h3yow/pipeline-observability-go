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
}

// NewStore creates an intake store with the given event-time lateness
// allowance (in nanoseconds). Events older than
// maxObservedOccurredAt - allowLateness are rejected as LATE.
func NewStore(allowLateness int64) *Store {
	return &Store{
		allowLateness: allowLateness,
		seen:          map[string]seenRecord{},
		runs:          map[string]*runAgg{},
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
		return Receipt{
			EventID:     dec.EventID,
			RunID:       dec.RunID,
			Accepted:    false,
			Disposition: DispositionLate,
			Reason: fmt.Sprintf("event time %s older than watermark %s (maxOccurred-lateness)",
				dec.OccurredAt.UTC().Format("15:04:05.000"),
				unixNsToTime(wm).Format("15:04:05.000")),
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
		return Receipt{
			EventID:     dec.EventID,
			RunID:       dec.RunID,
			Accepted:    false,
			Disposition: DispositionConflict,
			Reason:      fmt.Sprintf("run %s already belongs to job %s, event claims %s", dec.RunID, agg.jobID, dec.JobID),
		}
	}

	// 3) Same-run same-phase conflict with a DIFFERENT event id: existing
	//    applied state wins; the newcomer is rejected, nothing overwritten.
	if reason := phaseConflictLocked(agg, dec); reason != "" {
		s.seen[dec.EventID] = seenRecord{disp: DispositionConflict}
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
