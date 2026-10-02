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
//
// Missing-basis support:
//   - Rejected STARTED candidates are recorded per run as gap evidence, so a
//     finished run without a started event explains whether the start is
//     still missing, was dropped (invalid/late) or was displaced by another
//     delivery. PendingRuns lists such runs for backfill triage.
//   - Backfill injects a recovered event bypassing only the lateness window;
//     every other rule (validation, dedup, no-overwrite) still applies and
//     the aggregate is rebuilt canonically, making the repaired run
//     indistinguishable from one whose event was never lost.
package ingest

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

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
	// DispositionBackfilled: event explicitly injected via Backfill
	// (archive/manual recovery), bypassing the lateness window; the
	// aggregate is rebuilt canonically so the result is identical to an
	// event that was never lost.
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

// rejectedEvent is a candidate event that could not fill a run phase. It
// explains why a later gap exists (dropped / displaced) instead of the gap
// looking like "nothing ever arrived".
type rejectedEvent struct {
	eventID    string
	occurredNs int64 // zero when the envelope failed to decode
	code       model.GapCode
	detail     string
}

// PendingRun is one finished run whose time basis is incomplete and which
// therefore cannot be judged; it is one row of the "to be backfilled" view.
type PendingRun struct {
	RunID      string
	JobID      string
	FinishedAt time.Time
	AppliedSeq int64
	Gap        model.MissingBasis
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
	// rejected lists rejected STARTED candidates per run that may explain a
	// missing start. It is the evidence behind GapDropped*/GapDisplaced.
	rejected map[string][]rejectedEvent
}

// NewStore creates an intake store with the given event-time lateness
// allowance (in nanoseconds). Events older than
// maxObservedOccurredAt - allowLateness are rejected as LATE.
func NewStore(allowLateness int64) *Store {
	return &Store{
		allowLateness: allowLateness,
		seen:          map[string]seenRecord{},
		runs:          map[string]*runAgg{},
		rejected:      map[string][]rejectedEvent{},
	}
}

// addRejectedLocked records one rejected STARTED candidate as gap evidence
// for a run. Candidates with the same EventID are kept once (the original
// decision), so repeated invalid deliveries cannot grow the evidence list.
func (s *Store) addRejectedLocked(runID string, r rejectedEvent) {
	for _, ex := range s.rejected[runID] {
		if ex.eventID == r.eventID {
			return
		}
	}
	s.rejected[runID] = append(s.rejected[runID], r)
}

// removeRejectedLocked drops a rejected candidate (used when an event once
// dropped as LATE is successfully recovered via Backfill), so stale evidence
// cannot keep classifying a run whose basis is now complete.
func (s *Store) removeRejectedLocked(runID, eventID string) {
	rs := s.rejected[runID]
	for i, r := range rs {
		if r.eventID == eventID {
			s.rejected[runID] = append(rs[:i], rs[i+1:]...)
			if len(s.rejected[runID]) == 0 {
				delete(s.rejected, runID)
			}
			return
		}
	}
}

// Ingest validates, deduplicates and applies one envelope.
//
// Safe for concurrent use. The decision for an event depends only on the
// accepted input set, never on goroutine scheduling.
func (s *Store) Ingest(_ context.Context, env event.Envelope) Receipt {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.processLocked(env, false)
}

// Backfill explicitly injects one recovered event (e.g. restored from an
// archive or by manual repair). It is identical to Ingest except that the
// lateness window is bypassed: a recovered started event that the window
// would reject can still fill the missing phase. All other rules still
// apply (validation, duplicate idempotency, no overwrite of an occupied
// phase), and the aggregate is rebuilt canonically, so the repaired run is
// indistinguishable from one whose event arrived normally in order.
//
// Safe for concurrent use.
func (s *Store) Backfill(_ context.Context, env event.Envelope) Receipt {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.processLocked(env, true)
}

// processLocked implements the common intake/backfill decision. The two
// paths differ only in whether the watermark may reject the event.
func (s *Store) processLocked(env event.Envelope, backfill bool) Receipt {
	dec, err := env.Validate()
	if err != nil {
		// Only a candidate for the STARTED phase can explain a start gap;
		// a malformed finished event must not pollute the evidence.
		if env.RunID != "" && env.Type == event.TypeStarted {
			s.addRejectedLocked(env.RunID, rejectedEvent{
				eventID: env.EventID,
				code:    model.GapDroppedInvalid,
				detail:  fmt.Sprintf("candidate %s rejected: %s", env.EventID, err.Error()),
			})
		}
		return Receipt{
			EventID:     env.EventID,
			RunID:       env.RunID,
			Accepted:    false,
			Disposition: DispositionInvalid,
			Reason:      err.Error(),
		}
	}

	// 1) Duplicate delivery: return the ORIGINAL decision, never re-apply.
	//    The single exception is an event previously dropped as LATE that is
	//    now explicitly recovered via Backfill: the operator is asserting the
	//    event is genuine, so it is given a second chance (everything else,
	//    incl. validation and no-overwrite, is still enforced below).
	if rec, ok := s.seen[dec.EventID]; ok {
		if backfill && rec.disp == DispositionLate {
			delete(s.seen, dec.EventID)
			s.removeRejectedLocked(dec.RunID, dec.EventID)
		} else {
			return Receipt{
				EventID:     dec.EventID,
				RunID:       dec.RunID,
				Accepted:    false,
				Disposition: DispositionDuplicate,
				Reason: fmt.Sprintf("duplicate delivery of %s (first seen as %s, seq=%d)",
					dec.EventID, rec.disp, rec.seq),
			}
		}
	}

	occurredNs := dec.OccurredAt.UnixNano()

	// 2) Watermark / lateness check (before any state is touched). Backfill
	//    deliberately bypasses it: the event is explicitly recovered.
	if !backfill {
		wm := s.watermarkLocked()
		if occurredNs < wm {
			s.seen[dec.EventID] = seenRecord{disp: DispositionLate}
			if dec.Type == event.TypeStarted {
				s.addRejectedLocked(dec.RunID, rejectedEvent{
					eventID:    dec.EventID,
					occurredNs: occurredNs,
					code:       model.GapDroppedLate,
					detail: fmt.Sprintf("candidate %s dropped by watermark: event time %s older than %s",
						dec.EventID,
						dec.OccurredAt.UTC().Format("15:04:05.000"),
						unixNsToTime(wm).Format("15:04:05.000")),
				})
			}
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
		detail := fmt.Sprintf("run %s already belongs to job %s, event claims %s",
			dec.RunID, agg.jobID, dec.JobID)
		s.seen[dec.EventID] = seenRecord{disp: DispositionConflict}
		if dec.Type == event.TypeStarted {
			s.addRejectedLocked(dec.RunID, rejectedEvent{
				eventID:    dec.EventID,
				occurredNs: occurredNs,
				code:       model.GapDisplaced,
				detail:     fmt.Sprintf("candidate %s displaced: %s", dec.EventID, detail),
			})
		}
		return Receipt{
			EventID:     dec.EventID,
			RunID:       dec.RunID,
			Accepted:    false,
			Disposition: DispositionConflict,
			Reason:      detail,
		}
	}

	// 3) Same-run same-phase conflict with a DIFFERENT event id: existing
	//    applied state wins; the newcomer is rejected, nothing overwritten.
	if reason := phaseConflictLocked(agg, dec); reason != "" {
		s.seen[dec.EventID] = seenRecord{disp: DispositionConflict}
		if dec.Type == event.TypeStarted {
			s.addRejectedLocked(dec.RunID, rejectedEvent{
				eventID:    dec.EventID,
				occurredNs: occurredNs,
				code:       model.GapDisplaced,
				detail:     "candidate displaced: " + reason,
			})
		}
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
	seenDisp := DispositionApplied
	if backfill {
		seenDisp = DispositionBackfilled
	}
	s.seen[dec.EventID] = seenRecord{disp: seenDisp, seq: seq}
	if occurredNs > s.maxOccurredNs {
		s.maxOccurredNs = occurredNs
	}
	agg.rebuild()
	agg.state.BasisGap = s.gapForLocked(agg)

	disp := DispositionApplied
	reason := fmt.Sprintf("applied in event-time order (seq=%d)", seq)
	switch {
	case backfill && outOfOrder:
		disp = DispositionBackfilled
		reason = fmt.Sprintf("explicit backfill arrived after later event %s; aggregate rebuilt in event-time order (seq=%d)",
			laterEvent, seq)
	case backfill:
		disp = DispositionBackfilled
		reason = fmt.Sprintf("explicit backfill bypassed the lateness window; aggregate rebuilt canonically (seq=%d)", seq)
	case outOfOrder:
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

// gapForLocked derives the missing-basis explanation for a finished run that
// has no started event. It returns nil when the basis is complete. The
// explanation is a pure function of the accepted/rejected input set: the
// strongest evidence wins (DISPLACED > DROPPED_LATE > DROPPED_INVALID),
// ties broken by earliest event time then EventID; with no evidence the
// started event has simply not been observed yet.
func (s *Store) gapForLocked(agg *runAgg) *model.MissingBasis {
	if !agg.state.HasFinished || agg.state.HasStarted {
		return nil
	}
	rank := map[model.GapCode]int{
		model.GapDroppedInvalid: 1,
		model.GapDroppedLate:    2,
		model.GapDisplaced:      3,
	}
	var best *rejectedEvent
	for i := range s.rejected[agg.runID] {
		r := &s.rejected[agg.runID][i]
		if best == nil ||
			rank[r.code] > rank[best.code] ||
			(rank[r.code] == rank[best.code] &&
				(r.occurredNs < best.occurredNs ||
					(r.occurredNs == best.occurredNs && r.eventID < best.eventID))) {
			best = r
		}
	}
	if best == nil {
		return &model.MissingBasis{
			Phase: model.PhaseStarted,
			Code:  model.GapNotArrived,
			Detail: fmt.Sprintf(
				"run %s has a finished event but no run.started event has been observed; it may still arrive or be backfilled",
				agg.runID),
		}
	}
	return &model.MissingBasis{
		Phase:           model.PhaseStarted,
		Code:            best.code,
		Detail:          best.detail,
		EvidenceEventID: best.eventID,
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

// snapshotLocked returns a copy of a run aggregate's state with its current
// missing-basis explanation attached.
func (s *Store) snapshotLocked(agg *runAgg) model.RunState {
	agg.state.BasisGap = s.gapForLocked(agg)
	return agg.state
}

// Run returns the current aggregated state of one run.
func (s *Store) Run(runID string) (model.RunState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	agg, ok := s.runs[runID]
	if !ok {
		return model.RunState{}, false
	}
	return s.snapshotLocked(agg), true
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
		out = append(out, s.snapshotLocked(s.runs[id]))
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

// PendingRuns returns the "to be backfilled" view: finished runs whose time
// basis is incomplete (no started event), ordered by RunID. Because it is
// derived from the canonical aggregates plus the rejected-candidate index,
// its set and reasons are stable under out-of-order, duplicate and
// concurrent delivery.
func (s *Store) PendingRuns() []PendingRun {
	return s.PendingRunsPage("", 0)
}

// PendingRunsPage returns one deterministic page of the pending view: rows
// with RunID strictly greater than cursor, at most limit rows (limit <= 0
// means all), ordered by RunID. Use the last row's RunID as the next
// cursor to scroll large sets without any view over state changes.
func (s *Store) PendingRunsPage(cursor string, limit int) []PendingRun {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.runs))
	for id, agg := range s.runs {
		if id > cursor && agg.state.IsComplete() && !agg.state.HasStarted {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	out := make([]PendingRun, 0, len(ids))
	for _, id := range ids {
		agg := s.runs[id]
		gap := s.gapForLocked(agg)
		if gap == nil {
			continue
		}
		out = append(out, PendingRun{
			RunID:      id,
			JobID:      agg.jobID,
			FinishedAt: agg.state.FinishedAt,
			AppliedSeq: agg.state.AppliedSeq,
			Gap:        *gap,
		})
	}
	return out
}
