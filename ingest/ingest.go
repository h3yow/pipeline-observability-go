// Package ingest accepts run events and aggregates them into per-run state.
//
// The aggregator is safe for concurrent use. Repeated delivery of the same
// event, out-of-order arrival and late arrival are handled deterministically:
// every Ingest call returns a Decision whose Reason explains precisely how
// the event was treated, so callers can distinguish applied, duplicate and
// dropped events.
package ingest

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/h3yow/pipeline-observability-go/event"
)

// Reason explains how an ingested event was handled.
type Reason string

const (
	// ReasonApplied: event extended/changed the run state.
	ReasonApplied Reason = "APPLIED"
	// ReasonAppliedLate: event arrived after later-sequence events but
	// still carried missing information and was merged.
	ReasonAppliedLate Reason = "APPLIED_LATE"
	// ReasonDuplicate: identical event already seen (by EventID).
	ReasonDuplicate Reason = "DUPLICATE"
	// ReasonStale: sequence already applied / no new information.
	ReasonStale Reason = "STALE"
	// ReasonSuperseded: same sequence number seen with different content.
	ReasonSuperseded Reason = "SUPERSEDED"
	// ReasonClosed: run already reached a terminal phase and cannot reopen.
	ReasonClosed Reason = "CLOSED"
	// ReasonTooLate: event arrived beyond the configured lateness horizon.
	ReasonTooLate Reason = "TOO_LATE"
	// ReasonRejected: event failed validation (unknown version/missing field).
	ReasonRejected Reason = "REJECTED"
)

// Decision is the outcome of one Ingest call.
type Decision struct {
	EventID string
	RunID   string
	Applied bool
	Reason  Reason
	Detail  string
}

// RunState is the aggregated view of one run (a value type / DTO).
type RunState struct {
	RunID         string
	JobID         string
	SchemaVersion string
	StartSeq      int64
	EndSeq        int64
	StartedAt     time.Time
	FinishedAt    time.Time
	Status        event.Status
	Attempt       int
	Host          string
	Closed        bool
}

// seqRecord describes one already-applied sequence position.
type seqRecord struct {
	eventID    string
	phase      event.Phase
	status     event.Status
	occurredAt time.Time
}

// runRec is the mutable, mutex-protected internal state of one run.
type runRec struct {
	mu sync.Mutex

	jobID         string
	schemaVersion string
	startSeq      int64
	endSeq        int64
	startedAt     time.Time
	finishedAt    time.Time
	status        event.Status
	attempt       int
	host          string
	closed        bool

	maxSeq       int64
	watermark    time.Time // highest OccurredAt among applied events
	seqs         map[int64]seqRecord
	seenEventIDs map[string]string // EventID -> content digest
}

func newRunRec(jobID, schema string) *runRec {
	return &runRec{
		jobID:         jobID,
		schemaVersion: schema,
		seqs:          make(map[int64]seqRecord),
		seenEventIDs:  make(map[string]string),
	}
}

func (r *runRec) snapshot() RunState {
	return RunState{
		RunID:         "", // filled by caller
		JobID:         r.jobID,
		SchemaVersion: r.schemaVersion,
		StartSeq:      r.startSeq,
		EndSeq:        r.endSeq,
		StartedAt:     r.startedAt,
		FinishedAt:    r.finishedAt,
		Status:        r.status,
		Attempt:       r.attempt,
		Host:          r.host,
		Closed:        r.closed,
	}
}

// digest renders the semantically relevant content of an event. Two
// deliveries with the same EventID must carry identical content.
func digest(ev event.Event) string {
	return fmt.Sprintf("%s|%s|%d|%s|%s|%s|%d|%s",
		ev.SchemaVersion, ev.RunID, ev.Seq,
		ev.OccurredAt.UTC().Format(time.RFC3339Nano),
		ev.Phase, ev.Status, ev.Attempt, ev.Host)
}

// apply folds a validated event into the run record; the caller holds mu.
func (r *runRec) apply(ev event.Event) {
	if ev.OccurredAt.After(r.watermark) {
		r.watermark = ev.OccurredAt
	}
	r.maxSeq = ev.Seq
	r.seqs[ev.Seq] = seqRecord{
		eventID:    ev.EventID,
		phase:      ev.Phase,
		status:     ev.Status,
		occurredAt: ev.OccurredAt,
	}
	if ev.Attempt > r.attempt {
		r.attempt = ev.Attempt
	}
	if ev.Host != "" {
		r.host = ev.Host
	}
	switch ev.Phase {
	case event.PhaseStarted:
		if r.startedAt.IsZero() || ev.OccurredAt.Before(r.startedAt) {
			r.startedAt = ev.OccurredAt
		}
		if r.startSeq == 0 || ev.Seq < r.startSeq {
			r.startSeq = ev.Seq
		}
	case event.PhaseFinished:
		r.finishedAt = ev.OccurredAt
		r.status = ev.Status
		r.endSeq = ev.Seq
		r.closed = true
	}
}

// fillsGap reports whether a lower-sequence event carries information the
// run state is still missing. An earlier STARTED always improves the run's
// notion of when it truly began, even if a later STARTED was seen first.
func (r *runRec) fillsGap(ev event.Event) bool {
	switch ev.Phase {
	case event.PhaseStarted:
		return r.startedAt.IsZero() || ev.Seq < r.startSeq
	case event.PhaseFinished:
		return !r.closed
	default:
		return false
	}
}

// merge fills missing fields from an out-of-order event; caller holds mu.
func (r *runRec) merge(ev event.Event) {
	// watermark must not move backwards for a late event.
	r.seqs[ev.Seq] = seqRecord{
		eventID:    ev.EventID,
		phase:      ev.Phase,
		status:     ev.Status,
		occurredAt: ev.OccurredAt,
	}
	if ev.Attempt > r.attempt {
		r.attempt = ev.Attempt
	}
	if r.host == "" && ev.Host != "" {
		r.host = ev.Host
	}
	switch ev.Phase {
	case event.PhaseStarted:
		if r.startedAt.IsZero() || ev.Seq < r.startSeq {
			r.startedAt = ev.OccurredAt
			r.startSeq = ev.Seq
		}
	case event.PhaseFinished:
		if !r.closed {
			r.finishedAt = ev.OccurredAt
			r.status = ev.Status
			r.endSeq = ev.Seq
			r.closed = true
		}
	}
}

// Config configures an Aggregator.
type Config struct {
	// MaxLateness is the horizon beyond which an informative late event is
	// dropped as TOO_LATE, measured from the run's event-time watermark.
	// Zero disables the horizon (every informative late event is merged).
	MaxLateness time.Duration
}

// Aggregator aggregates events into run state; safe for concurrent use.
type Aggregator struct {
	cfg Config

	mu   sync.Mutex
	runs map[string]*runRec
}

// New constructs an Aggregator.
func New(cfg Config) *Aggregator {
	return &Aggregator{cfg: cfg, runs: make(map[string]*runRec)}
}

// Ingest validates and folds one event into run state. It always returns a
// Decision describing what happened; only unrecoverable internal errors
// produce a non-nil error.
//
// Decision precedence (deterministic under concurrency):
//
//  1. REJECTED      - schema/version/field validation failed, or schema
//     conflict within one run;
//  2. DUPLICATE     - same EventID was already delivered with same content;
//  3. SUPERSEDED    - same Seq already applied with different content;
//  4. CLOSED        - Seq higher than the terminal FINISHED of the run;
//  5. TOO_LATE/STALE/APPLIED_LATE - lower-Seq out-of-order events;
//  6. APPLIED       - new highest-Seq event.
func (a *Aggregator) Ingest(raw event.RawEvent, receivedAt time.Time) (Decision, error) {
	_ = receivedAt // ordering is decided on event time; kept for API symmetry

	ev, err := event.Decode(raw)
	if err != nil {
		return Decision{
			EventID: raw.EventID,
			RunID:   raw.RunID,
			Applied: false,
			Reason:  ReasonRejected,
			Detail:  err.Error(),
		}, nil
	}

	a.mu.Lock()
	r, exists := a.runs[ev.RunID]
	if !exists {
		r = newRunRec(ev.JobID, ev.SchemaVersion)
		a.runs[ev.RunID] = r
	}
	a.mu.Unlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	// Schema consistency within a run is mandatory.
	if r.schemaVersion != ev.SchemaVersion {
		return reject(ev, "schema version conflict within run: run uses %s, event uses %s",
			r.schemaVersion, ev.SchemaVersion), nil
	}

	// 1) Delivery dedup by EventID.
	d := digest(ev)
	if prev, ok := r.seenEventIDs[ev.EventID]; ok {
		if prev == d {
			return decide(ev, false, ReasonDuplicate,
				"event_id %s already applied with identical content", ev.EventID), nil
		}
		return reject(ev, "event_id %s reused with different content", ev.EventID), nil
	}

	// 2) Same sequence position already occupied.
	if rec, ok := r.seqs[ev.Seq]; ok {
		if rec.phase == ev.Phase && rec.status == ev.Status &&
			rec.occurredAt.Equal(ev.OccurredAt) {
			return decide(ev, false, ReasonStale,
				"seq %d already applied by event %s with identical content",
				ev.Seq, rec.eventID), nil
		}
		return decide(ev, false, ReasonSuperseded,
			"seq %d already applied by event %s with different content; first write wins",
			ev.Seq, rec.eventID), nil
	}

	// 3) Higher sequence after terminal FINISHED cannot reopen the run.
	if r.closed && ev.Seq > r.endSeq {
		return decide(ev, false, ReasonClosed,
			"run closed by FINISHED at seq %d", r.endSeq), nil
	}

	// 4) Out-of-order lower sequence: fill gaps or discard.
	if ev.Seq < r.maxSeq {
		if !r.fillsGap(ev) {
			return decide(ev, false, ReasonStale,
				"seq %d is below high-water mark %d and carries no new information",
				ev.Seq, r.maxSeq), nil
		}
		if a.cfg.MaxLateness > 0 && !r.watermark.IsZero() {
			behind := r.watermark.Sub(ev.OccurredAt)
			if behind > a.cfg.MaxLateness {
				return decide(ev, false, ReasonTooLate,
					"event time %s is %s behind watermark %s (limit %s)",
					ev.OccurredAt.UTC().Format(time.RFC3339Nano), behind,
					r.watermark.UTC().Format(time.RFC3339Nano), a.cfg.MaxLateness), nil
			}
		}
		r.seenEventIDs[ev.EventID] = d
		r.merge(ev)
		return decide(ev, true, ReasonAppliedLate,
			"seq %d arrived after high-water mark %d; merged missing %s information",
			ev.Seq, r.maxSeq, ev.Phase), nil
	}

	// 5) New highest sequence (also covers ev.Seq == r.maxSeq+).
	if a.cfg.MaxLateness > 0 && !r.watermark.IsZero() {
		behind := r.watermark.Sub(ev.OccurredAt)
		if behind > a.cfg.MaxLateness {
			return decide(ev, false, ReasonTooLate,
				"event time %s is %s behind watermark %s (limit %s)",
				ev.OccurredAt.UTC().Format(time.RFC3339Nano), behind,
				r.watermark.UTC().Format(time.RFC3339Nano), a.cfg.MaxLateness), nil
		}
	}
	r.seenEventIDs[ev.EventID] = d
	r.apply(ev)
	return decide(ev, true, ReasonApplied,
		"seq %d applied as new high-water mark", ev.Seq), nil
}

func decide(ev event.Event, applied bool, reason Reason, format string, args ...any) Decision {
	return Decision{
		EventID: ev.EventID,
		RunID:   ev.RunID,
		Applied: applied,
		Reason:  reason,
		Detail:  fmt.Sprintf(format, args...),
	}
}

func reject(ev event.Event, format string, args ...any) Decision {
	return decide(ev, false, ReasonRejected, format, args...)
}

// State returns a defensive copy of the aggregated state of a run, or ok=false.
func (a *Aggregator) State(runID string) (RunState, bool) {
	a.mu.Lock()
	r, ok := a.runs[runID]
	a.mu.Unlock()
	if !ok {
		return RunState{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.snapshot()
	s.RunID = runID
	return s, true
}

// Runs returns the known run IDs in lexicographic order (deterministic).
func (a *Aggregator) Runs() []string {
	a.mu.Lock()
	ids := make([]string, 0, len(a.runs))
	for id := range a.runs {
		ids = append(ids, id)
	}
	a.mu.Unlock()
	sort.Strings(ids)
	return ids
}
