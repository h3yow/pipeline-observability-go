// Package model defines the core domain types produced by the pipeline
// observability engine: run aggregates, evaluation results and alerts.
package model

import "time"

// RunID identifies a single execution of a job.
type RunID string

// JobID identifies a pipeline job.
type JobID string

// RunPhase is the lifecycle phase of a run.
type RunPhase string

// RunResult is the terminal outcome of a run.
type RunResult string

const (
	PhaseStarted  RunPhase = "STARTED"
	PhaseFinished RunPhase = "FINISHED"
)

const (
	ResultSuccess RunResult = "SUCCESS"
	ResultFailed  RunResult = "FAILED"
)

// RunState is the aggregated state of one job run.
//
// Timestamps use event time (OccurredAt). A run is built by applying
// validated events in event-time order; later application is tracked via
// the AppliedSeq field so aggregate updates are never silently overwritten.
type RunState struct {
	RunID       RunID
	JobID       JobID
	StartedAt   time.Time
	FinishedAt  time.Time
	Result      RunResult
	Attempt     int
	HasStarted  bool
	HasFinished bool
	// AppliedSeq is the global intake sequence of the last applied event;
	// it makes the aggregate's provenance explainable.
	AppliedSeq int64
	// AppliedEventIDs lists the event ids that contributed, in apply order.
	AppliedEventIDs []string
}

// Duration returns the run duration; zero when the run is not finished.
func (s RunState) Duration() time.Duration {
	if !s.HasFinished || !s.HasStarted {
		return 0
	}
	d := s.FinishedAt.Sub(s.StartedAt)
	if d < 0 {
		return 0
	}
	return d
}

// IsComplete reports whether a terminal (finished) event has been applied.
func (s RunState) IsComplete() bool { return s.HasFinished }
