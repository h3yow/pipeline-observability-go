package model

import "time"

// AlertState is the lifecycle state of an alert.
type AlertState string

const (
	// AlertStateOK: no breach observed (initial state or after recovery).
	AlertStateOK AlertState = "OK"
	// AlertStateFiring: breach confirmed after debounce; alert is active.
	AlertStateFiring AlertState = "FIRING"
	// AlertStateRecover: terminal marker emitted exactly once when a
	// firing alert returns to normal. Distinct from OK so recovery and
	// re-fire are never conflated.
	AlertStateRecover AlertState = "RECOVERED"
)

// AlertKind classifies an alert.
type AlertKind string

const (
	KindFreshness AlertKind = "FRESHNESS"
	KindSLA       AlertKind = "SLA"
)

// Alert is one alert lifecycle conclusion for a job.
type Alert struct {
	JobID     JobID
	Kind      AlertKind
	State     AlertState
	Version   int
	Reason    string
	Timestamp time.Time
	Seq       int64
}

// VerdictStatus is the ternary outcome of one judgement.
type VerdictStatus string

const (
	// StatusPass: the run meets the threshold.
	StatusPass VerdictStatus = "PASS"
	// StatusBreach: the run violates the threshold.
	StatusBreach VerdictStatus = "BREACH"
	// StatusUnknown: the time basis is incomplete (e.g. the started event
	// is missing), so no pass/breach conclusion can be drawn. Unknown
	// verdicts never raise alerts and never count towards alert debounce.
	StatusUnknown VerdictStatus = "UNKNOWN"
)

// GapCause explains why a required input event is absent from a run.
type GapCause string

const (
	// GapCauseNotArrived: no such event was ever seen at intake.
	GapCauseNotArrived GapCause = "NOT_ARRIVED"
	// GapCauseDroppedLate: the event arrived but was rejected by the
	// lateness watermark (disposition LATE).
	GapCauseDroppedLate GapCause = "DROPPED_LATE"
	// GapCauseDroppedInvalid: the event arrived but failed validation
	// (disposition INVALID).
	GapCauseDroppedInvalid GapCause = "DROPPED_INVALID"
	// GapCauseSuperseded: the event was rejected because another delivery
	// of the same run/phase was kept instead (disposition CONFLICT).
	GapCauseSuperseded GapCause = "SUPERSEDED"
)

// InputGap describes one missing input of a run and why it is missing.
type InputGap struct {
	// EventType is the missing event type, e.g. "run.started".
	EventType string
	Cause     GapCause
	Detail    string
}

// FreshnessVerdict is the freshness result for a run.
type FreshnessVerdict struct {
	// Status is the authoritative ternary outcome. OK is kept as a
	// convenience and is true exactly when Status == StatusPass.
	Status         VerdictStatus
	OK             bool
	ExpectedBy     time.Time
	ActualFinishAt time.Time
	Reason         string
}

// SLAVerdict is the SLA result for a run.
type SLAVerdict struct {
	// Status is the authoritative ternary outcome (see FreshnessVerdict).
	Status VerdictStatus
	OK     bool
	Limit  time.Duration
	Actual time.Duration
	Reason string
}

// Conclusion is the output of evaluating one completed run under one rule
// version. Keeping RuleID/RuleVer inside makes replays self-describing.
type Conclusion struct {
	JobID       JobID
	RunID       RunID
	RuleID      string
	RuleVer     int
	Freshness   FreshnessVerdict
	SLA         SLAVerdict
	Breaches    []AlertKind
	EvaluatedAt time.Time
	// Determinable is false when the run's time basis is incomplete (e.g.
	// only a finished event exists); Freshness/SLA are then UNKNOWN, no
	// breach is raised and the run appears in the pending view until the
	// missing event is backfilled.
	Determinable bool
	// Gaps lists the missing inputs (with causes) when Determinable is
	// false; empty for determinable conclusions.
	Gaps []InputGap
}
