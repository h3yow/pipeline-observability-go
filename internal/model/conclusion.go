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

// FreshnessVerdict is the freshness result for a run.
type FreshnessVerdict struct {
	OK             bool
	ExpectedBy     time.Time
	ActualFinishAt time.Time
	Reason         string
}

// SLAVerdict is the SLA result for a run.
type SLAVerdict struct {
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
}
