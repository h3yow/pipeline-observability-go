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

// VerdictStatus is the three-valued judgement status of a freshness/SLA
// verdict. A verdict is only PASS/FAIL when its full time basis is present;
// with a missing basis it is UNKNOWN and must never be treated as either.
type VerdictStatus string

const (
	// VerdictPass: determinable and within the threshold.
	VerdictPass VerdictStatus = "PASS"
	// VerdictFail: determinable and outside the threshold.
	VerdictFail VerdictStatus = "FAIL"
	// VerdictUnknown: time basis incomplete; no judgement is possible.
	VerdictUnknown VerdictStatus = "UNKNOWN"
)

// GapCode classifies why a run's time basis is incomplete.
type GapCode string

const (
	// GapNone: the basis is complete.
	GapNone GapCode = ""
	// GapNotArrived: the missing phase event has not been seen at all; it
	// may still be delivered (or explicitly backfilled).
	GapNotArrived GapCode = "START_NOT_ARRIVED"
	// GapDroppedLate: a candidate for the missing phase was rejected by the
	// watermark (event older than maxOccurred - allowLateness).
	GapDroppedLate GapCode = "START_DROPPED_LATE"
	// GapDroppedInvalid: a candidate for the missing phase failed schema
	// validation and was rejected.
	GapDroppedInvalid GapCode = "START_DROPPED_INVALID"
	// GapDisplaced: a candidate for the missing phase was rejected as a
	// CONFLICT (another delivery occupies the slot / the run belongs to a
	// different job), so it never filled the phase.
	GapDisplaced GapCode = "START_DISPLACED"
)

// MissingBasis describes one missing piece of a run's time basis.
type MissingBasis struct {
	// Phase is the lifecycle phase whose event is missing (STARTED).
	Phase RunPhase
	// Code classifies the input gap.
	Code GapCode
	// Detail is a human-readable explanation with the evidence event id.
	Detail string
	// EvidenceEventID is the rejected candidate that explains the gap, when
	// one exists; empty for the NOT_ARRIVED case.
	EvidenceEventID string
}

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
	// Status is the three-valued verdict; OK is meaningful only when
	// Status != VerdictUnknown (it is false for unknown verdicts).
	Status         VerdictStatus
	OK             bool
	ExpectedBy     time.Time
	ActualFinishAt time.Time
	Reason         string
}

// SLAVerdict is the SLA result for a run.
type SLAVerdict struct {
	// Status is the three-valued verdict; OK is meaningful only when
	// Status != VerdictUnknown (it is false for unknown verdicts).
	Status VerdictStatus
	OK     bool
	Limit  time.Duration
	Actual time.Duration
	Reason string
}

// Conclusion is the output of evaluating one completed run under one rule
// version. Keeping RuleID/RuleVer inside makes replays self-describing.
type Conclusion struct {
	JobID JobID
	RunID RunID
	// Determinable is false when the run's time basis is incomplete; in that
	// case Freshness/SLA are both UNKNOWN, Breaches is empty and the run
	// never participates in alert lifecycles.
	Determinable bool
	MissingBasis []MissingBasis
	RuleID       string
	RuleVer      int
	Freshness    FreshnessVerdict
	SLA          SLAVerdict
	Breaches     []AlertKind
	EvaluatedAt  time.Time
}

// IsUnknown reports whether the conclusion cannot be judged.
func (c Conclusion) IsUnknown() bool { return !c.Determinable }
