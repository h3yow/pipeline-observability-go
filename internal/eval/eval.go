// Package eval evaluates completed runs against versioned freshness and
// SLA rules.
//
// Judgement semantics (rule schema v1):
//
//	Freshness: a run is fresh when FinishedAt <= StartedAt + FreshnessDeadline.
//	SLA:       a run meets its SLA when FinishedAt-StartedAt <= SLADuration.
//
// Every verdict carries the threshold, the measured value and a textual
// reason, so conclusions are explainable and reproducible.
package eval

import (
	"fmt"
	"time"

	"github.com/h3yow/pipeline-observability-go/internal/event"
	"github.com/h3yow/pipeline-observability-go/internal/model"
	"github.com/h3yow/pipeline-observability-go/internal/rule"
)

// Evaluator evaluates runs against rule sets.
type Evaluator struct {
	now func() time.Time
}

// NewEvaluator creates an Evaluator.
func NewEvaluator() *Evaluator {
	return &Evaluator{now: func() time.Time { return time.Now().UTC() }}
}

// Evaluate judges one completed run under an explicit rule version.
// The caller is responsible for resolving the exact rule version; passing
// a versioned Set makes historical replay identical to the original run.
//
// When the run's time basis is incomplete (no started event applied), the
// conclusion is marked indeterminable: both verdicts are UNKNOWN, no breach
// is raised, and Conclusion.Gaps names the missing input. The gap's Cause
// is left for the caller (the engine enriches it from intake records).
func (e *Evaluator) Evaluate(run model.RunState, rs rule.Set) model.Conclusion {
	c := model.Conclusion{
		JobID:        run.JobID,
		RunID:        run.RunID,
		RuleID:       rs.RuleID,
		RuleVer:      rs.Version,
		EvaluatedAt:  e.now(),
		Determinable: true,
	}

	if !run.HasStarted || !run.HasFinished {
		return e.undeterminable(run, c, rs)
	}

	expectedBy := run.StartedAt.Add(rs.FreshnessDeadline)
	fresh := !run.FinishedAt.After(expectedBy) // FinishedAt <= ExpectedBy
	c.Freshness = model.FreshnessVerdict{
		Status:         statusOf(fresh),
		OK:             fresh,
		ExpectedBy:     expectedBy,
		ActualFinishAt: run.FinishedAt,
		Reason: fmt.Sprintf("finished=%s expectedBy=%s deadline=%s => %s (rule %s v%d)",
			run.FinishedAt.UTC().Format(time.RFC3339),
			expectedBy.UTC().Format(time.RFC3339),
			rs.FreshnessDeadline,
			verdict(fresh, "FRESH", "STALE"),
			rs.RuleID, rs.Version),
	}
	if !fresh {
		c.Breaches = append(c.Breaches, model.KindFreshness)
	}

	dur := run.Duration()
	okSLA := dur <= rs.SLADuration
	c.SLA = model.SLAVerdict{
		Status: statusOf(okSLA),
		OK:     okSLA,
		Limit:  rs.SLADuration,
		Actual: dur,
		Reason: fmt.Sprintf("duration=%s limit=%s => %s (rule %s v%d)",
			dur, rs.SLADuration,
			verdict(okSLA, "WITHIN_SLA", "SLA_BREACH"),
			rs.RuleID, rs.Version),
	}
	if !okSLA {
		c.Breaches = append(c.Breaches, model.KindSLA)
	}

	return c
}

// undeterminable builds an UNKNOWN conclusion for a run whose time basis is
// incomplete. It never raises a breach.
func (e *Evaluator) undeterminable(run model.RunState, c model.Conclusion, rs rule.Set) model.Conclusion {
	c.Determinable = false
	if !run.HasStarted {
		c.Gaps = append(c.Gaps, model.InputGap{
			EventType: string(event.TypeStarted),
			Detail:    "no run.started event applied to this run",
		})
	}
	if !run.HasFinished {
		c.Gaps = append(c.Gaps, model.InputGap{
			EventType: string(event.TypeFinished),
			Detail:    "no run.finished event applied to this run",
		})
	}
	basis := fmt.Sprintf("started=%v finished=%v", run.HasStarted, run.HasFinished)
	c.Freshness = model.FreshnessVerdict{
		Status:         model.StatusUnknown,
		ActualFinishAt: run.FinishedAt,
		Reason: fmt.Sprintf("UNKNOWN: incomplete time basis (%s); freshness not judgable (rule %s v%d)",
			basis, rs.RuleID, rs.Version),
	}
	c.SLA = model.SLAVerdict{
		Status: model.StatusUnknown,
		Reason: fmt.Sprintf("UNKNOWN: incomplete time basis (%s); duration not judgable (rule %s v%d)",
			basis, rs.RuleID, rs.Version),
	}
	return c
}

func statusOf(ok bool) model.VerdictStatus {
	if ok {
		return model.StatusPass
	}
	return model.StatusBreach
}

func verdict(ok bool, pass, fail string) string {
	if ok {
		return pass
	}
	return fail
}
