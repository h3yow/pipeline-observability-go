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
func (e *Evaluator) Evaluate(run model.RunState, rs rule.Set) model.Conclusion {
	c := model.Conclusion{
		JobID:       run.JobID,
		RunID:       run.RunID,
		RuleID:      rs.RuleID,
		RuleVer:     rs.Version,
		EvaluatedAt: e.now(),
	}

	expectedBy := run.StartedAt.Add(rs.FreshnessDeadline)
	fresh := !run.FinishedAt.After(expectedBy) // FinishedAt <= ExpectedBy
	c.Freshness = model.FreshnessVerdict{
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

func verdict(ok bool, pass, fail string) string {
	if ok {
		return pass
	}
	return fail
}
