// Package eval evaluates completed runs against versioned freshness and
// SLA rules.
//
// Judgement semantics (rule schema v1):
//
//	Freshness: a run is fresh when FinishedAt <= StartedAt + FreshnessDeadline.
//	SLA:       a run meets its SLA when FinishedAt-StartedAt <= SLADuration.
//
// Both judgements require the full time basis (started + finished). When the
// basis is incomplete, both verdicts are UNKNOWN rather than PASS/FAIL: no
// breach is inferred, Breaches stays empty, and the gap reasons recorded by
// intake are carried through. Every verdict carries the threshold (when
// computable), the measured value and a textual reason, so conclusions are
// explainable and reproducible.
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

// NewEvaluatorWithClock creates an Evaluator whose EvaluatedAt timestamps
// come from the supplied clock. It exists for deterministic tests; normal
// code uses NewEvaluator.
func NewEvaluatorWithClock(now func() time.Time) *Evaluator {
	return &Evaluator{now: now}
}

// Evaluate judges one completed run under an explicit rule version.
// The caller is responsible for resolving the exact rule version; passing
// a versioned Set makes historical replay identical to the original run.
//
// When the run's time basis is incomplete (HasFinished without HasStarted),
// both verdicts are VerdictUnknown, no breach is recorded and the intake gap
// reasons are carried on the conclusion. Such conclusions must never be fed
// to the alert lifecycle (the engine enforces this).
func (e *Evaluator) Evaluate(run model.RunState, rs rule.Set) model.Conclusion {
	c := model.Conclusion{
		JobID:       run.JobID,
		RunID:       run.RunID,
		RuleID:      rs.RuleID,
		RuleVer:     rs.Version,
		EvaluatedAt: e.now(),
	}

	if run.HasFinished && !run.HasStarted {
		c.Determinable = false
		if run.BasisGap != nil {
			c.MissingBasis = []model.MissingBasis{*run.BasisGap}
		} else {
			c.MissingBasis = []model.MissingBasis{{
				Phase: model.PhaseStarted,
				Code:  model.GapNotArrived,
			}}
		}
		gap := c.MissingBasis[0]
		c.Freshness = model.FreshnessVerdict{
			Status:         model.VerdictUnknown,
			ActualFinishAt: run.FinishedAt,
			Reason: fmt.Sprintf(
				"UNKNOWN: cannot judge freshness without run.started (finished=%s): gap=%s %s (rule %s v%d)",
				run.FinishedAt.UTC().Format(time.RFC3339),
				gap.Code, gap.Detail, rs.RuleID, rs.Version),
		}
		c.SLA = model.SLAVerdict{
			Status: model.VerdictUnknown,
			Reason: fmt.Sprintf(
				"UNKNOWN: cannot judge SLA without run.started; duration unmeasurable: gap=%s %s (rule %s v%d)",
				gap.Code, gap.Detail, rs.RuleID, rs.Version),
		}
		return c
	}

	c.Determinable = true
	expectedBy := run.StartedAt.Add(rs.FreshnessDeadline)
	fresh := !run.FinishedAt.After(expectedBy) // FinishedAt <= ExpectedBy
	c.Freshness = model.FreshnessVerdict{
		Status:         verdictStatus(fresh),
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
		Status: verdictStatus(okSLA),
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

func verdictStatus(ok bool) model.VerdictStatus {
	if ok {
		return model.VerdictPass
	}
	return model.VerdictFail
}
