package eval

import (
	"testing"
	"time"

	"github.com/h3yow/pipeline-observability-go/internal/model"
	"github.com/h3yow/pipeline-observability-go/internal/rule"
)

// TestEvaluateUnknownWhenStartMissing: a finished run without a started event
// yields UNKNOWN for both verdicts, no breaches, and carries the gap reason.
func TestEvaluateUnknownWhenStartMissing(t *testing.T) {
	e := NewEvaluator()
	run := model.RunState{
		RunID: "rX", JobID: "job-a",
		FinishedAt:  at(10),
		HasFinished: true,
		BasisGap: &model.MissingBasis{
			Phase:           model.PhaseStarted,
			Code:            model.GapDroppedLate,
			Detail:          "candidate lost-s dropped by watermark: ...",
			EvidenceEventID: "lost-s",
		},
	}
	rs := rule.Set{
		RuleID: "job-a", Version: 1,
		FreshnessDeadline: 30 * time.Minute, SLADuration: 10 * time.Minute,
		FireAfterN: 1, RecoverAfterN: 1,
	}
	c := e.Evaluate(run, rs)
	t.Logf("INPUT run=%s finished=%s basisGap=%s(%s) | DECISION determinable=%t freshness=%s sla=%s breaches=%v",
		run.RunID, run.FinishedAt.Format("15:04"), run.BasisGap.Code, run.BasisGap.EvidenceEventID,
		c.Determinable, c.Freshness.Status, c.SLA.Status, c.Breaches)

	if c.Determinable || !c.IsUnknown() {
		t.Fatalf("want unknown conclusion, got determinable=%v", c.Determinable)
	}
	if c.Freshness.Status != model.VerdictUnknown || c.SLA.Status != model.VerdictUnknown {
		t.Fatalf("want both UNKNOWN, got freshness=%s sla=%s", c.Freshness.Status, c.SLA.Status)
	}
	if len(c.Breaches) != 0 {
		t.Fatalf("unknown run must produce no breaches, got %v", c.Breaches)
	}
	if len(c.MissingBasis) != 1 || c.MissingBasis[0].Code != model.GapDroppedLate ||
		c.MissingBasis[0].EvidenceEventID != "lost-s" {
		t.Fatalf("gap basis not carried: %+v", c.MissingBasis)
	}
	if c.Freshness.OK {
		t.Fatalf("OK must not be true for an unknown verdict")
	}
	t.Logf("decision basis: freshness=%q", c.Freshness.Reason)
	t.Logf("decision basis: sla=%q", c.SLA.Reason)
}

// TestEvaluateDeterminableUnchanged: complete runs keep exact PASS/FAIL
// semantics (boundary inclusive) after the three-valued change.
func TestEvaluateDeterminableStatus(t *testing.T) {
	e := NewEvaluator()
	rs := rule.Set{
		RuleID: "job-a", Version: 1,
		FreshnessDeadline: 10 * time.Minute, SLADuration: 10 * time.Minute,
		FireAfterN: 1, RecoverAfterN: 1,
	}
	pass := e.Evaluate(runState(0, 10, model.ResultSuccess), rs)
	if pass.Freshness.Status != model.VerdictPass || pass.SLA.Status != model.VerdictPass ||
		!pass.Freshness.OK || !pass.SLA.OK || !pass.Determinable {
		t.Fatalf("boundary run must be PASS/PASS: f=%s s=%s",
			pass.Freshness.Status, pass.SLA.Status)
	}
	fail := e.Evaluate(runState(0, 15, model.ResultFailed), rs)
	if fail.Freshness.Status != model.VerdictFail || fail.SLA.Status != model.VerdictFail ||
		fail.Freshness.OK || fail.SLA.OK || len(fail.Breaches) != 2 {
		t.Fatalf("over-threshold run must be FAIL/FAIL: f=%s s=%s breaches=%v",
			fail.Freshness.Status, fail.SLA.Status, fail.Breaches)
	}
	t.Logf("INPUT complete runs | DECISION boundary=%s/%s over=%s/%s (unchanged semantics)",
		pass.Freshness.Status, pass.SLA.Status, fail.Freshness.Status, fail.SLA.Status)
}
