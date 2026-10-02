package eval

import (
	"testing"
	"time"

	"github.com/h3yow/pipeline-observability-go/internal/model"
	"github.com/h3yow/pipeline-observability-go/internal/rule"
)

func at(min int) time.Time {
	return time.Date(2026, 9, 23, 10, min, 0, 0, time.UTC)
}

func runState(start, finish int, result model.RunResult) model.RunState {
	return model.RunState{
		RunID:       "r1",
		JobID:       "job-a",
		StartedAt:   at(start),
		FinishedAt:  at(finish),
		Result:      result,
		HasStarted:  true,
		HasFinished: true,
	}
}

func rules(fresh, sla time.Duration) rule.Set {
	return rule.Set{
		RuleID: "job-a-rule", Version: 1,
		FreshnessDeadline: fresh, SLADuration: sla,
		FireAfterN: 1, RecoverAfterN: 1,
	}
}

func TestEvaluateFreshnessBoundary(t *testing.T) {
	e := NewEvaluator()
	// StartedAt 10:00, deadline 10m, finish exactly 10:10 => boundary = pass.
	r := runState(0, 10, model.ResultSuccess)
	c := e.Evaluate(r, rules(10*time.Minute, 30*time.Minute))
	t.Logf("INPUT run start=%s finish=%s | FRESHNESS %s | SLA %s",
		at(0).Format("15:04:05"), at(10).Format("15:04:05"),
		c.Freshness.Reason, c.SLA.Reason)
	if !c.Freshness.OK {
		t.Fatalf("boundary (finished == expectedBy) must be FRESH, got: %s", c.Freshness.Reason)
	}
	// One minute beyond => stale.
	c2 := e.Evaluate(runState(0, 11, model.ResultSuccess), rules(10*time.Minute, 30*time.Minute))
	t.Logf("INPUT run finish=%s | FRESHNESS %s", at(11).Format("15:04:05"), c2.Freshness.Reason)
	if c2.Freshness.OK {
		t.Fatal("10:11 must be STALE against 10m deadline")
	}
}

func TestEvaluateSLABoundary(t *testing.T) {
	e := NewEvaluator()
	// duration exactly 10m against a 10m limit => within SLA.
	c := e.Evaluate(runState(0, 10, model.ResultSuccess), rules(30*time.Minute, 10*time.Minute))
	t.Logf("SLA boundary: %s", c.SLA.Reason)
	if !c.SLA.OK {
		t.Fatal("duration == limit must be WITHIN_SLA")
	}
	c2 := e.Evaluate(runState(0, 11, model.ResultSuccess), rules(30*time.Minute, 10*time.Minute))
	t.Logf("SLA beyond: %s", c2.SLA.Reason)
	if c2.SLA.OK || len(c2.Breaches) == 0 {
		t.Fatal("11m duration must breach 10m SLA")
	}
}

// TestEvaluateVersionedReproducibility feeds the SAME historical run under
// two registered rule versions and asserts each version yields its own
// stable verdict, i.e. old decisions remain reproducible after evolution.
func TestEvaluateVersionedReproducibility(t *testing.T) {
	reg := rule.NewRegistry()
	v1 := rules(10*time.Minute, 10*time.Minute)
	v2 := rules(10*time.Minute, 10*time.Minute)
	v2.Version, v2.SLADuration = 2, 5*time.Minute // tightened SLA
	if err := reg.Register(v1); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(v2); err != nil {
		t.Fatal(err)
	}
	e := NewEvaluator()
	r := runState(0, 8, model.ResultSuccess) // duration 8m

	old, _ := reg.Get("job-a-rule", 1)
	newest, _ := reg.Get("job-a-rule", 2)
	c1 := e.Evaluate(r, old)
	c1again := e.Evaluate(r, old)
	c2 := e.Evaluate(r, newest)

	t.Logf("INPUT run duration=8m | under v1(limit=10m): %s | under v2(limit=5m): %s",
		c1.SLA.Reason, c2.SLA.Reason)
	if !c1.SLA.OK {
		t.Fatal("historical verdict under v1 must be WITHIN_SLA")
	}
	if c2.SLA.OK {
		t.Fatal("same event under tightened v2 must breach")
	}
	if c1.SLA.OK != c1again.SLA.OK || c1.RuleVer != 1 || c2.RuleVer != 2 {
		t.Fatal("rule version must be pinned and verdicts reproducible")
	}
	t.Logf("decision basis: explicit version binding => v1 replay stays pass while v2 fails; RuleVer recorded=%d/%d",
		c1.RuleVer, c2.RuleVer)
}

// TestEvaluateUnknownWhenBasisMissing: a run with only a finished event
// must not be judged pass/fail; both verdicts are UNKNOWN and no breach
// is raised.
func TestEvaluateUnknownWhenBasisMissing(t *testing.T) {
	e := NewEvaluator()
	r := model.RunState{
		RunID: "r-lost", JobID: "job-a",
		FinishedAt:  at(20),
		Result:      model.ResultSuccess,
		HasFinished: true, // HasStarted == false: start event lost
	}
	c := e.Evaluate(r, rules(10*time.Minute, 10*time.Minute))
	t.Logf("INPUT run=%s hasStarted=false hasFinished=true finish=%s", r.RunID, at(20).Format("15:04:05"))
	t.Logf("DECISION determinable=%v gaps=%v | FRESHNESS %s | SLA %s",
		c.Determinable, c.Gaps, c.Freshness.Reason, c.SLA.Reason)

	if c.Determinable {
		t.Fatal("run without start basis must be indeterminable")
	}
	if c.Freshness.Status != model.StatusUnknown || c.SLA.Status != model.StatusUnknown {
		t.Fatalf("both verdicts must be UNKNOWN, got %s/%s", c.Freshness.Status, c.SLA.Status)
	}
	if c.Freshness.OK || c.SLA.OK {
		t.Fatal("UNKNOWN must not be reported as OK")
	}
	if len(c.Breaches) != 0 {
		t.Fatalf("UNKNOWN must raise no breaches, got %v", c.Breaches)
	}
	if len(c.Gaps) != 1 || c.Gaps[0].EventType != "run.started" {
		t.Fatalf("gap must name the missing run.started input, got %+v", c.Gaps)
	}
	t.Logf("decision basis: incomplete time basis => UNKNOWN, no breach, gap=%s", c.Gaps[0].EventType)
}

// TestEvaluateDeterminableStatusMirrorsOK: for complete runs the new
// Status field agrees with the legacy OK flag.
func TestEvaluateDeterminableStatusMirrorsOK(t *testing.T) {
	e := NewEvaluator()
	pass := e.Evaluate(runState(0, 5, model.ResultSuccess), rules(30*time.Minute, 10*time.Minute))
	breach := e.Evaluate(runState(0, 20, model.ResultSuccess), rules(30*time.Minute, 10*time.Minute))
	t.Logf("INPUT duration=5m => %s | INPUT duration=20m => %s", pass.SLA.Reason, breach.SLA.Reason)
	if !pass.Determinable || pass.SLA.Status != model.StatusPass || !pass.SLA.OK {
		t.Fatalf("pass run: status=%s ok=%v", pass.SLA.Status, pass.SLA.OK)
	}
	if breach.SLA.Status != model.StatusBreach || breach.SLA.OK {
		t.Fatalf("breach run: status=%s ok=%v", breach.SLA.Status, breach.SLA.OK)
	}
	t.Logf("decision basis: Status mirrors OK for determinable runs (PASS/BREACH)")
}
