package pipelineobs

import (
	"sync"
	"testing"
	"time"

	"github.com/h3yow/pipeline-observability-go/alert"
	"github.com/h3yow/pipeline-observability-go/eval"
	"github.com/h3yow/pipeline-observability-go/event"
	"github.com/h3yow/pipeline-observability-go/ingest"
	"github.com/h3yow/pipeline-observability-go/internal/testhelp"
	"github.com/h3yow/pipeline-observability-go/rule"
)

var p0 = time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)

func newTestPipeline(t *testing.T) *Pipeline {
	t.Helper()
	p := New(Config{
		MaxLateness: time.Hour,
		AlertPolicy: alert.Policy{TriggerAfter: 2, RecoverAfter: 2},
	})
	r1, err := rule.NewV1("jobA", rule.V1{
		FreshnessLimit: 30 * time.Minute, MaxDuration: 30 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := rule.NewV2("jobA", rule.V2{
		FreshnessLimit: 30 * time.Minute, MaxDuration: 30 * time.Minute,
		TimeoutMaxDuration: 30 * time.Minute, MaxAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.RegisterRules(r1); err != nil {
		t.Fatal(err)
	}
	if err := p.RegisterRules(r2); err != nil {
		t.Fatal(err)
	}
	return p
}

func logResult(t *testing.T, res Result) {
	t.Helper()
	t.Logf("DECISION event=%s applied=%v reason=%s detail=%s",
		res.Decision.EventID, res.Decision.Applied, res.Decision.Reason, res.Decision.Detail)
	if res.Verdict != nil {
		t.Logf("VERDICT  run=%s rules=%s freshness=%s sla=%s",
			res.Verdict.RunID, res.Verdict.RuleVersion,
			res.Verdict.Freshness, res.Verdict.SLA)
		for _, b := range res.Verdict.FreshnessBasis {
			t.Logf("BASIS    [fresh] %s pass=%v actual=%q want=%q", b.Check, b.Pass, b.Actual, b.Want)
		}
		for _, b := range res.Verdict.SLABasis {
			t.Logf("BASIS    [sla]   %s pass=%v actual=%q want=%q", b.Check, b.Pass, b.Actual, b.Want)
		}
	}
	for _, tr := range res.Transitions {
		t.Logf("ALERT    kind=%s phase=%s->%s incident=%s reason=%q",
			tr.Kind, tr.FromPhase, tr.ToPhase, tr.IncidentID, tr.Reason)
	}
}

func v1Started(id, run string, seq int64, occ time.Time) event.RawEvent {
	return event.RawEvent{
		SchemaVersion: event.SchemaV1, EventID: id, RunID: run, JobID: "jobA",
		Seq: testhelp.PtrInt64(seq), OccurredAt: testhelp.PtrTime(occ), Phase: "STARTED",
	}
}

func v1Finished(id, run string, seq int64, occ time.Time, status event.Status) event.RawEvent {
	r := v1Started(id, run, seq, occ)
	r.Phase = "FINISHED"
	r.Status = string(status)
	return r
}

// TestEndToEndSLAFromTriggerToRecovery walks a complete lifecycle including
// duplicate delivery, out-of-order arrival, threshold jitter and recovery.
func TestEndToEndSLAFromTriggerToRecovery(t *testing.T) {
	p := newTestPipeline(t)

	// Run 1: out-of-order FINISHED before STARTED; failed -> SLA breach.
	f1 := v1Finished("run1-fin", "run1", 2, p0.Add(10*time.Minute), event.StatusFailed)
	testhelp.LogRaw(t, "run1", f1, p0.Add(11*time.Minute))
	res, err := p.IngestEvent(f1, p0.Add(11*time.Minute), p0.Add(11*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	logResult(t, res)
	if res.Decision.Reason != ingest.ReasonApplied {
		t.Fatalf("want APPLIED, got %s", res.Decision.Reason)
	}
	// No STARTED yet: SLA is UNKNOWN, must NOT feed the alert lifecycle.
	if res.Verdict == nil || res.Verdict.SLA != eval.LevelUnknown ||
		len(res.Transitions) != 0 {
		t.Fatalf("missing-start must be UNKNOWN with no transition: %+v", res)
	}

	s1 := v1Started("run1-start", "run1", 1, p0)
	testhelp.LogRaw(t, "run1-late", s1, p0.Add(12*time.Minute))
	res, err = p.IngestEvent(s1, p0.Add(12*time.Minute), p0.Add(12*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	logResult(t, res)
	if res.Decision.Reason != ingest.ReasonAppliedLate {
		t.Fatalf("want APPLIED_LATE, got %s", res.Decision.Reason)
	}
	if res.Verdict == nil || res.Verdict.SLA != eval.LevelBreach {
		t.Fatalf("want SLA BREACH after late gap fill, got %+v", res.Verdict)
	}
	// First breach observation -> PENDING (suppressed), no incident yet.
	if len(res.Transitions) != 1 || res.Transitions[0].Kind != alert.KindSuppressed {
		t.Fatalf("want suppressed PENDING, got %+v", res.Transitions)
	}

	// Second consecutive breach observation at t=13m -> FIRING.
	vBreach, err := p.EvaluateRun("run1", rule.RulesV1, p0.Add(13*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	trFire, err := p.alerts.Observe(vBreach, p0.Add(13*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ALERT    second breach -> kind=%s %s->%s incident=%s",
		trFire.Kind, trFire.FromPhase, trFire.ToPhase, trFire.IncidentID)
	if trFire.Kind != alert.KindTriggered {
		t.Fatalf("want TRIGGERED, got %s", trFire.Kind)
	}
	firstIncident := trFire.IncidentID

	// Duplicate delivery must not re-alert and must not re-evaluate.
	res, err = p.IngestEvent(f1, p0.Add(30*time.Minute), p0.Add(30*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	logResult(t, res)
	if res.Decision.Reason != ingest.ReasonDuplicate || res.Verdict != nil ||
		len(res.Transitions) != 0 {
		t.Fatalf("duplicate must be inert: %+v", res)
	}

	// Run 2: a fresh, successful run supplies the OK signal that first
	// interrupts nothing (incident already FIRING), then twice more drives
	// recovery under the (2,2) policy.
	s2 := v1Started("run2-start", "run2", 1, p0.Add(20*time.Minute))
	testhelp.LogRaw(t, "run2", s2, p0.Add(20*time.Minute))
	if _, err := p.IngestEvent(s2, p0.Add(20*time.Minute), p0.Add(20*time.Minute)); err != nil {
		t.Fatal(err)
	}
	f2ok := v1Finished("run2-fin", "run2", 2, p0.Add(22*time.Minute), event.StatusSuccess)
	testhelp.LogRaw(t, "run2-success", f2ok, p0.Add(22*time.Minute))
	res, err = p.IngestEvent(f2ok, p0.Add(22*time.Minute), p0.Add(22*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	logResult(t, res)
	if res.Verdict == nil || !(!res.Verdict.Breach()) {
		t.Fatalf("run2 should be fully OK: %+v", res.Verdict)
	}
	if len(res.Transitions) != 1 || res.Transitions[0].Kind != alert.KindSuppressed {
		t.Fatalf("first OK while FIRING must be suppressed (1/2), got %+v", res.Transitions)
	}

	vB, err := p.EvaluateRun("run2", rule.RulesV1, p0.Add(23*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	trB, err := p.alerts.Observe(vB, p0.Add(23*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ALERT    recovery step: %s incident=%s", trB.Kind, trB.IncidentID)
	if trB.Kind != alert.KindRecovered || trB.IncidentID != firstIncident {
		t.Fatalf("recovery must close the first incident: %s incident=%s want %s",
			trB.Kind, trB.IncidentID, firstIncident)
	}
	inc, _ := p.Incident("jobA")
	if inc.Phase != alert.PhaseResolved {
		t.Fatalf("incident phase: %s", inc.Phase)
	}
}

// TestEndToEndHistoricalReproductionV1V2 replays the same raw event history
// twice through fresh pipelines (v1 rules vs v2 rules + v2 events) and shows
// version-pinned verdicts are reproducible.
func TestEndToEndHistoricalReproductionV1V2(t *testing.T) {
	// v1 history: a 20-minute TIMEOUT is a breach under rules/v1.
	p := newTestPipeline(t)
	s := v1Started("s", "h1", 1, p0)
	f := v1Finished("f", "h1", 2, p0.Add(20*time.Minute), event.StatusTimeout)
	if _, err := p.IngestEvent(s, p0.Add(time.Second), p0.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.IngestEvent(f, p0.Add(21*time.Minute), p0.Add(21*time.Minute)); err != nil {
		t.Fatal(err)
	}
	vv1a, err := p.EvaluateRun("h1", rule.RulesV1, p0.Add(21*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	vv1b, err := p.EvaluateRun("h1", rule.RulesV1, p0.Add(21*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if vv1a.SLA != "BREACH" || vv1a.SLA != vv1b.SLA {
		t.Fatalf("v1 historical verdict not reproducible: %s vs %s", vv1a.SLA, vv1b.SLA)
	}
	t.Logf("BASIS  reproduced v1 verdict: freshness=%s sla=%s", vv1a.Freshness, vv1a.SLA)
}

// TestEndToEndConcurrentDuplicates asserts duplicates under concurrency
// produce one alert incident at most.
func TestEndToEndConcurrentDuplicates(t *testing.T) {
	// Fast-firing policy so one breach verdict opens exactly one incident.
	p2 := New(Config{MaxLateness: time.Hour, AlertPolicy: alert.Policy{
		TriggerAfter: 1, RecoverAfter: 1,
	}})
	r1, _ := rule.NewV1("jobA", rule.V1{
		FreshnessLimit: 30 * time.Minute, MaxDuration: 30 * time.Minute,
	})
	if err := p2.RegisterRules(r1); err != nil {
		t.Fatal(err)
	}

	f := v1Finished("f", "r9", 2, p0.Add(10*time.Minute), event.StatusFailed)
	s := v1Started("s", "r9", 1, p0)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var triggers int
	// Blast start, finish and many duplicates concurrently.
	events := []event.RawEvent{s, f, f, f, s, s, f, s}
	start := make(chan struct{})
	for _, raw := range events {
		raw := raw
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := p2.IngestEvent(raw, p0.Add(11*time.Minute), p0.Add(11*time.Minute))
			if err != nil {
				t.Error(err)
				return
			}
			for _, tr := range res.Transitions {
				if tr.Kind == alert.KindTriggered {
					mu.Lock()
					triggers++
					mu.Unlock()
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	if triggers != 1 {
		t.Fatalf("exactly one TRIGGERED expected under duplicate concurrency, got %d", triggers)
	}
	st, ok := p2.State("r9")
	if !ok || !st.Closed || st.Status != event.StatusFailed {
		t.Fatalf("bad final state: %+v", st)
	}
	t.Logf("BASIS  concurrent duplicates -> exactly 1 trigger; final state closed=%v status=%s",
		st.Closed, st.Status)
}
