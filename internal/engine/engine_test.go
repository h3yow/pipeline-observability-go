package engine

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/pipeline-observability-go/internal/alert"
	"github.com/h3yow/pipeline-observability-go/internal/event"
	"github.com/h3yow/pipeline-observability-go/internal/model"
	"github.com/h3yow/pipeline-observability-go/internal/rule"
)

const latenessWindow = int64(6 * time.Hour)

func runTime(day, hour, min int) time.Time {
	return time.Date(2026, 9, day, hour, min, 0, 0, time.UTC)
}

func started(id string, at time.Time, run, job string) event.Envelope {
	return event.Envelope{
		EventID: id, SchemaVersion: 1, Type: event.TypeStarted,
		OccurredAt: at, RunID: run, JobID: job,
		Payload: map[string]any{"attempt": 1},
	}
}
func finished(id string, at time.Time, run, job, result string) event.Envelope {
	return event.Envelope{
		EventID: id, SchemaVersion: 1, Type: event.TypeFinished,
		OccurredAt: at, RunID: run, JobID: job,
		Payload: map[string]any{"result": result, "attempt": 1},
	}
}

// scenarioEvents builds 4 runs of job-a:
//
//	run1 duration 5m  (within 10m SLA) -> pass
//	run2 duration 15m (breach)         -> breach #1 (debounce 2: no fire)
//	run3 duration 20m (breach)         -> breach #2 => FIRING
//	run4 duration 5m  (pass)           => RECOVERED
func scenarioEvents() []event.Envelope {
	d := 20
	return []event.Envelope{
		started("s1", runTime(d, 9, 0), "run-1", "job-a"),
		finished("f1", runTime(d, 9, 5), "run-1", "job-a", "SUCCESS"),
		started("s2", runTime(d, 10, 0), "run-2", "job-a"),
		finished("f2", runTime(d, 10, 15), "run-2", "job-a", "FAILED"),
		started("s3", runTime(d, 11, 0), "run-3", "job-a"),
		finished("f3", runTime(d, 11, 20), "run-3", "job-a", "FAILED"),
		started("s4", runTime(d, 12, 0), "run-4", "job-a"),
		finished("f4", runTime(d, 12, 5), "run-4", "job-a", "SUCCESS"),
	}
}

func registerV1(t *testing.T, e *Engine) {
	t.Helper()
	err := e.Rules().Register(rule.Set{
		RuleID: "job-a", Version: 1,
		FreshnessDeadline: 30 * time.Minute,
		SLADuration:       10 * time.Minute,
		FireAfterN:        2,
		RecoverAfterN:     1,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func feedShuffledConcurrent(t *testing.T, e *Engine, evs []event.Envelope, dupCount int) {
	t.Helper()
	var all []event.Envelope
	all = append(all, evs...)
	// Duplicate every event dupCount times (at-least-once delivery).
	for _, ev := range evs {
		for i := 0; i < dupCount; i++ {
			all = append(all, ev)
		}
	}
	r := rand.New(rand.NewSource(42))
	r.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })

	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	counts := map[string]int{}
	for _, ev := range all {
		wg.Add(1)
		go func(ev event.Envelope) {
			defer wg.Done()
			<-start
			rec := e.IngestEvent(context.Background(), ev)
			mu.Lock()
			counts[string(rec.Disposition)]++
			mu.Unlock()
		}(ev)
	}
	close(start)
	wg.Wait()
	t.Logf("INGEST dispositions across concurrent shuffled feed: %v", counts)
	if counts["APPLIED"]+counts["OUT_OF_ORDER_BUFFERED"] != len(evs) {
		t.Fatalf("exactly the %d unique events must be accepted, counts=%v", len(evs), counts)
	}
	if counts["DUPLICATE"] != len(evs)*dupCount {
		t.Fatalf("duplicate deliveries not all recognized: %v", counts)
	}
}

func transitionSeq(evs []alert.Transition) string {
	out := ""
	for _, tr := range evs {
		out += fmt.Sprintf("%s:%s->%s#%d ", tr.Alert.Kind, tr.FromState, tr.ToState, tr.Alert.Seq)
	}
	return out
}

func TestEndToEndLifecycleUnderChaos(t *testing.T) {
	e := New(latenessWindow)
	registerV1(t, e)

	evs := scenarioEvents()
	for _, ev := range evs {
		t.Logf("EVENT id=%s run=%s type=%s at=%s", ev.EventID, ev.RunID, ev.Type,
			ev.OccurredAt.Format("15:04"))
	}
	// Feed shuffled + every event duplicated twice, concurrently.
	feedShuffledConcurrent(t, e, evs, 2)

	conclusions, transitions, err := e.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range conclusions {
		t.Logf("CONCLUSION run=%s rule=%s v%d | freshness ok=%v (%s) | sla ok=%v (%s)",
			c.RunID, c.RuleID, c.RuleVer, c.Freshness.OK, c.Freshness.Reason,
			c.SLA.OK, c.SLA.Reason)
	}
	for _, tr := range transitions {
		t.Logf("ALERT %s job=%s %s->%s seq=%d reason=%q evidence=%q",
			tr.Alert.Kind, tr.Alert.JobID, tr.FromState, tr.ToState,
			tr.Alert.Seq, tr.Alert.Reason, tr.Evidence)
	}

	if len(conclusions) != 4 {
		t.Fatalf("want 4 conclusions, got %d", len(conclusions))
	}
	// Exactly one SLA FIRE (after run-3) and one RECOVERED (after run-4),
	// no freshness alerts (all finish within freshness deadline).
	var slaTrans []alert.Transition
	for _, tr := range transitions {
		if tr.Alert.Kind == model.KindSLA {
			slaTrans = append(slaTrans, tr)
		}
	}
	if len(slaTrans) != 2 ||
		slaTrans[0].ToState != model.AlertStateFiring ||
		slaTrans[1].ToState != model.AlertStateRecover {
		t.Fatalf("SLA lifecycle wrong: %s", transitionSeq(slaTrans))
	}
	st, ok := e.AlertState("job-a", model.KindSLA)
	if !ok || st != model.AlertStateOK {
		t.Fatalf("post-recovery state = %s", st)
	}
	// Re-evaluate is a no-op: conclusions and alerts stable.
	again, more, err := e.Evaluate()
	if err != nil || len(again) != 0 || len(more) != 0 {
		t.Fatalf("re-evaluation must be idempotent: %d conclusions %d alerts", len(again), len(more))
	}
	t.Logf("decision basis: transition seq = %s; re-evaluate produced no new alerts",
		transitionSeq(slaTrans))
}

// TestDeterminismAcrossChaosRuns reruns the same shuffled/duplicated feed
// many times and demands byte-identical conclusions and alert sequence.
func TestDeterminismAcrossChaosRuns(t *testing.T) {
	snapshot := func() string {
		e := New(latenessWindow)
		registerV1(t, e)
		feedShuffledConcurrent(t, e, scenarioEvents(), 1)
		conclusions, transitions, err := e.Evaluate()
		if err != nil {
			t.Fatal(err)
		}
		out := ""
		for _, c := range conclusions {
			out += fmt.Sprintf("%s:v%d:f%v:s%v|", c.RunID, c.RuleVer, c.Freshness.OK, c.SLA.OK)
		}
		out += "## " + transitionSeq(transitions)
		return out
	}
	first := snapshot()
	for i := 0; i < 5; i++ {
		if got := snapshot(); got != first {
			t.Fatalf("non-deterministic:\n%s\n%s", first, got)
		}
	}
	t.Logf("STABLE result across chaos runs: %s", first)
}

// TestVersionEvolutionDoesNotRewriteHistory registers a tighter v2 AFTER
// job-a was already bound to v1, then replays the same events in a fresh
// engine under each version: historical v1 verdicts stay reproducible,
// while v2 produces its own (different) conclusions.
func TestVersionEvolutionDoesNotRewriteHistory(t *testing.T) {
	evaluateUnder := func(version int) string {
		e := New(latenessWindow)
		registerV1(t, e)
		v2 := rule.Set{
			RuleID: "job-a", Version: 2,
			FreshnessDeadline: 30 * time.Minute,
			SLADuration:       3 * time.Minute, // tightened: run-1 (5m) now breaches too
			FireAfterN:        1,
			RecoverAfterN:     1,
		}
		if err := e.Rules().Register(v2); err != nil {
			t.Fatal(err)
		}
		if err := e.BindRule("job-a", "job-a", version); err != nil {
			t.Fatal(err)
		}
		feedShuffledConcurrent(t, e, scenarioEvents(), 1)
		conclusions, transitions, err := e.Evaluate()
		if err != nil {
			t.Fatal(err)
		}
		out := fmt.Sprintf("v%d ", version)
		for _, c := range conclusions {
			out += fmt.Sprintf("%s:sla=%v ", c.RunID, c.SLA.OK)
		}
		out += "alerts=[" + transitionSeq(transitions) + "]"
		return out
	}

	underV1 := evaluateUnder(1)
	underV2 := evaluateUnder(2)
	t.Logf("HISTORICAL: %s", underV1)
	t.Logf("EVOLVED   : %s", underV2)
	if underV1 == underV2 {
		t.Fatal("v1 replay must differ from v2 after rule tightening")
	}
	// v1: run-1/run-4 pass, run-2/run-3 breach; v2: all four breach.
	// Reproducibility: running v1 twice gives the same answer.
	if evaluateUnder(1) != underV1 {
		t.Fatal("historical v1 result not reproducible")
	}
	t.Logf("decision basis: pinned versions; v1 history preserved while v2 changes future interpretation")
}
