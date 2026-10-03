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
	"github.com/h3yow/pipeline-observability-go/internal/ingest"
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

// TestUnknownRunNoMisjudgmentNoAlert: a run whose started event never
// arrived must be judged UNKNOWN (not pass/fail), must raise no alert and
// must not touch debounce counters — while complete runs of the same job
// behave exactly as before.
func TestUnknownRunNoMisjudgmentNoAlert(t *testing.T) {
	e := New(latenessWindow)
	registerV1(t, e)

	d := 21
	evs := []event.Envelope{
		started("u-s1", runTime(d, 9, 0), "run-1", "job-a"),
		finished("u-f1", runTime(d, 9, 5), "run-1", "job-a", "SUCCESS"),
		// run-2: started event lost upstream; only the finish arrives.
		finished("u-f2", runTime(d, 10, 15), "run-2", "job-a", "FAILED"),
		started("u-s3", runTime(d, 11, 0), "run-3", "job-a"),
		finished("u-f3", runTime(d, 11, 20), "run-3", "job-a", "FAILED"),
	}
	for _, ev := range evs {
		t.Logf("EVENT id=%s run=%s type=%s at=%s", ev.EventID, ev.RunID, ev.Type, ev.OccurredAt.Format("15:04"))
	}
	feedShuffledConcurrent(t, e, evs, 1)

	conclusions, transitions, err := e.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	if len(conclusions) != 3 {
		t.Fatalf("want 3 conclusions, got %d", len(conclusions))
	}
	var unknown, determinable int
	for _, c := range conclusions {
		t.Logf("CONCLUSION run=%s determinable=%v gaps=%+v | freshness=%s | sla=%s",
			c.RunID, c.Determinable, c.Gaps, c.Freshness.Reason, c.SLA.Reason)
		if c.RunID == "run-2" {
			unknown++
			if c.Determinable || c.Freshness.Status != model.StatusUnknown || c.SLA.Status != model.StatusUnknown {
				t.Fatalf("run-2 must be UNKNOWN, got %+v", c)
			}
			if len(c.Breaches) != 0 {
				t.Fatalf("UNKNOWN run must not breach, got %v", c.Breaches)
			}
			if len(c.Gaps) != 1 || c.Gaps[0].Cause != model.GapCauseNotArrived {
				t.Fatalf("run-2 gap must be NOT_ARRIVED, got %+v", c.Gaps)
			}
		} else {
			determinable++
			if !c.Determinable {
				t.Fatalf("run %s has full basis and must be determinable", c.RunID)
			}
		}
	}
	if unknown != 1 || determinable != 2 {
		t.Fatalf("want 1 unknown + 2 determinable, got %d/%d", unknown, determinable)
	}
	// No alert at all: run-3's single SLA breach stays below FireAfterN=2;
	// the unknown run-2 must not count as a breach (nor as a pass reset).
	if len(transitions) != 0 {
		t.Fatalf("unknown run must not trigger alerts, got %v", transitionSeq(transitions))
	}
	if st, ok := e.AlertState("job-a", model.KindSLA); ok && st != model.AlertStateOK {
		t.Fatalf("alert state must be OK, got %s", st)
	}

	// Pending view lists run-2 with its gap cause.
	pending, next, err := e.PendingRuns("", 10)
	if err != nil {
		t.Fatal(err)
	}
	if next != "" || len(pending) != 1 || pending[0].RunID != "run-2" {
		t.Fatalf("pending view wrong: %+v next=%q", pending, next)
	}
	t.Logf("PENDING run=%s gaps=%+v", pending[0].RunID, pending[0].Gaps)
	if pending[0].Gaps[0].Cause != model.GapCauseNotArrived {
		t.Fatalf("pending cause = %s, want NOT_ARRIVED", pending[0].Gaps[0].Cause)
	}

	// Re-evaluate is idempotent: unchanged pending conclusion not re-reported.
	again, more, err := e.Evaluate()
	if err != nil || len(again) != 0 || len(more) != 0 {
		t.Fatalf("re-evaluation must be idempotent: %d conclusions %d alerts", len(again), len(more))
	}
	t.Logf("decision basis: run-2 UNKNOWN (start NOT_ARRIVED), no alerts; complete runs judged normally")
}

// conclusionCanon strips wall-clock fields so two engines' conclusions can
// be compared for equality of judgement.
func conclusionCanon(cs []model.Conclusion) string {
	out := ""
	for _, c := range cs {
		out += fmt.Sprintf("%s|v%d|det=%v|f=%s|s=%s|fr=%s|sr=%s|gaps=%v\n",
			c.RunID, c.RuleVer, c.Determinable, c.Freshness.Status, c.SLA.Status,
			c.Freshness.Reason, c.SLA.Reason, c.Gaps)
	}
	return out
}

// transitionCanon strips the emission seq so alert histories can be
// compared by content (kind, direction, reason, evidence, timestamp).
func transitionCanon(ts []alert.Transition) string {
	out := ""
	for _, tr := range ts {
		out += fmt.Sprintf("%s|%s->%s|%s|%s|%d\n",
			tr.Alert.Kind, tr.FromState, tr.ToState, tr.Alert.Reason, tr.Evidence,
			tr.Alert.Timestamp.UnixNano())
	}
	return out
}

// TestBackfillEqualsNeverLostPath: engine A loses run-2's started event
// and later gets it back via explicit backfill; engine B receives
// everything in order. After the backfill + re-evaluation, A's
// conclusions and alert history must equal B's exactly.
func TestBackfillEqualsNeverLostPath(t *testing.T) {
	d := 22
	fullSet := func() []event.Envelope {
		return []event.Envelope{
			started("b-s1", runTime(d, 9, 0), "run-1", "job-a"),
			finished("b-f1", runTime(d, 9, 5), "run-1", "job-a", "SUCCESS"),
			started("b-s2", runTime(d, 10, 0), "run-2", "job-a"),
			finished("b-f2", runTime(d, 10, 15), "run-2", "job-a", "FAILED"),
			started("b-s3", runTime(d, 11, 0), "run-3", "job-a"),
			finished("b-f3", runTime(d, 11, 20), "run-3", "job-a", "FAILED"),
			started("b-s4", runTime(d, 12, 0), "run-4", "job-a"),
			finished("b-f4", runTime(d, 12, 5), "run-4", "job-a", "SUCCESS"),
		}
	}

	// Engine B: the reference path — nothing lost, evaluated in one shot.
	ref := New(latenessWindow)
	registerV1(t, ref)
	feedShuffledConcurrent(t, ref, fullSet(), 1)
	refConcl, _, err := ref.Evaluate()
	if err != nil {
		t.Fatal(err)
	}

	// Engine A: run-2's started event never arrives.
	a := New(latenessWindow)
	registerV1(t, a)
	var partial []event.Envelope
	for _, ev := range fullSet() {
		if ev.EventID != "b-s2" {
			partial = append(partial, ev)
		}
	}
	feedShuffledConcurrent(t, a, partial, 1)
	firstConcl, firstTrans, err := a.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("before backfill: %d conclusions, transitions=%q", len(firstConcl), transitionSeq(firstTrans))
	if len(firstTrans) != 0 {
		t.Fatalf("with run-2 unknown, no alert may fire yet, got %s", transitionSeq(firstTrans))
	}

	// Backfill the recovered started event (e.g. from an archive), twice,
	// concurrently with a duplicate — must apply exactly once.
	lost := started("b-s2", runTime(d, 10, 0), "run-2", "job-a")
	var wg sync.WaitGroup
	var mu sync.Mutex
	disps := map[string]int{}
	start := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r := a.BackfillEvent(context.Background(), lost)
			mu.Lock()
			disps[string(r.Disposition)]++
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	t.Logf("BACKFILL dispositions: %v", disps)
	if disps["BACKFILLED"] != 1 || disps["DUPLICATE"] != 3 {
		t.Fatalf("backfill must apply exactly once, got %v", disps)
	}

	secondConcl, secondTrans, err := a.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range secondConcl {
		t.Logf("CONCLUSION after backfill run=%s det=%v freshness=%s sla=%s",
			c.RunID, c.Determinable, c.Freshness.Reason, c.SLA.Reason)
	}
	for _, tr := range secondTrans {
		t.Logf("ALERT after backfill %s %s->%s seq=%d reason=%q",
			tr.Alert.Kind, tr.FromState, tr.ToState, tr.Alert.Seq, tr.Alert.Reason)
	}

	// A must now have judged run-2 determinably; pending view is empty.
	if len(secondConcl) != 1 || secondConcl[0].RunID != "run-2" || !secondConcl[0].Determinable {
		t.Fatalf("backfilled run-2 must be re-judged determinably, got %+v", secondConcl)
	}
	pending, _, err := a.PendingRuns("", 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending view must be empty after backfill, got %+v", pending)
	}

	// Equality with the never-lost path: same conclusions per run, same
	// alert history (content-wise).
	var aAll []model.Conclusion
	for _, c := range firstConcl {
		if c.Determinable { // skip the UNKNOWN run-2; its re-judged form comes below
			aAll = append(aAll, c)
		}
	}
	aAll = append(aAll, secondConcl[0]) // re-judged run-2
	sortConclusions(aAll)
	if got, want := conclusionCanon(aAll), conclusionCanon(refConcl); got != want {
		t.Fatalf("conclusions differ from never-lost path:\nbackfilled:\n%s\nreference:\n%s", got, want)
	}
	if got, want := transitionCanon(a.AlertEvents()), transitionCanon(ref.AlertEvents()); got != want {
		t.Fatalf("alert history differs from never-lost path:\nbackfilled:\n%s\nreference:\n%s", got, want)
	}
	t.Logf("decision basis: post-backfill conclusions and alert history identical to never-lost path:\n%s",
		transitionCanon(a.AlertEvents()))
}

func sortConclusions(cs []model.Conclusion) {
	for i := 1; i < len(cs); i++ {
		for j := i; j > 0 && cs[j-1].RunID > cs[j].RunID; j-- {
			cs[j-1], cs[j] = cs[j], cs[j-1]
		}
	}
}

// TestLateDropThenBackfill: the started event is dropped by the lateness
// watermark; the pending view explains the gap as DROPPED_LATE, and an
// explicit backfill resolves the run to the same judgement the never-lost
// path produces.
func TestLateDropThenBackfill(t *testing.T) {
	d := 23
	e := New(int64(time.Minute)) // 1m lateness: the 09:00 start is way out of window
	registerV1(t, e)

	// run-1's finish applies while the watermark is still low.
	fin := finished("l-f1", runTime(d, 9, 5), "run-1", "job-a", "SUCCESS")
	if rec := e.IngestEvent(context.Background(), fin); !rec.Accepted {
		t.Fatalf("run-1 finish must be accepted, got %s", rec.Disposition)
	}
	t.Logf("INPUT l-f1 at=%s type=%s run=run-1 => APPLIED", runTime(d, 9, 5).Format("15:04"), event.TypeFinished)
	// A much later run advances the watermark far beyond run-1's start. Feed
	// start-before-finish SEQUENTIALLY on purpose: this pair only exists to
	// move the watermark, and feeding it concurrently would make its own
	// acceptance race the (advancing) watermark — non-deterministic and
	// unrelated to the late-drop behavior under test.
	for _, ev := range []event.Envelope{
		started("l-s9", runTime(d, 20, 0), "run-9", "job-a"),
		finished("l-f9", runTime(d, 20, 5), "run-9", "job-a", "SUCCESS"),
	} {
		rec := e.IngestEvent(context.Background(), ev)
		t.Logf("INPUT %s at=%s type=%s run=%s => %s", ev.EventID,
			ev.OccurredAt.Format("15:04"), ev.Type, ev.RunID, rec.Disposition)
		if !rec.Accepted {
			t.Fatalf("watermark-advancing event %s must be accepted, got %s (%s)",
				ev.EventID, rec.Disposition, rec.Reason)
		}
	}
	// The lost start now arrives and is dropped LATE.
	lateStart := started("l-s1", runTime(d, 9, 0), "run-1", "job-a")
	r1 := e.IngestEvent(context.Background(), lateStart)
	t.Logf("INPUT late start at=%s => %s (%s)", runTime(d, 9, 0).Format("15:04"), r1.Disposition, r1.Reason)
	if r1.Disposition != ingest.DispositionLate {
		t.Fatalf("start must be LATE, got %s", r1.Disposition)
	}

	conclusions, _, err := e.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	var pending []PendingRun
	pending, _, err = e.PendingRuns("", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].RunID != "run-1" {
		t.Fatalf("run-1 must be pending, got %+v", pending)
	}
	t.Logf("PENDING run=%s cause=%s detail=%s", pending[0].RunID, pending[0].Gaps[0].Cause, pending[0].Gaps[0].Detail)
	if pending[0].Gaps[0].Cause != model.GapCauseDroppedLate {
		t.Fatalf("cause = %s, want DROPPED_LATE", pending[0].Gaps[0].Cause)
	}
	var unknownSeen bool
	for _, c := range conclusions {
		if c.RunID == "run-1" {
			unknownSeen = !c.Determinable
		}
	}
	if !unknownSeen {
		t.Fatal("run-1 conclusion must be UNKNOWN")
	}

	// Explicit backfill of the very same dropped event resolves the run.
	rb := e.BackfillEvent(context.Background(), lateStart)
	t.Logf("BACKFILL => %s (%s)", rb.Disposition, rb.Reason)
	if !rb.Accepted {
		t.Fatalf("backfill of LATE-dropped event must be accepted, got %+v", rb)
	}
	second, _, err := e.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].RunID != "run-1" || !second[0].Determinable {
		t.Fatalf("run-1 must be re-judged determinably, got %+v", second)
	}
	if !second[0].SLA.OK || second[0].SLA.Actual != 5*time.Minute {
		t.Fatalf("run-1 must judge as 5m within SLA, got %+v", second[0].SLA)
	}
	if p, _, _ := e.PendingRuns("", 10); len(p) != 0 {
		t.Fatalf("pending must be empty after backfill, got %+v", p)
	}
	t.Logf("decision basis: DROPPED_LATE explained, backfill resolved run-1 to duration=5m WITHIN_SLA")
}

// TestLateArrivalResolvesPendingWithoutBackfill: when the missing start
// arrives late but inside the lateness window (normal out-of-order
// delivery), the next Evaluate re-judges the run without any backfill.
func TestLateArrivalResolvesPendingWithoutBackfill(t *testing.T) {
	d := 24
	e := New(latenessWindow) // 6h window covers the run span
	registerV1(t, e)

	feedShuffledConcurrent(t, e, []event.Envelope{
		finished("o-f1", runTime(d, 9, 5), "run-1", "job-a", "SUCCESS"),
	}, 0)
	first, _, err := e.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].Determinable {
		t.Fatalf("run-1 must start out UNKNOWN, got %+v", first)
	}
	t.Logf("before late arrival: run-1 UNKNOWN (gaps=%+v)", first[0].Gaps)

	// The started event arrives late (within window): normal ingest path.
	r := e.IngestEvent(context.Background(), started("o-s1", runTime(d, 9, 0), "run-1", "job-a"))
	t.Logf("late start ingest => %s", r.Disposition)
	second, _, err := e.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || !second[0].Determinable || second[0].SLA.Actual != 5*time.Minute {
		t.Fatalf("run-1 must resolve to a 5m determinable run, got %+v", second)
	}
	if p, _, _ := e.PendingRuns("", 10); len(p) != 0 {
		t.Fatalf("pending must be empty, got %+v", p)
	}
	t.Logf("decision basis: in-window late arrival resolves pending run without backfill")
}

// TestPendingViewPagination: many pending runs are listed stably through
// cursor pages, and the set is identical under shuffled/concurrent feeds.
func TestPendingViewPagination(t *testing.T) {
	build := func() *Engine {
		e := New(latenessWindow)
		registerV1(t, e)
		d := 25
		var evs []event.Envelope
		for i := 0; i < 7; i++ {
			run := fmt.Sprintf("run-%d", i)
			// Only finishes: every run is missing its start.
			evs = append(evs, finished(fmt.Sprintf("p-f%d", i), runTime(d, 9+i, 0), run, "job-a", "SUCCESS"))
		}
		feedShuffledConcurrent(t, e, evs, 1)
		if _, _, err := e.Evaluate(); err != nil {
			t.Fatal(err)
		}
		return e
	}

	collect := func(e *Engine, pageSize int) []string {
		var ids []string
		cursor := ""
		for {
			page, next, err := e.PendingRuns(cursor, pageSize)
			if err != nil {
				t.Fatal(err)
			}
			for _, p := range page {
				ids = append(ids, string(p.RunID))
			}
			if next == "" {
				return ids
			}
			cursor = next
		}
	}

	e := build()
	page1, next, err := e.PendingRuns("", 3)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("page1: %v hasNext=%v", runIDs(page1), next != "")
	if len(page1) != 3 || next == "" {
		t.Fatalf("first page must hold 3 entries with a cursor, got %d", len(page1))
	}
	ids3 := collect(e, 3)
	idsAll := collect(e, 1000)
	t.Logf("paginated(3): %v", ids3)
	t.Logf("single page : %v", idsAll)
	if fmt.Sprint(ids3) != fmt.Sprint(idsAll) {
		t.Fatalf("pagination must not change the set/order:\n%v\n%v", ids3, idsAll)
	}
	if len(idsAll) != 7 {
		t.Fatalf("want 7 pending runs, got %d", len(idsAll))
	}
	// Stability: a fresh engine fed the same shuffled/duplicated stream
	// yields the identical view.
	other := collect(build(), 3)
	if fmt.Sprint(other) != fmt.Sprint(ids3) {
		t.Fatalf("pending view not stable across chaos feeds:\n%v\n%v", ids3, other)
	}
	// Invalid cursor is an explicit error.
	if _, _, err := e.PendingRuns("!!not-a-cursor!!", 3); err == nil {
		t.Fatal("invalid cursor must be rejected")
	}
	t.Logf("decision basis: stable paginated pending view, identical across chaos feeds: %v", idsAll)
}

func runIDs(ps []PendingRun) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		out = append(out, string(p.RunID))
	}
	return out
}

// TestConcurrentBackfillAndEvaluateStable: concurrent duplicate backfills
// interleaved with concurrent Evaluate calls produce exactly one
// conclusion for the resolved run and an alert history with no
// duplicates.
func TestConcurrentBackfillAndEvaluateStable(t *testing.T) {
	d := 26
	e := New(latenessWindow)
	registerV1(t, e)

	feedShuffledConcurrent(t, e, []event.Envelope{
		started("c-s1", runTime(d, 9, 0), "run-1", "job-a"),
		finished("c-f1", runTime(d, 9, 20), "run-1", "job-a", "FAILED"), // breach #1
		// run-2 start missing; its finish is a second breach once resolved.
		finished("c-f2", runTime(d, 10, 20), "run-2", "job-a", "FAILED"),
	}, 1)
	if _, _, err := e.Evaluate(); err != nil {
		t.Fatal(err)
	}

	lost := started("c-s2", runTime(d, 10, 0), "run-2", "job-a")
	var wg sync.WaitGroup
	start := make(chan struct{})
	var mu sync.Mutex
	backfillDisp := map[string]int{}
	conclCount := 0
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			r := e.BackfillEvent(context.Background(), lost)
			mu.Lock()
			backfillDisp[string(r.Disposition)]++
			mu.Unlock()
		}()
		go func() {
			defer wg.Done()
			<-start
			cs, _, err := e.Evaluate()
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			for _, c := range cs {
				if c.RunID == "run-2" && c.Determinable {
					conclCount++
				}
			}
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	t.Logf("backfill dispositions=%v determinable run-2 conclusions observed=%d", backfillDisp, conclCount)
	if backfillDisp["BACKFILLED"] != 1 {
		t.Fatalf("backfill must apply exactly once, got %v", backfillDisp)
	}
	if conclCount > 1 {
		t.Fatalf("run-2 must be re-judged determinably at most once, got %d", conclCount)
	}

	// Settle: one final Evaluate; run-2 resolved exactly once overall,
	// alert history has exactly one FIRE (two consecutive breaches) and
	// no duplicate transitions.
	finalConcl, _, err := e.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	resolved := conclCount
	for _, c := range finalConcl {
		if c.RunID == "run-2" && c.Determinable {
			resolved++
		}
	}
	if resolved != 1 {
		t.Fatalf("run-2 determinable conclusion must appear exactly once, got %d", resolved)
	}
	events := e.AlertEvents()
	t.Logf("alert history: %s", transitionSeq(events))
	if len(events) != 1 || events[0].ToState != model.AlertStateFiring {
		t.Fatalf("exactly one FIRE expected (breaches run-1,run-2), got %s", transitionSeq(events))
	}
	seqs := map[int64]bool{}
	for _, tr := range events {
		if seqs[tr.Alert.Seq] {
			t.Fatalf("duplicate alert seq %d", tr.Alert.Seq)
		}
		seqs[tr.Alert.Seq] = true
	}
	if p, _, _ := e.PendingRuns("", 10); len(p) != 0 {
		t.Fatalf("pending must be empty, got %+v", p)
	}
	t.Logf("decision basis: concurrent backfill+evaluate stable; single FIRE seq=%d", events[0].Alert.Seq)
}

// TestBackfillUnderPinnedHistoricalVersion: a job pinned to rule v1 keeps
// judging backfilled runs under v1 even after a tighter v2 is registered;
// the v1 conclusions remain reproducible.
func TestBackfillUnderPinnedHistoricalVersion(t *testing.T) {
	d := 27
	mkEngine := func() *Engine {
		e := New(latenessWindow)
		registerV1(t, e) // v1: SLA 10m
		if err := e.BindRule("job-a", "job-a", 1); err != nil {
			t.Fatal(err)
		}
		// v2 registered afterwards: tighter SLA (3m). Must not affect job-a.
		if err := e.Rules().Register(rule.Set{
			RuleID: "job-a", Version: 2,
			FreshnessDeadline: 30 * time.Minute, SLADuration: 3 * time.Minute,
			FireAfterN: 1, RecoverAfterN: 1,
		}); err != nil {
			t.Fatal(err)
		}
		return e
	}
	fin := finished("h-f1", runTime(d, 9, 5), "run-1", "job-a", "SUCCESS") // 5m duration
	sta := started("h-s1", runTime(d, 9, 0), "run-1", "job-a")

	verdictOf := func(e *Engine) string {
		e.IngestEvent(context.Background(), fin)
		if _, _, err := e.Evaluate(); err != nil { // UNKNOWN first
			t.Fatal(err)
		}
		e.BackfillEvent(context.Background(), sta)
		cs, _, err := e.Evaluate()
		if err != nil {
			t.Fatal(err)
		}
		if len(cs) != 1 || !cs[0].Determinable {
			t.Fatalf("run-1 must resolve determinably, got %+v", cs)
		}
		t.Logf("backfilled run-1 under rule v%d: %s", cs[0].RuleVer, cs[0].SLA.Reason)
		return fmt.Sprintf("v%d:%s:%v", cs[0].RuleVer, cs[0].SLA.Status, cs[0].SLA.OK)
	}

	first := verdictOf(mkEngine())
	second := verdictOf(mkEngine())
	if first != "v1:PASS:true" {
		t.Fatalf("backfilled run must be judged under pinned v1 as WITHIN_SLA, got %s", first)
	}
	if second != first {
		t.Fatalf("historical v1 conclusion not reproducible: %s vs %s", first, second)
	}
	t.Logf("decision basis: pinned v1 governs backfilled runs; v2 registered later has no effect; replay stable (%s)", first)
}
