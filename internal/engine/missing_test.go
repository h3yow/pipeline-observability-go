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

var fixedEvalTime = time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)

func logInputs(t *testing.T, evs []event.Envelope) {
	t.Helper()
	for _, ev := range evs {
		t.Logf("INPUT event=%s run=%s type=%s at=%s",
			ev.EventID, ev.RunID, ev.Type, ev.OccurredAt.Format("15:04"))
	}
}

// TestUnknownRunNotJudgedAndNeverAlerts: runs arriving as finish-only yield
// UNKNOWN conclusions, no alert transitions, and must not consume fire
// debounce slots — a later determinable breach still needs the configured
// number of consecutive breaches before firing.
func TestUnknownRunNotJudgedAndNeverAlerts(t *testing.T) {
	e := New(latenessWindow)
	registerV1(t, e) // fire SLA after 2 breaches

	// Three finish-only runs (all unknown), then two determinable breaches.
	evs := []event.Envelope{
		finished("uf1", runTime(20, 9, 5), "u-run-1", "job-a", "FAILED"),
		finished("uf2", runTime(20, 10, 5), "u-run-2", "job-a", "FAILED"),
		finished("uf3", runTime(20, 11, 5), "u-run-3", "job-a", "FAILED"),
		started("bs1", runTime(20, 12, 0), "b-run-1", "job-a"),
		finished("bf1", runTime(20, 12, 15), "b-run-1", "job-a", "FAILED"), // breach #1
		started("bs2", runTime(20, 13, 0), "b-run-2", "job-a"),
		finished("bf2", runTime(20, 13, 15), "b-run-2", "job-a", "FAILED"), // breach #2 => FIRE
	}
	logInputs(t, evs)
	for _, ev := range evs {
		e.IngestEvent(context.Background(), ev)
	}

	conclusions, transitions, err := e.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	unknownByRun := map[string]bool{}
	var determinable int
	for _, c := range conclusions {
		if c.IsUnknown() {
			unknownByRun[string(c.RunID)] = true
			if c.Freshness.Status != model.VerdictUnknown || c.SLA.Status != model.VerdictUnknown ||
				len(c.Breaches) != 0 || len(c.MissingBasis) != 1 ||
				c.MissingBasis[0].Code != model.GapNotArrived {
				t.Fatalf("bad unknown conclusion for %s: %+v", c.RunID, c)
			}
			t.Logf("UNKNOWN CONCLUSION run=%s gap=%s evidence=%q | %s",
				c.RunID, c.MissingBasis[0].Code, c.MissingBasis[0].Detail, c.SLA.Reason)
		} else {
			determinable++
			t.Logf("CONCLUSION run=%s freshness=%s sla=%s",
				c.RunID, c.Freshness.Status, c.SLA.Status)
		}
	}
	if len(unknownByRun) != 3 || determinable != 2 {
		t.Fatalf("want 3 unknown + 2 determinable, got %d unknown %d determinable",
			len(unknownByRun), determinable)
	}
	// Exactly one SLA FIRE (the two real breaches), nothing from unknowns.
	var slaFire int
	for _, tr := range transitions {
		if tr.Alert.Kind == model.KindSLA && tr.ToState == model.AlertStateFiring {
			slaFire++
		}
	}
	if slaFire != 1 {
		t.Fatalf("unknown runs polluted debounce: transitions=%s", transitionSeq(transitions))
	}
	t.Logf("DECISION alert transitions after unknowns+breaches: %s", transitionSeq(transitions))

	// Pending view lists exactly the three unknown runs.
	pending := e.PendingRuns()
	if len(pending) != 3 {
		t.Fatalf("pending view = %d rows, want 3: %+v", len(pending), pending)
	}
	for _, p := range pending {
		t.Logf("PENDING run=%s job=%s finished=%s gap=%s",
			p.RunID, p.JobID, p.FinishedAt.Format("15:04"), p.Gap.Code)
	}

	// Re-evaluate: no new conclusions (unknown signature stable), no alerts.
	again, more, err := e.Evaluate()
	if err != nil || len(again) != 0 || len(more) != 0 {
		t.Fatalf("re-evaluate must be quiescent: %d %d", len(again), len(more))
	}
}

func renderTransitions(trs []alert.Transition) string {
	out := ""
	for _, tr := range trs {
		out += fmt.Sprintf("%s|%s|%s->%s|run=%s|#%d|%s||",
			tr.Alert.JobID, tr.Alert.Kind, tr.FromState, tr.ToState,
			tr.RunID, tr.Alert.Seq, tr.Alert.Reason)
	}
	return out
}

// backfillScenarioEvents: 4 job-a runs. run-2's start is "lost". On the
// normal (reference) path all 8 events are present from the start; on the
// lossy path s2 is first rejected by the watermark and later backfilled.
func backfillScenarioEvents() (all, lossy []event.Envelope, s2 event.Envelope) {
	d := 21
	s2 = started("s2", runTime(d, 10, 0), "run-2", "job-a")
	all = []event.Envelope{
		started("s1", runTime(d, 9, 0), "run-1", "job-a"),
		finished("f1", runTime(d, 9, 5), "run-1", "job-a", "SUCCESS"),
		s2,
		finished("f2", runTime(d, 10, 15), "run-2", "job-a", "FAILED"),
		started("s3", runTime(d, 11, 0), "run-3", "job-a"),
		finished("f3", runTime(d, 11, 20), "run-3", "job-a", "FAILED"),
		started("s4", runTime(d, 12, 0), "run-4", "job-a"),
		finished("f4", runTime(d, 12, 5), "run-4", "job-a", "SUCCESS"),
	}
	for _, ev := range all {
		if ev.EventID == "s2" {
			continue
		}
		lossy = append(lossy, ev)
	}
	return all, lossy, s2
}

// TestBackfillParityWithNormalPath: after a late start is recovered via
// BackfillEvent, conclusions and the COMPLETE alert history must be
// byte-identical to an engine where that event was never lost.
func TestBackfillParityWithNormalPath(t *testing.T) {
	// Reference: all events present, shuffled+duplicated concurrent feed.
	ref := New(latenessWindow)
	ref.clock = func() time.Time { return fixedEvalTime }
	registerV1(t, ref)
	all, _, s2 := backfillScenarioEvents()
	logInputs(t, all)
	feedShuffledConcurrent(t, ref, all, 1)
	if _, _, err := ref.Evaluate(); err != nil {
		t.Fatal(err)
	}

	// Lossy: tight window so s2 (10:00) is rejected when delivered after the
	// 12:05 events; run-2 stays UNKNOWN. Then explicitly backfill s2.
	// Lossy: a 2h window and a delivery order whose final watermark (10:05)
	// rejects s2 (10:00) but accepts f2 (10:15) and all other runs.
	loss := New(int64(2 * time.Hour))
	loss.clock = func() time.Time { return fixedEvalTime }
	registerV1(t, loss)
	d := 21
	// Earlier runs first, then the finish-only run-2, then later anchors;
	// finally the lost start arrives and is rejected by the watermark.
	ordered := []event.Envelope{
		started("s1", runTime(d, 9, 0), "run-1", "job-a"),
		finished("f1", runTime(d, 9, 5), "run-1", "job-a", "SUCCESS"),
		started("s3", runTime(d, 11, 0), "run-3", "job-a"),
		finished("f3", runTime(d, 11, 20), "run-3", "job-a", "FAILED"),
		finished("f2", runTime(d, 10, 15), "run-2", "job-a", "FAILED"),
		started("s4", runTime(d, 12, 0), "run-4", "job-a"),
		finished("f4", runTime(d, 12, 5), "run-4", "job-a", "SUCCESS"),
	}
	for _, ev := range ordered {
		rec := loss.IngestEvent(context.Background(), ev)
		t.Logf("LOSSY INGEST %s -> %s %s", ev.EventID, rec.Disposition, rec.Reason)
	}
	// The lost start now arrives through normal intake: rejected by watermark.
	rec := loss.IngestEvent(context.Background(), s2)
	t.Logf("LOSSY late start -> %s %s", rec.Disposition, rec.Reason)
	if rec.Disposition != ingest.DispositionLate {
		t.Fatalf("seed: want LATE rejection, got %s", rec.Disposition)
	}

	before, beforeTrans, err := loss.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	var unknownRun *model.Conclusion
	for i := range before {
		if before[i].RunID == "run-2" {
			unknownRun = &before[i]
		}
	}
	if unknownRun == nil || !unknownRun.IsUnknown() ||
		unknownRun.MissingBasis[0].Code != model.GapDroppedLate {
		t.Fatalf("run-2 must be UNKNOWN/START_DROPPED_LATE pre-backfill: %+v", unknownRun)
	}
	t.Logf("PRE-BACKFILL run-2: %s evidence=%s; alerts so far=%s",
		unknownRun.SLA.Status, unknownRun.MissingBasis[0].EvidenceEventID,
		renderTransitions(beforeTrans))
	if len(loss.PendingRuns()) != 1 || loss.PendingRuns()[0].RunID != "run-2" {
		t.Fatalf("pending view must list run-2: %+v", loss.PendingRuns())
	}

	// Recover the start from archive/manual repair.
	bf := loss.BackfillEvent(context.Background(), s2)
	t.Logf("BACKFILL s2 -> %s %s", bf.Disposition, bf.Reason)
	if bf.Disposition != ingest.DispositionBackfilled || !bf.Accepted {
		t.Fatalf("want BACKFILLED, got %s", bf.Disposition)
	}
	postConclusions, _, err := loss.Evaluate()
	if err != nil {
		t.Fatal(err)
	}

	// run-2 now has a determinable conclusion; compare full conclusion sets.
	byRun := map[string]model.Conclusion{}
	for _, c := range postConclusions {
		byRun[string(c.RunID)] = c
	}
	c2, ok := byRun["run-2"]
	if !ok || !c2.Determinable || c2.SLA.Status != model.VerdictFail ||
		c2.SLA.Actual != 15*time.Minute {
		t.Fatalf("post-backfill run-2 wrong: %+v", c2)
	}
	t.Logf("POST-BACKFILL run-2: freshness=%s sla=%s actual=%s",
		c2.Freshness.Status, c2.SLA.Status, c2.SLA.Actual)

	// Full final state comparison: every run's final determinable conclusion
	// (re-evaluated after backfill) and the complete alert transition history
	// must match the reference engine byte-for-byte.
	lossFinal, lossAlertLog := finalConclusions(t, loss)
	refFinal, refAlertLog := finalConclusions(t, ref)
	if lossFinal != refFinal {
		t.Fatalf("conclusion picture differs after backfill:\nloss: %s\nref:  %s",
			lossFinal, refFinal)
	}
	if lossAlertLog != refAlertLog {
		t.Fatalf("alert history differs after backfill:\nloss: %s\nref:  %s",
			lossAlertLog, refAlertLog)
	}
	t.Logf("PARITY conclusions=%s", lossFinal)
	t.Logf("PARITY alerts=%s", lossAlertLog)

	// Another Evaluate must be quiescent (no duplicate alerts/conclusions).
	again, more, err := loss.Evaluate()
	if err != nil || len(again) != 0 || len(more) != 0 {
		t.Fatalf("post-backfill re-evaluate not quiescent: %d %d", len(again), len(more))
	}
}

// finalConclusions forces a no-op Evaluate and renders a stable final-state
// signature from run aggregates (the source the conclusions are derived
// from) plus the complete alert transition history. Both are comparable
// across the normal and backfilled engines.
func finalConclusions(t *testing.T, e *Engine) (string, string) {
	t.Helper()
	if _, _, err := e.Evaluate(); err != nil {
		t.Fatal(err)
	}
	pic := ""
	for _, r := range e.Runs() {
		pic += fmt.Sprintf("%s{start=%s fin=%s hS=%t hF=%t gap=%v ev=%v} ",
			r.RunID, r.StartedAt.Format("15:04"), r.FinishedAt.Format("15:04"),
			r.HasStarted, r.HasFinished, r.BasisGap, r.AppliedEventIDs)
	}
	return pic, renderTransitions(e.AlertEvents())
}

// TestConcurrentBackfillNoDuplicates: when the recovered start is injected
// from many goroutines simultaneously (and repeatedly), exactly one is
// accepted and the final alert history contains no duplicate transitions.
func TestConcurrentBackfillNoDuplicates(t *testing.T) {
	build := func() *Engine {
		d := 22
		e := New(int64(2 * time.Hour))
		e.clock = func() time.Time { return fixedEvalTime }
		registerV1(t, e)
		ordered := []event.Envelope{
			started("s1", runTime(d, 9, 0), "run-1", "job-a"),
			finished("f1", runTime(d, 9, 5), "run-1", "job-a", "SUCCESS"),
			started("s3", runTime(d, 11, 0), "run-3", "job-a"),
			finished("f3", runTime(d, 11, 20), "run-3", "job-a", "FAILED"),
			finished("f2", runTime(d, 10, 15), "run-2", "job-a", "FAILED"),
			started("s4", runTime(d, 12, 0), "run-4", "job-a"),
			finished("f4", runTime(d, 12, 5), "run-4", "job-a", "SUCCESS"),
		}
		for _, ev := range ordered {
			e.IngestEvent(context.Background(), ev)
		}
		s2 := started("s2", runTime(d, 10, 0), "run-2", "job-a")
		if r := e.IngestEvent(context.Background(), s2); r.Disposition != ingest.DispositionLate {
			t.Fatalf("seed late rejection = %s", r.Disposition)
		}
		return e
	}

	// Reference signature: a single, ordered backfill.
	ref := build()
	if r := ref.BackfillEvent(context.Background(),
		started("s2", runTime(22, 10, 0), "run-2", "job-a")); r.Disposition != ingest.DispositionBackfilled {
		t.Fatalf("ref backfill = %s", r.Disposition)
	}
	if _, tr, err := ref.Evaluate(); err != nil || len(tr) != 2 {
		t.Fatalf("ref: want 2 transitions, got %d (err=%v)", len(tr), err)
	}
	wantLog := renderTransitions(ref.AlertEvents())

	// Concurrent + repeated backfills of the same event.
	e := build()
	s2 := started("s2", runTime(22, 10, 0), "run-2", "job-a")
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	disps := map[ingest.Disposition]int{}
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r := e.BackfillEvent(context.Background(), s2)
			mu.Lock()
			disps[r.Disposition]++
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	t.Logf("INPUT 12 concurrent duplicate backfills -> dispositions=%v", disps)
	if disps[ingest.DispositionBackfilled] != 1 || disps[ingest.DispositionDuplicate] != 11 {
		t.Fatalf("exactly one backfill must apply: %v", disps)
	}
	if _, tr, err := e.Evaluate(); err != nil || len(tr) != 2 {
		t.Fatalf("concurrent backfill: want 2 transitions total, got %d (err=%v)", len(tr), err)
	}
	gotLog := renderTransitions(e.AlertEvents())
	if gotLog != wantLog {
		t.Fatalf("concurrent backfill history differs:\n%s\n%s", gotLog, wantLog)
	}
	// Idempotent re-evaluate: no extra alerts.
	if _, more, err := e.Evaluate(); err != nil || len(more) != 0 {
		t.Fatalf("re-evaluate leaked alerts: %d", len(more))
	}
	t.Logf("DECISION unique alert history after concurrent backfill: %s", gotLog)
}

// TestOutOfOrderBackfillSameAsInOrder: the missing start recovered via
// Backfill under a shuffled/duplicated concurrent feed yields the same final
// picture as sequential backfill.
func TestOutOfOrderBackfillSameAsInOrder(t *testing.T) {
	d := 22
	s2 := started("s2", runTime(d, 10, 0), "run-2", "job-a")
	base := []event.Envelope{
		started("s1", runTime(d, 9, 0), "run-1", "job-a"),
		finished("f1", runTime(d, 9, 5), "run-1", "job-a", "SUCCESS"),
		finished("f2", runTime(d, 10, 15), "run-2", "job-a", "FAILED"),
		started("s3", runTime(d, 11, 0), "run-3", "job-a"),
		finished("f3", runTime(d, 11, 20), "run-3", "job-a", "FAILED"),
	}
	seq := func(backfillConcurrent bool) string {
		e := New(latenessWindow)
		e.clock = func() time.Time { return fixedEvalTime }
		registerV1(t, e)
		batch := append([]event.Envelope{}, base...)
		batch = append(batch, base...) // duplicates
		r := rand.New(rand.NewSource(99))
		r.Shuffle(len(batch), func(i, j int) { batch[i], batch[j] = batch[j], batch[i] })
		var wg sync.WaitGroup
		gate := make(chan struct{})
		for _, ev := range batch {
			wg.Add(1)
			go func(ev event.Envelope) {
				defer wg.Done()
				<-gate
				e.IngestEvent(context.Background(), ev)
			}(ev)
		}
		close(gate)
		wg.Wait()
		if backfillConcurrent {
			var wg2 sync.WaitGroup
			gate2 := make(chan struct{})
			for i := 0; i < 4; i++ {
				wg2.Add(1)
				go func() {
					defer wg2.Done()
					<-gate2
					e.BackfillEvent(context.Background(), s2)
				}()
			}
			close(gate2)
			wg2.Wait()
		} else {
			e.BackfillEvent(context.Background(), s2)
		}
		if _, _, err := e.Evaluate(); err != nil {
			t.Fatal(err)
		}
		runs, alerts := finalConclusions(t, e)
		return runs + "##" + alerts
	}
	one := seq(false)
	two := seq(true)
	if one != two {
		t.Fatalf("sequential vs concurrent backfill differ:\n%s\n%s", one, two)
	}
	t.Logf("STABLE out-of-order/concurrent backfill picture: %s", one)
}

// TestBackfillUnderPinnedOldRuleVersion: a run completed as UNKNOWN, then
// backfilled after a tighter v2 was registered, must still be judged under
// its pinned v1 binding — historical semantics are reproduced exactly.
func TestBackfillUnderPinnedOldRuleVersion(t *testing.T) {
	d := 23
	e := New(latenessWindow)
	e.clock = func() time.Time { return fixedEvalTime }
	registerV1(t, e) // SLA 10m, fire 2
	// Pin v1 explicitly before anything is evaluated.
	if err := e.BindRule("job-a", "job-a", 1); err != nil {
		t.Fatal(err)
	}
	// A tighter v2 registered before the run is repaired.
	v2 := rule.Set{
		RuleID: "job-a", Version: 2,
		FreshnessDeadline: 30 * time.Minute, SLADuration: 3 * time.Minute,
		FireAfterN: 1, RecoverAfterN: 1,
	}
	if err := e.Rules().Register(v2); err != nil {
		t.Fatal(err)
	}

	// finish-only run (15m duration) -> UNKNOWN.
	fin := finished("f-old", runTime(d, 10, 15), "run-old", "job-a", "FAILED")
	e.IngestEvent(context.Background(), fin)
	cs, tr, err := e.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	if len(cs) != 1 || !cs[0].IsUnknown() || len(tr) != 0 {
		t.Fatalf("pre-backfill: want one unknown conclusion, no alerts: %+v %v", cs, tr)
	}
	t.Logf("INPUT finish-only run registered while v2 exists; DECISION %s gap=%s",
		cs[0].SLA.Status, cs[0].MissingBasis[0].Code)

	// Recover the start (10:00 -> 15m duration). Under pinned v1, 15m is an
	// SLA breach; under v2 it breaches too but with FireAfterN=1. To isolate
	// version binding, compare against a fresh v1-pinned replay engine.
	start := started("s-old", runTime(d, 10, 0), "run-old", "job-a")
	if r := e.BackfillEvent(context.Background(), start); !r.Accepted {
		t.Fatalf("backfill rejected: %s %s", r.Disposition, r.Reason)
	}
	cs2, tr2, err := e.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	var c model.Conclusion
	for _, x := range cs2 {
		if x.RunID == "run-old" {
			c = x
		}
	}
	if !c.Determinable || c.RuleVer != 1 || c.SLA.Status != model.VerdictFail ||
		c.SLA.Limit != 10*time.Minute {
		t.Fatalf("backfilled run must be judged under pinned v1: %+v", c)
	}
	t.Logf("POST-BACKFILL rule=v%d sla=%s limit=%s actual=%s",
		c.RuleVer, c.SLA.Status, c.SLA.Limit, c.SLA.Actual)
	// v1 FireAfterN=2 with a single breach run => no firing yet.
	if len(tr2) != 0 {
		t.Fatalf("single breach under v1 (fireAfterN=2) must not fire: %s",
			renderTransitions(tr2))
	}

	// Reproducibility: same historical events in a fresh engine pinned to v1
	// (events never lost) produce the same single-run outcome.
	ref := New(latenessWindow)
	ref.clock = func() time.Time { return fixedEvalTime }
	registerV1(t, ref)
	ref.Rules().Register(v2)
	ref.BindRule("job-a", "job-a", 1)
	ref.IngestEvent(context.Background(), start)
	ref.IngestEvent(context.Background(), fin)
	csRef, trRef, err := ref.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	if len(csRef) != 1 || csRef[0].RuleVer != 1 || csRef[0].SLA.Status != c.SLA.Status ||
		csRef[0].SLA.Actual != c.SLA.Actual || len(trRef) != 0 {
		t.Fatalf("historical v1 replay not reproduced:\n%+v\ntrans=%s", csRef, renderTransitions(trRef))
	}
	t.Logf("REPLAY v1 reproduced: sla=%s actual=%s alerts=%d",
		csRef[0].SLA.Status, csRef[0].SLA.Actual, len(trRef))
}
