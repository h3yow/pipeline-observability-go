package engine

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/pipeline-observability-go/internal/alert"
	"github.com/h3yow/pipeline-observability-go/internal/event"
	"github.com/h3yow/pipeline-observability-go/internal/model"
	"github.com/h3yow/pipeline-observability-go/internal/rule"
)

// This file pins the "backfill == never-lost" guarantee for the alert
// HISTORY: after a missing event is supplied, the firing/recovery history
// (count, order, and which recovery closes which firing) must be identical
// to the path in which the event was never lost — whether the backfill
// moves the first firing earlier, moves it later, or makes it disappear.

// backfillRule is job-a v1 with FireAfterN=2, RecoverAfterN=1 and a 10m SLA.
func registerBackfillRule(t *testing.T, e *Engine) {
	t.Helper()
	if err := e.Rules().Register(rule.Set{
		RuleID: "job-a", Version: 1,
		FreshnessDeadline: 30 * time.Minute,
		SLADuration:       10 * time.Minute,
		FireAfterN:        2,
		RecoverAfterN:     1,
	}); err != nil {
		t.Fatal(err)
	}
}

func bfStarted(id string, at time.Time, run string) event.Envelope {
	return started(id, at, run, "job-a")
}

func bfFinished(id string, at time.Time, run, result string) event.Envelope {
	return finished(id, at, run, "job-a", result)
}

// breachRun is a 20m run (SLA breach); passRun is a 5m run (pass).
func breachRun(n, day, hour int) (event.Envelope, event.Envelope) {
	r := fmt.Sprintf("run-%d", n)
	t0 := runTime(day, hour, 0)
	return bfStarted(fmt.Sprintf("s%d", n), t0, r),
		bfFinished(fmt.Sprintf("f%d", n), t0.Add(20*time.Minute), r, "FAILED")
}
func passRun(n, day, hour int) (event.Envelope, event.Envelope) {
	r := fmt.Sprintf("run-%d", n)
	t0 := runTime(day, hour, 0)
	return bfStarted(fmt.Sprintf("s%d", n), t0, r),
		bfFinished(fmt.Sprintf("f%d", n), t0.Add(5*time.Minute), r, "SUCCESS")
}

func logInputs(t *testing.T, tag string, evs []event.Envelope) {
	t.Helper()
	for _, ev := range evs {
		t.Logf("[%s] INPUT id=%s run=%s type=%s at=%s", tag, ev.EventID, ev.RunID,
			ev.Type, ev.OccurredAt.Format("15:04"))
	}
}

func logHistory(t *testing.T, tag string, e *Engine) {
	t.Helper()
	for _, tr := range e.AlertEvents() {
		t.Logf("[%s] HISTORY %s %s->%s seq=%d at=%s reason=%q",
			tag, tr.Alert.Kind, tr.FromState, tr.ToState, tr.Alert.Seq,
			tr.Alert.Timestamp.Format("15:04"), tr.Alert.Reason)
	}
}

// neverLostEngine builds the reference: every event present, fed concurrently
// shuffled+duplicated (at-least-once chaos), evaluated once.
func neverLostEngine(t *testing.T, full []event.Envelope) *Engine {
	t.Helper()
	ref := New(latenessWindow)
	registerBackfillRule(t, ref)
	feedShuffledConcurrent(t, ref, full, 2)
	if _, _, err := ref.Evaluate(); err != nil {
		t.Fatal(err)
	}
	return ref
}

// assertHistoryEqualsNeverLost compares the full histories entry-by-entry:
// kind, direction, order, timestamp (the triggering run), reason and the
// firing seq referenced by each recovery.
func assertHistoryEqualsNeverLost(t *testing.T, got, ref *Engine) {
	t.Helper()
	g, r := got.AlertEvents(), ref.AlertEvents()
	if len(g) != len(r) {
		t.Fatalf("history length differs: backfilled=%d never-lost=%d\nbackfilled:\n%s\nnever-lost:\n%s",
			len(g), len(r), transitionCanon(g), transitionCanon(r))
	}
	for i := 0; i < len(g); i++ {
		a, b := g[i], r[i]
		if a.Alert.Kind != b.Alert.Kind || a.ToState != b.ToState || a.FromState != b.FromState ||
			!a.Alert.Timestamp.Equal(b.Alert.Timestamp) || a.Alert.Reason != b.Alert.Reason ||
			a.Evidence != b.Evidence {
			t.Fatalf("history entry %d differs:\nbackfilled: %s->%s at=%s %q\nnever-lost: %s->%s at=%s %q",
				i, a.FromState, a.ToState, a.Alert.Timestamp.Format("15:04"), a.Alert.Reason,
				b.FromState, b.ToState, b.Alert.Timestamp.Format("15:04"), b.Alert.Reason)
		}
	}
}

// TestBackfillMovesFirstFireEarlier: with run-2 unknown the first firing
// happens at run-3 (breach #2). Once run-2's start is backfilled, run-2 is a
// breach and becomes the FIRST firing; run-3 is a redundant breach; run-4
// recovers. The history must equal the never-lost path exactly (one fire, at
// run-2; one recovery, at run-4) instead of containing two firings.
func TestBackfillMovesFirstFireEarlier(t *testing.T) {
	d := 30
	s1, f1 := breachRun(1, d, 9)
	s2, f2 := breachRun(2, d, 10)
	s3, f3 := breachRun(3, d, 11)
	s4, f4 := passRun(4, d, 12)
	full := []event.Envelope{s1, f1, s2, f2, s3, f3, s4, f4}
	logInputs(t, "never-lost", full)
	ref := neverLostEngine(t, full)
	logHistory(t, "never-lost", ref)

	e := New(latenessWindow)
	registerBackfillRule(t, e)
	partial := []event.Envelope{s1, f1, f2, s3, f3, s4, f4} // s2 lost
	logInputs(t, "backfill", partial)
	feedShuffledConcurrent(t, e, partial, 2)
	_, tr, err := e.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[backfill] before backfill transitions: %s", transitionSeq(tr))
	if len(tr) != 2 || tr[0].ToState != model.AlertStateFiring || tr[1].ToState != model.AlertStateRecover {
		t.Fatalf("pre-backfill: fire at run-3 then recover at run-4 expected, got %s", transitionSeq(tr))
	}
	if !tr[0].Alert.Timestamp.Equal(runTime(d, 11, 20)) {
		t.Fatalf("pre-backfill fire must be on run-3 (11:20), got %s", tr[0].Alert.Timestamp)
	}

	// Backfill run-2's start, concurrently duplicated; settle with Evaluate.
	lost := bfStarted("s2", runTime(d, 10, 0), "run-2")
	runBackfillAndEvaluate(t, e, lost)
	logHistory(t, "backfill", e)

	assertHistoryEqualsNeverLost(t, e, ref)
	// Explicit expectations on the rebuilt history.
	evs := e.AlertEvents()
	if len(evs) != 2 ||
		evs[0].ToState != model.AlertStateFiring || !evs[0].Alert.Timestamp.Equal(runTime(d, 10, 20)) ||
		evs[1].ToState != model.AlertStateRecover || !evs[1].Alert.Timestamp.Equal(runTime(d, 12, 5)) {
		t.Fatalf("post-backfill history must be one fire at run-2 then one recovery at run-4, got %s",
			transitionSeq(evs))
	}
	if !recoveryClosesFiring(evs[1], evs[0]) {
		t.Fatalf("recovery must reference the single firing seq=%d, reason=%q",
			evs[0].Alert.Seq, evs[1].Alert.Reason)
	}
	t.Logf("[backfill] DECISION: fire moved 11:20 -> 10:20; exactly one fire + one recovery; recovery closes fire seq=%d",
		evs[0].Alert.Seq)
}

// TestBackfillMovesFirstFireLater: with run-2 unknown the first firing
// happens at run-3 (run-1 breach + run-3 breach). Backfilled run-2 turns out
// to be a PASS, which resets the debounce, so the first firing moves LATER to
// run-4 (run-3 breach + run-4 breach), with recovery at run-5. The history
// must equal the never-lost path (one fire, at run-4).
func TestBackfillMovesFirstFireLater(t *testing.T) {
	d := 31
	s1, f1 := breachRun(1, d, 9)
	s2, f2 := passRun(2, d, 10)
	s3, f3 := breachRun(3, d, 11)
	s4, f4 := breachRun(4, d, 12)
	s5, f5 := passRun(5, d, 13)
	full := []event.Envelope{s1, f1, s2, f2, s3, f3, s4, f4, s5, f5}
	logInputs(t, "never-lost", full)
	ref := neverLostEngine(t, full)
	logHistory(t, "never-lost", ref)

	e := New(latenessWindow)
	registerBackfillRule(t, e)
	partial := []event.Envelope{s1, f1, f2, s3, f3, s4, f4, s5, f5} // s2 lost
	logInputs(t, "backfill", partial)
	feedShuffledConcurrent(t, e, partial, 2)
	_, tr, err := e.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[backfill] before backfill transitions: %s", transitionSeq(tr))
	// Pre-backfill run-2 is UNKNOWN and skipped, so run-1+run-3 are two
	// consecutive breaches => fire at run-3 (11:20), recovered at run-5.
	if len(tr) != 2 ||
		tr[0].ToState != model.AlertStateFiring || !tr[0].Alert.Timestamp.Equal(runTime(d, 11, 20)) ||
		tr[1].ToState != model.AlertStateRecover || !tr[1].Alert.Timestamp.Equal(runTime(d, 13, 5)) {
		t.Fatalf("pre-backfill must fire at run-3 and recover at run-5, got %s", transitionSeq(tr))
	}

	lost := bfStarted("s2", runTime(d, 10, 0), "run-2")
	runBackfillAndEvaluate(t, e, lost)
	logHistory(t, "backfill", e)

	assertHistoryEqualsNeverLost(t, e, ref)
	evs := e.AlertEvents()
	// Never-lost sequence: breach, pass(reset), breach, breach=>FIRE@run-4,
	// pass=>RECOVER@run-5.
	if len(evs) != 2 ||
		evs[0].ToState != model.AlertStateFiring || !evs[0].Alert.Timestamp.Equal(runTime(d, 12, 20)) ||
		evs[1].ToState != model.AlertStateRecover || !evs[1].Alert.Timestamp.Equal(runTime(d, 13, 5)) {
		t.Fatalf("post-backfill history must be fire at run-4 + recovery at run-5, got %s",
			transitionSeq(evs))
	}
	if !recoveryClosesFiring(evs[1], evs[0]) {
		t.Fatalf("recovery must close firing seq=%d, reason=%q", evs[0].Alert.Seq, evs[1].Alert.Reason)
	}
	t.Logf("[backfill] DECISION: pass run-2 resets debounce; single fire stays at run-4 and recovers at run-5")
}

// TestBackfillRemovesFireEntirely: with run-2 unknown, run-1 and run-3 are
// two consecutive breaches and an alert fires (and recovers at run-4).
// Backfilled run-2 is a PASS: the never-lost path is breach, pass(reset),
// breach — only one breach in a row — so it must NEVER alert. The previously
// emitted firing AND its recovery must both disappear, leaving an empty
// history and a normal (OK) current state.
func TestBackfillRemovesFireEntirely(t *testing.T) {
	d := 32
	s1, f1 := breachRun(1, d, 9)
	s2, f2 := passRun(2, d, 10)
	s3, f3 := breachRun(3, d, 11)
	s4, f4 := passRun(4, d, 12)
	full := []event.Envelope{s1, f1, s2, f2, s3, f3, s4, f4}
	logInputs(t, "never-lost", full)
	ref := neverLostEngine(t, full)
	logHistory(t, "never-lost", ref)
	if len(ref.AlertEvents()) != 0 {
		t.Fatalf("never-lost path must not alert, got %s", transitionSeq(ref.AlertEvents()))
	}

	e := New(latenessWindow)
	registerBackfillRule(t, e)
	partial := []event.Envelope{s1, f1, f2, s3, f3, s4, f4} // s2 lost
	logInputs(t, "backfill", partial)
	feedShuffledConcurrent(t, e, partial, 2)
	_, tr, err := e.Evaluate()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[backfill] before backfill transitions: %s", transitionSeq(tr))
	if len(tr) != 2 { // fire at run-3, recover at run-4
		t.Fatalf("pre-backfill must show fire+recover, got %s", transitionSeq(tr))
	}

	lost := bfStarted("s2", runTime(d, 10, 0), "run-2")
	runBackfillAndEvaluate(t, e, lost)
	logHistory(t, "backfill", e)

	if len(e.AlertEvents()) != 0 {
		t.Fatalf("after backfill the alert must never have existed; history must be empty, got %s",
			transitionSeq(e.AlertEvents()))
	}
	if st, ok := e.AlertState("job-a", model.KindSLA); !ok || st != model.AlertStateOK {
		t.Fatalf("state must be OK (normal), got %q ok=%v", st, ok)
	}
	assertHistoryEqualsNeverLost(t, e, ref)
	t.Logf("[backfill] DECISION: pass run-2 resets debounce; former fire@run-3 and recover@run-4 both retracted; history empty")
}

// runBackfillAndEvaluate delivers the recovered event many times concurrently
// (duplicate backfills) interleaved with concurrent Evaluate calls, then
// settles with one final Evaluate. All operations are serialized by the
// engine and the alert log is canonically rebuilt, so the result is
// independent of goroutine scheduling.
func runBackfillAndEvaluate(t *testing.T, e *Engine, lost event.Envelope) {
	t.Helper()
	var wg sync.WaitGroup
	start := make(chan struct{})
	var mu sync.Mutex
	disps := map[string]int{}
	for i := 0; i < 6; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			r := e.BackfillEvent(context.Background(), lost)
			mu.Lock()
			disps[string(r.Disposition)]++
			mu.Unlock()
		}()
		go func() {
			defer wg.Done()
			<-start
			if _, _, err := e.Evaluate(); err != nil {
				t.Errorf("concurrent evaluate: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if _, _, err := e.Evaluate(); err != nil {
		t.Fatal(err)
	}
	t.Logf("[backfill] concurrent BACKFILL dispositions=%v (one BACKFILLED, rest DUPLICATE); settled with a final Evaluate",
		disps)
	if disps["BACKFILLED"] != 1 {
		t.Fatalf("lost event must backfill exactly once under concurrency, got %v", disps)
	}
}

var firingSeqRe = regexp.MustCompile(`firing alert seq=(\d+)`)

// recoveryClosesFiring reports whether the recovery reason references the
// given firing's global seq (which recovery closes which firing).
func recoveryClosesFiring(recovery, fire alert.Transition) bool {
	m := firingSeqRe.FindStringSubmatch(recovery.Alert.Reason)
	if m == nil {
		return false
	}
	return m[1] == fmt.Sprintf("%d", fire.Alert.Seq)
}

// TestConcurrentRejudgeAfterBackfillIdempotent: after the lost start is
// supplied, many concurrent Evaluate calls (concurrent re-judgment) together
// with repeated duplicate backfills must resolve the run exactly once and
// leave the alert history equal to the never-lost path.
func TestConcurrentRejudgeAfterBackfillIdempotent(t *testing.T) {
	d := 33
	s1, f1 := breachRun(1, d, 9)
	s2, f2 := breachRun(2, d, 10) // unknown until backfilled; then the 2nd consecutive breach => fire
	s3, f3 := passRun(3, d, 11)
	full := []event.Envelope{s1, f1, s2, f2, s3, f3}
	logInputs(t, "never-lost", full)
	ref := neverLostEngine(t, full)

	e := New(latenessWindow)
	registerBackfillRule(t, e)
	partial := []event.Envelope{s1, f1, f2, s3, f3} // s2 lost
	logInputs(t, "rejudge", partial)
	feedShuffledConcurrent(t, e, partial, 1)
	if _, _, err := e.Evaluate(); err != nil {
		t.Fatal(err)
	}

	lost := bfStarted("s2", runTime(d, 10, 0), "run-2")
	// Hammer backfill + Evaluate concurrently from many goroutines.
	var wg sync.WaitGroup
	start := make(chan struct{})
	var mu sync.Mutex
	backfilled, resolved := 0, 0
	for i := 0; i < 16; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			r := e.BackfillEvent(context.Background(), lost)
			mu.Lock()
			if r.Disposition == "BACKFILLED" {
				backfilled++
			}
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
					resolved++
				}
			}
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	if _, _, err := e.Evaluate(); err != nil { // settle
		t.Fatal(err)
	}
	t.Logf("[rejudge] BACKFILLED count=%d (want 1); determinable run-2 conclusions=%d (want <=1)",
		backfilled, resolved)
	if backfilled != 1 {
		t.Fatalf("backfill must apply once, got %d", backfilled)
	}
	if resolved > 1 {
		t.Fatalf("run-2 must be re-judged at most once across concurrent evaluates, got %d", resolved)
	}

	assertHistoryEqualsNeverLost(t, e, ref)
	evs := e.AlertEvents()
	if len(evs) != 2 ||
		evs[0].ToState != model.AlertStateFiring || !evs[0].Alert.Timestamp.Equal(runTime(d, 10, 20)) ||
		evs[1].ToState != model.AlertStateRecover {
		t.Fatalf("one fire at run-2 + one recovery expected, got %s", transitionSeq(evs))
	}
	// Seqs are unique across the history (no duplicate alert).
	seenSeq := map[int64]bool{}
	for _, tr := range evs {
		if seenSeq[tr.Alert.Seq] {
			t.Fatalf("duplicate alert seq %d", tr.Alert.Seq)
		}
		seenSeq[tr.Alert.Seq] = true
	}
	if p, _, _ := e.PendingRuns("", 10); len(p) != 0 {
		t.Fatalf("pending must be empty, got %d", len(p))
	}
	t.Logf("[rejudge] DECISION: single conclusion, single fire seq=%d, recovery seq=%d",
		evs[0].Alert.Seq, evs[1].Alert.Seq)
}
