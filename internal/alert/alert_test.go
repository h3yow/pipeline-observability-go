package alert

import (
	"fmt"
	"sync"
	"testing"

	"github.com/h3yow/pipeline-observability-go/internal/model"
)

// summarize logs an observation and the resulting transition.
func summarize(t *testing.T, m *Manager, job model.JobID, kind model.AlertKind, breach bool, ver, fn, rn int, run string) {
	t.Helper()
	tr := m.Observe(run, job, kind, breach, ver, fn, rn, 0,
		fmt.Sprintf("%s breach=%v fireAfter=%d recoverAfter=%d", run, breach, fn, rn))
	st, _ := m.State(job, kind)
	if tr != nil {
		t.Logf("INPUT %s job=%s kind=%s breach=%v => TRANSITION %s->%s seq=%d reason=%q stateNow=%s",
			run, job, kind, breach, tr.FromState, tr.ToState, tr.Alert.Seq, tr.Alert.Reason, st)
	} else {
		t.Logf("INPUT %s job=%s kind=%s breach=%v => no transition (debounced/redundant) state=%s",
			run, job, kind, breach, st)
	}
}

func TestFireRecoverRefireDistinguishable(t *testing.T) {
	m := NewManager()
	job, kind := model.JobID("job-a"), model.KindSLA
	// fireAfter=1, recoverAfter=1 for a clear lifecycle.
	summarize(t, m, job, kind, true, 1, 1, 1, "run-1")  // OK -> FIRING
	summarize(t, m, job, kind, true, 1, 1, 1, "run-2")  // breach while firing: no dup
	summarize(t, m, job, kind, true, 1, 1, 1, "run-3")  // still no dup
	summarize(t, m, job, kind, false, 1, 1, 1, "run-4") // FIRING -> RECOVERED -> OK
	summarize(t, m, job, kind, true, 1, 1, 1, "run-5")  // re-fire, NEW alert

	evs := m.Events()
	if len(evs) != 3 {
		t.Fatalf("want exactly 3 transitions (fire/recover/refire), got %d", len(evs))
	}
	if evs[0].ToState != model.AlertStateFiring ||
		evs[1].ToState != model.AlertStateRecover ||
		evs[2].ToState != model.AlertStateFiring {
		t.Fatalf("transition sequence wrong: %+v", evs)
	}
	if evs[0].Alert.Seq == evs[2].Alert.Seq {
		t.Fatal("re-fired alert must be a distinct, newly sequenced alert")
	}
	st, _ := m.State(job, kind)
	if st != model.AlertStateFiring {
		t.Fatalf("state after re-fire = %s", st)
	}
	t.Logf("decision basis: 3 transitions only; duplicate breaches suppressed; recovery seq=%d distinct from re-fire seq=%d",
		evs[1].Alert.Seq, evs[2].Alert.Seq)
}

func TestDebounceAbsorbsFlapping(t *testing.T) {
	m := NewManager()
	job, kind := model.JobID("job-b"), model.KindFreshness
	// fireAfter=2, recoverAfter=2.
	summarize(t, m, job, kind, true, 1, 2, 2, "run-1")  // 1/2 breaches, no fire
	summarize(t, m, job, kind, false, 1, 2, 2, "run-2") // pass resets fire counter
	summarize(t, m, job, kind, true, 1, 2, 2, "run-3")  // 1/2 again, no fire
	summarize(t, m, job, kind, true, 1, 2, 2, "run-4")  // 2/2 => FIRING
	if len(m.Events()) != 1 {
		t.Fatalf("flapping near threshold must not fire early, events=%d", len(m.Events()))
	}
	// One isolated pass must NOT recover (recoverAfter=2).
	summarize(t, m, job, kind, false, 1, 2, 2, "run-5")
	summarize(t, m, job, kind, true, 1, 2, 2, "run-6") // breach interrupts recovery
	summarize(t, m, job, kind, false, 1, 2, 2, "run-7")
	summarize(t, m, job, kind, false, 1, 2, 2, "run-8") // 2 consecutive passes => recover
	evs := m.Events()
	if len(evs) != 2 || evs[1].ToState != model.AlertStateRecover {
		t.Fatalf("want single fire then single recover, got %+v", evs)
	}
	t.Logf("decision basis: consecutive counters reset on opposite observation; events=%d", len(evs))
}

// TestObserveIdempotentReprocess verifies that re-delivering the SAME
// observation (same run id) never advances the lifecycle or duplicates an
// alert, even under concurrent reprocessing.
func TestObserveIdempotentReprocess(t *testing.T) {
	m := NewManager()
	job, kind := model.JobID("job-d"), model.KindSLA
	tr1 := m.Observe("run-1", job, kind, true, 1, 1, 1, 0, "first breach")
	if tr1 == nil {
		t.Fatal("first breach must fire")
	}
	// Same run delivered many times concurrently (retry storm).
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			tr := m.Observe("run-1", job, kind, true, 1, 1, 1, 0, "redelivered")
			if tr != tr1 {
				t.Errorf("redelivery must return original transition")
			}
		}()
	}
	close(start)
	wg.Wait()
	if len(m.Events()) != 1 {
		t.Fatalf("one fire expected despite 21 deliveries, got %d", len(m.Events()))
	}
	// A NEW run id then advances normally.
	trRec := m.Observe("run-2", job, kind, false, 1, 1, 1, 0, "first pass")
	if trRec == nil || trRec.ToState != model.AlertStateRecover {
		t.Fatalf("new run should recover, got %+v", trRec)
	}
	t.Logf("decision basis: obsID run-1 applied once; 20 redeliveries returned cached outcome; events=%v",
		len(m.Events())+1)
}

// TestObserveConcurrentIndependentKeys checks that different jobs/kinds
// progress independently under concurrent load without interference.
func TestObserveConcurrentIndependentKeys(t *testing.T) {
	m := NewManager()
	var wg sync.WaitGroup
	for j := 0; j < 10; j++ {
		job := model.JobID(fmt.Sprintf("job-%d", j))
		for k := 0; k < 2; k++ {
			kind := model.KindSLA
			if k == 0 {
				kind = model.KindFreshness
			}
			wg.Add(1)
			go func(job model.JobID, kind model.AlertKind) {
				defer wg.Done()
				m.Observe("r1", job, kind, true, 1, 1, 1, 0, "concurrent independent key")
			}(job, kind)
		}
	}
	wg.Wait()
	if len(m.Events()) != 20 {
		t.Fatalf("want 20 independent firings, got %d", len(m.Events()))
	}
	t.Logf("decision basis: 10 jobs x 2 kinds, each fired exactly once: %d events", len(m.Events()))
}

// TestRecomputeMatchesIncremental: replaying the canonical observation
// list via Recompute yields the same lifecycle as incremental Observe,
// and replays never re-emit transitions.
func TestRecomputeMatchesIncremental(t *testing.T) {
	// Observations: breach, breach (fire at 2), pass (recover), breach (refire).
	mk := func() []Observation {
		return []Observation{
			{ObsID: "run-1:SLA", Breach: true, RuleVer: 1, FireAfterN: 2, RecoverAfterN: 1, At: 100, Evidence: "run-1 breach"},
			{ObsID: "run-2:SLA", Breach: true, RuleVer: 1, FireAfterN: 2, RecoverAfterN: 1, At: 200, Evidence: "run-2 breach"},
			{ObsID: "run-3:SLA", Breach: false, RuleVer: 1, FireAfterN: 2, RecoverAfterN: 1, At: 300, Evidence: "run-3 pass"},
			{ObsID: "run-4:SLA", Breach: true, RuleVer: 1, FireAfterN: 2, RecoverAfterN: 1, At: 400, Evidence: "run-4 breach"},
		}
	}

	inc := NewManager()
	for _, o := range mk() {
		tr := inc.Observe(o.ObsID, "job-r", model.KindSLA, o.Breach, o.RuleVer, o.FireAfterN, o.RecoverAfterN, o.At, o.Evidence)
		if tr != nil {
			t.Logf("INCREMENTAL obs=%s => %s->%s seq=%d", o.ObsID, tr.FromState, tr.ToState, tr.Alert.Seq)
		}
	}

	rec := NewManager()
	var all []Transition
	// Feed the observations incrementally as growing prefixes, as the
	// engine does: each new run triggers a full canonical replay.
	obs := mk()
	for i := 1; i <= len(obs); i++ {
		newOnes := rec.Recompute("job-r", model.KindSLA, obs[:i])
		for _, tr := range newOnes {
			t.Logf("RECOMPUTE prefix=%d => %s->%s seq=%d reason=%q", i, tr.FromState, tr.ToState, tr.Alert.Seq, tr.Alert.Reason)
		}
		all = append(all, newOnes...)
	}

	incEvents := inc.Events()
	if len(all) != len(incEvents) {
		t.Fatalf("recompute emitted %d transitions, incremental emitted %d", len(all), len(incEvents))
	}
	for i := range all {
		a, b := all[i], incEvents[i]
		if a.ToState != b.ToState || a.FromState != b.FromState ||
			a.Alert.Reason != b.Alert.Reason || a.Evidence != b.Evidence ||
			a.Alert.Timestamp != b.Alert.Timestamp {
			t.Fatalf("transition %d differs:\nrecompute:   %+v\nincremental: %+v", i, a, b)
		}
	}
	// Replaying the same prefix again emits nothing.
	if again := rec.Recompute("job-r", model.KindSLA, obs); len(again) != 0 {
		t.Fatalf("idempotent replay must emit nothing, got %d", len(again))
	}
	st, _ := rec.State("job-r", model.KindSLA)
	stInc, _ := inc.State("job-r", model.KindSLA)
	if st != stInc {
		t.Fatalf("final state differs: recompute=%s incremental=%s", st, stInc)
	}
	t.Logf("decision basis: %d transitions identical between replay and incremental feed; final state=%s", len(all), st)
}

// TestRecomputeInsertBackfilledObservation: an observation inserted into
// its canonical position afterwards (backfill) produces the transitions
// the in-order history would have produced, without duplicating emitted
// ones.
func TestRecomputeInsertBackfilledObservation(t *testing.T) {
	m := NewManager()
	obs := func(id string, breach bool, at int64) Observation {
		return Observation{ObsID: id, Breach: breach, RuleVer: 1, FireAfterN: 2, RecoverAfterN: 1, At: at, Evidence: "ev " + id}
	}
	// run-2 is pending (unknown) at first: only run-1, run-3 observed.
	first := m.Recompute("job-bf", model.KindSLA, []Observation{
		obs("run-1:SLA", true, 100),
		obs("run-3:SLA", false, 300),
	})
	t.Logf("before backfill: %d transitions", len(first))
	if len(first) != 0 {
		t.Fatalf("single breach then pass must not fire, got %v", first)
	}
	// Backfill resolves run-2 (a breach) into its canonical position.
	second := m.Recompute("job-bf", model.KindSLA, []Observation{
		obs("run-1:SLA", true, 100),
		obs("run-2:SLA", true, 200),
		obs("run-3:SLA", false, 300),
	})
	for _, tr := range second {
		t.Logf("after backfill => %s->%s seq=%d reason=%q", tr.FromState, tr.ToState, tr.Alert.Seq, tr.Alert.Reason)
	}
	// Canonical history breach,breach,pass => FIRE at run-2, RECOVER at run-3.
	if len(second) != 2 || second[0].ToState != model.AlertStateFiring || second[1].ToState != model.AlertStateRecover {
		t.Fatalf("backfilled replay must fire then recover, got %+v", second)
	}
	if second[1].Alert.Reason == "" || second[0].Alert.Seq == second[1].Alert.Seq {
		t.Fatal("transitions must have distinct seqs")
	}
	// A third identical replay emits nothing.
	if third := m.Recompute("job-bf", model.KindSLA, []Observation{
		obs("run-1:SLA", true, 100), obs("run-2:SLA", true, 200), obs("run-3:SLA", false, 300),
	}); len(third) != 0 {
		t.Fatalf("replay must be idempotent, got %d new transitions", len(third))
	}
	t.Logf("decision basis: canonical replay after backfill emits exactly FIRE+RECOVER once")
}
