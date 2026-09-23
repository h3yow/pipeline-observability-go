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
