package ingest

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/pipeline-observability-go/event"
	"github.com/h3yow/pipeline-observability-go/internal/testhelp"
)

// buildEventSet creates one fixed set of deliveries: in-order events,
// duplicates, a same-seq conflicting event and a post-terminal event.
func buildEventSet() []event.RawEvent {
	return []event.RawEvent{
		startedV1("e1", 1, base),
		finishedV1("e2", 2, base.Add(10*time.Second), event.StatusSuccess),
		startedV1("e1", 1, base), // duplicate
		startedV1("e1", 1, base), // duplicate
		finishedV1("e9", 2, base.Add(10*time.Second), event.StatusFailed), // same seq, other content
		startedV1("e3", 3, base.Add(20*time.Second)),                      // post-terminal
	}
}

// ingestSetConcurrently pushes one delivery set through an aggregator with
// arbitrary concurrency/interleaving, returns per-reason counts.
func ingestSetConcurrently(t *testing.T, a *Aggregator, evs []event.RawEvent, rng *rand.Rand) map[Reason]int {
	t.Helper()
	order := rng.Perm(len(evs))
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	counts := map[Reason]int{}
	for _, idx := range order {
		raw := evs[idx]
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			d, err := a.Ingest(raw, base.Add(time.Minute))
			if err != nil {
				t.Errorf("ingest error: %v", err)
				return
			}
			mu.Lock()
			counts[d.Reason]++
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	return counts
}

// TestConcurrentIngestIsDeterministic: for every arbitrary interleaving the
// outcome is internally consistent and fully explained - exactly one first
// writer wins each seq position, dupes never apply, the terminal event never
// reopens the run, and state matches the winner.
func TestConcurrentIngestIsDeterministic(t *testing.T) {
	for _, raw := range buildEventSet() {
		testhelp.LogRaw(t, "concurrent-set", raw, base.Add(time.Minute))
	}

	for seed := int64(1); seed <= 20; seed++ {
		a := New(Config{MaxLateness: time.Hour})
		counts := ingestSetConcurrently(t, a, buildEventSet(), rand.New(rand.NewSource(seed)))
		st, ok := a.State("run1")
		if !ok || !st.Closed {
			t.Fatalf("seed %d: run not closed: %+v", seed, st)
		}
		// Order-independent canonical state: the earliest STARTED (seq1) is
		// always the run start, FINISHED closes at seq2, regardless of which
		// delivery physically reached the aggregator first.
		if !st.StartedAt.Equal(base) || st.StartSeq != 1 || st.EndSeq != 2 ||
			!st.FinishedAt.Equal(base.Add(10*time.Second)) {
			t.Fatalf("seed %d: canonical state wrong: %+v", seed, st)
		}
		// seq2 winner must be one of the two contenders; the loser must be
		// reported SUPERSEDED - never silently overwriting the winner.
		switch st.Status {
		case event.StatusSuccess, event.StatusFailed:
		default:
			t.Fatalf("seed %d: unexpected winner status %s", seed, st.Status)
		}
		if counts[ReasonSuperseded] != 1 {
			t.Fatalf("seed %d: SUPERSEDED count=%d want 1 (%v)",
				seed, counts[ReasonSuperseded], counts)
		}
		// Order-independent invariants:
		//  - the three non-conflicting facts (start, finish, post-terminal
		//    start) are accounted for exactly once each as some combination
		//    of APPLIED / APPLIED_LATE / CLOSED / STALE;
		//  - duplicates are always exactly the two redeliveries;
		//  - exactly one of the conflicting seq-2 contents is superseded.
		facts := counts[ReasonApplied] + counts[ReasonAppliedLate] +
			counts[ReasonClosed] + counts[ReasonStale]
		if facts != 3 {
			t.Fatalf("seed %d: facts total=%d want 3 (%v)", seed, facts, counts)
		}
		if counts[ReasonDuplicate] != 2 {
			t.Fatalf("seed %d: DUPLICATE count=%d want 2 (%v)", seed, counts[ReasonDuplicate], counts)
		}
		total := 0
		for _, n := range counts {
			total += n
		}
		if total != len(buildEventSet()) {
			t.Fatalf("seed %d: reasons do not account for all deliveries: %v", seed, counts)
		}
		t.Logf("BASIS  seed=%d self-consistent: winner status=%s counts=%v",
			seed, st.Status, counts)
	}
}

// TestFixedOrderStability replays the same set single-threaded N times and
// requires identical conclusions (duplicates included).
func TestFixedOrderStability(t *testing.T) {
	var prev *RunState
	var prevCounts map[Reason]int
	for i := 0; i < 10; i++ {
		a := New(Config{MaxLateness: time.Hour})
		counts := map[Reason]int{}
		for _, raw := range buildEventSet() {
			d, err := a.Ingest(raw, base.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			counts[d.Reason]++
		}
		st, _ := a.State("run1")
		if prev != nil && *prev != st {
			t.Fatalf("run %d: fixed-order result drifted: got %+v want %+v", i, st, *prev)
		}
		if prevCounts != nil && !mapsEqual(prevCounts, counts) {
			t.Fatalf("run %d: fixed-order reasons drifted: got %v want %v", i, counts, prevCounts)
		}
		st2 := st
		prev = &st2
		prevCounts = counts
	}
	t.Logf("BASIS  fixed-order replay stable: %v", prevCounts)
}

func mapsEqual(a, b map[Reason]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// TestConcurrentRunsIsolation fans out across many distinct run ids.
func TestConcurrentRunsIsolation(t *testing.T) {
	a := New(Config{MaxLateness: time.Hour})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			runID := fmt.Sprintf("run-%02d", i)
			s := startedV1("e-%d-1", 1, base)
			s.RunID = runID
			f := finishedV1("e-%d-2", 2, base.Add(5*time.Second), event.StatusSuccess)
			f.RunID = runID
			if _, err := a.Ingest(s, base.Add(time.Second)); err != nil {
				t.Error(err)
			}
			if _, err := a.Ingest(f, base.Add(6*time.Second)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if got := len(a.Runs()); got != 50 {
		t.Fatalf("runs: got %d want 50", got)
	}
	for _, id := range a.Runs() {
		st, ok := a.State(id)
		if !ok || !st.Closed || st.Status != event.StatusSuccess {
			t.Fatalf("run %s bad state: %+v", id, st)
		}
	}
	t.Log("BASIS  all 50 concurrent runs closed SUCCESS, isolated and consistent")
}
