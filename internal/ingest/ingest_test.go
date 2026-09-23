package ingest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/pipeline-observability-go/internal/event"
)

func ts(min int, sec ...int) time.Time {
	s := 0
	if len(sec) > 0 {
		s = sec[0]
	}
	return time.Date(2026, 9, 23, 10, min, s, 0, time.UTC)
}

func env(id string, v int, typ event.Type, at time.Time, run, job string, payload map[string]any) event.Envelope {
	return event.Envelope{
		EventID:       id,
		SchemaVersion: v,
		Type:          typ,
		OccurredAt:    at,
		RunID:         run,
		JobID:         job,
		Payload:       payload,
	}
}

func started(id string, at time.Time, run string) event.Envelope {
	return env(id, 1, event.TypeStarted, at, run, "job-a", map[string]any{"attempt": 1})
}
func finished(id string, at time.Time, run, result string) event.Envelope {
	return env(id, 1, event.TypeFinished, at, run, "job-a", map[string]any{"result": result, "attempt": 1})
}

func logReceipt(t *testing.T, e event.Envelope, r Receipt) {
	t.Helper()
	t.Logf("INPUT event=%s run=%s type=%s occurredAt=%s | DECISION accepted=%t disposition=%s reason=%q",
		e.EventID, e.RunID, e.Type, e.OccurredAt.Format("15:04:05"), r.Accepted, r.Disposition, r.Reason)
}

func TestIngestInOrderAndAggregate(t *testing.T) {
	s := NewStore(int64(time.Minute))
	evs := []event.Envelope{
		started("e1", ts(0), "r1"),
		finished("e2", ts(10), "r1", "SUCCESS"),
	}
	for _, e := range evs {
		r := s.Ingest(context.Background(), e)
		logReceipt(t, e, r)
		if r.Disposition != DispositionApplied {
			t.Fatalf("event %s: want APPLIED, got %s", e.EventID, r.Disposition)
		}
	}
	run, ok := s.Run("r1")
	if !ok || !run.IsComplete() {
		t.Fatalf("run not complete: %+v", run)
	}
	if d := run.Duration(); d != 10*time.Minute {
		t.Fatalf("duration = %v, want 10m", d)
	}
	t.Logf("aggregate: started=%s finished=%s duration=%s events=%v",
		run.StartedAt.Format("15:04:05"), run.FinishedAt.Format("15:04:05"), run.Duration(), run.AppliedEventIDs)
}

func TestIngestDuplicateDelivery(t *testing.T) {
	s := NewStore(int64(time.Minute))
	e := finished("e1", ts(5), "r1", "FAILED")
	r1 := s.Ingest(context.Background(), e)
	logReceipt(t, e, r1)
	r2 := s.Ingest(context.Background(), e) // redelivered, identical bytes
	logReceipt(t, e, r2)
	if r2.Disposition != DispositionDuplicate || r2.Accepted {
		t.Fatalf("redelivery must be DUPLICATE, got %+v", r2)
	}
	if r2.AppliedSeq != 0 {
		t.Fatalf("duplicate must not be re-applied, seq=%d", r2.AppliedSeq)
	}
	run, _ := s.Run("r1")
	if len(run.AppliedEventIDs) != 1 {
		t.Fatalf("aggregate must contain event once, got %v", run.AppliedEventIDs)
	}
	t.Logf("decision basis: same EventID already applied at seq=%d; aggregate untouched %v",
		r1.AppliedSeq, run.AppliedEventIDs)
}

func TestIngestOutOfOrderRebuild(t *testing.T) {
	// Lateness window (1h) covers the run span: older events are buffered,
	// not dropped. A window smaller than the gap would instead be LATE.
	s := NewStore(int64(time.Hour))
	// finished arrives BEFORE started.
	fin := finished("e-fin", ts(10), "r9", "SUCCESS")
	sta := started("e-sta", ts(0), "r9")
	r1 := s.Ingest(context.Background(), fin)
	logReceipt(t, fin, r1)
	if r1.Disposition != DispositionApplied {
		t.Fatalf("first event should APPLY, got %s", r1.Disposition)
	}
	r2 := s.Ingest(context.Background(), sta)
	logReceipt(t, sta, r2)
	if r2.Disposition != DispositionOutOfOrder || !r2.Accepted {
		t.Fatalf("older event must be OUT_OF_ORDER_BUFFERED+accepted, got %s", r2.Disposition)
	}
	run, _ := s.Run("r9")
	if !run.IsComplete() || run.Duration() != 10*time.Minute {
		t.Fatalf("rebuilt aggregate wrong: %+v dur=%v", run, run.Duration())
	}
	wantIDs := []string{"e-sta", "e-fin"}
	if fmt.Sprint(run.AppliedEventIDs) != fmt.Sprint(wantIDs) {
		t.Fatalf("event order in aggregate = %v, want %v", run.AppliedEventIDs, wantIDs)
	}
	t.Logf("decision basis: canonical rebuild by (occurredAt,eventId) => %v duration=%s",
		run.AppliedEventIDs, run.Duration())
}

func TestIngestLateRejected(t *testing.T) {
	// Allow 1 minute of lateness. Watermark = maxOccurred - 1m.
	s := NewStore(int64(60 * 1000000000))
	late := started("e-old", ts(0), "rL")
	newer := started("e-new", ts(10), "rN")
	// Newer arrives first to advance the watermark.
	rn := s.Ingest(context.Background(), newer)
	logReceipt(t, newer, rn)
	rl := s.Ingest(context.Background(), late)
	logReceipt(t, late, rl)
	if rl.Disposition != DispositionLate || rl.Accepted {
		t.Fatalf("old event must be LATE, got %s (%s)", rl.Disposition, rl.Reason)
	}
	if _, ok := s.Run("rL"); ok {
		t.Fatal("late event must not create run aggregate")
	}
	t.Logf("decision basis: event time %s < watermark 10:09:00 => dropped", ts(0).Format("15:04:05"))
}

func TestIngestSamePhaseConflict(t *testing.T) {
	s := NewStore(int64(time.Minute))
	a := started("e1", ts(0), "rC")
	b := started("e2", ts(1), "rC") // different EventID, same phase
	s.Ingest(context.Background(), a)
	rb := s.Ingest(context.Background(), b)
	logReceipt(t, b, rb)
	if rb.Disposition != DispositionConflict {
		t.Fatalf("want CONFLICT, got %s", rb.Disposition)
	}
	run, _ := s.Run("rC")
	if run.StartedAt != ts(0) {
		t.Fatalf("first-write must win, startedAt=%s", run.StartedAt.Format("15:04:05"))
	}
	t.Logf("decision basis: existing state preserved (%s), newcomer rejected", run.StartedAt.Format("15:04:05"))
}

func TestIngestInvalidEnvelope(t *testing.T) {
	s := NewStore(int64(time.Minute))
	bad := env("e-bad", 99, event.TypeStarted, ts(0), "rB", "job-a", nil)
	r := s.Ingest(context.Background(), bad)
	logReceipt(t, bad, r)
	if r.Disposition != DispositionInvalid {
		t.Fatalf("want INVALID, got %s", r.Disposition)
	}
}

// TestIngestConcurrentDeterministic fires the same event set from many
// goroutines (with shuffled/duplicated delivery) and asserts the resulting
// aggregate is identical across repeated runs.
func TestIngestConcurrentDeterministic(t *testing.T) {
	mk := func() []event.Envelope {
		return []event.Envelope{
			started("c1", ts(0), "rX"),
			finished("c2", ts(5), "rX", "SUCCESS"),
			started("c1", ts(0), "rX"), // duplicate
			finished("c2", ts(5), "rX", "SUCCESS"),
		}
	}
	snapshot := func(s *Store) string {
		r, ok := s.Run("rX")
		if !ok {
			return ""
		}
		return fmt.Sprintf("%d|%d|%s|%s|%v|seq=%d",
			r.StartedAt.UnixNano(), r.FinishedAt.UnixNano(), r.Result,
			r.JobID, r.AppliedEventIDs, r.AppliedSeq)
	}
	var first string
	for iter := 0; iter < 5; iter++ {
		s := NewStore(int64(time.Hour))
		evs := mk()
		var wg sync.WaitGroup
		var mu sync.Mutex
		receipts := map[string]int{}
		for _, e := range evs {
			wg.Add(1)
			go func(e event.Envelope) {
				defer wg.Done()
				r := s.Ingest(context.Background(), e)
				mu.Lock()
				receipts[string(r.Disposition)]++
				mu.Unlock()
			}(e)
		}
		wg.Wait()
		got := snapshot(s)
		t.Logf("iter=%d concurrent receipts=%v aggregate=%s", iter, receipts, got)
		if first == "" {
			first = got
		} else if got != first {
			t.Fatalf("non-deterministic aggregate across concurrency:\n%s\n%s", first, got)
		}
		if receipts["DUPLICATE"] != 2 {
			t.Fatalf("want 2 duplicates, got %v", receipts)
		}
	}
	t.Logf("decision basis: mutex-serialized intake + canonical rebuild; stable snapshot=%s", first)
}
