package ingest

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/pipeline-observability-go/internal/event"
	"github.com/h3yow/pipeline-observability-go/internal/model"
)

// TestPendingGapNotArrived: a finished run with no started event at all is
// pending with gap START_NOT_ARRIVED; its BasisGap propagates onto Run.
func TestPendingGapNotArrived(t *testing.T) {
	s := NewStore(int64(time.Hour))
	fin := finished("f1", ts(10), "rX", "SUCCESS")
	r := s.Ingest(context.Background(), fin)
	logReceipt(t, fin, r)

	run, ok := s.Run("rX")
	if !ok || !run.HasFinished || run.HasStarted {
		t.Fatalf("unexpected run: %+v", run)
	}
	if run.BasisGap == nil || run.BasisGap.Code != model.GapNotArrived {
		t.Fatalf("want START_NOT_ARRIVED gap, got %+v", run.BasisGap)
	}
	t.Logf("DECISION basis gap: code=%s detail=%q", run.BasisGap.Code, run.BasisGap.Detail)

	pending := s.PendingRuns()
	if len(pending) != 1 || pending[0].RunID != "rX" ||
		pending[0].Gap.Code != model.GapNotArrived || pending[0].Gap.EvidenceEventID != "" {
		t.Fatalf("pending view wrong: %+v", pending)
	}
}

// TestPendingGapDroppedLate: a started candidate that the watermark rejects
// explains the gap as START_DROPPED_LATE with the event id as evidence.
func TestPendingGapDroppedLate(t *testing.T) {
	s := NewStore(int64(time.Minute))
	// Advance the watermark with a far-later finished event of another run.
	lateAnchor := finished("anchor-f", ts(30), "anchor", "SUCCESS")
	r := s.Ingest(context.Background(), lateAnchor)
	logReceipt(t, lateAnchor, r)

	cand := started("lost-s", ts(0), "anchor")
	r = s.Ingest(context.Background(), cand)
	logReceipt(t, cand, r)
	if r.Disposition != DispositionLate {
		t.Fatalf("want LATE, got %s", r.Disposition)
	}

	pending := s.PendingRuns()
	var found *PendingRun
	for i := range pending {
		if pending[i].RunID == "anchor" {
			found = &pending[i]
		}
	}
	if found == nil {
		t.Fatalf("anchor run missing from pending view: %+v", pending)
	}
	if found.Gap.Code != model.GapDroppedLate || found.Gap.EvidenceEventID != "lost-s" {
		t.Fatalf("gap wrong: %+v", found.Gap)
	}
	t.Logf("DECISION gap=%s evidence=%s detail=%q",
		found.Gap.Code, found.Gap.EvidenceEventID, found.Gap.Detail)
}

// TestPendingGapDroppedInvalid: a malformed started candidate explains the
// gap as START_DROPPED_INVALID; a malformed *finished* candidate must not
// pollute the start-gap evidence.
func TestPendingGapDroppedInvalid(t *testing.T) {
	s := NewStore(int64(time.Hour))
	bad := started("bad-s", ts(0), "rY")
	bad.SchemaVersion = 9 // unknown version -> INVALID
	r := s.Ingest(context.Background(), bad)
	logReceipt(t, bad, r)
	if r.Disposition != DispositionInvalid {
		t.Fatalf("want INVALID, got %s", r.Disposition)
	}
	fin := finished("fY", ts(5), "rY", "SUCCESS")
	r = s.Ingest(context.Background(), fin)
	logReceipt(t, fin, r)

	pending := s.PendingRuns()
	if len(pending) != 1 || pending[0].Gap.Code != model.GapDroppedInvalid ||
		pending[0].Gap.EvidenceEventID != "bad-s" {
		t.Fatalf("pending wrong: %+v", pending)
	}

	// Repeated invalid delivery of the same id must not grow evidence.
	s.Ingest(context.Background(), bad)
	if got := s.PendingRuns(); len(got) != 1 ||
		got[0].Gap.Code != model.GapDroppedInvalid {
		t.Fatalf("evidence must stay stable on repeat: %+v", got)
	}
}

// TestPendingGapDisplaced: a started candidate rejected as CONFLICT (run id
// claimed by another job) explains the gap as START_DISPLACED.
func TestPendingGapDisplaced(t *testing.T) {
	s := NewStore(int64(time.Hour))
	// rZ starts life under job-a with a finished event only.
	fin := finished("fZ", ts(5), "rZ", "SUCCESS")
	s.Ingest(context.Background(), fin)
	// A started event for the SAME run id claims a different job -> CONFLICT.
	other := event.Envelope{
		EventID: "sZ", SchemaVersion: 1, Type: event.TypeStarted,
		OccurredAt: ts(0), RunID: "rZ", JobID: "job-b",
		Payload: map[string]any{"attempt": 1},
	}
	r := s.Ingest(context.Background(), other)
	logReceipt(t, other, r)
	if r.Disposition != DispositionConflict {
		t.Fatalf("want CONFLICT, got %s", r.Disposition)
	}
	pending := s.PendingRuns()
	if len(pending) != 1 || pending[0].Gap.Code != model.GapDisplaced ||
		pending[0].Gap.EvidenceEventID != "sZ" {
		t.Fatalf("pending wrong: %+v", pending)
	}
	t.Logf("DECISION gap=%s evidence=%s", pending[0].Gap.Code, pending[0].Gap.EvidenceEventID)
}

// TestPendingViewStableUnderChaos: the same input set (incl. duplicates),
// fed concurrently in shuffled order repeatedly, always yields the same
// pending set with the same gap reasons.
func TestPendingViewStableUnderChaos(t *testing.T) {
	evs := []event.Envelope{
		// p1: nothing ever arrives for the start -> NOT_ARRIVED
		finished("fp1", ts(11), "p1", "SUCCESS"),
		// p2: malformed started -> DROPPED_INVALID
		func() event.Envelope {
			e := started("sp2-bad", ts(0), "p2")
			e.SchemaVersion = 3
			return e
		}(),
		finished("fp2", ts(12), "p2", "FAILED"),
		// ok-run: complete, must never appear in the pending view
		started("so", ts(0), "ok-run"),
		finished("fo", ts(13), "ok-run", "SUCCESS"),
	}
	snapshot := func(seed int64) string {
		s := NewStore(int64(time.Hour))
		batch := append([]event.Envelope{}, evs...)
		batch = append(batch, evs...) // duplicate every envelope
		r := rand.New(rand.NewSource(seed))
		r.Shuffle(len(batch), func(i, j int) { batch[i], batch[j] = batch[j], batch[i] })
		start := make(chan struct{})
		var wg sync.WaitGroup
		for _, e := range batch {
			wg.Add(1)
			go func(e event.Envelope) {
				defer wg.Done()
				<-start
				s.Ingest(context.Background(), e)
			}(e)
		}
		close(start)
		wg.Wait()
		out := ""
		for _, p := range s.PendingRuns() {
			out += fmt.Sprintf("%s:%s:%s|", p.RunID, p.Gap.Code, p.Gap.EvidenceEventID)
		}
		return out
	}
	first := snapshot(1)
	t.Logf("INPUT events=%d; STABLE pending view=%s", len(evs), first)
	if first != "p1:START_NOT_ARRIVED:|p2:START_DROPPED_INVALID:sp2-bad|" {
		t.Fatalf("unexpected pending snapshot: %q", first)
	}
	for _, seed := range []int64{2, 3, 4, 5} {
		if got := snapshot(seed); got != first {
			t.Fatalf("pending view not stable under chaos:\n%s\n%s", first, got)
		}
	}
}

// TestPendingRunsPagination: pages scroll deterministically and cover the
// whole set without dropping or duplicating rows.
func TestPendingRunsPagination(t *testing.T) {
	s := NewStore(int64(time.Hour))
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("run-%d", i)
		s.Ingest(context.Background(), finished("f"+id, ts(i+1), id, "SUCCESS"))
	}
	var got []string
	cursor := ""
	for {
		page := s.PendingRunsPage(cursor, 2)
		if len(page) == 0 {
			break
		}
		for _, p := range page {
			got = append(got, p.RunID)
		}
		cursor = page[len(page)-1].RunID
	}
	want := []string{"run-0", "run-1", "run-2", "run-3", "run-4"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("pagination = %v, want %v", got, want)
	}
	t.Logf("DECISION paged through pending runs: %v", got)
}

// TestBackfillRepairsRunAndIsIdempotent: backfilling a late-rejected start
// bypasses the watermark, repairs the aggregate canonically, removes the run
// from the pending view, and repeated/concurrent backfill is a no-op.
func TestBackfillRepairsRunAndIsIdempotent(t *testing.T) {
	s := NewStore(int64(time.Minute))
	anchor := finished("anchor-f", ts(40), "b1", "SUCCESS")
	s.Ingest(context.Background(), anchor)
	lost := started("lost-start", ts(0), "b1")
	if r := s.Ingest(context.Background(), lost); r.Disposition != DispositionLate {
		t.Fatalf("seed: want LATE, got %s", r.Disposition)
	}
	if len(s.PendingRuns()) != 1 {
		t.Fatalf("seed: want 1 pending run")
	}

	// Explicit backfill bypasses the lateness window.
	r := s.Backfill(context.Background(), lost)
	logReceipt(t, lost, r)
	if r.Disposition != DispositionBackfilled || !r.Accepted {
		t.Fatalf("want BACKFILLED, got %s (%s)", r.Disposition, r.Reason)
	}
	run, _ := s.Run("b1")
	if !run.HasStarted || run.BasisGap != nil || run.Duration() != 40*time.Minute {
		t.Fatalf("repaired run wrong: %+v duration=%s", run, run.Duration())
	}
	if len(s.PendingRuns()) != 0 {
		t.Fatalf("repaired run must leave the pending view: %+v", s.PendingRuns())
	}
	t.Logf("DECISION repaired: started=%s finished=%s duration=%s events=%v",
		run.StartedAt.Format("15:04"), run.FinishedAt.Format("15:04"),
		run.Duration(), run.AppliedEventIDs)

	// Repeated backfill is a duplicate no-op (no second application).
	if r2 := s.Backfill(context.Background(), lost); r2.Disposition != DispositionDuplicate {
		t.Fatalf("repeat backfill want DUPLICATE, got %s", r2.Disposition)
	}
	run2, _ := s.Run("b1")
	if len(run2.AppliedEventIDs) != 2 {
		t.Fatalf("duplicate backfill changed aggregate: %v", run2.AppliedEventIDs)
	}

	// Concurrent duplicate backfills: exactly one accepted.
	s2 := NewStore(int64(time.Minute))
	s2.Ingest(context.Background(), anchor)
	var wg sync.WaitGroup
	start := make(chan struct{})
	var mu sync.Mutex
	disps := map[Disposition]int{}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rec := s2.Backfill(context.Background(), lost)
			mu.Lock()
			disps[rec.Disposition]++
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	if disps[DispositionBackfilled] != 1 || disps[DispositionDuplicate] != 7 {
		t.Fatalf("concurrent backfill dispositions wrong: %v", disps)
	}
	t.Logf("DECISION concurrent backfill dispositions: %v", disps)
}

// TestBackfillDoesNotOverwrite: once a started phase is occupied, backfilling
// a different started event is CONFLICT and never overwrites state.
func TestBackfillDoesNotOverwrite(t *testing.T) {
	s := NewStore(int64(time.Hour))
	s.Ingest(context.Background(), started("s1", ts(0), "r9"))
	s.Ingest(context.Background(), finished("f1", ts(5), "r9", "SUCCESS"))
	intruder := started("s-intruder", ts(0), "r9") // same run/phase, other id
	r := s.Backfill(context.Background(), intruder)
	logReceipt(t, intruder, r)
	if r.Disposition != DispositionConflict {
		t.Fatalf("want CONFLICT, got %s", r.Disposition)
	}
	run, _ := s.Run("r9")
	if len(run.AppliedEventIDs) != 2 {
		t.Fatalf("backfill must not overwrite: %v", run.AppliedEventIDs)
	}
}
