package ingest

import (
	"testing"
	"time"

	"github.com/h3yow/pipeline-observability-go/event"
	"github.com/h3yow/pipeline-observability-go/internal/testhelp"
)

var base = time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)

func startedV1(id string, seq int64, occ time.Time) event.RawEvent {
	return event.RawEvent{
		SchemaVersion: event.SchemaV1,
		EventID:       id, RunID: "run1", JobID: "jobA",
		Seq: testhelp.PtrInt64(seq), OccurredAt: testhelp.PtrTime(occ),
		Phase: "STARTED",
	}
}

func finishedV1(id string, seq int64, occ time.Time, status event.Status) event.RawEvent {
	r := startedV1(id, seq, occ)
	r.Phase = "FINISHED"
	r.Status = string(status)
	return r
}

func ingestLogged(t *testing.T, a *Aggregator, raw event.RawEvent, recv time.Time, want Reason) Decision {
	t.Helper()
	testhelp.LogRaw(t, string(want), raw, recv)
	d, err := a.Ingest(raw, recv)
	if err != nil {
		t.Fatalf("ingest error: %v", err)
	}
	if d.Reason != want {
		t.Fatalf("decision reason: got %s want %s (%s)", d.Reason, want, d.Detail)
	}
	t.Logf("DECISION event=%s applied=%-5v reason=%s detail=%s",
		d.EventID, d.Applied, d.Reason, d.Detail)
	return d
}

func TestInOrderAggregation(t *testing.T) {
	a := New(Config{MaxLateness: time.Minute})
	ingestLogged(t, a, startedV1("e1", 1, base), base.Add(time.Second), ReasonApplied)
	ingestLogged(t, a, finishedV1("e2", 2, base.Add(10*time.Second), event.StatusSuccess),
		base.Add(11*time.Second), ReasonApplied)

	st, ok := a.State("run1")
	if !ok {
		t.Fatal("run missing")
	}
	if !st.Closed || st.Status != event.StatusSuccess ||
		!st.StartedAt.Equal(base) || !st.FinishedAt.Equal(base.Add(10*time.Second)) {
		t.Fatalf("bad aggregated state: %+v", st)
	}
	t.Logf("BASIS  aggregated state: started=%s finished=%s status=%s closed=%v",
		st.StartedAt, st.FinishedAt, st.Status, st.Closed)
}

func TestDuplicateDeliveryIsStable(t *testing.T) {
	a := New(Config{MaxLateness: time.Minute})
	raw := startedV1("e1", 1, base)
	ingestLogged(t, a, raw, base.Add(time.Second), ReasonApplied)
	// identical redelivery, even later: must be DUPLICATE and change nothing.
	for i := 0; i < 3; i++ {
		ingestLogged(t, a, raw, base.Add(time.Hour), ReasonDuplicate)
	}
	st, _ := a.State("run1")
	if !st.StartedAt.Equal(base) {
		t.Fatalf("state changed by duplicate: %+v", st)
	}
}

func TestOutOfOrderLowerSeqFillsGap(t *testing.T) {
	// FINISHED (seq 2) arrives before STARTED (seq 1): start is missing.
	a := New(Config{MaxLateness: time.Hour})
	ingestLogged(t, a, finishedV1("e2", 2, base.Add(10*time.Second), event.StatusSuccess),
		base.Add(11*time.Second), ReasonApplied)
	ingestLogged(t, a, startedV1("e1", 1, base), base.Add(12*time.Second), ReasonAppliedLate)
	st, _ := a.State("run1")
	if !st.Closed || st.StartedAt.IsZero() || st.StartSeq != 1 || st.EndSeq != 2 {
		t.Fatalf("gap not merged: %+v", st)
	}
	t.Logf("BASIS  after late merge: startSeq=%d endSeq=%d started=%s",
		st.StartSeq, st.EndSeq, st.StartedAt)

	// Another redundant lower-seq event now carries nothing new: STALE.
	dup := startedV1("e1b", 1, base)
	ingestLogged(t, a, dup, base.Add(13*time.Second), ReasonStale)
}

func TestTooLateDropped(t *testing.T) {
	a := New(Config{MaxLateness: 30 * time.Second})
	ingestLogged(t, a, finishedV1("e2", 2, base.Add(10*time.Second), event.StatusSuccess),
		base.Add(11*time.Second), ReasonApplied)
	// STARTED is 10s behind watermark but received 5min late: within horizon.
	ingestLogged(t, a, startedV1("e1", 1, base), base.Add(5*time.Minute), ReasonAppliedLate)

	a2 := New(Config{MaxLateness: 5 * time.Second})
	ingestLogged(t, a2, finishedV1("e2", 2, base.Add(10*time.Second), event.StatusSuccess),
		base.Add(11*time.Second), ReasonApplied)
	// 10s behind watermark, horizon 5s.
	ingestLogged(t, a2, startedV1("e1", 1, base), base.Add(12*time.Second), ReasonTooLate)
}

func TestClosedRunDoesNotReopen(t *testing.T) {
	a := New(Config{MaxLateness: time.Hour})
	ingestLogged(t, a, startedV1("e1", 1, base), base.Add(time.Second), ReasonApplied)
	ingestLogged(t, a, finishedV1("e2", 2, base.Add(10*time.Second), event.StatusFailed),
		base.Add(11*time.Second), ReasonApplied)
	// Any later seq (e.g. a phantom post-terminal event) cannot reopen.
	later := startedV1("e3", 3, base.Add(20*time.Second))
	ingestLogged(t, a, later, base.Add(21*time.Second), ReasonClosed)
	st, _ := a.State("run1")
	if st.Status != event.StatusFailed || !st.Closed {
		t.Fatalf("terminal state changed: %+v", st)
	}
}

func TestSameSeqDifferentContentSuperseded(t *testing.T) {
	a := New(Config{MaxLateness: time.Hour})
	ingestLogged(t, a, finishedV1("e2", 2, base.Add(10*time.Second), event.StatusSuccess),
		base.Add(11*time.Second), ReasonApplied)
	// Same seq 2 but claiming FAILED: first write wins.
	ingestLogged(t, a, finishedV1("e2-other", 2, base.Add(10*time.Second), event.StatusFailed),
		base.Add(12*time.Second), ReasonSuperseded)
	st, _ := a.State("run1")
	if st.Status != event.StatusSuccess {
		t.Fatalf("superseded content overwrote state: %s", st.Status)
	}
}

func TestRejectedInvalidEvent(t *testing.T) {
	a := New(Config{})
	bad := startedV1("e-bad", 1, base)
	bad.SchemaVersion = "event/v42"
	ingestLogged(t, a, bad, base.Add(time.Second), ReasonRejected)
	if len(a.Runs()) != 0 {
		t.Fatalf("rejected event created run state: %v", a.Runs())
	}
}
