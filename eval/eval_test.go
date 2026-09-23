package eval

import (
	"strings"
	"testing"
	"time"

	"github.com/h3yow/pipeline-observability-go/event"
	"github.com/h3yow/pipeline-observability-go/ingest"
	"github.com/h3yow/pipeline-observability-go/rule"
)

var t0 = time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC)

func logBasis(t *testing.T, v Verdict) {
	t.Helper()
	t.Logf("VERDICT job=%s run=%s rules=%s schema=%s freshness=%s sla=%s",
		v.JobID, v.RunID, v.RuleVersion, v.SchemaVersion, v.Freshness, v.SLA)
	for _, b := range v.FreshnessBasis {
		t.Logf("BASIS  [freshness] check=%s pass=%v actual=%q want=%q", b.Check, b.Pass, b.Actual, b.Want)
	}
	for _, b := range v.SLABasis {
		t.Logf("BASIS  [sla]       check=%s pass=%v actual=%q want=%q", b.Check, b.Pass, b.Actual, b.Want)
	}
}

func startedState(schema string) ingest.RunState {
	return ingest.RunState{
		RunID: "r1", JobID: "jobA", SchemaVersion: schema,
		StartSeq: 1, StartedAt: t0, Attempt: attemptOf(schema),
		Host: hostOf(schema),
	}
}

func finishedState(schema string, status event.Status, dur time.Duration, attempt int) ingest.RunState {
	st := startedState(schema)
	st.Closed = true
	st.EndSeq = 2
	st.FinishedAt = t0.Add(dur)
	st.Status = status
	st.Attempt = attempt
	return st
}

func attemptOf(schema string) int {
	if schema == event.SchemaV2 {
		return 1
	}
	return 1
}
func hostOf(schema string) string {
	if schema == event.SchemaV2 {
		return "host-1"
	}
	return ""
}

func TestV1FreshnessThreshold(t *testing.T) {
	r, err := rule.NewV1("jobA", rule.V1{
		FreshnessLimit: 30 * time.Minute, MaxDuration: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	ev, _ := NewEvaluator(r)

	st := startedState(event.SchemaV1)
	vFresh, err := ev.Evaluate(st, t0.Add(29*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	logBasis(t, vFresh)
	if vFresh.Freshness != LevelOK {
		t.Fatalf("freshness just inside limit: got %s want OK", vFresh.Freshness)
	}

	vBreach, err := ev.Evaluate(st, t0.Add(31*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	logBasis(t, vBreach)
	if vBreach.Freshness != LevelBreach {
		t.Fatalf("freshness past limit: got %s want BREACH", vBreach.Freshness)
	}
}

func TestV1SLARules(t *testing.T) {
	r, _ := rule.NewV1("jobA", rule.V1{
		FreshnessLimit: time.Hour, MaxDuration: 30 * time.Minute,
	})
	ev, _ := NewEvaluator(r)

	cases := []struct {
		name    string
		status  event.Status
		dur     time.Duration
		wantSLA Level
	}{
		{"success in time", event.StatusSuccess, 20 * time.Minute, LevelOK},
		{"success too slow", event.StatusSuccess, 40 * time.Minute, LevelBreach},
		{"failed always breaches", event.StatusFailed, 1 * time.Minute, LevelBreach},
		{"timeout breaches under v1", event.StatusTimeout, 1 * time.Minute, LevelBreach},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := finishedState(event.SchemaV1, tc.status, tc.dur, 1)
			v, err := ev.Evaluate(st, t0.Add(tc.dur))
			if err != nil {
				t.Fatal(err)
			}
			logBasis(t, v)
			if v.SLA != tc.wantSLA {
				t.Fatalf("SLA: got %s want %s", v.SLA, tc.wantSLA)
			}
		})
	}
}

func TestUnstartedRunIsUnknown(t *testing.T) {
	r, _ := rule.NewV1("jobA", rule.V1{
		FreshnessLimit: time.Hour, MaxDuration: time.Hour,
	})
	ev, _ := NewEvaluator(r)
	v, err := ev.Evaluate(ingest.RunState{RunID: "r1", JobID: "jobA", SchemaVersion: event.SchemaV1}, t0)
	if err != nil {
		t.Fatal(err)
	}
	logBasis(t, v)
	if v.Freshness != LevelUnknown || v.SLA != LevelUnknown {
		t.Fatalf("empty run must be UNKNOWN/UNKNOWN, got %s/%s", v.Freshness, v.SLA)
	}
}

func TestV2TimeoutCanMeetSLA(t *testing.T) {
	r, _ := rule.NewV2("jobA", rule.V2{
		FreshnessLimit:     time.Hour,
		MaxDuration:        30 * time.Minute,
		TimeoutMaxDuration: 10 * time.Minute,
		MaxAttempts:        2,
	})
	ev, _ := NewEvaluator(r)

	fastTimeout := finishedState(event.SchemaV2, event.StatusTimeout, 5*time.Minute, 1)
	v, _ := ev.Evaluate(fastTimeout, t0.Add(5*time.Minute))
	logBasis(t, v)
	if v.SLA != LevelOK {
		t.Fatalf("fast timeout meets v2 SLA: got %s want OK", v.SLA)
	}

	slowTimeout := finishedState(event.SchemaV2, event.StatusTimeout, 15*time.Minute, 1)
	v2, _ := ev.Evaluate(slowTimeout, t0.Add(15*time.Minute))
	logBasis(t, v2)
	if v2.SLA != LevelBreach {
		t.Fatalf("slow timeout breaches: got %s want BREACH", v2.SLA)
	}

	retried := finishedState(event.SchemaV2, event.StatusSuccess, 10*time.Minute, 3)
	v3, _ := ev.Evaluate(retried, t0.Add(10*time.Minute))
	logBasis(t, v3)
	if v3.SLA != LevelBreach {
		t.Fatalf("too many attempts breach: got %s want BREACH", v3.SLA)
	}
}

func TestV2RulesRejectV1Events(t *testing.T) {
	r, _ := rule.NewV2("jobA", rule.V2{
		FreshnessLimit: time.Hour, MaxDuration: time.Hour,
		TimeoutMaxDuration: time.Hour, MaxAttempts: 2,
	})
	ev, _ := NewEvaluator(r)
	_, err := ev.Evaluate(finishedState(event.SchemaV1, event.StatusSuccess, time.Minute, 1), t0)
	if err == nil || !strings.Contains(err.Error(), "require event/v2") {
		t.Fatalf("expected explicit schema mismatch error, got %v", err)
	}
	t.Logf("BASIS  version mismatch rejected explicitly: %v", err)
}

// TestHistoricalReproduction proves the same historical state evaluated under
// two rule versions can legitimately produce two different verdicts, and that
// repeating evaluation reproduces each verdict exactly.
func TestHistoricalReproduction(t *testing.T) {
	r1, _ := rule.NewV1("jobA", rule.V1{
		FreshnessLimit: time.Hour, MaxDuration: 30 * time.Minute,
	})
	r2, _ := rule.NewV2("jobA", rule.V2{
		FreshnessLimit: time.Hour, MaxDuration: 30 * time.Minute,
		TimeoutMaxDuration: 30 * time.Minute, MaxAttempts: 2,
	})
	ev1, _ := NewEvaluator(r1)
	ev2, _ := NewEvaluator(r2)

	// A 20-minute TIMEOUT run on event/v2 data.
	hist := finishedState(event.SchemaV2, event.StatusTimeout, 20*time.Minute, 1)
	at := t0.Add(20 * time.Minute)

	// Old rules/v1 applied to equivalent v1 historical data: breach.
	histV1 := finishedState(event.SchemaV1, event.StatusTimeout, 20*time.Minute, 1)
	vOld1, err := ev1.Evaluate(histV1, at)
	if err != nil {
		t.Fatal(err)
	}
	vOld2, _ := ev1.Evaluate(histV1, at)
	logBasis(t, vOld1)
	if vOld1.SLA != LevelBreach || vOld1.SLA != vOld2.SLA {
		t.Fatalf("v1 historical verdict not reproducible: %s vs %s", vOld1.SLA, vOld2.SLA)
	}

	// New rules/v2 on v2 data: tolerated timeout -> OK.
	vNew1, err := ev2.Evaluate(hist, at)
	if err != nil {
		t.Fatal(err)
	}
	vNew2, _ := ev2.Evaluate(hist, at)
	logBasis(t, vNew1)
	if vNew1.SLA != LevelOK || vNew1.SLA != vNew2.SLA {
		t.Fatalf("v2 historical verdict not reproducible: %s vs %s", vNew1.SLA, vNew2.SLA)
	}
	if vOld1.SLA == vNew1.SLA {
		t.Fatal("rule versions must be able to yield different historical verdicts")
	}
	t.Log("BASIS  old rules/v1 reproduce BREACH; new rules/v2 reproduce OK for the same timeout run")
}
