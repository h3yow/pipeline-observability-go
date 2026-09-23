package alert

import (
	"testing"
	"time"

	"github.com/h3yow/pipeline-observability-go/eval"
)

var t0 = time.Date(2026, 9, 23, 9, 0, 0, 0, time.UTC)

func verdict(job, run string, fresh, sla eval.Level) eval.Verdict {
	return eval.Verdict{
		JobID: job, RunID: run, RuleVersion: "rules/v1", SchemaVersion: "event/v1",
		EvaluatedAt: t0, Freshness: fresh, SLA: sla,
	}
}

func breach() eval.Verdict  { return verdict("jobA", "run1", eval.LevelBreach, eval.LevelOK) }
func okv() eval.Verdict     { return verdict("jobA", "run1", eval.LevelOK, eval.LevelOK) }
func unknown() eval.Verdict { return verdict("jobA", "run1", eval.LevelUnknown, eval.LevelUnknown) }

func logTransition(t *testing.T, i int, tr Transition, note string) {
	t.Helper()
	v := tr.Verdict
	t.Logf("INPUT  #%d [%s] freshness=%s sla=%s -> kind=%s phase=%s->%s incident=%s reason=%q",
		i, note, v.Freshness, v.SLA, tr.Kind, tr.FromPhase, tr.ToPhase, tr.IncidentID, tr.Reason)
}

// TestTriggerThenRecover covers the full lifecycle with policy (2,2).
func TestTriggerThenRecover(t *testing.T) {
	m := NewManager(Policy{TriggerAfter: 2, RecoverAfter: 2})
	steps := []struct {
		v        eval.Verdict
		wantKind TransitionKind
	}{
		{breach(), KindSuppressed}, // streak 1/2 -> PENDING
		{breach(), KindTriggered},  // streak 2/2 -> FIRING
		{breach(), KindSuppressed}, // continued breach: no duplicate alert
		{okv(), KindSuppressed},    // ok streak 1/2
		{okv(), KindRecovered},     // recovered
	}
	var incidentID string
	for i, st := range steps {
		tr, err := m.Observe(st.v, t0.Add(time.Duration(i)*time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		logTransition(t, i+1, tr, "lifecycle")
		if tr.Kind != st.wantKind {
			t.Fatalf("step %d: kind=%s want %s", i+1, tr.Kind, st.wantKind)
		}
		if i == 1 {
			incidentID = tr.IncidentID
			if incidentID == "" {
				t.Fatal("triggered transition must carry an incident id")
			}
		}
		if i == 4 {
			if tr.IncidentID != incidentID {
				t.Fatalf("recovery must reference original incident %s, got %s",
					incidentID, tr.IncidentID)
			}
		}
	}
	inc, ok := m.Incident("jobA")
	if !ok || inc.Phase != PhaseResolved {
		t.Fatalf("incident after recovery: %+v ok=%v", inc, ok)
	}
}

// TestJitterNearThresholdNeverAlerts: breach/ok flips while streak < 2.
func TestJitterNearThresholdNeverAlerts(t *testing.T) {
	m := NewManager(Policy{TriggerAfter: 2, RecoverAfter: 2})
	seq := []eval.Verdict{breach(), okv(), breach(), okv(), breach(), okv()}
	var alerts, recovs int
	for i, v := range seq {
		tr, err := m.Observe(v, t0.Add(time.Duration(i)*time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		logTransition(t, i+1, tr, "jitter")
		switch tr.Kind {
		case KindTriggered:
			alerts++
		case KindRecovered:
			recovs++
		}
	}
	if alerts != 0 || recovs != 0 {
		t.Fatalf("jitter produced alerts=%d recoveries=%d, want 0/0", alerts, recovs)
	}
	inc, exists := m.Incident("jobA")
	if exists {
		t.Fatalf("no incident should exist after jitter, got %+v", inc)
	}
}

// TestRecoveredThenRetriggerIsNewIncident proves recovery and re-trigger are
// distinguishable via distinct incident ids.
func TestRecoveredThenRetriggerIsNewIncident(t *testing.T) {
	m := NewManager(Policy{TriggerAfter: 1, RecoverAfter: 1})
	tr1, _ := m.Observe(breach(), t0)
	tr2, _ := m.Observe(okv(), t0.Add(time.Minute))
	tr3, _ := m.Observe(breach(), t0.Add(2*time.Minute))
	tr4, _ := m.Observe(okv(), t0.Add(3*time.Minute))
	for i, tr := range []Transition{tr1, tr2, tr3, tr4} {
		logTransition(t, i+1, tr, "retrigger")
	}
	if tr1.Kind != KindTriggered || tr2.Kind != KindRecovered ||
		tr3.Kind != KindTriggered || tr4.Kind != KindRecovered {
		t.Fatalf("unexpected lifecycle kinds: %s %s %s %s", tr1.Kind, tr2.Kind, tr3.Kind, tr4.Kind)
	}
	if tr1.IncidentID == tr3.IncidentID {
		t.Fatalf("re-trigger must open a NEW incident, both were %s", tr1.IncidentID)
	}
	if tr2.IncidentID != tr1.IncidentID || tr4.IncidentID != tr3.IncidentID {
		t.Fatal("recoveries must reference their own incident")
	}
	t.Logf("BASIS  incidents: first=%s recovered, second=%s recovered (distinct)",
		tr1.IncidentID, tr3.IncidentID)
}

// TestUnknownSignalFreezesLifecycle: no counters move, no transitions.
func TestUnknownSignalFreezesLifecycle(t *testing.T) {
	m := NewManager(Policy{TriggerAfter: 2, RecoverAfter: 2})
	m.Observe(breach(), t0) // streak 1/2 -> pending
	tr, err := m.Observe(unknown(), t0.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if tr.Kind != "" {
		t.Fatalf("UNKNOWN must emit nothing, got %s", tr.Kind)
	}
	tr, _ = m.Observe(breach(), t0.Add(2*time.Minute))
	logTransition(t, 1, tr, "after-unknown")
	if tr.Kind != KindTriggered {
		t.Fatalf("UNKNOWN must not reset streak; want TRIGGERED, got %s", tr.Kind)
	}
}

// TestBackwardsClockRejected enforces explainable ordering errors.
func TestBackwardsClockRejected(t *testing.T) {
	m := NewManager(DefaultPolicy())
	if _, err := m.Observe(breach(), t0.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Observe(breach(), t0); err == nil {
		t.Fatal("expected error on backwards event time")
	} else {
		t.Logf("BASIS  backwards time rejected: %v", err)
	}
}
