package model

import (
	"testing"
	"time"
)

func TestRunDurationAndComplete(t *testing.T) {
	t0 := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	open := RunState{StartedAt: t0, HasStarted: true}
	t.Logf("INPUT open run HasStarted=true HasFinished=false | IsComplete=%v Duration=%v",
		open.IsComplete(), open.Duration())
	if open.IsComplete() || open.Duration() != 0 {
		t.Fatal("open run must be incomplete with zero duration")
	}
	done := RunState{
		StartedAt: t0, FinishedAt: t0.Add(12 * time.Minute),
		HasStarted: true, HasFinished: true, Result: ResultSuccess,
	}
	t.Logf("INPUT finished run start=%s finish=%s | IsComplete=%v Duration=%v",
		done.StartedAt.Format("15:04"), done.FinishedAt.Format("15:04"),
		done.IsComplete(), done.Duration())
	if !done.IsComplete() || done.Duration() != 12*time.Minute {
		t.Fatalf("complete run duration = %v", done.Duration())
	}
	// Defensive: negative duration (clock skew in source data) clamps to 0.
	bad := RunState{
		StartedAt: t0.Add(time.Hour), FinishedAt: t0,
		HasStarted: true, HasFinished: true,
	}
	if bad.Duration() != 0 {
		t.Fatalf("negative duration must clamp to zero, got %v", bad.Duration())
	}
	t.Logf("decision basis: duration=FinishedAt-StartedAt only when both phases applied; negative clamped")
}
