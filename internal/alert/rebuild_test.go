package alert

import (
	"testing"
	"time"

	"github.com/h3yow/pipeline-observability-go/internal/model"
)

func rebuildAt(day, hour, min int) int64 {
	return time.Date(2026, 9, day, hour, min, 0, 0, time.UTC).UnixNano()
}

// summarizeEvents renders the full transition log in a way that captures
// identity, order, seq and explainability — compared across paths.
func summarizeEvents(evs []Transition) string {
	out := ""
	for _, tr := range evs {
		out += "[" + string(tr.Alert.Kind) + " " +
			string(tr.Alert.JobID) + " run=" + string(tr.RunID) +
			" " + string(tr.FromState) + "->" + string(tr.ToState) +
			"#" + itoa(tr.Alert.Seq) + " " + tr.Alert.Reason + "] "
	}
	return out
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// TestRebuildEquivalentToIncremental: inserting an earlier recovered run via
// Rebuild (all runs re-folded in canonical order) yields the exact same
// transition log as feeding the same runs in order through Observe.
func TestRebuildEquivalentToIncremental(t *testing.T) {
	// job-a SLA: fire after 2 breaches, recover after 1 pass.
	// run-1 pass 09:05; run-2 breach 10:15; run-3 breach 11:20 (FIRE);
	// run-4 pass 12:05 (RECOVER).
	day := 23
	type row struct {
		run    string
		min    [3]int // day-hour-min passed below; use helper explicitly
		breach bool
		at     int64
	}
	rows := []row{
		{"run-1", [3]int{}, false, rebuildAt(day, 9, 5)},
		{"run-2", [3]int{}, true, rebuildAt(day, 10, 15)},
		{"run-3", [3]int{}, true, rebuildAt(day, 11, 20)},
		{"run-4", [3]int{}, false, rebuildAt(day, 12, 5)},
	}
	kind := model.KindSLA
	job := model.JobID("job-a")

	// Path A: incremental, in order.
	inc := NewManager()
	for _, r := range rows {
		tr := inc.Observe(r.run+":"+string(kind), job, kind, r.breach,
			1, 2, 1, r.at, "sla basis")
		if tr != nil {
			t.Logf("INCREMENTAL %s -> %s seq=%d", r.run, tr.ToState, tr.Alert.Seq)
		}
	}

	// Path B: first observe WITHOUT run-1 (simulating it lost), then Rebuild
	// with the full canonical set (simulating backfill).
	rec := NewManager()
	for _, r := range rows[1:] {
		rec.Observe(r.run+":"+string(kind), job, kind, r.breach, 1, 2, 1, r.at, "sla basis")
	}
	var obs []Observation
	for _, r := range rows {
		obs = append(obs, Observation{
			RunID: model.RunID(r.run), JobID: job, Kind: kind, Breach: r.breach,
			RuleVer: 1, FireAfterN: 2, RecoverAfterN: 1, At: r.at, Evidence: "sla basis",
		})
	}
	got := rec.Rebuild(obs)
	for _, tr := range got {
		t.Logf("REBUILT     %s -> %s seq=%d", tr.RunID, tr.ToState, tr.Alert.Seq)
	}

	a, b := summarizeEvents(inc.Events()), summarizeEvents(rec.Events())
	if a != b {
		t.Fatalf("rebuilt log differs from incremental:\nA: %s\nB: %s", a, b)
	}
	if st, _ := rec.State(job, kind); st != model.AlertStateOK {
		t.Fatalf("post-rebuild state = %s, want OK", st)
	}
	t.Logf("EQUIVALENT canonical log: %s", a)
}

// TestRebuildIdempotent: rebuilding the same set twice changes nothing and
// never duplicates transitions.
func TestRebuildIdempotent(t *testing.T) {
	job := model.JobID("job-b")
	kind := model.KindFreshness
	m := NewManager()
	m.Observe("r1:"+string(kind), job, kind, true, 1, 1, 1, rebuildAt(23, 9, 5), "x")
	m.Observe("r2:"+string(kind), job, kind, false, 1, 1, 1, rebuildAt(23, 10, 5), "x")
	obs := []Observation{
		{RunID: "r1", JobID: job, Kind: kind, Breach: true, RuleVer: 1, FireAfterN: 1, RecoverAfterN: 1, At: rebuildAt(23, 9, 5), Evidence: "x"},
		{RunID: "r2", JobID: job, Kind: kind, Breach: false, RuleVer: 1, FireAfterN: 1, RecoverAfterN: 1, At: rebuildAt(23, 10, 5), Evidence: "x"},
	}
	first := summarizeEvents(m.Events())
	m.Rebuild(obs)
	second := summarizeEvents(m.Events())
	m.Rebuild(obs)
	third := summarizeEvents(m.Events())
	if first != second || second != third {
		t.Fatalf("rebuild not idempotent:\n%s\n%s\n%s", first, second, third)
	}
	t.Logf("STABLE under repeated rebuild: %s", third)
}
