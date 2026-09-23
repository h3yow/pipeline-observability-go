// Package testhelp provides shared logging helpers for the test suites.
package testhelp

import (
	"fmt"
	"testing"
	"time"

	"github.com/h3yow/pipeline-observability-go/event"
)

// LogRaw prints the raw input event into the test log.
func LogRaw(t *testing.T, prefix string, raw event.RawEvent, receivedAt time.Time) {
	t.Helper()
	seq, occ, att := "-", "-", "-"
	if raw.Seq != nil {
		seq = fmt.Sprintf("%d", *raw.Seq)
	}
	if raw.OccurredAt != nil {
		occ = raw.OccurredAt.UTC().Format(time.RFC3339Nano)
	}
	if raw.Attempt != nil {
		att = fmt.Sprintf("%d", *raw.Attempt)
	}
	t.Logf("INPUT  [%s] schema=%s event_id=%s run=%s job=%s seq=%s occurred=%s phase=%s status=%s attempt=%s host=%q received=%s",
		prefix, raw.SchemaVersion, raw.EventID, raw.RunID, raw.JobID, seq, occ,
		raw.Phase, raw.Status, att, raw.Host, receivedAt.UTC().Format(time.RFC3339Nano))
}

// Ptr helpers for building raw events.
func PtrInt64(v int64) *int64 { return &v }
func PtrInt(v int) *int       { return &v }
func PtrTime(v time.Time) *time.Time {
	u := v.UTC()
	return &u
}
