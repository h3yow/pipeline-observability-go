package event

import (
	"errors"
	"testing"
	"time"
)

func baseRaw() RawEvent {
	t := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	seq := int64(1)
	return RawEvent{
		SchemaVersion: SchemaV1,
		EventID:       "e1",
		RunID:         "r1",
		JobID:         "jobA",
		Seq:           &seq,
		OccurredAt:    &t,
		Phase:         "STARTED",
	}
}

func TestDecodeV1NormalizesAttempt(t *testing.T) {
	raw := baseRaw()
	t.Logf("INPUT  %+v", raw)
	ev, err := Decode(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Attempt != 1 {
		t.Fatalf("v1 attempt: got %d want 1 (schema-defined normalization)", ev.Attempt)
	}
	t.Logf("BASIS  decoded=%+v -> attempt normalized to 1 by event/v1", ev)
}

func TestDecodeV2RequiresHostAndAttempt(t *testing.T) {
	raw := baseRaw()
	raw.SchemaVersion = SchemaV2
	t.Logf("INPUT  v2 without host/attempt: %+v", raw)
	if _, err := Decode(raw); err == nil {
		t.Fatal("expected error for missing attempt")
	} else {
		t.Logf("BASIS  rejected: %v", err)
		var fe *FieldError
		if !errors.As(err, &fe) || fe.Field != "attempt" {
			t.Fatalf("want FieldError on attempt, got %v", err)
		}
	}

	att := 1
	raw.Attempt = &att
	if _, err := Decode(raw); err == nil {
		t.Fatal("expected error for missing host")
	} else {
		t.Logf("BASIS  rejected: %v", err)
	}
}

func TestDecodeUnknownVersionAndMissingFields(t *testing.T) {
	cases := []struct {
		name  string
		mut   func(*RawEvent)
		field string
	}{
		{"unknown version", func(r *RawEvent) { r.SchemaVersion = "event/v99" }, "schema_version"},
		{"missing event id", func(r *RawEvent) { r.EventID = "" }, "event_id"},
		{"missing seq", func(r *RawEvent) { r.Seq = nil }, "seq"},
		{"missing occurred_at", func(r *RawEvent) { r.OccurredAt = nil }, "occurred_at"},
		{"missing phase", func(r *RawEvent) { r.Phase = "" }, "phase"},
		{"finished without status", func(r *RawEvent) {
			r.Phase = "FINISHED"
		}, "status"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := baseRaw()
			tc.mut(&raw)
			t.Logf("INPUT  case=%s raw=%+v", tc.name, raw)
			_, err := Decode(raw)
			if err == nil {
				t.Fatalf("expected FieldError on %q", tc.field)
			}
			var fe *FieldError
			if !errors.As(err, &fe) {
				t.Fatalf("want *FieldError, got %T: %v", err, err)
			}
			if fe.Field != tc.field {
				t.Fatalf("field: got %q want %q", fe.Field, tc.field)
			}
			t.Logf("BASIS  rejected as expected: %v", err)
		})
	}
}

func TestDecodeV2FinishedStatuses(t *testing.T) {
	for _, st := range []Status{StatusSuccess, StatusFailed, StatusTimeout} {
		raw := baseRaw()
		raw.SchemaVersion = SchemaV2
		raw.Phase = string(PhaseFinished)
		raw.Status = string(st)
		att := 3
		raw.Attempt = &att
		raw.Host = "host-1"
		t.Logf("INPUT  status=%s raw_event_id=%s", st, raw.EventID)
		ev, err := Decode(raw)
		if err != nil {
			t.Fatalf("status %s: %v", st, err)
		}
		if ev.Status != st || ev.Host != "host-1" || ev.Attempt != 3 {
			t.Fatalf("unexpected decoded event: %+v", ev)
		}
	}
}
