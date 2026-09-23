package event

import (
	"errors"
	"testing"
	"time"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("bad time %q: %v", s, err)
	}
	return tm
}

func TestValidateV1(t *testing.T) {
	base := func() Envelope {
		return Envelope{
			EventID:       "e1",
			SchemaVersion: 1,
			Type:          TypeFinished,
			OccurredAt:    mustTime(t, "2026-09-23T10:00:00Z"),
			RunID:         "r1",
			JobID:         "job-a",
			Payload:       map[string]any{"result": "SUCCESS"},
		}
	}

	cases := []struct {
		name    string
		mutate  func(*Envelope)
		wantErr error
		want    func(*DecodedEvent) bool
	}{
		{
			name: "finished success, attempt defaults to 1",
			want: func(d *DecodedEvent) bool {
				return d.Result == "SUCCESS" && d.Attempt == 1 && !d.HasAttempt
			},
		},
		{
			name:    "unknown schema version rejected",
			mutate:  func(e *Envelope) { e.SchemaVersion = 99 },
			wantErr: ErrUnknownVersion,
		},
		{
			name:    "missing result rejected, not defaulted",
			mutate:  func(e *Envelope) { delete(e.Payload, "result") },
			wantErr: ErrMissingField,
		},
		{
			name:    "illegal result rejected",
			mutate:  func(e *Envelope) { e.Payload["result"] = "MAYBE" },
			wantErr: ErrBadField,
		},
		{
			name:    "zero attempt rejected",
			mutate:  func(e *Envelope) { e.Payload["attempt"] = 0 },
			wantErr: ErrBadField,
		},
		{
			name: "explicit attempt preserved",
			mutate: func(e *Envelope) {
				e.Payload["attempt"] = 3
			},
			want: func(d *DecodedEvent) bool { return d.Attempt == 3 && d.HasAttempt },
		},
		{
			name:    "missing eventId rejected",
			mutate:  func(e *Envelope) { e.EventID = "" },
			wantErr: ErrMissingField,
		},
		{
			name:    "missing occurredAt rejected",
			mutate:  func(e *Envelope) { e.OccurredAt = time.Time{} },
			wantErr: ErrMissingField,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := base()
			if tc.mutate != nil {
				tc.mutate(&env)
			}
			t.Logf("input event: %+v payload=%v", env, env.Payload)
			got, err := env.Validate()
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("want error %v, got %v", tc.wantErr, err)
				}
				t.Logf("decision: REJECTED reason=%v", err)
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			t.Logf("decision: ACCEPTED decoded=%+v", got)
			if tc.want != nil && !tc.want(got) {
				t.Fatalf("decoded event mismatch: %+v", got)
			}
		})
	}
}

func TestValidateV1Started(t *testing.T) {
	env := Envelope{
		EventID:       "e2",
		SchemaVersion: 1,
		Type:          TypeStarted,
		OccurredAt:    mustTime(t, "2026-09-23T09:00:00Z"),
		RunID:         "r2",
		JobID:         "job-b",
		Payload:       map[string]any{},
	}
	t.Logf("input event: %+v", env)
	d, err := env.Validate()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Attempt != 1 {
		t.Fatalf("default attempt = %d, want 1", d.Attempt)
	}
	t.Logf("decision: ACCEPTED attempt defaulted per V1 schema: %+v", d)
}
