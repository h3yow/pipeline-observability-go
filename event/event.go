// Package event defines versioned pipeline run events.
//
// Every event carries an explicit SchemaVersion. Decoders in this package
// never fill missing fields with defaults: a structurally invalid event
// (unknown version or missing required field) is rejected with an error so
// that downstream components can explain why the input was not applied.
package event

import (
	"fmt"
	"time"
)

// Schema versions supported by this build.
const (
	SchemaV1 = "event/v1"
	SchemaV2 = "event/v2"
)

// Event is the decoded, validated representation of a job run event.
type Event struct {
	// SchemaVersion identifies the wire format; only SchemaV1/SchemaV2 accepted.
	SchemaVersion string
	// EventID uniquely identifies a single physical event delivery (dedup key).
	EventID string
	// RunID identifies the job run this event belongs to.
	RunID string
	// JobID identifies the job.
	JobID string
	// Seq is the monotonically increasing sequence number of the event within a run.
	Seq int64
	// OccurredAt is when the event factually happened (event time).
	OccurredAt time.Time
	// Phase is the lifecycle phase reported by the event.
	Phase Phase
	// Status is the terminal status, empty unless Phase == PhaseFinished.
	Status Status
	// Attempt counts execution attempts (>=1); v1 events are normalized to 1.
	Attempt int
	// Host reports the producing host; v2 only, required there.
	Host string
}

// Phase of a run lifecycle.
type Phase string

const (
	PhaseStarted  Phase = "STARTED"
	PhaseFinished Phase = "FINISHED"
)

// Status of a finished run.
type Status string

const (
	StatusSuccess Status = "SUCCESS"
	StatusFailed  Status = "FAILED"
	StatusTimeout Status = "TIMEOUT"
)

// RawEvent is the unvalidated wire payload. Pointer/scalar fields are used
// for scalars so that "absent" and "zero value" are distinguishable.
type RawEvent struct {
	SchemaVersion string     `json:"schema_version"`
	EventID       string     `json:"event_id"`
	RunID         string     `json:"run_id"`
	JobID         string     `json:"job_id"`
	Seq           *int64     `json:"seq"`
	OccurredAt    *time.Time `json:"occurred_at"`
	Phase         string     `json:"phase"`
	Status        string     `json:"status"`
	Attempt       *int       `json:"attempt"`
	Host          string     `json:"host"`
}

// FieldError explains why an event could not be decoded.
type FieldError struct {
	Field  string
	Reason string
}

func (e *FieldError) Error() string {
	return fmt.Sprintf("event decode failed at field %q: %s", e.Field, e.Reason)
}

func missing(field string) error {
	return &FieldError{Field: field, Reason: "field is required but absent"}
}

func invalid(field, format string, args ...any) error {
	return &FieldError{Field: field, Reason: fmt.Sprintf(format, args...)}
}

// Decode validates a RawEvent and produces an Event, or returns a FieldError
// describing the first missing/invalid field. No silent defaults:
//   - unknown/empty schema_version is rejected;
//   - every required scalar must be explicitly present;
//   - event/v1 normalizes Attempt to 1 (defined by that schema, not a default);
//   - event/v2 requires Attempt>=1 and a non-empty Host.
func Decode(raw RawEvent) (Event, error) {
	if raw.SchemaVersion == "" {
		return Event{}, missing("schema_version")
	}
	if raw.SchemaVersion != SchemaV1 && raw.SchemaVersion != SchemaV2 {
		return Event{}, invalid("schema_version",
			"unknown version %q: supported versions are "+SchemaV1+", "+SchemaV2, raw.SchemaVersion)
	}
	if raw.EventID == "" {
		return Event{}, missing("event_id")
	}
	if raw.RunID == "" {
		return Event{}, missing("run_id")
	}
	if raw.JobID == "" {
		return Event{}, missing("job_id")
	}
	if raw.Seq == nil {
		return Event{}, missing("seq")
	}
	if *raw.Seq < 0 {
		return Event{}, invalid("seq", "must be >= 0")
	}
	if raw.OccurredAt == nil {
		return Event{}, missing("occurred_at")
	}
	if raw.OccurredAt.IsZero() {
		return Event{}, invalid("occurred_at", "must not be the zero time")
	}

	var phase Phase
	switch Phase(raw.Phase) {
	case PhaseStarted:
		phase = PhaseStarted
		if raw.Status != "" {
			return Event{}, invalid("status", "must be empty for STARTED events")
		}
	case PhaseFinished:
		phase = PhaseFinished
		if raw.Status == "" {
			return Event{}, missing("status")
		}
	default:
		if raw.Phase == "" {
			return Event{}, missing("phase")
		}
		return Event{}, invalid("phase",
			"must be STARTED or FINISHED, got %q", raw.Phase)
	}

	var status Status
	if phase == PhaseFinished {
		switch Status(raw.Status) {
		case StatusSuccess, StatusFailed, StatusTimeout:
			status = Status(raw.Status)
		default:
			return Event{}, invalid("status",
				"must be SUCCESS, FAILED or TIMEOUT, got %q", raw.Status)
		}
	}

	ev := Event{
		SchemaVersion: raw.SchemaVersion,
		EventID:       raw.EventID,
		RunID:         raw.RunID,
		JobID:         raw.JobID,
		Seq:           *raw.Seq,
		OccurredAt:    *raw.OccurredAt,
		Phase:         phase,
		Status:        status,
	}

	switch raw.SchemaVersion {
	case SchemaV1:
		// event/v1 predates retry tracking: attempt count is defined as 1.
		if raw.Attempt != nil && *raw.Attempt != 1 {
			return Event{}, invalid("attempt",
				"event/v1 does not support attempts other than 1, got %d", *raw.Attempt)
		}
		ev.Attempt = 1
	case SchemaV2:
		if raw.Attempt == nil {
			return Event{}, missing("attempt")
		}
		if *raw.Attempt < 1 {
			return Event{}, invalid("attempt", "must be >= 1")
		}
		ev.Attempt = *raw.Attempt
		if raw.Host == "" {
			return Event{}, missing("host")
		}
		ev.Host = raw.Host
	}
	return ev, nil
}
