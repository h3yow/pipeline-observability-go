// Package event defines versioned raw run-event envelopes and their
// validation/decoding rules.
//
// Versioning contract:
//   - Envelope.SchemaVersion selects an explicitly registered decoder.
//   - An unknown version is rejected (ErrUnknownVersion), never defaulted.
//   - Required payload fields that are absent are rejected with
//     ErrMissingField rather than silently treated as zero values.
package event

import (
	"errors"
	"fmt"
	"time"
)

// Type distinguishes run event kinds.
type Type string

const (
	TypeStarted  Type = "run.started"
	TypeFinished Type = "run.finished"
)

// Envelope is the versioned wire representation of one run event.
//
// Version-specific fields live in Payload so that "field absent" can be
// distinguished from "field present with zero value".
type Envelope struct {
	EventID       string         `json:"eventId"`
	SchemaVersion int            `json:"schemaVersion"`
	Type          Type           `json:"type"`
	OccurredAt    time.Time      `json:"occurredAt"`
	IngestedAt    time.Time      `json:"ingestedAt"`
	RunID         string         `json:"runId"`
	JobID         string         `json:"jobId"`
	Payload       map[string]any `json:"payload,omitempty"`
}

// DecodedEvent is the canonical, validated in-memory event.
type DecodedEvent struct {
	EventID       string
	SchemaVersion int
	Type          Type
	OccurredAt    time.Time
	RunID         string
	JobID         string
	Attempt       int
	Result        string
	HasAttempt    bool
	HasResult     bool
}

// Decode errors. Callers may errors.Is against these sentinels.
var (
	// ErrUnknownVersion: SchemaVersion has no registered decoder.
	ErrUnknownVersion = errors.New("unknown event schema version")
	// ErrMissingField: a field required by the declared schema is absent.
	ErrMissingField = errors.New("missing required field")
	// ErrBadField: a field is present but has the wrong type/value.
	ErrBadField = errors.New("invalid field value")
)

// Validate performs envelope-wide checks and then delegates type-specific
// validation to the decoder registered for the schema version.
func (e *Envelope) Validate() (*DecodedEvent, error) {
	if e == nil {
		return nil, errors.New("nil event envelope")
	}
	if e.EventID == "" {
		return nil, fmt.Errorf("%w: eventId", ErrMissingField)
	}
	if e.SchemaVersion <= 0 {
		return nil, fmt.Errorf("%w: schemaVersion=%d", ErrUnknownVersion, e.SchemaVersion)
	}
	if e.OccurredAt.IsZero() {
		return nil, fmt.Errorf("%w: occurredAt", ErrMissingField)
	}
	switch e.Type {
	case TypeStarted, TypeFinished:
	default:
		return nil, fmt.Errorf("%w: unknown event type %q", ErrBadField, e.Type)
	}
	if e.RunID == "" {
		return nil, fmt.Errorf("%w: runId", ErrMissingField)
	}
	if e.JobID == "" {
		return nil, fmt.Errorf("%w: jobId", ErrMissingField)
	}
	dec, ok := decoders[e.SchemaVersion]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrUnknownVersion, e.SchemaVersion)
	}
	out := &DecodedEvent{
		EventID:       e.EventID,
		SchemaVersion: e.SchemaVersion,
		Type:          e.Type,
		OccurredAt:    e.OccurredAt,
		RunID:         e.RunID,
		JobID:         e.JobID,
	}
	if err := dec(e, out); err != nil {
		return nil, err
	}
	return out, nil
}

// RegisteredVersions lists schema versions this binary can decode, sorted.
func RegisteredVersions() []int {
	vs := make([]int, 0, len(decoders))
	for v := range decoders {
		vs = append(vs, v)
	}
	for i := 1; i < len(vs); i++ {
		for j := i; j > 0 && vs[j-1] > vs[j]; j-- {
			vs[j-1], vs[j] = vs[j], vs[j-1]
		}
	}
	return vs
}
