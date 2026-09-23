package event

import (
	"fmt"
	"time"
)

// schemaVersion1 is the first event schema:
//
//	run.started  payload: {"attempt": <int>=1}
//	run.finished payload: {"result": "SUCCESS"|"FAILED", "attempt": <int>=1}
//
// Attempt is optional (defaults only when the schema explicitly says so);
// result is mandatory for finished events.
const schemaVersion1 = 1

func init() {
	RegisterDecoder(schemaVersion1, decodeV1)
}

// RegisterDecoder registers a decoder for a schema version. It panics on
// double registration so schema evolution mistakes fail fast at startup.
func RegisterDecoder(version int, dec func(*Envelope, *DecodedEvent) error) {
	if _, exists := decoders[version]; exists {
		panic(fmt.Sprintf("event: decoder already registered for version %d", version))
	}
	decoders[version] = dec
}

func decodeV1(env *Envelope, out *DecodedEvent) error {
	switch env.Type {
	case TypeStarted:
		if a, ok := env.Payload["attempt"]; ok {
			n, err := asInt(a)
			if err != nil {
				return fmt.Errorf("%w: attempt: %v", ErrBadField, err)
			}
			if n < 1 {
				return fmt.Errorf("%w: attempt must be >= 1, got %d", ErrBadField, n)
			}
			out.Attempt, out.HasAttempt = n, true
		} else {
			// V1 explicitly defines attempt as optional with default 1.
			out.Attempt, out.HasAttempt = 1, false
		}
	case TypeFinished:
		rv, present := env.Payload["result"]
		if !present {
			return fmt.Errorf("%w: result", ErrMissingField)
		}
		rs, ok := rv.(string)
		if !ok {
			return fmt.Errorf("%w: result is not a string", ErrBadField)
		}
		switch rs {
		case "SUCCESS", "FAILED":
		default:
			return fmt.Errorf("%w: result %q", ErrBadField, rs)
		}
		out.Result, out.HasResult = rs, true
		if a, ok := env.Payload["attempt"]; ok {
			n, err := asInt(a)
			if err != nil {
				return fmt.Errorf("%w: attempt: %v", ErrBadField, err)
			}
			if n < 1 {
				return fmt.Errorf("%w: attempt must be >= 1, got %d", ErrBadField, n)
			}
			out.Attempt, out.HasAttempt = n, true
		} else {
			out.Attempt, out.HasAttempt = 1, false
		}
	default:
		return fmt.Errorf("%w: unsupported event type %q in v1", ErrBadField, env.Type)
	}
	// OccurredAt monotonicity sanity is left to the intake layer, which
	// owns the run lifecycle.
	if env.OccurredAt.Location() != time.UTC && env.OccurredAt.Location().String() != "UTC" {
		// Not an error: normalize for deterministic comparisons.
		out.OccurredAt = env.OccurredAt.UTC()
	}
	return nil
}

func asInt(v any) (int, error) {
	switch n := v.(type) {
	case int:
		return n, nil
	case int64:
		return int(n), nil
	case float64:
		if n != float64(int(n)) {
			return 0, fmt.Errorf("non-integer number %v", n)
		}
		return int(n), nil
	default:
		return 0, fmt.Errorf("got %T", v)
	}
}
