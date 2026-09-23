// Package rule defines versioned freshness/SLA evaluation rules.
//
// A rule set is an immutable, versioned document. Evaluators are always
// bound to one concrete Rules version, so re-evaluating historical events
// under an old rule version reproduces the historical verdict.
package rule

import (
	"fmt"
	"sync"
	"time"
)

// Known rule versions.
const (
	RulesV1 = "rules/v1"
	RulesV2 = "rules/v2"
)

// Rules is the generic, tagged rule container. Consumers obtain a concrete
// versioned implementation through AsV1/AsV2; an unknown version is an error
// rather than a fallback to defaults.
type Rules struct {
	Version string
	JobID   string
	body    any
}

// V1 is the first rule version.
//
//	Freshness: a run is fresh while now - runStart <= FreshnessLimit.
//	SLA:       a finished run meets SLA iff status==SUCCESS and
//	           duration <= MaxDuration; any non-success status breaches.
type V1 struct {
	FreshnessLimit time.Duration
	MaxDuration    time.Duration
}

// V2 is the evolved rule version.
//
//	Freshness: unchanged from V1.
//	SLA:       SUCCESS meets SLA iff duration <= MaxDuration;
//	           TIMEOUT meets SLA iff duration <= TimeoutMaxDuration
//	           (a timeout that returns fast is treated as recoverable);
//	           FAILED always breaches. Requires event/v2 input: attempts
//	           beyond MaxAttempts breach the retry policy regardless of status.
type V2 struct {
	FreshnessLimit     time.Duration
	MaxDuration        time.Duration
	TimeoutMaxDuration time.Duration
	MaxAttempts        int
}

// VersionError indicates an unknown or mismatched rule version.
type VersionError struct {
	Have string
	Want string
}

func (e *VersionError) Error() string {
	if e.Want == "" {
		return fmt.Sprintf("unknown rule version %q", e.Have)
	}
	return fmt.Sprintf("rule version mismatch: have %s, want %s", e.Have, e.Want)
}

// NewV1 wraps a V1 payload into a tagged Rules. Zero/negative thresholds are
// rejected: a missing threshold must not silently become "no limit".
func NewV1(jobID string, v1 V1) (Rules, error) {
	if jobID == "" {
		return Rules{}, fmt.Errorf("rule: jobID is required")
	}
	if v1.FreshnessLimit <= 0 {
		return Rules{}, fmt.Errorf("rule v1: FreshnessLimit must be > 0")
	}
	if v1.MaxDuration <= 0 {
		return Rules{}, fmt.Errorf("rule v1: MaxDuration must be > 0")
	}
	return Rules{Version: RulesV1, JobID: jobID, body: v1}, nil
}

// NewV2 wraps a V2 payload into a tagged Rules.
func NewV2(jobID string, v2 V2) (Rules, error) {
	if jobID == "" {
		return Rules{}, fmt.Errorf("rule: jobID is required")
	}
	if v2.FreshnessLimit <= 0 {
		return Rules{}, fmt.Errorf("rule v2: FreshnessLimit must be > 0")
	}
	if v2.MaxDuration <= 0 {
		return Rules{}, fmt.Errorf("rule v2: MaxDuration must be > 0")
	}
	if v2.TimeoutMaxDuration <= 0 {
		return Rules{}, fmt.Errorf("rule v2: TimeoutMaxDuration must be > 0")
	}
	if v2.MaxAttempts < 1 {
		return Rules{}, fmt.Errorf("rule v2: MaxAttempts must be >= 1")
	}
	return Rules{Version: RulesV2, JobID: jobID, body: v2}, nil
}

// AsV1 returns the V1 payload, or a VersionError if the tagged version differs.
func (r Rules) AsV1() (V1, error) {
	v, ok := r.body.(V1)
	if !ok || r.Version != RulesV1 {
		return V1{}, &VersionError{Have: r.Version, Want: RulesV1}
	}
	return v, nil
}

// AsV2 returns the V2 payload, or a VersionError if the tagged version differs.
func (r Rules) AsV2() (V2, error) {
	v, ok := r.body.(V2)
	if !ok || r.Version != RulesV2 {
		return V2{}, &VersionError{Have: r.Version, Want: RulesV2}
	}
	return v, nil
}

// Registry stores immutable rule documents keyed by (jobID, version).
type Registry struct {
	mu sync.RWMutex
	m  map[string]Rules
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{m: make(map[string]Rules)}
}

func regKey(jobID, version string) string { return jobID + "|" + version }

// Put adds a rule document; versions are write-once and cannot be overwritten,
// which keeps historical evaluation reproducible.
func (reg *Registry) Put(r Rules) error {
	if r.Version == "" || r.JobID == "" {
		return fmt.Errorf("rule registry: rules must have a version and jobID")
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	k := regKey(r.JobID, r.Version)
	if _, exists := reg.m[k]; exists {
		return fmt.Errorf("rule registry: %s for job %q is already registered and immutable",
			r.Version, r.JobID)
	}
	reg.m[k] = r
	return nil
}

// Lookup returns the exact rule version registered for a job; a missing or
// unknown version is an error, never a defaulted rule set.
func (reg *Registry) Lookup(jobID, version string) (Rules, error) {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	r, ok := reg.m[regKey(jobID, version)]
	if !ok {
		return Rules{}, &VersionError{Have: version}
	}
	return r, nil
}
