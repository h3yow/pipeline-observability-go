// Package rule defines versioned freshness/SLA decision rules and a
// version registry so historical events can always be replayed under the
// exact rule version they were first judged with.
//
// Reproducibility contract:
//   - A registered (RuleID, Version) pair is immutable; re-registering the
//     same version with a different definition is an error.
//   - Evaluation always binds to an explicit version; "unknown version"
//     is an error, never an implicit fallback to latest.
package rule

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

// ErrRuleNotFound is returned when a rule id/version is not registered.
var ErrRuleNotFound = errors.New("rule not found")

// ErrVersionExists is returned when an immutable version is re-registered
// with a conflicting definition.
var ErrVersionExists = errors.New("rule version already registered")

// Set is one immutable versioned rule definition for a job.
type Set struct {
	RuleID string
	// Version must be >= 1 and monotonically increasing within a RuleID.
	Version int
	// FreshnessDeadline bounds how late a run may finish relative to its
	// scheduled start (StartedAt + FreshnessDeadline).
	FreshnessDeadline time.Duration
	// SLADuration bounds the run duration (FinishedAt - StartedAt).
	SLADuration time.Duration
	// FireAfterN consecutive breaches required before FIRING (debounce).
	FireAfterN int
	// RecoverAfterN consecutive passes required before RECOVERED.
	RecoverAfterN int
}

// Validate checks a rule set for internal consistency.
func (s Set) Validate() error {
	if s.RuleID == "" {
		return fmt.Errorf("rule: empty rule id")
	}
	if s.Version < 1 {
		return fmt.Errorf("rule %s: version must be >= 1, got %d", s.RuleID, s.Version)
	}
	if s.FreshnessDeadline <= 0 {
		return fmt.Errorf("rule %s v%d: freshness deadline must be > 0", s.RuleID, s.Version)
	}
	if s.SLADuration <= 0 {
		return fmt.Errorf("rule %s v%d: sla duration must be > 0", s.RuleID, s.Version)
	}
	if s.FireAfterN < 1 {
		return fmt.Errorf("rule %s v%d: fireAfterN must be >= 1", s.RuleID, s.Version)
	}
	if s.RecoverAfterN < 1 {
		return fmt.Errorf("rule %s v%d: recoverAfterN must be >= 1", s.RuleID, s.Version)
	}
	return nil
}

// Registry resolves rule versions.
type Registry struct {
	sets map[string]map[int]Set
}

// NewRegistry creates an empty rule registry.
func NewRegistry() *Registry {
	return &Registry{sets: map[string]map[int]Set{}}
}

// Register adds a rule version. The same (id, version) may be registered
// again only with an identical definition (idempotent replay).
func (r *Registry) Register(s Set) error {
	if err := s.Validate(); err != nil {
		return err
	}
	versions, ok := r.sets[s.RuleID]
	if !ok {
		versions = map[int]Set{}
		r.sets[s.RuleID] = versions
	}
	if existing, dup := versions[s.Version]; dup && existing != s {
		return fmt.Errorf("%w: %s v%d re-registered with a different definition",
			ErrVersionExists, s.RuleID, s.Version)
	}
	versions[s.Version] = s
	return nil
}

// Get resolves a specific rule version.
func (r *Registry) Get(id string, version int) (Set, error) {
	versions, ok := r.sets[id]
	if !ok {
		return Set{}, fmt.Errorf("%w: %s", ErrRuleNotFound, id)
	}
	s, ok := versions[version]
	if !ok {
		return Set{}, fmt.Errorf("%w: %s v%d", ErrRuleNotFound, id, version)
	}
	return s, nil
}

// Latest resolves the newest version of a rule.
func (r *Registry) Latest(id string) (Set, error) {
	vs := r.Versions(id)
	if len(vs) == 0 {
		return Set{}, fmt.Errorf("%w: %s", ErrRuleNotFound, id)
	}
	return r.Get(id, vs[len(vs)-1])
}

// Versions lists all registered versions of a rule, ascending.
func (r *Registry) Versions(id string) []int {
	versions, ok := r.sets[id]
	if !ok {
		return nil
	}
	out := make([]int, 0, len(versions))
	for v := range versions {
		out = append(out, v)
	}
	sort.Ints(out)
	return out
}
