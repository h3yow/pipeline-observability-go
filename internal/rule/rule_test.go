package rule

import (
	"errors"
	"testing"
	"time"
)

func sample() Set {
	return Set{
		RuleID:            "job-a-rule",
		Version:           1,
		FreshnessDeadline: 30 * time.Minute,
		SLADuration:       time.Hour,
		FireAfterN:        2,
		RecoverAfterN:     1,
	}
}

func TestRegistryImmutabilityAndResolution(t *testing.T) {
	r := NewRegistry()
	s1 := sample()
	t.Logf("register rule: %+v", s1)
	if err := r.Register(s1); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Idempotent identical registration.
	if err := r.Register(s1); err != nil {
		t.Fatalf("identical re-register should be allowed: %v", err)
	}
	// Conflicting registration rejected.
	s1b := s1
	s1b.SLADuration = 2 * time.Hour
	if err := r.Register(s1b); !errors.Is(err, ErrVersionExists) {
		t.Fatalf("want ErrVersionExists, got %v", err)
	}
	// New version.
	s2 := s1
	s2.Version, s2.SLADuration = 2, 90*time.Minute
	if err := r.Register(s2); err != nil {
		t.Fatalf("register v2: %v", err)
	}
	got, err := r.Get("job-a-rule", 1)
	if err != nil || got.SLADuration != time.Hour {
		t.Fatalf("v1 resolution = %+v, %v", got, err)
	}
	latest, err := r.Latest("job-a-rule")
	if err != nil || latest.Version != 2 {
		t.Fatalf("latest = %+v, %v", latest, err)
	}
	if _, err := r.Get("job-a-rule", 9); !errors.Is(err, ErrRuleNotFound) {
		t.Fatalf("unknown version should fail, got %v", err)
	}
	t.Logf("decision: v1=%v latest=v%d versions=%v (unknown versions rejected)",
		got.SLADuration, latest.Version, r.Versions("job-a-rule"))
}

func TestSetValidate(t *testing.T) {
	bad := sample()
	bad.FireAfterN = 0
	if err := bad.Validate(); err == nil {
		t.Fatal("expected validation error for FireAfterN=0")
	}
}
