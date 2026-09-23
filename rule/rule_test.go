package rule

import (
	"errors"
	"testing"
	"time"
)

func TestRegistryWriteOnceAndLookup(t *testing.T) {
	reg := NewRegistry()
	r1, err := NewV1("jobA", V1{FreshnessLimit: time.Hour, MaxDuration: 2 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Put(r1); err != nil {
		t.Fatal(err)
	}
	if err := reg.Put(r1); err == nil {
		t.Fatal("re-registering the same version must fail (immutability)")
	} else {
		t.Logf("BASIS  overwrite rejected: %v", err)
	}
	got, err := reg.Lookup("jobA", RulesV1)
	if err != nil {
		t.Fatal(err)
	}
	v1, err := got.AsV1()
	if err != nil || v1.FreshnessLimit != time.Hour {
		t.Fatalf("lookup/asV1 failed: %v %+v", err, v1)
	}
	if _, err := reg.Lookup("jobA", "rules/v99"); err == nil {
		t.Fatal("unknown version must error, never default")
	} else {
		var ve *VersionError
		if !errors.As(err, &ve) {
			t.Fatalf("want VersionError, got %v", err)
		}
		t.Logf("BASIS  unknown version rejected: %v", err)
	}
}

func TestZeroThresholdsRejected(t *testing.T) {
	if _, err := NewV1("jobA", V1{}); err == nil {
		t.Fatal("zero thresholds must be rejected rather than meaning 'no limit'")
	}
	if _, err := NewV2("jobA", V2{FreshnessLimit: time.Hour, MaxDuration: time.Hour,
		TimeoutMaxDuration: time.Hour, MaxAttempts: 0}); err == nil {
		t.Fatal("MaxAttempts=0 must be rejected")
	}
}

func TestVersionMismatchTypeAssertion(t *testing.T) {
	r1, _ := NewV1("jobA", V1{FreshnessLimit: time.Hour, MaxDuration: time.Hour})
	if _, err := r1.AsV2(); err == nil {
		t.Fatal("AsV2 on v1 rules must fail with VersionError")
	}
}
