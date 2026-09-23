// Package eval derives freshness and SLA verdicts from aggregated run state.
//
// Evaluation is a pure function of (RunState, Rules, evaluation timestamp):
// the same historical state re-evaluated with the same rule version always
// yields the same Verdict and the same human-readable basis.
package eval

import (
	"fmt"
	"time"

	"github.com/h3yow/pipeline-observability-go/event"
	"github.com/h3yow/pipeline-observability-go/ingest"
	"github.com/h3yow/pipeline-observability-go/rule"
)

// Level is a tristate verdict level.
type Level string

const (
	LevelOK      Level = "OK"
	LevelBreach  Level = "BREACH"
	LevelUnknown Level = "UNKNOWN"
)

// BasisItem is one human- and machine-readable fact behind a verdict.
type BasisItem struct {
	Check  string `json:"check"`
	Actual string `json:"actual"`
	Want   string `json:"want"`
	Pass   bool   `json:"pass"`
}

// Verdict is the freshness/SLA conclusion for one run at one point in time.
type Verdict struct {
	JobID          string      `json:"job_id"`
	RunID          string      `json:"run_id"`
	RuleVersion    string      `json:"rule_version"`
	SchemaVersion  string      `json:"schema_version"`
	EvaluatedAt    time.Time   `json:"evaluated_at"`
	Freshness      Level       `json:"freshness"`
	FreshnessBasis []BasisItem `json:"freshness_basis"`
	SLA            Level       `json:"sla"`
	SLABasis       []BasisItem `json:"sla_basis"`
}

// Breach reports whether either dimension breaches (drives alerting).
func (v Verdict) Breach() bool {
	return v.Freshness == LevelBreach || v.SLA == LevelBreach
}

// Evaluator is bound to an immutable rule version.
type Evaluator struct {
	rules rule.Rules
}

// NewEvaluator constructs an Evaluator for an exact rule document; an
// unsupported rule version is an error.
func NewEvaluator(rules rule.Rules) (*Evaluator, error) {
	switch rules.Version {
	case rule.RulesV1, rule.RulesV2:
	default:
		return nil, &rule.VersionError{Have: rules.Version}
	}
	return &Evaluator{rules: rules}, nil
}

// Evaluate computes a Verdict.
//
//   - A run that has not started yields UNKNOWN with an explicit basis;
//   - freshness compares (at - StartedAt) with the rule's FreshnessLimit;
//   - an unfinished run has SLA = UNKNOWN (cannot conclude yet);
//   - rule v2 applied to event/v1 is an error rather than a defaulted
//     comparison, because retry policy needs an attempt count that v1
//     does not carry.
func (e *Evaluator) Evaluate(st ingest.RunState, at time.Time) (Verdict, error) {
	v := Verdict{
		JobID:         st.JobID,
		RunID:         st.RunID,
		RuleVersion:   e.rules.Version,
		SchemaVersion: st.SchemaVersion,
		EvaluatedAt:   at.UTC(),
		Freshness:     LevelUnknown,
		SLA:           LevelUnknown,
	}

	switch e.rules.Version {
	case rule.RulesV1:
		r1, err := e.rules.AsV1()
		if err != nil {
			return Verdict{}, err
		}
		v.evalFreshness(st, at, r1.FreshnessLimit)
		v.evalSLAV1(st, r1)
	case rule.RulesV2:
		r2, err := e.rules.AsV2()
		if err != nil {
			return Verdict{}, err
		}
		// Versioned data/rule compatibility must be explicit.
		if st.SchemaVersion != event.SchemaV2 {
			return Verdict{}, fmt.Errorf("eval: rules/v2 require event/v2 input, run %q is %s",
				st.RunID, st.SchemaVersion)
		}
		v.evalFreshness(st, at, r2.FreshnessLimit)
		v.evalSLAV2(st, r2)
	default:
		return Verdict{}, &rule.VersionError{Have: e.rules.Version}
	}
	return v, nil
}

func (v *Verdict) evalFreshness(st ingest.RunState, at time.Time, limit time.Duration) {
	if st.StartedAt.IsZero() {
		v.Freshness = LevelUnknown
		v.FreshnessBasis = []BasisItem{{
			Check:  "started",
			Actual: "no STARTED event observed",
			Want:   "a STARTED event",
			Pass:   false,
		}}
		return
	}
	// Running run: freshness = age since start (start deadline).
	// Finished run: freshness = staleness of output since finish.
	reference := st.StartedAt
	check := "start_age_within_limit"
	if st.Closed {
		reference = st.FinishedAt
		check = "staleness_within_limit"
	}
	age := at.Sub(reference)
	pass := age <= limit
	v.FreshnessBasis = []BasisItem{
		{
			Check: check,
			Actual: fmt.Sprintf("age=%s (reference %s, evaluated %s)",
				age, reference.UTC().Format(time.RFC3339Nano),
				v.EvaluatedAt.Format(time.RFC3339Nano)),
			Want: "age <= " + limit.String(),
			Pass: pass,
		},
	}
	if pass {
		v.Freshness = LevelOK
	} else {
		v.Freshness = LevelBreach
	}
}

func finishBasis(st ingest.RunState) ([]BasisItem, time.Duration, bool) {
	if st.StartedAt.IsZero() {
		// FINISHED observed before any STARTED (gap not yet filled): the
		// duration is genuinely unknown, not zero/negative.
		return []BasisItem{{
			Check:  "start_known",
			Actual: "FINISHED observed but STARTED is missing (late gap)",
			Want:   "a STARTED event before concluding SLA",
			Pass:   false,
		}}, 0, false
	}
	if !st.Closed {
		return []BasisItem{{
			Check:  "finished",
			Actual: "run not finished",
			Want:   "a FINISHED event",
			Pass:   false,
		}}, 0, false
	}
	dur := st.FinishedAt.Sub(st.StartedAt)
	return []BasisItem{{
		Check: "finished",
		Actual: fmt.Sprintf("FINISHED at %s with status %s, duration %s",
			st.FinishedAt.UTC().Format(time.RFC3339Nano), st.Status, dur),
		Want: "a FINISHED event",
		Pass: true,
	}}, dur, true
}

func (v *Verdict) evalSLAV1(st ingest.RunState, r1 rule.V1) {
	base, dur, finished := finishBasis(st)
	if !finished {
		v.SLABasis = base
		return
	}
	statusOK := st.Status == event.StatusSuccess
	limitOK := dur <= r1.MaxDuration
	v.SLABasis = append(base,
		BasisItem{
			Check:  "status_success",
			Actual: string(st.Status),
			Want:   string(event.StatusSuccess),
			Pass:   statusOK,
		},
		BasisItem{
			Check:  "duration_within_max",
			Actual: dur.String(),
			Want:   "duration <= " + r1.MaxDuration.String(),
			Pass:   limitOK,
		},
	)
	v.SLA = boolLevel(statusOK && limitOK)
}

func (v *Verdict) evalSLAV2(st ingest.RunState, r2 rule.V2) {
	base, dur, finished := finishBasis(st)
	if !finished {
		v.SLABasis = base
		return
	}

	attemptsOK := st.Attempt <= r2.MaxAttempts
	v.SLABasis = append(base, BasisItem{
		Check:  "attempts_within_max",
		Actual: fmt.Sprintf("attempt=%d", st.Attempt),
		Want:   fmt.Sprintf("attempt <= %d", r2.MaxAttempts),
		Pass:   attemptsOK,
	})

	var statusOK bool
	switch st.Status {
	case event.StatusSuccess:
		statusOK = dur <= r2.MaxDuration
		v.SLABasis = append(v.SLABasis, BasisItem{
			Check:  "success_duration",
			Actual: fmt.Sprintf("SUCCESS duration=%s", dur),
			Want:   "duration <= " + r2.MaxDuration.String(),
			Pass:   statusOK,
		})
	case event.StatusTimeout:
		statusOK = dur <= r2.TimeoutMaxDuration
		v.SLABasis = append(v.SLABasis, BasisItem{
			Check:  "timeout_duration",
			Actual: fmt.Sprintf("TIMEOUT duration=%s", dur),
			Want:   "duration <= " + r2.TimeoutMaxDuration.String(),
			Pass:   statusOK,
		})
	case event.StatusFailed:
		statusOK = false
		v.SLABasis = append(v.SLABasis, BasisItem{
			Check:  "failure_not_allowed",
			Actual: "FAILED",
			Want:   "SUCCESS or tolerated TIMEOUT",
			Pass:   false,
		})
	default:
		v.SLABasis = append(v.SLABasis, BasisItem{
			Check:  "status_known",
			Actual: string(st.Status),
			Want:   "SUCCESS / FAILED / TIMEOUT",
			Pass:   false,
		})
	}
	v.SLA = boolLevel(attemptsOK && statusOK)
}

func boolLevel(ok bool) Level {
	if ok {
		return LevelOK
	}
	return LevelBreach
}
