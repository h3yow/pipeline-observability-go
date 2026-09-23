// Package pipelineobs wires event ingestion, versioned evaluation and alert
// lifecycle management into one local, concurrency-safe facade.
package pipelineobs

import (
	"time"

	"github.com/h3yow/pipeline-observability-go/alert"
	"github.com/h3yow/pipeline-observability-go/eval"
	"github.com/h3yow/pipeline-observability-go/event"
	"github.com/h3yow/pipeline-observability-go/ingest"
	"github.com/h3yow/pipeline-observability-go/rule"
)

// Config configures a Pipeline.
type Config struct {
	MaxLateness time.Duration
	AlertPolicy alert.Policy
}

// Pipeline is the top-level facade.
type Pipeline struct {
	cfg       Config
	agg       *ingest.Aggregator
	registry  *rule.Registry
	alerts    *alert.Manager
	evaluator map[string]map[string]*eval.Evaluator // jobID -> ruleVersion -> evaluator
}

// New constructs a Pipeline with its own rule registry.
func New(cfg Config) *Pipeline {
	return &Pipeline{
		cfg:       cfg,
		agg:       ingest.New(ingest.Config{MaxLateness: cfg.MaxLateness}),
		registry:  rule.NewRegistry(),
		alerts:    alert.NewManager(cfg.AlertPolicy),
		evaluator: make(map[string]map[string]*eval.Evaluator),
	}
}

// RegisterRules adds a write-once versioned rule document.
func (p *Pipeline) RegisterRules(r rule.Rules) error {
	if err := p.registry.Put(r); err != nil {
		return err
	}
	ev, err := eval.NewEvaluator(r)
	if err != nil {
		return err
	}
	byVersion, ok := p.evaluator[r.JobID]
	if !ok {
		byVersion = make(map[string]*eval.Evaluator)
		p.evaluator[r.JobID] = byVersion
	}
	byVersion[r.Version] = ev
	return nil
}

// Result bundles everything one IngestEvent call produced.
type Result struct {
	Decision    ingest.Decision
	Verdict     *eval.Verdict
	Transitions []alert.Transition
}

// IngestEvent decodes, aggregates and — when the event changed state —
// evaluates the run under the rule version matching the event's schema
// (event/v1 -> rules/v1, event/v2 -> rules/v2) and feeds the alert lifecycle.
//
// Events that are rejected, duplicated, stale, superseded, closed or too
// late do not trigger re-evaluation: aggregated conclusions therefore do
// not change for repeated delivery.
func (p *Pipeline) IngestEvent(raw event.RawEvent, receivedAt, evaluatedAt time.Time) (Result, error) {
	dec, err := p.agg.Ingest(raw, receivedAt)
	if err != nil {
		return Result{}, err
	}
	res := Result{Decision: dec}
	if !dec.Applied {
		return res, nil
	}

	st, ok := p.agg.State(dec.RunID)
	if !ok {
		return res, nil
	}
	ruleVersion := mapSchemaToRules(st.SchemaVersion)
	v, err := p.evaluate(st, ruleVersion, evaluatedAt)
	if err != nil {
		return res, err
	}
	res.Verdict = &v

	tr, err := p.alerts.Observe(v, evaluatedAt)
	if err != nil {
		return res, err
	}
	if tr.Kind != "" {
		res.Transitions = []alert.Transition{tr}
	}
	return res, nil
}

func mapSchemaToRules(schema string) string {
	switch schema {
	case event.SchemaV1:
		return rule.RulesV1
	case event.SchemaV2:
		return rule.RulesV2
	default:
		return ""
	}
}

func (p *Pipeline) evaluate(st ingest.RunState, ruleVersion string, at time.Time) (eval.Verdict, error) {
	byVersion, ok := p.evaluator[st.JobID]
	if !ok {
		return eval.Verdict{}, &rule.VersionError{Have: ruleVersion}
	}
	ev, ok := byVersion[ruleVersion]
	if !ok {
		return eval.Verdict{}, &rule.VersionError{Have: ruleVersion}
	}
	return ev.Evaluate(st, at)
}

// EvaluateRun re-runs evaluation for a run at time at under an explicit rule
// version. Because rules are immutable and evaluation is pure, the same
// historical state plus the same rule version always reproduces the verdict.
func (p *Pipeline) EvaluateRun(runID, ruleVersion string, at time.Time) (eval.Verdict, error) {
	st, ok := p.agg.State(runID)
	if !ok {
		return eval.Verdict{}, errUnknownRun(runID)
	}
	return p.evaluate(st, ruleVersion, at)
}

// State returns the aggregated run state.
func (p *Pipeline) State(runID string) (ingest.RunState, bool) {
	return p.agg.State(runID)
}

// Incident returns the open/last incident for a job.
func (p *Pipeline) Incident(jobID string) (alert.IncidentView, bool) {
	return p.alerts.Incident(jobID)
}
