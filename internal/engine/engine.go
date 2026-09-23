// Package engine wires intake, evaluation and alerting into one
// deterministic, concurrency-safe pipeline observability facade.
//
// Binding & reproducibility:
//   - Each job is bound to an explicit (RuleID, version). BindRule pins a
//     version; otherwise the first completed run auto-binds to the newest
//     rule version registered at that moment. The binding is recorded and
//     never silently changed, so later rule versions cannot alter the
//     interpretation of a job already in flight.
//   - Evaluate processes completed runs in event-time order, so the alert
//     lifecycle always sees observations in run order regardless of how
//     the raw events were ingested.
package engine

import (
	"context"
	"sort"
	"sync"

	"github.com/h3yow/pipeline-observability-go/internal/alert"
	"github.com/h3yow/pipeline-observability-go/internal/event"
	"github.com/h3yow/pipeline-observability-go/internal/ingest"
	"github.com/h3yow/pipeline-observability-go/internal/model"
	"github.com/h3yow/pipeline-observability-go/internal/rule"
)

type jobBinding struct {
	ruleID  string
	version int
	auto    bool
}

// Engine is the top-level facade.
type Engine struct {
	mu      sync.Mutex
	store   *ingest.Store
	rules   *rule.Registry
	alerts  *alert.Manager
	binding map[model.JobID]jobBinding
	// evaluated records runs already fed to the alert lifecycle (per kind),
	// keyed by RunID; each run affects alerts exactly once.
	evaluated map[model.RunID]bool
}

// New creates an Engine with the given event-time lateness allowance (ns).
func New(allowLateness int64) *Engine {
	return &Engine{
		store:     ingest.NewStore(allowLateness),
		rules:     rule.NewRegistry(),
		alerts:    alert.NewManager(),
		binding:   map[model.JobID]jobBinding{},
		evaluated: map[model.RunID]bool{},
	}
}

// Rules exposes the rule registry for registration.
func (e *Engine) Rules() *rule.Registry { return e.rules }

// BindRule pins a job to an explicit rule version before evaluation.
func (e *Engine) BindRule(jobID model.JobID, ruleID string, version int) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.rules.Get(ruleID, version); err != nil {
		return err
	}
	e.binding[jobID] = jobBinding{ruleID: ruleID, version: version}
	return nil
}

// IngestEvent accepts one raw envelope and returns its intake receipt.
func (e *Engine) IngestEvent(ctx context.Context, env event.Envelope) ingest.Receipt {
	return e.store.Ingest(ctx, env)
}

// Evaluate evaluates every not-yet-evaluated completed run in event-time
// order, feeds breaches into alert lifecycles, and returns the new
// conclusions plus any alert transitions they caused.
func (e *Engine) Evaluate() ([]model.Conclusion, []alert.Transition, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	runs := e.store.CompleteRuns()
	sort.Slice(runs, func(i, j int) bool {
		ti, tj := runs[i].FinishedAt.UnixNano(), runs[j].FinishedAt.UnixNano()
		if ti != tj {
			return ti < tj
		}
		return string(runs[i].RunID) < string(runs[j].RunID)
	})

	ev := newEvaluator(e.rules)
	var conclusions []model.Conclusion
	var transitions []alert.Transition

	for _, run := range runs {
		if e.evaluated[run.RunID] {
			continue
		}
		b, err := e.resolveBindingLocked(run.JobID)
		if err != nil {
			return nil, nil, err
		}
		rs, err := e.rules.Get(b.ruleID, b.version)
		if err != nil {
			return nil, nil, err
		}
		c := ev.evaluate(run, rs)
		conclusions = append(conclusions, c)

		obsAt := run.FinishedAt.UnixNano()
		obsID := string(run.RunID) + ":" + string(model.KindFreshness)
		if tr := e.alerts.Observe(obsID, run.JobID, model.KindFreshness,
			contains(c.Breaches, model.KindFreshness),
			rs.Version, rs.FireAfterN, rs.RecoverAfterN, obsAt, c.Freshness.Reason); tr != nil {
			transitions = append(transitions, *tr)
		}
		obsID = string(run.RunID) + ":" + string(model.KindSLA)
		if tr := e.alerts.Observe(obsID, run.JobID, model.KindSLA,
			contains(c.Breaches, model.KindSLA),
			rs.Version, rs.FireAfterN, rs.RecoverAfterN, obsAt, c.SLA.Reason); tr != nil {
			transitions = append(transitions, *tr)
		}

		e.evaluated[run.RunID] = true
	}
	return conclusions, transitions, nil
}

// resolveBindingLocked returns the explicit binding or auto-binds to the
// newest registered version for the job's convention: rule id == job id.
func (e *Engine) resolveBindingLocked(jobID model.JobID) (jobBinding, error) {
	if b, ok := e.binding[jobID]; ok {
		return b, nil
	}
	id := string(jobID)
	latest, err := e.rules.Latest(id)
	if err != nil {
		return jobBinding{}, err
	}
	b := jobBinding{ruleID: id, version: latest.Version, auto: true}
	e.binding[jobID] = b
	return b, nil
}

// AlertEvents returns all alert lifecycle transitions emitted so far.
func (e *Engine) AlertEvents() []alert.Transition { return e.alerts.Events() }

// AlertState reports the current state for a job/kind.
func (e *Engine) AlertState(jobID model.JobID, kind model.AlertKind) (model.AlertState, bool) {
	return e.alerts.State(jobID, kind)
}

// Runs returns the current aggregate snapshot.
func (e *Engine) Runs() []model.RunState { return e.store.Runs() }

func contains(ks []model.AlertKind, k model.AlertKind) bool {
	for _, x := range ks {
		if x == k {
			return true
		}
	}
	return false
}
