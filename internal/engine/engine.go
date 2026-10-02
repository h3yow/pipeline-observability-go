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
	"fmt"
	"sort"
	"sync"
	"time"

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
	// evaluated records determinable runs already folded into the alert
	// lifecycle (keyed by RunID). Unknown runs are never recorded here.
	evaluated map[model.RunID]bool
	// unknownSig records the last emitted unknown conclusion signature per
	// run (basis gap code + evidence + accepted event set). An unknown run
	// is re-emitted only while its gap status changes; once backfilled it
	// moves to `evaluated` and becomes stable exactly like a normal run.
	unknownSig map[model.RunID]string
	// clock optionally fixes EvaluatedAt; nil means wall clock (test hook).
	clock func() time.Time
}

// New creates an Engine with the given event-time lateness allowance (ns).
func New(allowLateness int64) *Engine {
	return &Engine{
		store:      ingest.NewStore(allowLateness),
		rules:      rule.NewRegistry(),
		alerts:     alert.NewManager(),
		binding:    map[model.JobID]jobBinding{},
		evaluated:  map[model.RunID]bool{},
		unknownSig: map[model.RunID]string{},
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

// BackfillEvent explicitly injects one recovered event (archive restore /
// manual repair). It bypasses only the intake lateness window; validation,
// deduplication and no-overwrite still apply, and the run is rebuilt
// canonically. A subsequent Evaluate re-judges the run and recomputes the
// job's alert lifecycle in canonical event-time order, so the outcome is
// identical to the event having arrived normally.
//
// Repeated/concurrent backfill of the same EventID is a duplicate no-op and
// can never create duplicate state or duplicate alerts.
func (e *Engine) BackfillEvent(ctx context.Context, env event.Envelope) ingest.Receipt {
	return e.store.Backfill(ctx, env)
}

// PendingRuns returns the "to be backfilled" view: finished runs whose time
// basis is incomplete, each with its gap reason. Ordered by RunID and
// stable under out-of-order/duplicate/concurrent delivery.
func (e *Engine) PendingRuns() []ingest.PendingRun {
	return e.store.PendingRuns()
}

// PendingRunsPage returns one deterministic page (RunID > cursor, at most
// limit rows) for scrolling large pending sets.
func (e *Engine) PendingRunsPage(cursor string, limit int) []ingest.PendingRun {
	return e.store.PendingRunsPage(cursor, limit)
}

// unknownSignature identifies the gap status of an unknown run; it changes
// exactly when the missing-basis explanation or the accepted event set
// changes, which is when callers need a fresh unknown conclusion.
func unknownSignature(run model.RunState) string {
	gap := model.MissingBasis{Code: model.GapNotArrived}
	if run.BasisGap != nil {
		gap = *run.BasisGap
	}
	return fmt.Sprintf("%s|%s|%s|%d", gap.Code, gap.EvidenceEventID, gap.Detail, run.AppliedSeq)
}

// Evaluate evaluates every not-yet-finalized completed run in event-time
// order and returns the new conclusions plus alert transitions.
//
// Determinable runs behave exactly as before: they are folded into the alert
// lifecycles once each. Runs with an incomplete time basis get UNKNOWN
// conclusions (never PASS/FAIL), produce NO alert observations and therefore
// affect neither debounce nor recovery counters; they are re-emitted only
// while their gap status changes. Backfilling the missing event makes a run
// determinable, at which point the whole job's alert lifecycle is recomputed
// in canonical event-time order — identical to a run whose event was never
// lost — without duplicate alerts.
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
	if e.clock != nil {
		ev = newFixedEvaluator(e.clock)
	}
	var conclusions []model.Conclusion
	var transitions []alert.Transition
	// jobs that received a newly determinable run this call: their whole
	// lifecycle must be rebuilt canonically (the new run may predate runs
	// the alert manager has already folded).
	rebuildJobs := map[model.JobID]bool{}

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

		if !c.Determinable {
			// Unknown: no breach inference, no alert observation. Emit a
			// fresh conclusion only while the gap status itself changes.
			sig := unknownSignature(run)
			if e.unknownSig[run.RunID] == sig {
				conclusions = conclusions[:len(conclusions)-1]
				continue
			}
			e.unknownSig[run.RunID] = sig
			continue
		}

		// Became determinable (possibly after backfill). Finalize it.
		e.evaluated[run.RunID] = true
		delete(e.unknownSig, run.RunID)
		rebuildJobs[run.JobID] = true
	}

	if len(rebuildJobs) == 0 {
		return conclusions, transitions, nil
	}

	if len(rebuildJobs) == 0 {
		return conclusions, transitions, nil
	}

	// Recompute the alert lifecycle for every affected job from ALL of its
	// determinable runs in canonical event-time order (the newly finalized
	// run may predate runs the manager already folded). Unknown runs are
	// excluded by definition, so they never touch fire/recover counters.
	transitions = e.rebuildAffectedJobsLocked(runs, ev, rebuildJobs)
	return conclusions, transitions, nil
}

// rebuildAffectedJobsLocked folds every determinable run of each affected
// job (both kinds) through a fresh lifecycle and returns the resulting
// canonical transitions for those jobs. Rebuild is idempotent for an
// unchanged input set, so calling it without new determinable runs is a
// no-op and cannot duplicate alerts.
func (e *Engine) rebuildAffectedJobsLocked(
	runs []model.RunState, ev *evaluator, jobs map[model.JobID]bool,
) []alert.Transition {
	var out []alert.Transition
	for job := range jobs {
		var obs []alert.Observation
		for _, run := range runs {
			if run.JobID != job || !run.HasStarted || !run.HasFinished {
				continue
			}
			b, err := e.resolveBindingLocked(run.JobID)
			if err != nil {
				continue // binding errors are surfaced by the main loop
			}
			rs, err := e.rules.Get(b.ruleID, b.version)
			if err != nil {
				continue
			}
			c := ev.evaluate(run, rs)
			at := run.FinishedAt.UnixNano()
			obs = append(obs,
				alert.Observation{
					RunID: run.RunID, JobID: job, Kind: model.KindFreshness,
					Breach:  contains(c.Breaches, model.KindFreshness),
					RuleVer: rs.Version, FireAfterN: rs.FireAfterN,
					RecoverAfterN: rs.RecoverAfterN, At: at,
					Evidence: c.Freshness.Reason,
				},
				alert.Observation{
					RunID: run.RunID, JobID: job, Kind: model.KindSLA,
					Breach:  contains(c.Breaches, model.KindSLA),
					RuleVer: rs.Version, FireAfterN: rs.FireAfterN,
					RecoverAfterN: rs.RecoverAfterN, At: at,
					Evidence: c.SLA.Reason,
				},
			)
		}
		if len(obs) > 0 {
			out = append(out, e.alerts.Rebuild(obs)...)
		}
	}
	// Canonical order across jobs for stable caller output.
	sort.SliceStable(out, func(i, j int) bool {
		ti, tj := out[i].Alert.Timestamp.UnixNano(), out[j].Alert.Timestamp.UnixNano()
		if ti != tj {
			return ti < tj
		}
		if ri, rj := string(out[i].RunID), string(out[j].RunID); ri != rj {
			return ri < rj
		}
		return string(out[i].Alert.Kind) < string(out[j].Alert.Kind)
	})
	return out
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
