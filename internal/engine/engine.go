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
	"encoding/base64"
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

// evalRecord pins a determinable run to the conclusion and rule version it
// was judged with, so alert replays and historical reproduction always use
// the original inputs.
type evalRecord struct {
	run        model.RunState
	conclusion model.Conclusion
	rule       rule.Set
}

// pendingRecord tracks a completed run whose time basis is incomplete.
type pendingRecord struct {
	conclusion  model.Conclusion
	fingerprint string // stable identity of the gap set; reported once per change
}

// Engine is the top-level facade.
type Engine struct {
	mu      sync.Mutex
	store   *ingest.Store
	rules   *rule.Registry
	alerts  *alert.Manager
	binding map[model.JobID]jobBinding
	// records holds runs with a determinable conclusion, keyed by RunID;
	// each run affects the alert lifecycle exactly once via canonical replay.
	records map[model.RunID]evalRecord
	// pending holds completed runs judged UNKNOWN (incomplete time basis).
	// They raise no alerts and wait for backfill or late arrival.
	pending map[model.RunID]pendingRecord
}

// New creates an Engine with the given event-time lateness allowance (ns).
func New(allowLateness int64) *Engine {
	return &Engine{
		store:   ingest.NewStore(allowLateness),
		rules:   rule.NewRegistry(),
		alerts:  alert.NewManager(),
		binding: map[model.JobID]jobBinding{},
		records: map[model.RunID]evalRecord{},
		pending: map[model.RunID]pendingRecord{},
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

// BackfillEvent explicitly inserts a historically missing event (e.g. one
// recovered from an archive) into its run, bypassing the lateness
// watermark. It is idempotent and conflict-safe; call Evaluate afterwards
// to re-judge the affected run. See ingest.Store.Backfill.
func (e *Engine) BackfillEvent(ctx context.Context, env event.Envelope) ingest.Receipt {
	return e.store.Backfill(ctx, env)
}

// Evaluate evaluates every not-yet-evaluated completed run in event-time
// order, feeds breaches into alert lifecycles, and returns the new
// conclusions plus any alert transitions they caused.
//
// Runs with an incomplete time basis (e.g. only a finished event) produce
// UNKNOWN conclusions: they are recorded in the pending view, raise no
// alerts and do not count towards alert debounce. When the missing event
// later arrives (late delivery or explicit backfill), the next Evaluate
// re-judges the run and replays the job's alert observations in canonical
// order, so the outcome equals the never-lost path without duplicate alerts.
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
		if _, ok := e.records[run.RunID]; ok {
			continue // already judged determinably; records are immutable
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

		if !c.Determinable {
			e.enrichGapsLocked(&c)
			fp := gapFingerprint(c.Gaps)
			if p, ok := e.pending[run.RunID]; !ok || p.fingerprint != fp {
				e.pending[run.RunID] = pendingRecord{conclusion: c, fingerprint: fp}
				conclusions = append(conclusions, c)
			}
			continue // no alert observation for undecidable runs
		}

		delete(e.pending, run.RunID)
		e.records[run.RunID] = evalRecord{run: run, conclusion: c, rule: rs}
		conclusions = append(conclusions, c)
		transitions = append(transitions, e.recomputeJobLocked(run.JobID)...)
	}
	return conclusions, transitions, nil
}

// recomputeJobLocked replays the job's determinable observations (in
// canonical run order) through both alert lifecycles and returns only the
// transitions that have never been emitted before.
func (e *Engine) recomputeJobLocked(jobID model.JobID) []alert.Transition {
	recs := make([]evalRecord, 0, len(e.records))
	for _, r := range e.records {
		if r.run.JobID == jobID {
			recs = append(recs, r)
		}
	}
	sort.Slice(recs, func(i, j int) bool {
		ti, tj := recs[i].run.FinishedAt.UnixNano(), recs[j].run.FinishedAt.UnixNano()
		if ti != tj {
			return ti < tj
		}
		return recs[i].run.RunID < recs[j].run.RunID
	})

	var out []alert.Transition
	for _, kind := range []model.AlertKind{model.KindFreshness, model.KindSLA} {
		obs := make([]alert.Observation, 0, len(recs))
		for _, r := range recs {
			evidence := r.conclusion.Freshness.Reason
			if kind == model.KindSLA {
				evidence = r.conclusion.SLA.Reason
			}
			obs = append(obs, alert.Observation{
				ObsID:         string(r.run.RunID) + ":" + string(kind),
				Breach:        contains(r.conclusion.Breaches, kind),
				RuleVer:       r.rule.Version,
				FireAfterN:    r.rule.FireAfterN,
				RecoverAfterN: r.rule.RecoverAfterN,
				At:            r.run.FinishedAt.UnixNano(),
				Evidence:      evidence,
			})
		}
		out = append(out, e.alerts.Recompute(jobID, kind, obs)...)
	}
	return out
}

// enrichGapsLocked fills each gap's Cause from the intake rejection records
// (late / invalid / superseded), defaulting to NOT_ARRIVED.
func (e *Engine) enrichGapsLocked(c *model.Conclusion) {
	rejected := e.store.RejectedFor(string(c.RunID))
	for i := range c.Gaps {
		cause, detail := gapCauseFor(c.Gaps[i].EventType, rejected)
		c.Gaps[i].Cause = cause
		if detail != "" {
			c.Gaps[i].Detail = detail
		}
	}
}

// gapCauseFor derives the most informative cause for a missing event type
// from the run's rejection records. Priority: LATE > CONFLICT > INVALID.
func gapCauseFor(eventType string, rejected []ingest.RejectedEvent) (model.GapCause, string) {
	best := ""
	var bestReason string
	rank := map[ingest.Disposition]int{
		ingest.DispositionLate:     3,
		ingest.DispositionConflict: 2,
		ingest.DispositionInvalid:  1,
	}
	for _, r := range rejected {
		if string(r.Type) != eventType {
			continue
		}
		if rank[r.Disposition] > rank[ingest.Disposition(best)] || best == "" {
			best = string(r.Disposition)
			bestReason = r.Reason
		}
	}
	switch ingest.Disposition(best) {
	case ingest.DispositionLate:
		return model.GapCauseDroppedLate, eventType + " event dropped as LATE: " + bestReason
	case ingest.DispositionConflict:
		return model.GapCauseSuperseded, eventType + " event superseded by another delivery: " + bestReason
	case ingest.DispositionInvalid:
		return model.GapCauseDroppedInvalid, eventType + " event dropped as INVALID: " + bestReason
	}
	return model.GapCauseNotArrived, ""
}

// gapFingerprint is a stable identity of a gap set, used to report a
// pending conclusion once per distinct gap situation.
func gapFingerprint(gaps []model.InputGap) string {
	out := ""
	for _, g := range gaps {
		out += g.EventType + "|" + string(g.Cause) + "|" + g.Detail + ";"
	}
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

// PendingRun is one entry of the pending-completion view: a completed run
// whose conclusion is UNKNOWN because its time basis is incomplete.
type PendingRun struct {
	RunID      model.RunID
	JobID      model.JobID
	FinishedAt time.Time
	RuleID     string
	RuleVer    int
	// Gaps names each missing input and why (not arrived / dropped late /
	// dropped invalid / superseded).
	Gaps []model.InputGap
}

// DefaultPendingPageSize is used when a non-positive limit is passed.
const DefaultPendingPageSize = 100

// PendingRuns returns the undecidable runs (conclusion UNKNOWN) as a
// stable, paginated view ordered by (FinishedAt, RunID). Pass cursor=""
// for the first page and feed back the returned nextCursor to continue;
// nextCursor == "" means the listing is exhausted.
//
// The view is a pure function of intake state, so it is identical under
// out-of-order, duplicated or concurrent delivery of the same event set.
func (e *Engine) PendingRuns(cursor string, limit int) ([]PendingRun, string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if limit <= 0 {
		limit = DefaultPendingPageSize
	}
	var afterKey string
	if cursor != "" {
		var err error
		afterKey, err = decodePendingCursor(cursor)
		if err != nil {
			return nil, "", err
		}
	}

	all := make([]PendingRun, 0, len(e.pending))
	for _, p := range e.pending {
		c := p.conclusion
		all = append(all, PendingRun{
			RunID:      c.RunID,
			JobID:      c.JobID,
			FinishedAt: pendingFinishedAt(e, c.RunID),
			RuleID:     c.RuleID,
			RuleVer:    c.RuleVer,
			Gaps:       append([]model.InputGap(nil), c.Gaps...),
		})
	}
	sort.Slice(all, func(i, j int) bool { return pendingKey(all[i]) < pendingKey(all[j]) })

	page := make([]PendingRun, 0, limit)
	next := ""
	lastKey := ""
	for _, pr := range all {
		if pendingKey(pr) <= afterKey {
			continue
		}
		if len(page) == limit {
			// More entries exist: the cursor resumes after the last
			// entry of this page.
			next = encodePendingCursor(lastKey)
			break
		}
		page = append(page, pr)
		lastKey = pendingKey(pr)
	}
	return page, next, nil
}

// pendingFinishedAt resolves the run's finish time from the store so the
// view reflects intake state rather than a stale conclusion snapshot.
func pendingFinishedAt(e *Engine, runID model.RunID) time.Time {
	if st, ok := e.store.Run(string(runID)); ok {
		return st.FinishedAt
	}
	return time.Time{}
}

// pendingKey is the canonical sort key of a pending entry.
func pendingKey(p PendingRun) string {
	return fmt.Sprintf("%020d|%s", p.FinishedAt.UnixNano(), p.RunID)
}

func encodePendingCursor(key string) string {
	return base64.RawStdEncoding.EncodeToString([]byte(key))
}

func decodePendingCursor(cursor string) (string, error) {
	b, err := base64.RawStdEncoding.DecodeString(cursor)
	if err != nil {
		return "", fmt.Errorf("invalid pending cursor: %w", err)
	}
	return string(b), nil
}

func contains(ks []model.AlertKind, k model.AlertKind) bool {
	for _, x := range ks {
		if x == k {
			return true
		}
	}
	return false
}
