// Package alert drives the NORMAL -> PENDING -> FIRING -> RESOLVED lifecycle
// from evaluation verdicts, with flapping suppression near thresholds.
//
// Stability guarantees:
//
//   - A breach must be observed TriggerAfter times consecutively before an
//     incident opens; threshold jitter that flips BREACH/OK in between
//     resets the counter and is reported as SUPPRESSED, not as an alert.
//   - Recovery likewise needs RecoverAfter consecutive OK observations,
//     after which the incident is RESOLVED. A breach after recovery opens a
//     brand-new incident with a new IncidentID, so "recovered" and
//     "re-triggered" are always unambiguously distinguishable.
//   - Every Observe call returns either a real Transition, a SUPPRESSED
//     audit record (Kind=KindSuppressed, zero IncidentID), or nothing
//     (zero Transition) on UNKNOWN/no-signal observations.
package alert

import (
	"fmt"
	"sync"
	"time"

	"github.com/h3yow/pipeline-observability-go/eval"
)

// Phase of an alert's lifecycle.
type Phase string

const (
	PhaseNormal   Phase = "NORMAL"
	PhasePending  Phase = "PENDING"
	PhaseFiring   Phase = "FIRING"
	PhaseResolved Phase = "RESOLVED"
)

// TransitionKind distinguishes lifecycle events.
type TransitionKind string

const (
	KindTriggered  TransitionKind = "TRIGGERED"
	KindRecovered  TransitionKind = "RECOVERED"
	KindSuppressed TransitionKind = "SUPPRESSED"
)

// Transition is one emitted lifecycle event.
type Transition struct {
	Kind       TransitionKind
	JobID      string
	IncidentID string
	FromPhase  Phase
	ToPhase    Phase
	At         time.Time
	RunID      string
	Reason     string
	Verdict    eval.Verdict
}

// Policy configures flapping suppression.
type Policy struct {
	// TriggerAfter: consecutive breach observations before FIRING (>=1).
	TriggerAfter int
	// RecoverAfter: consecutive ok observations before RESOLVED (>=1).
	RecoverAfter int
}

// DefaultPolicy: fire after 2 breaches, recover after 2 oks.
func DefaultPolicy() Policy {
	return Policy{TriggerAfter: 2, RecoverAfter: 2}
}

// IncidentView is an immutable snapshot of one incident.
type IncidentView struct {
	ID         string
	JobID      string
	Phase      Phase
	OpenedAt   time.Time
	ResolvedAt time.Time
	RunID      string
	Reason     string
}

// jobAlert holds per-job lifecycle state.
type jobAlert struct {
	phase Phase

	breachStreak  int
	okStreak      int
	incidentSeq   int
	openIncident  *IncidentView
	lastIncident  *IncidentView
	lastEventTime time.Time
}

func newJobAlert() *jobAlert {
	return &jobAlert{phase: PhaseNormal}
}

func (j *jobAlert) newIncidentID(jobID string, at time.Time, runID, reason string) string {
	j.incidentSeq++
	id := fmt.Sprintf("INC-%s-%d", jobID, j.incidentSeq)
	j.openIncident = &IncidentView{
		ID:       id,
		JobID:    jobID,
		Phase:    PhaseFiring,
		OpenedAt: at,
		RunID:    runID,
		Reason:   reason,
	}
	return id
}

// Manager maintains alert lifecycle state per job; safe for concurrent use.
type Manager struct {
	policy Policy
	mu     sync.Mutex
	jobs   map[string]*jobAlert
}

// NewManager constructs a Manager.
func NewManager(policy Policy) *Manager {
	if policy.TriggerAfter < 1 {
		policy.TriggerAfter = 1
	}
	if policy.RecoverAfter < 1 {
		policy.RecoverAfter = 1
	}
	return &Manager{policy: policy, jobs: make(map[string]*jobAlert)}
}

// Observe feeds one verdict. Timestamps for one job must be monotonically
// non-decreasing; otherwise an error is returned rather than silently
// reordering history.
func (m *Manager) Observe(v eval.Verdict, at time.Time) (Transition, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	j, ok := m.jobs[v.JobID]
	if !ok {
		j = newJobAlert()
		m.jobs[v.JobID] = j
	}
	at = at.UTC()
	if !j.lastEventTime.IsZero() && at.Before(j.lastEventTime) {
		return Transition{}, fmt.Errorf("alert: event time for job %q went backwards: %s < %s",
			v.JobID, at, j.lastEventTime)
	}
	j.lastEventTime = at

	// UNKNOWN carries no alerting signal: counters untouched, no transition.
	signal := signalOf(v)
	if signal == 0 {
		return Transition{}, nil
	}

	switch j.phase {
	case PhaseNormal:
		return m.normalObserve(j, v, at, signal)
	case PhasePending:
		return m.pendingObserve(j, v, at, signal)
	case PhaseFiring:
		return m.firingObserve(j, v, at, signal)
	default:
		// Resolved is a terminal phase for an incident record; the job
		// itself is back to normal accounting.
		j.phase = PhaseNormal
		return m.normalObserve(j, v, at, signal)
	}
}

// signal is +1 breach, -1 ok, 0 unknown.
func signalOf(v eval.Verdict) int {
	switch {
	case v.Breach():
		return 1
	case v.Freshness == eval.LevelOK && v.SLA == eval.LevelOK:
		return -1
	default:
		return 0
	}
}

func breachReason(v eval.Verdict) string {
	var dims []string
	if v.Freshness == eval.LevelBreach {
		dims = append(dims, "freshness")
	}
	if v.SLA == eval.LevelBreach {
		dims = append(dims, "sla")
	}
	return fmt.Sprintf("breach dimensions: %v (rules %s, schema %s)",
		dims, v.RuleVersion, v.SchemaVersion)
}

func (m *Manager) normalObserve(j *jobAlert, v eval.Verdict, at time.Time, signal int) (Transition, error) {
	if signal < 0 {
		j.breachStreak = 0
		return Transition{}, nil
	}
	j.breachStreak = 1
	return m.advanceBreach(j, v, at, PhaseNormal)
}

// advanceBreach is the shared PENDING/FIRING entry for breach streaks.
func (m *Manager) advanceBreach(j *jobAlert, v eval.Verdict, at time.Time, from Phase) (Transition, error) {
	if j.breachStreak < m.policy.TriggerAfter {
		j.phase = PhasePending
		return suppressed(v, at, from, PhasePending,
			"breach streak %d/%d: PENDING, no alert emitted (flap protection)",
			j.breachStreak, m.policy.TriggerAfter), nil
	}
	j.phase = PhaseFiring
	j.okStreak = 0
	id := j.newIncidentID(v.JobID, at, v.RunID, breachReason(v))
	return Transition{
		Kind:       KindTriggered,
		JobID:      v.JobID,
		IncidentID: id,
		FromPhase:  from,
		ToPhase:    PhaseFiring,
		At:         at,
		RunID:      v.RunID,
		Reason:     breachReason(v),
		Verdict:    v,
	}, nil
}

func (m *Manager) pendingObserve(j *jobAlert, v eval.Verdict, at time.Time, signal int) (Transition, error) {
	if signal < 0 {
		// One OK wipes the unconfirmed streak: jitter never alerts.
		prev := j.breachStreak
		j.breachStreak = 0
		j.phase = PhaseNormal
		return suppressed(v, at, PhasePending, PhaseNormal,
			"OK observed after breach streak %d/%d: streak reset, no alert",
			prev, m.policy.TriggerAfter), nil
	}
	j.breachStreak++
	return m.advanceBreach(j, v, at, PhasePending)
}

func (m *Manager) firingObserve(j *jobAlert, v eval.Verdict, at time.Time, signal int) (Transition, error) {
	if signal > 0 {
		// Continued / repeated breach while firing must not re-alert.
		j.okStreak = 0
		return suppressed(v, at, PhaseFiring, PhaseFiring,
			"breach continues while incident is already FIRING: no duplicate alert"), nil
	}
	j.okStreak++
	if j.okStreak < m.policy.RecoverAfter {
		return suppressed(v, at, PhaseFiring, PhaseFiring,
			"ok streak %d/%d: FIRING held; flapping protection",
			j.okStreak, m.policy.RecoverAfter), nil
	}
	if j.openIncident == nil {
		j.phase = PhaseNormal
		return Transition{}, nil
	}
	inc := j.openIncident
	inc.Phase = PhaseResolved
	inc.ResolvedAt = at
	j.openIncident = nil
	j.lastIncident = inc
	j.phase = PhaseNormal
	j.breachStreak = 0
	j.okStreak = 0
	return Transition{
		Kind:       KindRecovered,
		JobID:      v.JobID,
		IncidentID: inc.ID,
		FromPhase:  PhaseFiring,
		ToPhase:    PhaseResolved,
		At:         at,
		RunID:      v.RunID,
		Reason: fmt.Sprintf("%d consecutive OK observations after incident %s",
			m.policy.RecoverAfter, inc.ID),
		Verdict: v,
	}, nil
}

func suppressed(v eval.Verdict, at time.Time, from, to Phase, format string, args ...any) Transition {
	return Transition{
		Kind:      KindSuppressed,
		JobID:     v.JobID,
		FromPhase: from,
		ToPhase:   to,
		At:        at,
		RunID:     v.RunID,
		Reason:    fmt.Sprintf(format, args...),
		Verdict:   v,
	}
}

// Incident returns the open incident, or the most recently resolved one.
func (m *Manager) Incident(jobID string) (IncidentView, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[jobID]
	if !ok {
		return IncidentView{}, false
	}
	if j.openIncident != nil {
		return *j.openIncident, true
	}
	if j.lastIncident != nil {
		return *j.lastIncident, true
	}
	return IncidentView{}, false
}
