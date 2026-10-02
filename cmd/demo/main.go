// Command demo runs a scripted scenario through the observability engine:
// concurrent + shuffled + duplicated delivery, threshold flapping, and a
// rule-version replay. Every printed line includes the input and the
// decision basis.
package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/h3yow/pipeline-observability-go/internal/engine"
	"github.com/h3yow/pipeline-observability-go/internal/event"
	"github.com/h3yow/pipeline-observability-go/internal/rule"
)

func at(day, hour, min int) time.Time {
	return time.Date(2026, 9, day, hour, min, 0, 0, time.UTC)
}

func started(id string, t time.Time, run, job string) event.Envelope {
	return event.Envelope{EventID: id, SchemaVersion: 1, Type: event.TypeStarted,
		OccurredAt: t, RunID: run, JobID: job, Payload: map[string]any{"attempt": 1}}
}
func finished(id string, t time.Time, run, job, res string) event.Envelope {
	return event.Envelope{EventID: id, SchemaVersion: 1, Type: event.TypeFinished,
		OccurredAt: t, RunID: run, JobID: job, Payload: map[string]any{"result": res, "attempt": 1}}
}

func main() {
	fmt.Println("== pipeline observability demo ==")

	// Runs of job-a; SLA limit 10m, fire after 2 breaches, recover after 1 pass.
	evs := []event.Envelope{
		started("s1", at(23, 9, 0), "run-1", "job-a"),
		finished("f1", at(23, 9, 5), "run-1", "job-a", "SUCCESS"),
		started("s2", at(23, 10, 0), "run-2", "job-a"),
		finished("f2", at(23, 10, 15), "run-2", "job-a", "FAILED"),
		started("s3", at(23, 11, 0), "run-3", "job-a"),
		finished("f3", at(23, 11, 20), "run-3", "job-a", "FAILED"),
		started("s4", at(23, 12, 0), "run-4", "job-a"),
		finished("f4", at(23, 12, 5), "run-4", "job-a", "SUCCESS"),
	}

	e := engine.New(int64(6 * time.Hour))
	if err := e.Rules().Register(rule.Set{
		RuleID: "job-a", Version: 1,
		FreshnessDeadline: 30 * time.Minute, SLADuration: 10 * time.Minute,
		FireAfterN: 2, RecoverAfterN: 1,
	}); err != nil {
		panic(err)
	}

	// Duplicate every event once and deliver concurrently in random order.
	batch := append([]event.Envelope{}, evs...)
	batch = append(batch, evs...)
	r := rand.New(rand.NewSource(7))
	r.Shuffle(len(batch), func(i, j int) { batch[i], batch[j] = batch[j], batch[i] })

	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, ev := range batch {
		wg.Add(1)
		go func(ev event.Envelope) {
			defer wg.Done()
			<-start
			rec := e.IngestEvent(context.Background(), ev)
			fmt.Printf("INGEST event=%-3s run=%s -> %-20s %s\n",
				ev.EventID, ev.RunID, rec.Disposition, rec.Reason)
		}(ev)
	}
	close(start)
	wg.Wait()

	conclusions, alerts, err := e.Evaluate()
	if err != nil {
		panic(err)
	}
	fmt.Println("\n-- conclusions --")
	for _, c := range conclusions {
		fmt.Printf("RUN %s rule=%s v%d\n  freshness ok=%v (%s)\n  sla       ok=%v (%s)\n",
			c.RunID, c.RuleID, c.RuleVer, c.Freshness.OK, c.Freshness.Reason,
			c.SLA.OK, c.SLA.Reason)
	}
	fmt.Println("\n-- alert lifecycle --")
	for _, tr := range alerts {
		fmt.Printf("ALERT job=%s kind=%s %s -> %s seq=%d reason=%s\n",
			tr.Alert.JobID, tr.Alert.Kind, tr.FromState, tr.ToState, tr.Alert.Seq, tr.Alert.Reason)
	}

	// ---- missing-basis scenario -------------------------------------
	// run-5's started event is lost upstream: only its finish arrives.
	fmt.Println("\n== missing start event: unknown conclusion & backfill ==")
	e2 := engine.New(int64(6 * time.Hour))
	if err := e2.Rules().Register(rule.Set{
		RuleID: "job-b", Version: 1,
		FreshnessDeadline: 30 * time.Minute, SLADuration: 10 * time.Minute,
		FireAfterN: 1, RecoverAfterN: 1,
	}); err != nil {
		panic(err)
	}
	rec := e2.IngestEvent(context.Background(),
		finished("g-f5", at(24, 9, 15), "run-5", "job-b", "FAILED"))
	fmt.Printf("INGEST event=g-f5 run=run-5 -> %s %s\n", rec.Disposition, rec.Reason)

	unknown, trs, err := e2.Evaluate()
	if err != nil {
		panic(err)
	}
	for _, c := range unknown {
		fmt.Printf("RUN %s determinable=%v\n  freshness %s\n  sla       %s\n  gaps      %+v\n",
			c.RunID, c.Determinable, c.Freshness.Reason, c.SLA.Reason, c.Gaps)
	}
	fmt.Printf("alerts raised by unknown run: %d (must be 0)\n", len(trs))

	pending, cursor, _ := e2.PendingRuns("", 10)
	fmt.Printf("\n-- pending view (cursor=%q) --\n", cursor)
	for _, p := range pending {
		fmt.Printf("PENDING run=%s job=%s finished=%s\n  gap: %s cause=%s (%s)\n",
			p.RunID, p.JobID, p.FinishedAt.Format("15:04"), p.Gaps[0].EventType, p.Gaps[0].Cause, p.Gaps[0].Detail)
	}

	// The lost event is recovered from an archive and explicitly backfilled.
	rec = e2.BackfillEvent(context.Background(),
		started("g-s5", at(24, 9, 0), "run-5", "job-b"))
	fmt.Printf("\nBACKFILL event=g-s5 run=run-5 -> %s %s\n", rec.Disposition, rec.Reason)
	resolved, trs, err := e2.Evaluate()
	if err != nil {
		panic(err)
	}
	for _, c := range resolved {
		fmt.Printf("RUN %s determinable=%v\n  freshness %s\n  sla       %s\n",
			c.RunID, c.Determinable, c.Freshness.Reason, c.SLA.Reason)
	}
	for _, tr := range trs {
		fmt.Printf("ALERT job=%s kind=%s %s -> %s seq=%d reason=%s\n",
			tr.Alert.JobID, tr.Alert.Kind, tr.FromState, tr.ToState, tr.Alert.Seq, tr.Alert.Reason)
	}
	pending, _, _ = e2.PendingRuns("", 10)
	fmt.Printf("pending after backfill: %d (must be 0)\n", len(pending))
}
