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
	"github.com/h3yow/pipeline-observability-go/internal/model"
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
		fmt.Printf("RUN %s rule=%s v%d determinable=%t\n  freshness status=%s ok=%v (%s)\n  sla       status=%s ok=%v (%s)\n",
			c.RunID, c.RuleID, c.RuleVer, c.Determinable,
			c.Freshness.Status, c.Freshness.OK, c.Freshness.Reason,
			c.SLA.Status, c.SLA.OK, c.SLA.Reason)
	}
	fmt.Println("\n-- alert lifecycle --")
	for _, tr := range alerts {
		fmt.Printf("ALERT job=%s kind=%s %s -> %s seq=%d run=%s reason=%s\n",
			tr.Alert.JobID, tr.Alert.Kind, tr.FromState, tr.ToState,
			tr.Alert.Seq, tr.RunID, tr.Alert.Reason)
	}

	// Missing-basis scenario: run-5's start is lost; only the finish shows
	// up, so freshness/SLA stay UNKNOWN and the run is listed as pending.
	e2 := engine.New(int64(2 * time.Hour))
	if err := e2.Rules().Register(rule.Set{
		RuleID: "job-a", Version: 1,
		FreshnessDeadline: 30 * time.Minute, SLADuration: 10 * time.Minute,
		FireAfterN: 2, RecoverAfterN: 1,
	}); err != nil {
		panic(err)
	}
	// Anchor the watermark past the lost start (14:00) with later events.
	e2.IngestEvent(context.Background(), started("x-s", at(23, 16, 0), "run-x", "job-a"))
	e2.IngestEvent(context.Background(), finished("x-f", at(23, 16, 5), "run-x", "job-a", "SUCCESS"))
	e2.IngestEvent(context.Background(), finished("g-f", at(23, 14, 15), "run-5", "job-a", "FAILED"))
	lostStart := started("g-s", at(23, 14, 0), "run-5", "job-a")
	if r := e2.IngestEvent(context.Background(), lostStart); !r.Accepted {
		fmt.Printf("\n-- missing basis --\nlost start rejected at intake: %s | %s\n",
			r.Disposition, r.Reason)
	}
	unk, unkAlerts, _ := e2.Evaluate()
	for _, c := range unk {
		if !c.IsUnknown() {
			continue
		}
		gap := model.MissingBasis{Code: "UNKNOWN_GAP"}
		if len(c.MissingBasis) > 0 {
			gap = c.MissingBasis[0]
		}
		fmt.Printf("UNKNOWN RUN %s: freshness=%s sla=%s gap=%s (%s)\n",
			c.RunID, c.Freshness.Status, c.SLA.Status, gap.Code, gap.Detail)
	}
	fmt.Printf("alerts caused by unknown runs: %d (must be 0)\n", len(unkAlerts))
	for _, p := range e2.PendingRuns() {
		fmt.Printf("PENDING run=%s finished=%s gap=%s evidence=%s\n",
			p.RunID, p.FinishedAt.Format("15:04"), p.Gap.Code, p.Gap.EvidenceEventID)
	}

	// Recover the start from an archive and re-evaluate.
	rec := e2.BackfillEvent(context.Background(), lostStart)
	fmt.Printf("BACKFILL -> %s | %s\n", rec.Disposition, rec.Reason)
	repaired, repairedAlerts, _ := e2.Evaluate()
	for _, c := range repaired {
		fmt.Printf("REPAIRED RUN %s: freshness=%s ok=%v sla=%s ok=%v\n",
			c.RunID, c.Freshness.Status, c.Freshness.OK, c.SLA.Status, c.SLA.OK)
	}
	for _, tr := range repairedAlerts {
		fmt.Printf("ALERT(after repair) %s %s -> %s run=%s\n",
			tr.Alert.Kind, tr.FromState, tr.ToState, tr.RunID)
	}
}
