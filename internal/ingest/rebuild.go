package ingest

import (
	"sort"
	"time"

	"github.com/h3yow/pipeline-observability-go/internal/event"
	"github.com/h3yow/pipeline-observability-go/internal/model"
)

// rebuild reconstructs the run aggregate from its accepted event set in
// canonical event-time order (OccurredAt, then EventID). Because this is a
// pure function of the event set, arrival order cannot influence the
// result: out-of-order delivery yields the same state as in-order delivery.
func (a *runAgg) rebuild() {
	aes := make([]acceptedEvent, 0, len(a.events))
	for _, ae := range a.events {
		aes = append(aes, ae)
	}
	sort.Slice(aes, func(i, j int) bool {
		ti, tj := aes[i].ev.OccurredAt.UnixNano(), aes[j].ev.OccurredAt.UnixNano()
		if ti != tj {
			return ti < tj
		}
		return aes[i].ev.EventID < aes[j].ev.EventID
	})

	st := model.RunState{
		RunID: model.RunID(a.runID),
		JobID: model.JobID(a.jobID),
	}
	var maxSeq int64
	for _, ae := range aes {
		e := ae.ev
		if ae.seq > maxSeq {
			maxSeq = ae.seq
		}
		switch e.Type {
		case event.TypeStarted:
			if !st.HasStarted {
				st.HasStarted = true
				st.StartedAt = e.OccurredAt
				st.Attempt = e.Attempt
			}
		case event.TypeFinished:
			if !st.HasFinished {
				st.HasFinished = true
				st.FinishedAt = e.OccurredAt
				st.Result = model.RunResult(e.Result)
			}
		}
		st.AppliedEventIDs = append(st.AppliedEventIDs, e.EventID)
	}
	st.AppliedSeq = maxSeq
	a.state = st
}

func unixNsToTime(ns int64) time.Time {
	return time.Unix(0, ns).UTC()
}
