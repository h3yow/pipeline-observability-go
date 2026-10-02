package engine

import (
	"time"

	"github.com/h3yow/pipeline-observability-go/internal/eval"
	"github.com/h3yow/pipeline-observability-go/internal/model"
	"github.com/h3yow/pipeline-observability-go/internal/rule"
)

// evaluator is a thin wrapper so engine tests stay independent of wall time.
type evaluator struct {
	inner *eval.Evaluator
}

func newEvaluator(_ *rule.Registry) *evaluator {
	return &evaluator{inner: eval.NewEvaluator()}
}

// newFixedEvaluator pins EvaluatedAt for deterministic cross-path (normal vs
// backfill) conclusion comparisons.
func newFixedEvaluator(now func() time.Time) *evaluator {
	return &evaluator{inner: eval.NewEvaluatorWithClock(now)}
}

func (x *evaluator) evaluate(run model.RunState, rs rule.Set) model.Conclusion {
	return x.inner.Evaluate(run, rs)
}
