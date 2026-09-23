package pipelineobs

import "fmt"

type unknownRunError struct{ runID string }

func (e *unknownRunError) Error() string {
	return fmt.Sprintf("pipeline: unknown run %q", e.runID)
}

func errUnknownRun(runID string) error { return &unknownRunError{runID: runID} }
