package core

import (
	"context"
	"encoding/json"
)

type BaseTask struct{}

func (t *BaseTask) Priority() int {
	// Default priority value (e.g., 0)
	return 1
}

func (t *BaseTask) Retries() int {
	// Default number of retries (e.g., 1)
	return 1
}

type TaskInterface interface {
	Name() string

	// New method to execute task
	Execute(ctx context.Context, payload json.RawMessage) (interface{}, error)

	// New method to get task payload
	Payload() interface{}

	// New method to get task output
	Output() interface{}

	// New method to get task priority
	Priority() int

	// New method to get the number of retries allowed
	Retries() int
}
