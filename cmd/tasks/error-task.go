package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"go-redis-queue/core"
	"log/slog"
	"time"

	"golang.org/x/exp/rand"
)

type ErrorTaskPayload struct{}
type ErrorTaskOutput struct{}

type ErrorTask struct {
	core.BaseTask
}

func (t *ErrorTask) Name() string {
	return "errorTask"
}

func (t *ErrorTask) Execute(ctx context.Context, payload json.RawMessage) (interface{}, error) {
	var data ErrorTaskPayload
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, err
	}

	slog.Info("Handling error task")
	// sleep for random time between 1 and 10 seconds
	time.Sleep(time.Duration(rand.Intn(10)+1) * time.Second)

	// return an error using errors.New
	return nil, errors.New("error")
}

func (t *ErrorTask) Payload() interface{} {
	return &ErrorTaskPayload{}
}

func (t *ErrorTask) Output() interface{} {
	return &SendEmailTaskOutput{}
}

func (t *ErrorTask) Retries() int {
	return 3
}

func (t *ErrorTask) Priority() int {
	return 1
}
