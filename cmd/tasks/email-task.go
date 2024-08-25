package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"go-redis-queue/core"
	"log/slog"
	"time"

	"golang.org/x/exp/rand"
)

type SendEmailTaskPayload struct {
	To      string `json:"to" validate:"required,email"`
	Subject string `json:"subject" validate:"required"`
	Body    string `json:"body" validate:"required"`
}
type SendEmailTaskOutput struct{}

type SendEmailTask struct {
	core.BaseTask
}

func (t *SendEmailTask) Name() string {
	return "sendEmail"
}

func (t *SendEmailTask) Execute(ctx context.Context, payload json.RawMessage) (interface{}, error) {
	var data SendEmailTaskPayload
	var output SendEmailTaskOutput
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, err
	}

	slog.Info("Handling email task")
	time.Sleep(time.Duration(rand.Intn(10)+1) * time.Second)

	message := fmt.Sprintf("Sent email to %s with subject %s\n", data.To, data.Subject)
	slog.Info(message)

	return &output, nil
}

func (t *SendEmailTask) Payload() interface{} {
	return &SendEmailTaskPayload{}
}

func (t *SendEmailTask) Output() interface{} {
	return &SendEmailTaskOutput{}
}

func (t *SendEmailTask) Priority() int {
	return 5
}
