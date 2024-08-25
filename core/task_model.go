package core

import (
	"encoding/json"
	"errors"

	"github.com/google/uuid"
)

type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"
	StatusActive    TaskStatus = "active"
	StatusCompleted TaskStatus = "completed"
	StatusFailed    TaskStatus = "failed"
)

type Task struct {
	BaseTask
	ID         string          `json:"id"`
	TaskName   string          `json:"name"`
	Status     TaskStatus      `json:"status"`
	Payload    json.RawMessage `json:"payload"`
	Output     json.RawMessage `json:"output"`
	Error      *string         `json:"error"`
	RetryCount int             `json:"retry_count"` // Track the number of retries
}

func NewTask(taskName string, payload interface{}) (*Task, error) {
	id := uuid.New().String()

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return &Task{
		ID:       id,
		TaskName: taskName,
		Status:   StatusPending,
		Payload:  jsonPayload,
	}, nil
}

// SetOutput sets the output of the task after execution.
func (t *Task) SetOutput(output interface{}) error {
	jsonOutput, err := json.Marshal(output)
	if err != nil {
		return err
	}
	t.Output = jsonOutput
	return nil
}

// GetOutput unmarshals the output into the provided interface using reflection.
func (t *Task) GetOutput(output interface{}) error {
	if t.Output == nil {
		return errors.New("output is not set")
	}
	return json.Unmarshal(t.Output, output)
}

// SetError sets the error message for the task if it fails during execution.
func (t *Task) SetError(err error) {
	s := err.Error()
	t.Status = StatusFailed
	t.Error = &s
}
