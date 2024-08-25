package queue

import (
	"context"
	"go-redis-queue/core"
)

type Queue interface {
	RegisterTask(task core.TaskInterface)
	Enqueue(ctx context.Context, taskName string, payload interface{}) (*core.Task, error)
	// Consume(ctx context.Context, handler func(ctx context.Context, task *core.Task) error) error
	Consume(ctx context.Context) (*core.Task, error)
	GetTask(taskName string) (core.TaskInterface, bool)
	UpdateTask(ctx context.Context, task *core.Task, status core.TaskStatus) error
	RequeueTask(ctx context.Context, task *core.Task) error
	ReEnqueuePendingTasks(ctx context.Context) error
	MoveTaskToPending(ctx context.Context, task *core.Task) error
	MoveTaskToCompleted(ctx context.Context, task *core.Task) error
	MoveTaskToFailed(ctx context.Context, task *core.Task) error
	MoveActiveTaskToPending(ctx context.Context, task *core.Task) error
}
