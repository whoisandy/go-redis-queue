package queue

import (
	"context"
	"encoding/json"
	"go-redis-queue/core"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/exp/slog"
)

const (
	InitialQueue   = "tasks:queued"    // Temporary list for blocking operations
	PendingQueue   = "tasks:pending"   // ZSET for pending tasks
	ActiveQueue    = "tasks:active"    // ZSET for active tasks
	CompletedQueue = "tasks:completed" // ZSET for completed tasks
	FailedQueue    = "tasks:failed"    // ZSET for failed tasks
	TaskHashKey    = "tasks:hash"      // Hash for task details
	Placeholder    = "__placeholder__" // Placeholder to keep ZSET keys alive
)

type RedisQueueAdapter struct {
	client       *redis.Client
	taskRegistry *core.TaskRegistry
}

func NewRedisQueueAdapter(client *redis.Client) *RedisQueueAdapter {
	return &RedisQueueAdapter{
		client:       client,
		taskRegistry: core.NewTaskRegistry(),
	}
}

func (q *RedisQueueAdapter) RegisterTask(task core.TaskInterface) {
	q.taskRegistry.RegisterTask(task)
}

func (q *RedisQueueAdapter) GetTask(taskName string) (core.TaskInterface, bool) {
	return q.taskRegistry.GetTask(taskName)
}

func (q *RedisQueueAdapter) Enqueue(ctx context.Context, taskName string, payload interface{}) (*core.Task, error) {
	task, err := core.NewTask(taskName, payload)
	if err != nil {
		return nil, err
	}

	taskJSON, err := json.Marshal(task)
	if err != nil {
		return nil, err
	}

	priority := task.Priority()
	score := float64(priority)*1e12 - float64(time.Now().UnixNano()) // Prioritize by score

	// Start a Redis transaction
	_, err = q.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		// Store task in Redis hash
		if err := pipe.HSet(ctx, TaskHashKey, task.ID, taskJSON).Err(); err != nil {
			return err
		}

		// Push task ID to the InitialQueue (temporary list)
		if err := pipe.LPush(ctx, InitialQueue, task.ID).Err(); err != nil {
			return err
		}

		// Add task to the PendingQueue with priority
		if err := pipe.ZAdd(ctx, PendingQueue, redis.Z{Score: score, Member: task.ID}).Err(); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return task, nil
}

// FetchTask fetches the next task from the queue and moves it to the active state
func (q *RedisQueueAdapter) Consume(ctx context.Context) (*core.Task, error) {
	// Ensure placeholder elements are in the ZSETs to keep them alive
	q.ensureZSetPlaceholders(ctx)

	taskID, err := q.popFromInitialQueue(ctx)
	if err != nil {
		return nil, err
	}

	// Retrieve task details
	taskJSON, err := q.client.HGet(ctx, TaskHashKey, taskID).Result()
	if err != nil {
		slog.Error("Failed to get task", "taskID", taskID, "error", err)
		return nil, err
	}

	var task core.Task
	if err := json.Unmarshal([]byte(taskJSON), &task); err != nil {
		slog.Error("Failed to unmarshal task", "taskID", taskID, "error", err)
		return nil, err
	}

	// Move task from InitialQueue to PendingQueue atomically
	if err := q.MoveTaskToPending(ctx, &task); err != nil {
		slog.Error("Failed to move task to pending state", "taskID", taskID, "error", err)
		return nil, err
	}

	// Move task from pending to active state
	if err := q.moveTask(ctx, task.ID, PendingQueue, ActiveQueue); err != nil {
		slog.Error("Failed to move task to active state", "taskID", taskID, "error", err)
		return nil, err
	}

	return &task, nil
}

// UpdateTask updates the task status and saves it to Redis
func (q *RedisQueueAdapter) UpdateTask(ctx context.Context, task *core.Task, status core.TaskStatus) error {
	task.Status = status
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return err
	}

	return q.client.HSet(ctx, TaskHashKey, task.ID, taskJSON).Err()
}

// MoveTaskToPending moves a task from InitialQueue to PendingQueue atomically
func (q *RedisQueueAdapter) MoveTaskToPending(ctx context.Context, task *core.Task) error {
	score := float64(time.Now().UnixNano())

	// Start a Redis transaction
	_, err := q.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		// Remove from InitialQueue
		if err := pipe.LRem(ctx, InitialQueue, 0, task.ID).Err(); err != nil {
			return err
		}

		// Add to PendingQueue ZSET
		if err := pipe.ZAdd(ctx, PendingQueue, redis.Z{Score: score, Member: task.ID}).Err(); err != nil {
			return err
		}

		// Update the task status to pending
		err := q.UpdateTask(ctx, task, core.StatusActive)
		if err != nil {
			return err
		}

		return nil
	})

	return err
}

// MoveActiveTaskToPending moves a currently active task back to the PendingQueue
func (q *RedisQueueAdapter) MoveActiveTaskToPending(ctx context.Context, task *core.Task) error {
	// Create a new context that is not cancelled by the main worker context
	independentCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Update the task status to pending
	if err := q.UpdateTask(independentCtx, task, core.StatusPending); err != nil {
		return err
	}

	return q.moveTask(independentCtx, task.ID, ActiveQueue, PendingQueue)
}

// MoveTaskToCompleted moves a task from active to completed
func (q *RedisQueueAdapter) MoveTaskToCompleted(ctx context.Context, task *core.Task) error {
	// Update the task status
	if err := q.UpdateTask(ctx, task, core.StatusCompleted); err != nil {
		return err
	}

	// Move task from active to completed
	return q.moveTask(ctx, task.ID, ActiveQueue, CompletedQueue)
}

// MoveTaskToFailed moves a task from active to failed
func (q *RedisQueueAdapter) MoveTaskToFailed(ctx context.Context, task *core.Task) error {
	// Update the task status
	if err := q.UpdateTask(ctx, task, core.StatusFailed); err != nil {
		return err
	}

	// Move task from active to failed
	return q.moveTask(ctx, task.ID, ActiveQueue, FailedQueue)
}

// ReEnqueuePendingTasks moves tasks from the PendingQueue back to the InitialQueue
func (q *RedisQueueAdapter) ReEnqueuePendingTasks(ctx context.Context) error {
	// Retrieve all pending tasks
	tasks, err := q.client.ZRange(ctx, PendingQueue, 0, -1).Result()
	if err != nil {
		return err
	}

	// Start a Redis transaction
	_, err = q.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, taskID := range tasks {
			if taskID == Placeholder {
				// Skip the placeholder task
				continue
			}

			// Remove task from PendingQueue
			if err := pipe.ZRem(ctx, PendingQueue, taskID).Err(); err != nil {
				return err
			}

			// Push task ID back to InitialQueue
			if err := pipe.LPush(ctx, InitialQueue, taskID).Err(); err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

// RequeueTask requeues a task back to the InitialQueue
func (q *RedisQueueAdapter) RequeueTask(ctx context.Context, task *core.Task) error {
	// Increment retry count and marshal the task back to JSON
	task.RetryCount++
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return err
	}

	// Start a Redis transaction
	_, err = q.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		// Update task in the hash
		if err := pipe.HSet(ctx, TaskHashKey, task.ID, taskJSON).Err(); err != nil {
			return err
		}

		// Push task ID back to InitialQueue (temporary list for blocking operation)
		if err := pipe.LPush(ctx, InitialQueue, task.ID).Err(); err != nil {
			return err
		}

		return nil
	})

	return err
}

// popFromInitialQueue pops a task from the InitialQueue and moves it to the PendingQueue
func (q *RedisQueueAdapter) popFromInitialQueue(ctx context.Context) (string, error) {
	result := make(chan string, 1)
	errChan := make(chan error, 1)

	go func() {
		res, err := q.client.BRPop(ctx, 0, InitialQueue).Result()
		if err != nil {
			errChan <- err
			return
		}
		result <- res[1] // res[0] is the queue name, res[1] is the task ID
	}()

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case err := <-errChan:
		return "", err
	case res := <-result:
		return res, nil
	}
}

// ensureZSetPlaceholders ensures that placeholder elements are in the ZSETs to keep them alive
func (q *RedisQueueAdapter) ensureZSetPlaceholders(ctx context.Context) {
	// Placeholder score (infinity) ensures it's never processed
	placeholderScore := math.Inf(1)

	q.client.ZAdd(ctx, PendingQueue, redis.Z{Score: placeholderScore, Member: Placeholder})
	q.client.ZAdd(ctx, ActiveQueue, redis.Z{Score: placeholderScore, Member: Placeholder})
	q.client.ZAdd(ctx, CompletedQueue, redis.Z{Score: placeholderScore, Member: Placeholder})
	q.client.ZAdd(ctx, FailedQueue, redis.Z{Score: placeholderScore, Member: Placeholder})
}

// moveTask moves a task from one queue to another
func (q *RedisQueueAdapter) moveTask(ctx context.Context, taskID, fromQueue, toQueue string) error {
	score := float64(time.Now().UnixNano())
	if taskID == Placeholder {
		return nil
	}

	// Start a Redis transaction
	_, err := q.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		// Remove from current state queue
		if err := pipe.ZRem(ctx, fromQueue, taskID).Err(); err != nil {
			return err
		}

		// Add to next state queue
		if err := pipe.ZAdd(ctx, toQueue, redis.Z{Score: score, Member: taskID}).Err(); err != nil {
			return err
		}

		return nil
	})

	return err
}
