package worker

import (
	"context"
	"encoding/json"
	"go-redis-queue/core"
	"go-redis-queue/queue"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"
	"time"

	"golang.org/x/exp/slog"
)

type Worker struct {
	queue       queue.Queue
	concurrency int
	maxTasks    int
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	semaphore   chan struct{} // Semaphore to limit the number of tasks
}

func NewWorker(ctx context.Context, q queue.Queue, concurrency int, maxTasks int) *Worker {
	ctx, cancel := context.WithCancel(ctx)
	return &Worker{
		queue:       q,
		concurrency: concurrency,
		maxTasks:    maxTasks,
		ctx:         ctx,
		cancel:      cancel,
		semaphore:   make(chan struct{}, maxTasks), // Initialize the semaphore
	}
}

func (w *Worker) Start() {
	slog.Info("Worker started", "concurrency", w.concurrency)

	// Re-enqueue pending tasks on startup
	if err := w.queue.ReEnqueuePendingTasks(w.ctx); err != nil {
		slog.Error("Failed to re-enqueue pending tasks", "error", err)
		return
	}

	// Start processing tasks
	go w.processTasks()

	// Handle graceful shutdown on signals
	w.handleShutdown()
}

func (w *Worker) Stop() {
	slog.Info("Stopping worker...")
	w.cancel()
	w.wg.Wait()
	slog.Info("Worker stopped gracefully.")
}

func (w *Worker) handleShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	slog.Info("Termination signal received, shutting down worker...")
	w.Stop()
}

func (w *Worker) processTasks() {
	taskChan := make(chan *core.Task)

	// Start worker goroutines based on concurrency
	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go w.worker(taskChan)
	}

	// Distribute tasks to the workers
	for {
		select {
		case <-w.ctx.Done():
			close(taskChan)
			return
		default:
			task, err := w.queue.Consume(w.ctx)
			if err != nil {
				if w.ctx.Err() == context.Canceled {
					slog.Info("Worker shutdown triggered, waiting for tasks to complete")
					close(taskChan)
					return
				}
				slog.Error("Error fetching tasks", "error", err)
				continue
			}

			if task != nil {
				taskChan <- task
			}
		}
	}
}

func (w *Worker) worker(taskChan chan *core.Task) {
	defer w.wg.Done()

	for task := range taskChan {
		w.executeTask(task)
	}
}

func (w *Worker) executeTask(task *core.Task) {
	// Acquire a slot in the semaphore
	w.semaphore <- struct{}{}
	defer func() { <-w.semaphore }() // Release the slot when done

	// Create a timeout context for the task
	shutdownTaskCtx, shutdownCancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer shutdownCancel()

	taskHandler, exists := w.queue.GetTask(task.TaskName)
	if !exists {
		slog.Warn("Task handler not found", "taskName", task.TaskName)
		w.queue.MoveTaskToFailed(shutdownTaskCtx, task)
		return
	}

	payloadInstance := reflect.New(reflect.TypeOf(taskHandler.Payload()).Elem()).Interface()
	if err := json.Unmarshal(task.Payload, payloadInstance); err != nil {
		task.SetError(err)
		slog.Error("Failed to unmarshal task payload", "taskID", task.ID, "error", err)
		w.queue.MoveTaskToFailed(shutdownTaskCtx, task)
		return
	}

	doneChan := make(chan error, 1)
	var output interface{}
	go func() {
		var err error
		output, err = taskHandler.Execute(shutdownTaskCtx, task.Payload)
		doneChan <- err
	}()

	select {
	case <-w.ctx.Done():
		select {
		case err := <-doneChan:
			if err != nil {
				w.handleTaskError(w.ctx, task, err)
			} else {
				w.handleTaskCompletion(w.ctx, task, taskHandler, output)
			}
		case <-shutdownTaskCtx.Done():
			slog.Warn("Task exceeded shutdown timeout, moving back to pending", "taskID", task.ID)
			err := w.handleTaskTimeout(shutdownTaskCtx, task)
			if err != nil {
				slog.Error("Failed to move task to pending", "taskID", task.ID, "error", err)
				w.handleTaskError(w.ctx, task, err)
			}
		}
	case err := <-doneChan:
		if err != nil {
			// w.handleTaskError(w.ctx, task, err)
			// Check if we exceeded max retries
			if task.RetryCount >= taskHandler.Retries() {
				slog.Warn("Task exceeded max retries, moving to failed", "taskID", task.ID, "retryCount", task.RetryCount)
				w.handleTaskError(w.ctx, task, err)
			} else {
				// Increment retry count and requeue the task
				task.RetryCount++
				slog.Warn("Task failed, requeuing", "taskID", task.ID, "retryCount", task.RetryCount)
				if err := w.queue.RequeueTask(w.ctx, task); err != nil {
					slog.Error("Failed to requeue task", "taskID", task.ID, "error", err)
					w.queue.MoveTaskToFailed(w.ctx, task)
				}
			}
		} else {
			w.handleTaskCompletion(w.ctx, task, taskHandler, output)
		}
	}
}

func (w *Worker) handleTaskCompletion(ctx context.Context, task *core.Task, taskHandler core.TaskInterface, output interface{}) {
	// Set and unmarshal the output into the expected type
	if err := task.SetOutput(output); err != nil {
		slog.Error("Failed to set task output", "taskID", task.ID, "error", err)
		w.queue.MoveTaskToFailed(ctx, task)
		return
	}

	outputInstance := reflect.New(reflect.TypeOf(taskHandler.Output()).Elem()).Interface()
	if err := task.GetOutput(outputInstance); err != nil {
		slog.Error("Failed to unmarshal task output", "taskID", task.ID, "error", err)
		w.queue.MoveTaskToFailed(ctx, task)
		return
	}

	task.Status = core.StatusCompleted
	if err := w.queue.UpdateTask(ctx, task, core.StatusCompleted); err != nil {
		slog.Error("Failed to update task status", "taskID", task.ID, "error", err)
		w.queue.MoveTaskToFailed(ctx, task)
		return
	}

	if err := w.queue.MoveTaskToCompleted(ctx, task); err != nil {
		slog.Error("Failed to move task to completed queue", "taskID", task.ID, "error", err)
		w.queue.MoveTaskToFailed(ctx, task)
		return
	}

	slog.Info("Task completed successfully", "taskID", task.ID)
}

func (w *Worker) handleTaskError(ctx context.Context, task *core.Task, err error) {
	task.SetError(err)
	slog.Error("Task failed", "taskID", task.ID, "error", err)
	w.queue.MoveTaskToFailed(ctx, task)
}

func (w *Worker) handleTaskTimeout(ctx context.Context, task *core.Task) error {
	return w.queue.MoveActiveTaskToPending(ctx, task)
}
