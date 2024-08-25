package core

import "sync"

type TaskRegistry struct {
	tasks map[string]TaskInterface
	mu    sync.RWMutex
}

func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{
		tasks: make(map[string]TaskInterface),
	}
}

func (r *TaskRegistry) RegisterTask(task TaskInterface) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tasks[task.Name()] = task
}

func (r *TaskRegistry) GetTask(taskName string) (TaskInterface, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	task, exists := r.tasks[taskName]
	return task, exists
}
