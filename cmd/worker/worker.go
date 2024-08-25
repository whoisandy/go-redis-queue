package main

import (
	"context"
	"go-redis-queue/cmd/tasks"
	"go-redis-queue/queue"
	"go-redis-queue/worker"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	q := queue.NewRedisQueueAdapter(redisClient)
	q.RegisterTask(&tasks.SendEmailTask{})
	q.RegisterTask(&tasks.ErrorTask{})

	wrk := worker.NewWorker(ctx, q, 5, 3)
	wrk.Start()
}
