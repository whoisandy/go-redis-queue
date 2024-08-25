package main

import (
	"context"
	"go-redis-queue/cmd/tasks"
	"go-redis-queue/queue"
	"go-redis-queue/server"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

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

	srv := server.NewServer(ctx, q)
	go func() {
		if err := srv.Start(":8080"); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Println("Termination signal received. Shutting down server...")
	if err := srv.Stop(); err != nil {
		log.Fatalf("Failed to gracefully shut down server: %v", err)
	}
}
