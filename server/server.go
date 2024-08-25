package server

import (
	"context"
	"go-redis-queue/queue"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"golang.org/x/exp/slog"
)

var validate = validator.New()

type Server struct {
	router *gin.Engine
	queue  queue.Queue
	srv    *http.Server
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewServer(ctx context.Context, q queue.Queue) *Server {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	ctx, cancel := context.WithCancel(ctx)
	return &Server{
		router: router,
		queue:  q,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (s *Server) Start(addr string) error {
	s.router.POST("/:taskName", s.handleTaskRequest)

	s.srv = &http.Server{
		Addr:    addr,
		Handler: s.router,
	}

	// Handle graceful shutdown on signals
	go s.handleSignals()

	slog.Info("Starting server...", "addr", addr)
	return s.srv.ListenAndServe()
}

func (s *Server) Stop() error {
	slog.Info("Shutting down server...")
	s.cancel()
	s.wg.Wait()
	return s.srv.Shutdown(s.ctx)
}

func (s *Server) handleSignals() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	slog.Info("Termination signal received, shutting down server...")
	if err := s.Stop(); err != nil {
		slog.Error("Failed to gracefully shut down server", "error", err)
	}
}

func (s *Server) handleTaskRequest(c *gin.Context) {
	taskName := c.Param("taskName")
	task, exists := s.queue.GetTask(taskName)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Task not found"})
		return
	}

	payloadInstance := reflect.New(reflect.TypeOf(task.Payload()).Elem()).Interface()

	if err := c.ShouldBindJSON(payloadInstance); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request payload", "details": err.Error()})
		return
	}

	if err := validate.Struct(payloadInstance); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Validation failed", "details": err.Error()})
		return
	}

	ctx := c.Request.Context()
	taskInstance, err := s.queue.Enqueue(ctx, taskName, payloadInstance)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to enqueue task"})
		return
	}

	// slog.Info("Task enqueued successfully", "task", taskInstance)
	c.JSON(http.StatusOK, taskInstance)
}
