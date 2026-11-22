package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"ai-job-queue/internal/domain"
	"ai-job-queue/internal/ids"
	"ai-job-queue/internal/postgres"
	"ai-job-queue/internal/redisqueue"
	"ai-job-queue/internal/worker"
)

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required for worker")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	dbCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	db, err := postgres.Open(dbCtx, databaseURL)
	cancel()
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}
	defer db.Close()

	if os.Getenv("RUN_MIGRATIONS") != "false" {
		migrationsDir := getenv("MIGRATIONS_DIR", "migrations")
		if err := postgres.RunMigrations(ctx, db, migrationsDir); err != nil {
			log.Fatalf("run migrations: %v", err)
		}
	}

	workerID := getenv("WORKER_ID", "worker-"+ids.NewUUID())
	leaseDuration := durationEnv("LEASE_DURATION", time.Minute)
	retryPolicy := domain.RetryPolicy{
		InitialDelay: durationEnv("RETRY_INITIAL_DELAY", time.Second),
		MaxDelay:     durationEnv("RETRY_MAX_DELAY", time.Minute),
	}

	if os.Getenv("QUEUE_BACKEND") == "redis" {
		runRedisWorker(ctx, postgres.NewStore(db), workerID, leaseDuration, retryPolicy)
		return
	}

	processor, err := worker.NewProcessor(postgres.NewStore(db), worker.Config{
		WorkerID:      workerID,
		LeaseDuration: leaseDuration,
		RetryPolicy:   retryPolicy,
	})
	if err != nil {
		log.Fatalf("new processor: %v", err)
	}

	pollInterval := durationEnv("POLL_INTERVAL", time.Second)
	log.Printf("postgres worker %s started", workerID)
	if err := processor.Run(ctx, pollInterval); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("worker stopped: %v", err)
	}
	log.Printf("postgres worker %s stopped", workerID)
}

func runRedisWorker(ctx context.Context, store *postgres.Store, workerID string, leaseDuration time.Duration, retryPolicy domain.RetryPolicy) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Fatal("REDIS_URL is required when QUEUE_BACKEND=redis")
	}

	stream, err := redisqueue.Open(ctx, redisURL, os.Getenv("REDIS_STREAM"), os.Getenv("REDIS_GROUP"))
	if err != nil {
		log.Fatalf("open redis stream: %v", err)
	}
	defer stream.Close()

	processor, err := worker.NewStreamProcessor(store, stream, worker.StreamConfig{
		WorkerID:      workerID,
		LeaseDuration: leaseDuration,
		RetryPolicy:   retryPolicy,
		BatchSize:     int64Env("STREAM_BATCH_SIZE", 10),
		PollInterval:  durationEnv("POLL_INTERVAL", time.Second),
	})
	if err != nil {
		log.Fatalf("new redis stream processor: %v", err)
	}

	log.Printf("redis worker %s started", workerID)
	if err := processor.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("redis worker stopped: %v", err)
	}
	log.Printf("redis worker %s stopped", workerID)
}

func getenv(name string, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
}

func durationEnv(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		log.Fatalf("%s must be a valid Go duration: %v", name, err)
	}
	return parsed
}

func int64Env(name string, fallback int64) int64 {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}

	var parsed int64
	if _, err := fmt.Sscan(value, &parsed); err != nil || parsed <= 0 {
		log.Fatalf("%s must be a positive integer: %s", name, value)
	}
	return parsed
}
