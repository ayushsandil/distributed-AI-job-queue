package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"time"

	"ai-job-queue/internal/domain"
	"ai-job-queue/internal/ids"
	"ai-job-queue/internal/postgres"
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
	processor, err := worker.NewProcessor(postgres.NewStore(db), worker.Config{
		WorkerID:      workerID,
		LeaseDuration: durationEnv("LEASE_DURATION", time.Minute),
		RetryPolicy: domain.RetryPolicy{
			InitialDelay: durationEnv("RETRY_INITIAL_DELAY", time.Second),
			MaxDelay:     durationEnv("RETRY_MAX_DELAY", time.Minute),
		},
	})
	if err != nil {
		log.Fatalf("new processor: %v", err)
	}

	pollInterval := durationEnv("POLL_INTERVAL", time.Second)
	log.Printf("worker %s started", workerID)
	if err := processor.Run(ctx, pollInterval); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("worker stopped: %v", err)
	}
	log.Printf("worker %s stopped", workerID)
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
