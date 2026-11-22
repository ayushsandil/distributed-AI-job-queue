package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"ai-job-queue/internal/api"
	"ai-job-queue/internal/jobstore"
	"ai-job-queue/internal/postgres"
	"ai-job-queue/internal/redisqueue"
)

func main() {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	store, cleanup := openStore()
	defer cleanup()
	publisher, cleanupPublisher := openPublisher()
	defer cleanupPublisher()

	if publisher == nil {
		server := api.NewServer(store)
		startServer(addr, server)
		return
	}

	server := api.NewServer(store, publisher)
	startServer(addr, server)
}

func startServer(addr string, server http.Handler) {

	log.Printf("api listening on %s", addr)
	if err := http.ListenAndServe(addr, server); err != nil {
		log.Fatal(err)
	}
}

func openStore() (jobstore.Store, func()) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		if os.Getenv("QUEUE_BACKEND") == "redis" {
			log.Fatal("DATABASE_URL is required when QUEUE_BACKEND=redis")
		}
		log.Printf("DATABASE_URL not set; using in-memory store")
		return jobstore.NewMemoryStore(), func() {}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}

	if os.Getenv("RUN_MIGRATIONS") != "false" {
		migrationsDir := os.Getenv("MIGRATIONS_DIR")
		if migrationsDir == "" {
			migrationsDir = "migrations"
		}
		if err := postgres.RunMigrations(ctx, db, migrationsDir); err != nil {
			_ = db.Close()
			log.Fatalf("run migrations: %v", err)
		}
	}

	log.Printf("using PostgreSQL store")
	return postgres.NewStore(db), func() {
		_ = db.Close()
	}
}

func openPublisher() (jobstore.Publisher, func()) {
	if os.Getenv("QUEUE_BACKEND") != "redis" {
		return nil, func() {}
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Fatal("REDIS_URL is required when QUEUE_BACKEND=redis")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := redisqueue.Open(ctx, redisURL, os.Getenv("REDIS_STREAM"), os.Getenv("REDIS_GROUP"))
	if err != nil {
		log.Fatalf("open redis stream: %v", err)
	}

	log.Printf("using Redis Stream queue")
	return stream, func() {
		_ = stream.Close()
	}
}
