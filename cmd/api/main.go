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
)

func main() {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	store, cleanup := openStore()
	defer cleanup()

	server := api.NewServer(store)

	log.Printf("api listening on %s", addr)
	if err := http.ListenAndServe(addr, server); err != nil {
		log.Fatal(err)
	}
}

func openStore() (jobstore.Store, func()) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
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
