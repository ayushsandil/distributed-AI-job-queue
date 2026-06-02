package postgres

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"ai-job-queue/internal/domain"
	"ai-job-queue/internal/jobstore"
)

func TestStoreIntegrationCreateGetList(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()
	db, err := Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	resetTestDatabase(t, ctx, db)
	if err := RunMigrations(ctx, db, "../../migrations"); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	store := NewStore(db)
	now := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	job, err := domain.NewJob(domain.NewJobInput{
		ID:       "00000000-0000-4000-8000-000000000001",
		Type:     "email.send",
		Payload:  []byte(`{"to":"user@example.com"}`),
		Priority: 5,
		RetryPolicy: domain.RetryPolicy{
			MaxRetries: 3,
		},
	}, now)
	if err != nil {
		t.Fatalf("new job: %v", err)
	}

	created, err := store.Create(ctx, job)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if created.ID != job.ID {
		t.Fatalf("expected job ID %s, got %s", job.ID, created.ID)
	}

	got, err := store.GetByID(ctx, job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got.Type != "email.send" || got.Status != domain.JobStatusPending {
		t.Fatalf("unexpected job: %+v", got)
	}

	status := domain.JobStatusPending
	jobs, err := store.List(ctx, jobstore.ListFilter{Status: &status})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
}

func resetTestDatabase(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()

	_, err := db.ExecContext(ctx, `
		DROP TABLE IF EXISTS job_executions;
		DROP TABLE IF EXISTS jobs;
		DROP TABLE IF EXISTS schema_migrations;
	`)
	if err != nil {
		t.Fatalf("reset test database: %v", err)
	}
}
