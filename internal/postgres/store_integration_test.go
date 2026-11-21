package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"ai-job-queue/internal/domain"
	"ai-job-queue/internal/jobstore"
	"ai-job-queue/internal/worker"
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

func TestStoreIntegrationClaimAndComplete(t *testing.T) {
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
	now := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)
	job := createIntegrationJob(t, ctx, store, "00000000-0000-4000-8000-000000000002", "noop", now)

	claimed, err := store.ClaimNext(ctx, "worker-1", time.Minute, now)
	if err != nil {
		t.Fatalf("claim next: %v", err)
	}
	if claimed.ID != job.ID || claimed.Status != domain.JobStatusRunning {
		t.Fatalf("unexpected claimed job: %+v", claimed)
	}
	if claimed.LockedBy == nil || *claimed.LockedBy != "worker-1" {
		t.Fatalf("expected claimed job lock owner")
	}

	execution, err := store.CreateExecution(ctx, domain.JobExecution{
		ID:        "00000000-0000-4000-8000-000000000102",
		JobID:     job.ID,
		WorkerID:  "worker-1",
		StartedAt: now,
		Status:    domain.ExecutionStatusRunning,
	})
	if err != nil {
		t.Fatalf("create execution: %v", err)
	}
	if err := store.CompleteExecution(ctx, execution.ID, now.Add(time.Second)); err != nil {
		t.Fatalf("complete execution: %v", err)
	}

	completed, err := store.CompleteJob(ctx, job.ID, now.Add(time.Second))
	if err != nil {
		t.Fatalf("complete job: %v", err)
	}
	if completed.Status != domain.JobStatusCompleted {
		t.Fatalf("expected completed job, got %s", completed.Status)
	}
}

func TestStoreIntegrationMultipleWorkersProcessBatchWithoutDuplicates(t *testing.T) {
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
	now := time.Date(2026, 6, 4, 10, 0, 0, 0, time.UTC)
	const jobCount = 50
	const workerCount = 5

	for i := 0; i < jobCount; i++ {
		createIntegrationJob(t, ctx, store, fmt.Sprintf("00000000-0000-4000-8000-%012d", i+200), "tracked", now)
	}

	var mu sync.Mutex
	processedByJob := make(map[string]int)
	handlers := map[string]worker.Handler{
		"tracked": func(_ context.Context, job domain.Job) error {
			mu.Lock()
			defer mu.Unlock()
			processedByJob[job.ID]++
			return nil
		},
	}

	var wg sync.WaitGroup
	errs := make(chan error, workerCount)
	for i := 0; i < workerCount; i++ {
		processor, err := worker.NewProcessor(store, worker.Config{
			WorkerID:      fmt.Sprintf("worker-%d", i+1),
			LeaseDuration: time.Minute,
			RetryPolicy: domain.RetryPolicy{
				InitialDelay: time.Second,
				MaxDelay:     time.Minute,
			},
			Handlers: handlers,
			Now:      func() time.Time { return now },
		})
		if err != nil {
			t.Fatalf("new processor: %v", err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				processed, err := processor.ProcessOne(ctx)
				if err != nil {
					errs <- err
					return
				}
				if !processed {
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("worker returned error: %v", err)
	}

	if len(processedByJob) != jobCount {
		t.Fatalf("expected %d processed jobs, got %d", jobCount, len(processedByJob))
	}
	for jobID, count := range processedByJob {
		if count != 1 {
			t.Fatalf("expected job %s to be processed once, got %d", jobID, count)
		}
	}

	completedCount := countRows(t, ctx, db, "SELECT COUNT(*) FROM jobs WHERE status = 'COMPLETED'")
	if completedCount != jobCount {
		t.Fatalf("expected %d completed jobs, got %d", jobCount, completedCount)
	}

	executionCount := countRows(t, ctx, db, "SELECT COUNT(*) FROM job_executions WHERE status = 'COMPLETED'")
	if executionCount != jobCount {
		t.Fatalf("expected %d completed executions, got %d", jobCount, executionCount)
	}

	duplicateSuccesses := countRows(t, ctx, db, `
		SELECT COUNT(*)
		FROM (
			SELECT job_id
			FROM job_executions
			WHERE status = 'COMPLETED'
			GROUP BY job_id
			HAVING COUNT(*) > 1
		) duplicate_jobs
	`)
	if duplicateSuccesses != 0 {
		t.Fatalf("expected no duplicate completed executions, got %d", duplicateSuccesses)
	}

	_, err = store.ClaimNext(ctx, "late-worker", time.Minute, now)
	if !errors.Is(err, jobstore.ErrNoJobAvailable) {
		t.Fatalf("expected no remaining jobs, got %v", err)
	}
}

func TestStoreIntegrationReclaimsExpiredLease(t *testing.T) {
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
	claimedAt := time.Date(2026, 6, 4, 10, 0, 0, 0, time.UTC)
	recoveryAt := claimedAt.Add(2 * time.Minute)
	job := createIntegrationJob(t, ctx, store, "00000000-0000-4000-8000-000000000300", "noop", claimedAt)

	claimed, err := store.ClaimNext(ctx, "crashed-worker", time.Minute, claimedAt)
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}

	_, err = store.CreateExecution(ctx, domain.JobExecution{
		ID:        "00000000-0000-4000-8000-000000000301",
		JobID:     claimed.ID,
		WorkerID:  "crashed-worker",
		StartedAt: claimedAt,
		Status:    domain.ExecutionStatusRunning,
	})
	if err != nil {
		t.Fatalf("create execution: %v", err)
	}

	reclaimed, err := store.ReclaimExpired(ctx, recoveryAt)
	if err != nil {
		t.Fatalf("reclaim expired job: %v", err)
	}
	if reclaimed != 1 {
		t.Fatalf("expected 1 reclaimed job, got %d", reclaimed)
	}

	requeued, err := store.GetByID(ctx, job.ID)
	if err != nil {
		t.Fatalf("get requeued job: %v", err)
	}
	if requeued.Status != domain.JobStatusPending || requeued.LockedBy != nil {
		t.Fatalf("expected unlocked pending job, got %+v", requeued)
	}

	failedExecutions := countRows(t, ctx, db, "SELECT COUNT(*) FROM job_executions WHERE status = 'FAILED' AND error_message = 'worker lease expired'")
	if failedExecutions != 1 {
		t.Fatalf("expected 1 failed expired execution, got %d", failedExecutions)
	}
}

func createIntegrationJob(t *testing.T, ctx context.Context, store *Store, id string, jobType string, now time.Time) domain.Job {
	t.Helper()

	job, err := domain.NewJob(domain.NewJobInput{
		ID:       id,
		Type:     jobType,
		Payload:  []byte(`{}`),
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

	return created
}

func countRows(t *testing.T, ctx context.Context, db *sql.DB, query string) int {
	t.Helper()

	var count int
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return count
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
