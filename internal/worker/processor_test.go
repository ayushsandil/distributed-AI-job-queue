package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"ai-job-queue/internal/domain"
	"ai-job-queue/internal/jobstore"
)

func TestProcessOneCompletesSuccessfulJob(t *testing.T) {
	store := jobstore.NewMemoryStore()
	now := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)
	job := createWorkerTestJob(t, store, "noop", now)
	processor := newTestProcessor(t, store, now, map[string]Handler{
		"noop": func(context.Context, domain.Job) error {
			return nil
		},
	})

	processed, err := processor.ProcessOne(context.Background())
	if err != nil {
		t.Fatalf("ProcessOne returned error: %v", err)
	}
	if !processed {
		t.Fatalf("expected one job to be processed")
	}

	updated, err := store.GetByID(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if updated.Status != domain.JobStatusCompleted {
		t.Fatalf("expected completed job, got %s", updated.Status)
	}
	if updated.LockedBy != nil || updated.LockedAt != nil || updated.LockExpiresAt != nil {
		t.Fatalf("expected job lock to be cleared")
	}
}

func TestProcessOneRetriesFailedJob(t *testing.T) {
	store := jobstore.NewMemoryStore()
	now := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)
	job := createWorkerTestJob(t, store, "always.fail", now)
	processor := newTestProcessor(t, store, now, map[string]Handler{
		"always.fail": func(context.Context, domain.Job) error {
			return errors.New("boom")
		},
	})

	processed, err := processor.ProcessOne(context.Background())
	if err != nil {
		t.Fatalf("ProcessOne returned error: %v", err)
	}
	if !processed {
		t.Fatalf("expected one job to be processed")
	}

	updated, err := store.GetByID(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if updated.Status != domain.JobStatusPending {
		t.Fatalf("expected failed job to return to pending, got %s", updated.Status)
	}
	if updated.RetryCount != 1 {
		t.Fatalf("expected retry count 1, got %d", updated.RetryCount)
	}
	if updated.LastError == nil || *updated.LastError != "boom" {
		t.Fatalf("expected last error to be recorded")
	}
	if !updated.ExecuteAfter.After(now) {
		t.Fatalf("expected retry to be scheduled in the future")
	}
}

func TestProcessOneReturnsFalseWhenNoJobAvailable(t *testing.T) {
	store := jobstore.NewMemoryStore()
	now := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)
	processor := newTestProcessor(t, store, now, nil)

	processed, err := processor.ProcessOne(context.Background())
	if err != nil {
		t.Fatalf("ProcessOne returned error: %v", err)
	}
	if processed {
		t.Fatalf("expected no job to be processed")
	}
}

func TestExpiredLeaseIsReclaimedAndCanBeProcessedByAnotherWorker(t *testing.T) {
	store := jobstore.NewMemoryStore()
	claimedAt := time.Date(2026, 6, 4, 10, 0, 0, 0, time.UTC)
	recoveryAt := claimedAt.Add(2 * time.Minute)
	job := createWorkerTestJob(t, store, "tracked", claimedAt)

	claimed, err := store.ClaimNext(context.Background(), "crashed-worker", time.Minute, claimedAt)
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}
	if claimed.ID != job.ID {
		t.Fatalf("expected claimed job %s, got %s", job.ID, claimed.ID)
	}

	execution, err := store.CreateExecution(context.Background(), domain.JobExecution{
		ID:        "00000000-0000-4000-8000-000000000099",
		JobID:     job.ID,
		WorkerID:  "crashed-worker",
		StartedAt: claimedAt,
		Status:    domain.ExecutionStatusRunning,
	})
	if err != nil {
		t.Fatalf("create execution: %v", err)
	}

	reclaimed, err := store.ReclaimExpired(context.Background(), recoveryAt)
	if err != nil {
		t.Fatalf("reclaim expired job: %v", err)
	}
	if reclaimed != 1 {
		t.Fatalf("expected 1 reclaimed job, got %d", reclaimed)
	}

	requeued, err := store.GetByID(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("get requeued job: %v", err)
	}
	if requeued.Status != domain.JobStatusPending {
		t.Fatalf("expected requeued job to be pending, got %s", requeued.Status)
	}
	if requeued.LockedBy != nil || requeued.LockedAt != nil || requeued.LockExpiresAt != nil {
		t.Fatalf("expected expired lease fields to be cleared")
	}

	processor := newTestProcessorWithWorkerID(t, store, "recovery-worker", recoveryAt, map[string]Handler{
		"tracked": func(context.Context, domain.Job) error { return nil },
	})
	processed, err := processor.ProcessOne(context.Background())
	if err != nil {
		t.Fatalf("process recovered job: %v", err)
	}
	if !processed {
		t.Fatalf("expected recovery worker to process requeued job")
	}

	completed, err := store.GetByID(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("get completed job: %v", err)
	}
	if completed.Status != domain.JobStatusCompleted {
		t.Fatalf("expected recovered job to complete, got %s", completed.Status)
	}

	if execution.ID == "" {
		t.Fatalf("expected execution to be recorded")
	}
}

func TestMultipleWorkersProcessBatchWithoutDuplicates(t *testing.T) {
	store := jobstore.NewMemoryStore()
	now := time.Date(2026, 6, 4, 10, 0, 0, 0, time.UTC)
	ctx := context.Background()
	const jobCount = 100
	const workerCount = 5

	for i := 0; i < jobCount; i++ {
		createWorkerTestJobWithID(t, store, fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1), "tracked", now)
	}

	var mu sync.Mutex
	processedByJob := make(map[string]int)
	handlers := map[string]Handler{
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
		processor := newTestProcessorWithWorkerID(t, store, fmt.Sprintf("worker-%d", i+1), now, handlers)
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

	status := domain.JobStatusCompleted
	completedJobs, err := store.List(ctx, jobstore.ListFilter{Status: &status})
	if err != nil {
		t.Fatalf("list completed jobs: %v", err)
	}
	if len(completedJobs) != jobCount {
		t.Fatalf("expected %d completed jobs, got %d", jobCount, len(completedJobs))
	}
}

func createWorkerTestJob(t *testing.T, store *jobstore.MemoryStore, jobType string, now time.Time) domain.Job {
	t.Helper()

	return createWorkerTestJobWithID(t, store, "00000000-0000-4000-8000-000000000001", jobType, now)
}

func createWorkerTestJobWithID(t *testing.T, store *jobstore.MemoryStore, id string, jobType string, now time.Time) domain.Job {
	t.Helper()

	job, err := domain.NewJob(domain.NewJobInput{
		ID:       id,
		Type:     jobType,
		Payload:  []byte(`{}`),
		Priority: 1,
		RetryPolicy: domain.RetryPolicy{
			MaxRetries: 2,
		},
	}, now)
	if err != nil {
		t.Fatalf("new job: %v", err)
	}
	created, err := store.Create(context.Background(), job)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	return created
}

func newTestProcessor(t *testing.T, store Store, now time.Time, handlers map[string]Handler) *Processor {
	t.Helper()

	return newTestProcessorWithWorkerID(t, store, "worker-1", now, handlers)
}

func newTestProcessorWithWorkerID(t *testing.T, store Store, workerID string, now time.Time, handlers map[string]Handler) *Processor {
	t.Helper()

	processor, err := NewProcessor(store, Config{
		WorkerID:      workerID,
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

	return processor
}
