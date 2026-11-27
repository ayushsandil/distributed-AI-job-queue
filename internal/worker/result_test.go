package worker

import (
	"context"
	"testing"
	"time"

	"ai-job-queue/internal/domain"
	"ai-job-queue/internal/jobstore"
)

func TestProcessOnePersistsHandlerResult(t *testing.T) {
	store := jobstore.NewMemoryStore()
	now := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)
	job, err := domain.NewJob(domain.NewJobInput{
		ID:      "00000000-0000-4000-8000-000000000050",
		Type:    "result.job",
		Payload: []byte(`{}`),
		RetryPolicy: domain.RetryPolicy{
			MaxRetries: 1,
		},
	}, now)
	if err != nil {
		t.Fatalf("new job: %v", err)
	}
	if _, err := store.Create(context.Background(), job); err != nil {
		t.Fatalf("create job: %v", err)
	}

	processor, err := NewProcessor(store, Config{
		WorkerID:      "worker-1",
		LeaseDuration: time.Minute,
		ResultHandlers: map[string]ResultHandler{
			"result.job": func(context.Context, domain.Job) (HandlerResult, error) {
				return HandlerResult{Result: []byte(`{"answer":"ok"}`)}, nil
			},
		},
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	processed, err := processor.ProcessOne(context.Background())
	if err != nil || !processed {
		t.Fatalf("process job: processed=%v err=%v", processed, err)
	}

	completed, err := store.GetByID(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("get completed job: %v", err)
	}
	if string(completed.Result) != `{"answer":"ok"}` {
		t.Fatalf("expected persisted result, got %s", completed.Result)
	}
}
