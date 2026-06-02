package domain

import (
	"errors"
	"testing"
	"time"
)

func TestNewJobDefaultsToPendingAndImmediateExecution(t *testing.T) {
	now := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)

	job, err := NewJob(NewJobInput{
		ID:       "job-1",
		Type:     "email.send",
		Priority: 10,
		Payload:  []byte(`{"to":"user@example.com"}`),
		RetryPolicy: RetryPolicy{
			MaxRetries: 3,
		},
	}, now)

	if err != nil {
		t.Fatalf("NewJob returned error: %v", err)
	}
	if job.Status != JobStatusPending {
		t.Fatalf("expected status %s, got %s", JobStatusPending, job.Status)
	}
	if !job.ExecuteAfter.Equal(now) {
		t.Fatalf("expected execute_after to default to now")
	}
	if job.RetryCount != 0 {
		t.Fatalf("expected retry count 0, got %d", job.RetryCount)
	}
}

func TestNewJobValidatesInput(t *testing.T) {
	now := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		input   NewJobInput
		wantErr error
	}{
		{
			name: "missing type",
			input: NewJobInput{
				RetryPolicy: RetryPolicy{MaxRetries: 1},
			},
			wantErr: ErrInvalidJobType,
		},
		{
			name: "negative priority",
			input: NewJobInput{
				Type:        "email.send",
				Priority:    -1,
				RetryPolicy: RetryPolicy{MaxRetries: 1},
			},
			wantErr: ErrInvalidPriority,
		},
		{
			name: "negative max retries",
			input: NewJobInput{
				Type:        "email.send",
				RetryPolicy: RetryPolicy{MaxRetries: -1},
			},
			wantErr: ErrInvalidMaxRetries,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewJob(tt.input, now)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("expected %v, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestJobEligibilityRequiresPendingStatusAndExecuteAfter(t *testing.T) {
	now := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	future := now.Add(time.Minute)

	pendingNow := Job{Status: JobStatusPending, ExecuteAfter: now}
	pendingFuture := Job{Status: JobStatusPending, ExecuteAfter: future}
	runningNow := Job{Status: JobStatusRunning, ExecuteAfter: now}

	if !pendingNow.IsEligible(now) {
		t.Fatalf("expected pending immediate job to be eligible")
	}
	if pendingFuture.IsEligible(now) {
		t.Fatalf("expected future job to be ineligible")
	}
	if runningNow.IsEligible(now) {
		t.Fatalf("expected running job to be ineligible")
	}
}

func TestFailRetriesUntilMaxRetriesThenDeadLetters(t *testing.T) {
	now := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	policy := RetryPolicy{
		MaxRetries:   2,
		InitialDelay: time.Second,
		MaxDelay:     10 * time.Second,
	}
	job := Job{
		ID:            "job-1",
		Status:        JobStatusRunning,
		RetryCount:    0,
		MaxRetries:    policy.MaxRetries,
		LockedBy:      strPtr("worker-1"),
		LockedAt:      timePtr(now),
		LockExpiresAt: timePtr(now.Add(time.Minute)),
		UpdatedAt:     now,
	}

	job = job.Fail("first failure", policy, now)
	if job.Status != JobStatusPending {
		t.Fatalf("expected retryable job to return to pending, got %s", job.Status)
	}
	if job.RetryCount != 1 {
		t.Fatalf("expected retry count 1, got %d", job.RetryCount)
	}
	if !job.ExecuteAfter.Equal(now.Add(2 * time.Second)) {
		t.Fatalf("expected second attempt to use exponential delay")
	}
	if job.LockedBy != nil || job.LockedAt != nil || job.LockExpiresAt != nil {
		t.Fatalf("expected lock fields to be cleared after failure")
	}

	job.Status = JobStatusRunning
	job = job.Fail("second failure", policy, now)
	if job.Status != JobStatusPending {
		t.Fatalf("expected final retry to return to pending, got %s", job.Status)
	}
	if job.RetryCount != 2 {
		t.Fatalf("expected retry count 2, got %d", job.RetryCount)
	}

	job.Status = JobStatusRunning
	job = job.Fail("third failure", policy, now)
	if job.Status != JobStatusDeadLetter {
		t.Fatalf("expected exhausted job to be dead-lettered, got %s", job.Status)
	}
	if job.RetryCount != 2 {
		t.Fatalf("expected retry count to stay at max retries, got %d", job.RetryCount)
	}
}

func TestCompleteMarksJobCompletedAndClearsLock(t *testing.T) {
	now := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	job := Job{
		Status:        JobStatusRunning,
		LockedBy:      strPtr("worker-1"),
		LockedAt:      timePtr(now.Add(-time.Minute)),
		LockExpiresAt: timePtr(now.Add(time.Minute)),
	}

	job = job.Complete(now)

	if job.Status != JobStatusCompleted {
		t.Fatalf("expected completed status, got %s", job.Status)
	}
	if job.CompletedAt == nil || !job.CompletedAt.Equal(now) {
		t.Fatalf("expected completed_at to be set")
	}
	if job.LockedBy != nil || job.LockedAt != nil || job.LockExpiresAt != nil {
		t.Fatalf("expected lock fields to be cleared after completion")
	}
}

func strPtr(value string) *string {
	return &value
}

func timePtr(value time.Time) *time.Time {
	return &value
}
