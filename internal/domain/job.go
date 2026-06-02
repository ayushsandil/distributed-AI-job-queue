package domain

import (
	"errors"
	"time"
)

type JobStatus string

const (
	JobStatusPending    JobStatus = "PENDING"
	JobStatusRunning    JobStatus = "RUNNING"
	JobStatusCompleted  JobStatus = "COMPLETED"
	JobStatusFailed     JobStatus = "FAILED"
	JobStatusDeadLetter JobStatus = "DEAD_LETTER"
)

var (
	ErrInvalidJobType    = errors.New("job type is required")
	ErrInvalidMaxRetries = errors.New("max retries cannot be negative")
	ErrInvalidPriority   = errors.New("priority cannot be negative")
)

type RetryPolicy struct {
	MaxRetries   int
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

type Job struct {
	ID            string
	Type          string
	Payload       []byte
	Priority      int
	Status        JobStatus
	RetryCount    int
	MaxRetries    int
	ExecuteAfter  time.Time
	LockedBy      *string
	LockedAt      *time.Time
	LockExpiresAt *time.Time
	LastError     *string
	CompletedAt   *time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type NewJobInput struct {
	ID           string
	Type         string
	Payload      []byte
	Priority     int
	ExecuteAfter *time.Time
	RetryPolicy  RetryPolicy
}

func NewJob(input NewJobInput, now time.Time) (Job, error) {
	if input.Type == "" {
		return Job{}, ErrInvalidJobType
	}
	if input.Priority < 0 {
		return Job{}, ErrInvalidPriority
	}
	if input.RetryPolicy.MaxRetries < 0 {
		return Job{}, ErrInvalidMaxRetries
	}

	executeAfter := now
	if input.ExecuteAfter != nil {
		executeAfter = *input.ExecuteAfter
	}

	return Job{
		ID:           input.ID,
		Type:         input.Type,
		Payload:      input.Payload,
		Priority:     input.Priority,
		Status:       JobStatusPending,
		RetryCount:   0,
		MaxRetries:   input.RetryPolicy.MaxRetries,
		ExecuteAfter: executeAfter,
		CreatedAt:    now,
		UpdatedAt:    now,
	}, nil
}

func (j Job) IsEligible(now time.Time) bool {
	return j.Status == JobStatusPending && !j.ExecuteAfter.After(now)
}

func (j Job) CanRetry() bool {
	return j.RetryCount < j.MaxRetries
}

func (j Job) NextRetryDelay(policy RetryPolicy) time.Duration {
	if policy.InitialDelay <= 0 {
		return 0
	}

	delay := policy.InitialDelay
	for i := 0; i < j.RetryCount; i++ {
		delay *= 2
		if policy.MaxDelay > 0 && delay >= policy.MaxDelay {
			return policy.MaxDelay
		}
	}

	return delay
}

func (j Job) Fail(errorMessage string, policy RetryPolicy, now time.Time) Job {
	j.LastError = &errorMessage
	j.UpdatedAt = now
	j.clearLock()

	if j.CanRetry() {
		j.RetryCount++
		j.Status = JobStatusPending
		j.ExecuteAfter = now.Add(j.NextRetryDelay(policy))
		return j
	}

	j.Status = JobStatusDeadLetter
	return j
}

func (j Job) Complete(now time.Time) Job {
	j.Status = JobStatusCompleted
	j.CompletedAt = &now
	j.UpdatedAt = now
	j.clearLock()
	return j
}

func (j *Job) clearLock() {
	j.LockedBy = nil
	j.LockedAt = nil
	j.LockExpiresAt = nil
}
