package jobstore

import (
	"context"
	"sort"
	"sync"
	"time"

	"ai-job-queue/internal/domain"
)

type MemoryStore struct {
	mu         sync.RWMutex
	jobs       map[string]domain.Job
	executions map[string]domain.JobExecution
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		jobs:       make(map[string]domain.Job),
		executions: make(map[string]domain.JobExecution),
	}
}

func (s *MemoryStore) Create(_ context.Context, job domain.Job) (domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs[job.ID] = job
	return job, nil
}

func (s *MemoryStore) GetByID(_ context.Context, id string) (domain.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, ok := s.jobs[id]
	if !ok {
		return domain.Job{}, ErrJobNotFound
	}

	return job, nil
}

func (s *MemoryStore) List(_ context.Context, filter ListFilter) ([]domain.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make([]domain.Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		if filter.Status != nil && job.Status != *filter.Status {
			continue
		}
		if filter.Type != nil && job.Type != *filter.Type {
			continue
		}
		if filter.CreatedFrom != nil && job.CreatedAt.Before(*filter.CreatedFrom) {
			continue
		}
		if filter.CreatedTo != nil && job.CreatedAt.After(*filter.CreatedTo) {
			continue
		}

		jobs = append(jobs, job)
	}

	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].CreatedAt.After(jobs[j].CreatedAt)
	})

	return jobs, nil
}

func (s *MemoryStore) ClaimNext(_ context.Context, workerID string, leaseDuration time.Duration, now time.Time) (domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var claimed *domain.Job
	for _, candidate := range s.jobs {
		if !candidate.IsEligible(now) {
			continue
		}
		if claimed == nil || shouldClaimBefore(candidate, *claimed) {
			job := candidate
			claimed = &job
		}
	}

	if claimed == nil {
		return domain.Job{}, ErrNoJobAvailable
	}

	lockedBy := workerID
	lockedAt := now
	lockExpiresAt := now.Add(leaseDuration)

	claimed.Status = domain.JobStatusRunning
	claimed.LockedBy = &lockedBy
	claimed.LockedAt = &lockedAt
	claimed.LockExpiresAt = &lockExpiresAt
	claimed.UpdatedAt = now
	s.jobs[claimed.ID] = *claimed

	return *claimed, nil
}

func (s *MemoryStore) ReclaimExpired(_ context.Context, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	reclaimed := 0
	for jobID, job := range s.jobs {
		if job.Status != domain.JobStatusRunning || job.LockExpiresAt == nil || job.LockExpiresAt.After(now) {
			continue
		}

		job.Status = domain.JobStatusPending
		job.LockedBy = nil
		job.LockedAt = nil
		job.LockExpiresAt = nil
		job.UpdatedAt = now
		s.jobs[jobID] = job
		reclaimed++

		for executionID, execution := range s.executions {
			if execution.JobID != jobID || execution.Status != domain.ExecutionStatusRunning {
				continue
			}

			errorMessage := LeaseExpiredError
			execution.Status = domain.ExecutionStatusFailed
			execution.EndedAt = &now
			execution.ErrorMessage = &errorMessage
			s.executions[executionID] = execution
		}
	}

	return reclaimed, nil
}

func (s *MemoryStore) CreateExecution(_ context.Context, execution domain.JobExecution) (domain.JobExecution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.executions[execution.ID] = execution
	return execution, nil
}

func (s *MemoryStore) CompleteExecution(_ context.Context, executionID string, endedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	execution, ok := s.executions[executionID]
	if !ok {
		return ErrExecutionNotFound
	}

	execution.Status = domain.ExecutionStatusCompleted
	execution.EndedAt = &endedAt
	s.executions[executionID] = execution
	return nil
}

func (s *MemoryStore) FailExecution(_ context.Context, executionID string, endedAt time.Time, errorMessage string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	execution, ok := s.executions[executionID]
	if !ok {
		return ErrExecutionNotFound
	}

	execution.Status = domain.ExecutionStatusFailed
	execution.EndedAt = &endedAt
	execution.ErrorMessage = &errorMessage
	s.executions[executionID] = execution
	return nil
}

func (s *MemoryStore) CompleteJob(_ context.Context, jobID string, now time.Time) (domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return domain.Job{}, ErrJobNotFound
	}

	job = job.Complete(now)
	s.jobs[jobID] = job
	return job, nil
}

func (s *MemoryStore) FailJob(_ context.Context, jobID string, errorMessage string, policy domain.RetryPolicy, now time.Time) (domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return domain.Job{}, ErrJobNotFound
	}

	job = job.Fail(errorMessage, policy, now)
	s.jobs[jobID] = job
	return job, nil
}

func shouldClaimBefore(candidate, current domain.Job) bool {
	if candidate.Priority != current.Priority {
		return candidate.Priority > current.Priority
	}
	return candidate.CreatedAt.Before(current.CreatedAt)
}
