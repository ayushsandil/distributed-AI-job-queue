package jobstore

import (
	"context"
	"sort"
	"sync"

	"ai-job-queue/internal/domain"
)

type MemoryStore struct {
	mu   sync.RWMutex
	jobs map[string]domain.Job
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		jobs: make(map[string]domain.Job),
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
		if filter.CreatedAt != nil && job.CreatedAt.Before(*filter.CreatedAt) {
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
