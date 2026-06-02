package jobstore

import (
	"context"
	"errors"
	"time"

	"ai-job-queue/internal/domain"
)

var ErrJobNotFound = errors.New("job not found")

type ListFilter struct {
	Status      *domain.JobStatus
	Type        *string
	CreatedFrom *time.Time
	CreatedTo   *time.Time
}

type Store interface {
	Create(ctx context.Context, job domain.Job) (domain.Job, error)
	GetByID(ctx context.Context, id string) (domain.Job, error)
	List(ctx context.Context, filter ListFilter) ([]domain.Job, error)
}
