package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"ai-job-queue/internal/domain"
	"ai-job-queue/internal/ids"
	"ai-job-queue/internal/jobstore"
)

type Handler func(ctx context.Context, job domain.Job) error

type Store interface {
	ClaimNext(ctx context.Context, workerID string, leaseDuration time.Duration, now time.Time) (domain.Job, error)
	ReclaimExpired(ctx context.Context, now time.Time) (int, error)
	CreateExecution(ctx context.Context, execution domain.JobExecution) (domain.JobExecution, error)
	CompleteExecution(ctx context.Context, executionID string, endedAt time.Time) error
	FailExecution(ctx context.Context, executionID string, endedAt time.Time, errorMessage string) error
	CompleteJob(ctx context.Context, jobID string, now time.Time) (domain.Job, error)
	FailJob(ctx context.Context, jobID string, errorMessage string, policy domain.RetryPolicy, now time.Time) (domain.Job, error)
}

type Config struct {
	WorkerID      string
	LeaseDuration time.Duration
	RetryPolicy   domain.RetryPolicy
	Handlers      map[string]Handler
	Now           func() time.Time
}

type Processor struct {
	store         Store
	workerID      string
	leaseDuration time.Duration
	retryPolicy   domain.RetryPolicy
	handlers      map[string]Handler
	now           func() time.Time
}

func NewProcessor(store Store, config Config) (*Processor, error) {
	if config.WorkerID == "" {
		return nil, errors.New("worker ID is required")
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = time.Minute
	}
	if config.RetryPolicy.InitialDelay <= 0 {
		config.RetryPolicy.InitialDelay = time.Second
	}
	if config.RetryPolicy.MaxDelay <= 0 {
		config.RetryPolicy.MaxDelay = time.Minute
	}
	if config.Handlers == nil {
		config.Handlers = BuiltInHandlers()
	}
	if config.Now == nil {
		config.Now = time.Now
	}

	return &Processor{
		store:         store,
		workerID:      config.WorkerID,
		leaseDuration: config.LeaseDuration,
		retryPolicy:   config.RetryPolicy,
		handlers:      config.Handlers,
		now:           config.Now,
	}, nil
}

func (p *Processor) ProcessOne(ctx context.Context) (bool, error) {
	job, err := p.store.ClaimNext(ctx, p.workerID, p.leaseDuration, p.now().UTC())
	if errors.Is(err, jobstore.ErrNoJobAvailable) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	startedAt := p.now().UTC()
	execution := domain.JobExecution{
		ID:        ids.NewUUID(),
		JobID:     job.ID,
		WorkerID:  p.workerID,
		StartedAt: startedAt,
		Status:    domain.ExecutionStatusRunning,
	}
	createdExecution, err := p.store.CreateExecution(ctx, execution)
	if err != nil {
		return true, err
	}

	handler, ok := p.handlers[job.Type]
	if !ok {
		err = fmt.Errorf("no handler registered for job type %q", job.Type)
	} else {
		err = handler(ctx, job)
	}

	endedAt := p.now().UTC()
	if err != nil {
		errorMessage := err.Error()
		if markErr := p.store.FailExecution(ctx, createdExecution.ID, endedAt, errorMessage); markErr != nil {
			return true, markErr
		}
		if _, failErr := p.store.FailJob(ctx, job.ID, errorMessage, p.retryPolicy, endedAt); failErr != nil {
			return true, failErr
		}
		return true, nil
	}

	if err := p.store.CompleteExecution(ctx, createdExecution.ID, endedAt); err != nil {
		return true, err
	}
	if _, err := p.store.CompleteJob(ctx, job.ID, endedAt); err != nil {
		return true, err
	}

	return true, nil
}

func (p *Processor) Run(ctx context.Context, pollInterval time.Duration) error {
	if pollInterval <= 0 {
		pollInterval = time.Second
	}

	for {
		if _, err := p.store.ReclaimExpired(ctx, p.now().UTC()); err != nil {
			return err
		}

		processed, err := p.ProcessOne(ctx)
		if err != nil {
			return err
		}
		if processed {
			continue
		}

		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
