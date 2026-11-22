package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"ai-job-queue/internal/domain"
	"ai-job-queue/internal/ids"
	"ai-job-queue/internal/jobstore"
	"ai-job-queue/internal/redisqueue"
)

type StreamStore interface {
	GetByID(ctx context.Context, id string) (domain.Job, error)
	StartJob(ctx context.Context, jobID, workerID string, leaseDuration time.Duration, now time.Time) (domain.Job, error)
	CreateExecution(ctx context.Context, execution domain.JobExecution) (domain.JobExecution, error)
	CompleteExecution(ctx context.Context, executionID string, endedAt time.Time) error
	FailExecution(ctx context.Context, executionID string, endedAt time.Time, errorMessage string) error
	CompleteJob(ctx context.Context, jobID string, now time.Time) (domain.Job, error)
	FailJob(ctx context.Context, jobID string, errorMessage string, policy domain.RetryPolicy, now time.Time) (domain.Job, error)
}

type StreamClient interface {
	Read(ctx context.Context, consumer string, count int64, block time.Duration) ([]redisqueue.Message, error)
	AutoClaim(ctx context.Context, consumer string, minIdle time.Duration, start string, count int64) ([]redisqueue.Message, string, error)
	Ack(ctx context.Context, messageID string) error
	Publish(ctx context.Context, job domain.Job) error
	PublishDeadLetter(ctx context.Context, job domain.Job) error
}

type StreamConfig struct {
	WorkerID      string
	LeaseDuration time.Duration
	RetryPolicy   domain.RetryPolicy
	Handlers      map[string]Handler
	Now           func() time.Time
	BatchSize     int64
	PollInterval  time.Duration
}

type StreamProcessor struct {
	store         StreamStore
	stream        StreamClient
	workerID      string
	leaseDuration time.Duration
	retryPolicy   domain.RetryPolicy
	handlers      map[string]Handler
	now           func() time.Time
	batchSize     int64
	pollInterval  time.Duration
}

func NewStreamProcessor(store StreamStore, stream StreamClient, config StreamConfig) (*StreamProcessor, error) {
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
	if config.BatchSize <= 0 {
		config.BatchSize = 10
	}
	if config.PollInterval <= 0 {
		config.PollInterval = time.Second
	}

	return &StreamProcessor{
		store:         store,
		stream:        stream,
		workerID:      config.WorkerID,
		leaseDuration: config.LeaseDuration,
		retryPolicy:   config.RetryPolicy,
		handlers:      config.Handlers,
		now:           config.Now,
		batchSize:     config.BatchSize,
		pollInterval:  config.PollInterval,
	}, nil
}

func (p *StreamProcessor) Run(ctx context.Context) error {
	claimCursor := "0-0"
	for {
		claimed, nextCursor, err := p.stream.AutoClaim(ctx, p.workerID, p.leaseDuration, claimCursor, p.batchSize)
		if err != nil {
			return err
		}
		claimCursor = nextCursor
		for _, message := range claimed {
			if err := p.processMessage(ctx, message); err != nil {
				return err
			}
		}

		messages, err := p.stream.Read(ctx, p.workerID, p.batchSize, p.pollInterval)
		if err != nil {
			return err
		}
		for _, message := range messages {
			if err := p.processMessage(ctx, message); err != nil {
				return err
			}
		}
	}
}

func (p *StreamProcessor) processMessage(ctx context.Context, message redisqueue.Message) error {
	job, err := p.store.GetByID(ctx, message.JobID)
	if errors.Is(err, jobstore.ErrJobNotFound) {
		return p.stream.Ack(ctx, message.ID)
	}
	if err != nil {
		return fmt.Errorf("load streamed job %s: %w", message.JobID, err)
	}
	if job.Status != domain.JobStatusPending {
		return p.stream.Ack(ctx, message.ID)
	}

	if delay := job.ExecuteAfter.Sub(p.now().UTC()); delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}

	now := p.now().UTC()
	job, err = p.store.StartJob(ctx, job.ID, p.workerID, p.leaseDuration, now)
	if errors.Is(err, jobstore.ErrJobNotClaimable) {
		return p.stream.Ack(ctx, message.ID)
	}
	if err != nil {
		return fmt.Errorf("start streamed job %s: %w", message.JobID, err)
	}

	execution, err := p.store.CreateExecution(ctx, domain.JobExecution{
		ID:        ids.NewUUID(),
		JobID:     job.ID,
		WorkerID:  p.workerID,
		StartedAt: now,
		Status:    domain.ExecutionStatusRunning,
	})
	if err != nil {
		return err
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
		if markErr := p.store.FailExecution(ctx, execution.ID, endedAt, errorMessage); markErr != nil {
			return markErr
		}
		failedJob, failErr := p.store.FailJob(ctx, job.ID, errorMessage, p.retryPolicy, endedAt)
		if failErr != nil {
			return failErr
		}
		if failedJob.Status == domain.JobStatusPending {
			if publishErr := p.stream.Publish(ctx, failedJob); publishErr != nil {
				return publishErr
			}
		} else if failedJob.Status == domain.JobStatusDeadLetter {
			if publishErr := p.stream.PublishDeadLetter(ctx, failedJob); publishErr != nil {
				return publishErr
			}
		}
		return p.stream.Ack(ctx, message.ID)
	}

	if err := p.store.CompleteExecution(ctx, execution.ID, endedAt); err != nil {
		return err
	}
	if _, err := p.store.CompleteJob(ctx, job.ID, endedAt); err != nil {
		return err
	}

	return p.stream.Ack(ctx, message.ID)
}
