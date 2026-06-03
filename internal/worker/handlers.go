package worker

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"ai-job-queue/internal/domain"
)

var ErrForcedFailure = errors.New("forced job failure")

type SleepPayload struct {
	DurationMS int `json:"duration_ms"`
}

func BuiltInHandlers() map[string]Handler {
	return map[string]Handler{
		"noop": func(context.Context, domain.Job) error {
			return nil
		},
		"email.send": func(context.Context, domain.Job) error {
			return nil
		},
		"image.resize": func(context.Context, domain.Job) error {
			return nil
		},
		"always.fail": func(context.Context, domain.Job) error {
			return ErrForcedFailure
		},
		"sleep": sleepHandler,
	}
}

func sleepHandler(ctx context.Context, job domain.Job) error {
	var payload SleepPayload
	if len(job.Payload) > 0 {
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return err
		}
	}
	if payload.DurationMS <= 0 {
		return nil
	}

	timer := time.NewTimer(time.Duration(payload.DurationMS) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
