package ai

import (
	"context"
	"errors"
	"fmt"
)

type GenerateRequest struct {
	Model           string
	Prompt          string
	MaxOutputTokens int
	Temperature     *float64
}

type GenerateResponse struct {
	Text         string
	Provider     string
	Model        string
	RequestID    string
	InputTokens  int
	OutputTokens int
	TotalTokens  int
}

type Provider interface {
	Generate(ctx context.Context, request GenerateRequest) (GenerateResponse, error)
}

type ProviderError struct {
	StatusCode int
	Retryable  bool
	Message    string
}

func (e *ProviderError) Error() string {
	if e.StatusCode == 0 {
		return e.Message
	}
	return fmt.Sprintf("AI provider returned HTTP %d: %s", e.StatusCode, e.Message)
}

type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}

func IsRetryable(err error) bool {
	var providerErr *ProviderError
	if errors.As(err, &providerErr) {
		return providerErr.Retryable
	}
	var validationErr *ValidationError
	if errors.As(err, &validationErr) {
		return false
	}
	return !errors.Is(err, context.Canceled)
}
