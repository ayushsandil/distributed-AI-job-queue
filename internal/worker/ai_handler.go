package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"ai-job-queue/internal/ai"
	"ai-job-queue/internal/domain"
)

type AITextGeneratePayload struct {
	Prompt          string   `json:"prompt"`
	Model           string   `json:"model,omitempty"`
	MaxOutputTokens int      `json:"max_output_tokens,omitempty"`
	Temperature     *float64 `json:"temperature,omitempty"`
}

type AITextGenerateResult struct {
	Text         string `json:"text"`
	Provider     string `json:"provider"`
	Model        string `json:"model"`
	RequestID    string `json:"request_id,omitempty"`
	InputTokens  int    `json:"input_tokens"`
	OutputTokens int    `json:"output_tokens"`
	TotalTokens  int    `json:"total_tokens"`
}

func NewAITextGenerateHandler(provider ai.Provider, timeout time.Duration) ResultHandler {
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}

	return func(ctx context.Context, job domain.Job) (HandlerResult, error) {
		var payload AITextGeneratePayload
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return HandlerResult{}, Permanent(fmt.Errorf("invalid ai.text.generate payload: %w", err))
		}
		if strings.TrimSpace(payload.Prompt) == "" {
			return HandlerResult{}, Permanent(fmt.Errorf("ai.text.generate prompt is required"))
		}
		if payload.MaxOutputTokens < 0 {
			return HandlerResult{}, Permanent(fmt.Errorf("max_output_tokens cannot be negative"))
		}
		if payload.Temperature != nil && (*payload.Temperature < 0 || *payload.Temperature > 2) {
			return HandlerResult{}, Permanent(fmt.Errorf("temperature must be between 0 and 2"))
		}

		requestContext, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		response, err := provider.Generate(requestContext, ai.GenerateRequest{
			Model:           payload.Model,
			Prompt:          payload.Prompt,
			MaxOutputTokens: payload.MaxOutputTokens,
			Temperature:     payload.Temperature,
		})
		if err != nil {
			if !ai.IsRetryable(err) {
				return HandlerResult{}, Permanent(err)
			}
			return HandlerResult{}, err
		}

		result, err := json.Marshal(AITextGenerateResult{
			Text:         response.Text,
			Provider:     response.Provider,
			Model:        response.Model,
			RequestID:    response.RequestID,
			InputTokens:  response.InputTokens,
			OutputTokens: response.OutputTokens,
			TotalTokens:  response.TotalTokens,
		})
		if err != nil {
			return HandlerResult{}, fmt.Errorf("encode ai.text.generate result: %w", err)
		}
		return HandlerResult{Result: result}, nil
	}
}
