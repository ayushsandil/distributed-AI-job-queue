package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"ai-job-queue/internal/ai"
	"ai-job-queue/internal/domain"
)

func TestAITextGenerateHandlerReturnsStructuredResult(t *testing.T) {
	provider := &fakeAIProvider{
		response: ai.GenerateResponse{
			Text:         "generated text",
			Provider:     "openai",
			Model:        "test-model",
			RequestID:    "resp_123",
			InputTokens:  5,
			OutputTokens: 7,
			TotalTokens:  12,
		},
	}
	handler := NewAITextGenerateHandler(provider, time.Second)

	result, err := handler(context.Background(), domain.Job{
		Payload: []byte(`{"prompt":"write a title","model":"test-model","max_output_tokens":30}`),
	})
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	var decoded AITextGenerateResult
	if err := json.Unmarshal(result.Result, &decoded); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if decoded.Text != "generated text" || decoded.TotalTokens != 12 {
		t.Fatalf("unexpected result: %+v", decoded)
	}
	if provider.request.Prompt != "write a title" || provider.request.Model != "test-model" {
		t.Fatalf("unexpected provider request: %+v", provider.request)
	}
}

func TestAITextGenerateHandlerDeadLettersInvalidPayload(t *testing.T) {
	handler := NewAITextGenerateHandler(&fakeAIProvider{}, time.Second)
	_, err := handler(context.Background(), domain.Job{Payload: []byte(`{"prompt":""}`)})
	if err == nil || !isPermanent(err) {
		t.Fatalf("expected permanent validation error, got %v", err)
	}
}

type fakeAIProvider struct {
	request  ai.GenerateRequest
	response ai.GenerateResponse
}

func (p *fakeAIProvider) Generate(_ context.Context, request ai.GenerateRequest) (ai.GenerateResponse, error) {
	p.request = request
	return p.response, nil
}
