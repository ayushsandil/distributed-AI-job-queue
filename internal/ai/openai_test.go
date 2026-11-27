package ai

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestOpenAIProviderGenerate(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/v1/responses" {
			t.Fatalf("expected /v1/responses, got %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Fatalf("unexpected authorization header: %s", got)
		}

		var request struct {
			Model           string  `json:"model"`
			Input           string  `json:"input"`
			MaxOutputTokens int     `json:"max_output_tokens"`
			Temperature     float64 `json:"temperature"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if request.Model != "test-model" || request.Input != "hello" || request.MaxOutputTokens != 25 || request.Temperature != 0.2 {
			t.Fatalf("unexpected request: %+v", request)
		}

		return response(http.StatusOK, `{"id":"resp_test_123","model":"test-model","output":[{"type":"message","content":[{"type":"output_text","text":"hello back"}]}],"usage":{"input_tokens":4,"output_tokens":3,"total_tokens":7}}`), nil
	})}

	temperature := 0.2
	provider, err := NewOpenAIProvider(OpenAIConfig{
		APIKey:       "test-key",
		BaseURL:      "https://test.local/v1",
		DefaultModel: "test-model",
		HTTPClient:   client,
	})
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}

	response, err := provider.Generate(context.Background(), GenerateRequest{
		Prompt:          "hello",
		MaxOutputTokens: 25,
		Temperature:     &temperature,
	})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if response.Text != "hello back" || response.RequestID != "resp_test_123" {
		t.Fatalf("unexpected response: %+v", response)
	}
	if response.TotalTokens != 7 {
		t.Fatalf("expected total tokens 7, got %d", response.TotalTokens)
	}
}

func TestOpenAIProviderClassifiesHTTPFailures(t *testing.T) {
	tests := []struct {
		status    int
		retryable bool
	}{
		{status: http.StatusTooManyRequests, retryable: true},
		{status: http.StatusBadRequest, retryable: false},
		{status: http.StatusInternalServerError, retryable: true},
	}

	for _, tt := range tests {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			provider, err := NewOpenAIProvider(OpenAIConfig{
				APIKey:  "test-key",
				BaseURL: "https://test.local/v1",
				HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return response(tt.status, `{"error":{"message":"provider failure"}}`), nil
				})},
			})
			if err != nil {
				t.Fatalf("new provider: %v", err)
			}

			_, err = provider.Generate(context.Background(), GenerateRequest{Prompt: "hello"})
			if err == nil || IsRetryable(err) != tt.retryable {
				t.Fatalf("expected retryable=%v, got err=%v", tt.retryable, err)
			}
			if !strings.Contains(err.Error(), "provider failure") {
				t.Fatalf("expected provider error message, got %v", err)
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func response(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
