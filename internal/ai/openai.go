package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const defaultOpenAIBaseURL = "https://api.openai.com/v1"

type OpenAIConfig struct {
	APIKey       string
	BaseURL      string
	DefaultModel string
	HTTPClient   *http.Client
}

type OpenAIProvider struct {
	apiKey       string
	endpoint     string
	defaultModel string
	client       *http.Client
}

func NewOpenAIProvider(config OpenAIConfig) (*OpenAIProvider, error) {
	if strings.TrimSpace(config.APIKey) == "" {
		return nil, &ValidationError{Message: "OPENAI_API_KEY is required"}
	}

	baseURL := strings.TrimRight(config.BaseURL, "/")
	if baseURL == "" {
		baseURL = defaultOpenAIBaseURL
	}
	if !strings.HasSuffix(baseURL, "/responses") {
		baseURL += "/responses"
	}

	model := strings.TrimSpace(config.DefaultModel)
	if model == "" {
		model = "gpt-4o-mini"
	}

	client := config.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	return &OpenAIProvider{
		apiKey:       config.APIKey,
		endpoint:     baseURL,
		defaultModel: model,
		client:       client,
	}, nil
}

func (p *OpenAIProvider) Generate(ctx context.Context, request GenerateRequest) (GenerateResponse, error) {
	prompt := strings.TrimSpace(request.Prompt)
	if prompt == "" {
		return GenerateResponse{}, &ValidationError{Message: "AI prompt is required"}
	}

	model := strings.TrimSpace(request.Model)
	if model == "" {
		model = p.defaultModel
	}

	body := struct {
		Model           string   `json:"model"`
		Input           string   `json:"input"`
		MaxOutputTokens *int     `json:"max_output_tokens,omitempty"`
		Temperature     *float64 `json:"temperature,omitempty"`
	}{
		Model:       model,
		Input:       request.Prompt,
		Temperature: request.Temperature,
	}
	if request.MaxOutputTokens > 0 {
		body.MaxOutputTokens = &request.MaxOutputTokens
	}

	encoded, err := json.Marshal(body)
	if err != nil {
		return GenerateResponse{}, fmt.Errorf("encode OpenAI request: %w", err)
	}

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, strings.NewReader(string(encoded)))
	if err != nil {
		return GenerateResponse{}, fmt.Errorf("create OpenAI request: %w", err)
	}
	httpRequest.Header.Set("Authorization", "Bearer "+p.apiKey)
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")
	httpRequest.Header.Set("User-Agent", "ai-job-queue/1.0")

	response, err := p.client.Do(httpRequest)
	if err != nil {
		return GenerateResponse{}, err
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return GenerateResponse{}, parseOpenAIError(response)
	}

	var decoded responsesResponse
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		return GenerateResponse{}, &ProviderError{
			Retryable: true,
			Message:   fmt.Sprintf("decode OpenAI response: %v", err),
		}
	}

	var output strings.Builder
	for _, item := range decoded.Output {
		for _, content := range item.Content {
			if content.Type == "output_text" || content.Type == "text" {
				output.WriteString(content.Text)
			}
		}
	}
	if output.Len() == 0 {
		return GenerateResponse{}, &ProviderError{
			Retryable: false,
			Message:   "OpenAI response did not contain output text",
		}
	}

	responseModel := decoded.Model
	if responseModel == "" {
		responseModel = model
	}

	return GenerateResponse{
		Text:         output.String(),
		Provider:     "openai",
		Model:        responseModel,
		RequestID:    decoded.ID,
		InputTokens:  decoded.Usage.InputTokens,
		OutputTokens: decoded.Usage.OutputTokens,
		TotalTokens:  decoded.Usage.TotalTokens,
	}, nil
}

type responsesResponse struct {
	ID     string `json:"id"`
	Model  string `json:"model"`
	Output []struct {
		Type    string `json:"type"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	} `json:"output"`
	Usage struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
		TotalTokens  int `json:"total_tokens"`
	} `json:"usage"`
}

func parseOpenAIError(response *http.Response) error {
	var envelope struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 64*1024)).Decode(&envelope); err != nil {
		return &ProviderError{
			StatusCode: response.StatusCode,
			Retryable:  isRetryableStatus(response.StatusCode),
			Message:    http.StatusText(response.StatusCode),
		}
	}

	message := envelope.Error.Message
	if message == "" {
		message = http.StatusText(response.StatusCode)
	}
	return &ProviderError{
		StatusCode: response.StatusCode,
		Retryable:  isRetryableStatus(response.StatusCode),
		Message:    message,
	}
}

func isRetryableStatus(statusCode int) bool {
	return statusCode == http.StatusRequestTimeout ||
		statusCode == http.StatusConflict ||
		statusCode == http.StatusTooManyRequests ||
		statusCode >= http.StatusInternalServerError
}
