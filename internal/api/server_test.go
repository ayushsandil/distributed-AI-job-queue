package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"ai-job-queue/internal/domain"
	"ai-job-queue/internal/jobstore"
)

func TestCreateJob(t *testing.T) {
	server := newTestServer()
	body := `{"type":"email.send","payload":{"to":"user@example.com"},"priority":5,"max_retries":3}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var response jobResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.ID == "" {
		t.Fatalf("expected generated job ID")
	}
	if response.Status != "PENDING" {
		t.Fatalf("expected pending status, got %s", response.Status)
	}
	if response.RetryCount != 0 || response.MaxRetries != 3 {
		t.Fatalf("unexpected retry fields: count=%d max=%d", response.RetryCount, response.MaxRetries)
	}
}

func TestCreateJobPublishesWhenPublisherConfigured(t *testing.T) {
	var publishedID string
	publisher := publisherFunc(func(_ context.Context, job domain.Job) error {
		publishedID = job.ID
		return nil
	})
	server := NewServer(jobstore.NewMemoryStore(), publisher)

	response := createJobForTest(t, server, `{"type":"noop","payload":{}}`)
	if publishedID != response.ID {
		t.Fatalf("expected publisher to receive job %s, got %s", response.ID, publishedID)
	}
}

func TestCreateJobReturnsUnavailableWhenPublishingFails(t *testing.T) {
	publisher := publisherFunc(func(context.Context, domain.Job) error {
		return errors.New("redis unavailable")
	})
	server := NewServer(jobstore.NewMemoryStore(), publisher)
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"type":"noop"}`))
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestCreateJobValidatesRequest(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "invalid json", body: `{`},
		{name: "missing type", body: `{"payload":{},"max_retries":1}`},
		{name: "negative priority", body: `{"type":"email.send","priority":-1}`},
		{name: "negative retries", body: `{"type":"email.send","max_retries":-1}`},
		{name: "invalid payload", body: `{"type":"email.send","payload":`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newTestServer()
			req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(tt.body))
			rec := httptest.NewRecorder()

			server.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected status 400, got %d: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestGetJob(t *testing.T) {
	server := newTestServer()
	created := createJobForTest(t, server, `{"type":"image.resize","payload":{"path":"a.png"}}`)
	req := httptest.NewRequest(http.MethodGet, "/jobs/"+created.ID, nil)
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var response jobResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.ID != created.ID {
		t.Fatalf("expected job ID %s, got %s", created.ID, response.ID)
	}
	if string(response.Payload) == "" {
		t.Fatalf("expected payload in get response")
	}
}

func TestGetJobReturnsNotFound(t *testing.T) {
	server := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/jobs/missing", nil)
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rec.Code)
	}
}

func TestListJobsFiltersByStatusAndType(t *testing.T) {
	server := newTestServer()
	createJobForTest(t, server, `{"type":"email.send","payload":{},"priority":1}`)
	createJobForTest(t, server, `{"type":"image.resize","payload":{},"priority":1}`)

	req := httptest.NewRequest(http.MethodGet, "/jobs?status=PENDING&type=email.send", nil)
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var response []jobResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response) != 1 {
		t.Fatalf("expected 1 job, got %d", len(response))
	}
	if response[0].Type != "email.send" {
		t.Fatalf("expected email.send job, got %s", response[0].Type)
	}
	if len(response[0].Payload) != 0 {
		t.Fatalf("expected list response to omit payload")
	}
}

func TestListJobsRejectsInvalidFilters(t *testing.T) {
	server := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/jobs?status=NOPE", nil)
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
}

func newTestServer() *Server {
	server := NewServer(jobstore.NewMemoryStore())
	now := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	return server
}

func createJobForTest(t *testing.T, server *Server, body string) jobResponse {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("create job failed with status %d: %s", rec.Code, rec.Body.String())
	}

	var response jobResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	return response
}

type publisherFunc func(context.Context, domain.Job) error

func (f publisherFunc) Publish(ctx context.Context, job domain.Job) error {
	return f(ctx, job)
}
