package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"ai-job-queue/internal/domain"
	"ai-job-queue/internal/ids"
	"ai-job-queue/internal/jobstore"
)

type Server struct {
	store     jobstore.Store
	publisher jobstore.Publisher
	now       func() time.Time
	mux       *http.ServeMux
}

func NewServer(store jobstore.Store, publishers ...jobstore.Publisher) *Server {
	server := &Server{
		store: store,
		now:   time.Now,
		mux:   http.NewServeMux(),
	}
	if len(publishers) > 0 {
		server.publisher = publishers[0]
	}
	server.routes()
	return server
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /jobs", s.createJob)
	s.mux.HandleFunc("GET /jobs/{id}", s.getJob)
	s.mux.HandleFunc("GET /jobs", s.listJobs)
	s.mux.HandleFunc("GET /healthz", s.healthz)
}

type createJobRequest struct {
	Type         string          `json:"type"`
	Payload      json.RawMessage `json:"payload"`
	Priority     int             `json:"priority"`
	ExecuteAfter *time.Time      `json:"execute_after"`
	MaxRetries   int             `json:"max_retries"`
}

type jobResponse struct {
	ID           string          `json:"id"`
	Type         string          `json:"type"`
	Payload      json.RawMessage `json:"payload,omitempty"`
	Priority     int             `json:"priority"`
	Status       string          `json:"status"`
	RetryCount   int             `json:"retry_count"`
	MaxRetries   int             `json:"max_retries"`
	ExecuteAfter time.Time       `json:"execute_after"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
	LastError    *string         `json:"last_error,omitempty"`
	CompletedAt  *time.Time      `json:"completed_at,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage(`{}`)
	}
	if !json.Valid(req.Payload) {
		writeError(w, http.StatusBadRequest, "payload must be valid JSON")
		return
	}

	now := s.now().UTC()
	job, err := domain.NewJob(domain.NewJobInput{
		ID:           ids.NewUUID(),
		Type:         req.Type,
		Payload:      []byte(req.Payload),
		Priority:     req.Priority,
		ExecuteAfter: req.ExecuteAfter,
		RetryPolicy: domain.RetryPolicy{
			MaxRetries: req.MaxRetries,
		},
	}, now)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	created, err := s.store.Create(r.Context(), job)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create job")
		return
	}
	if s.publisher != nil {
		if err := s.publisher.Publish(r.Context(), created); err != nil {
			writeError(w, http.StatusServiceUnavailable, "job persisted but could not be queued")
			return
		}
	}

	writeJSON(w, http.StatusCreated, toJobResponse(created, true))
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.store.GetByID(r.Context(), r.PathValue("id"))
	if errors.Is(err, jobstore.ErrJobNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get job")
		return
	}

	writeJSON(w, http.StatusOK, toJobResponse(job, true))
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	filter, ok := parseListFilter(w, r)
	if !ok {
		return
	}

	jobs, err := s.store.List(r.Context(), filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list jobs")
		return
	}

	response := make([]jobResponse, 0, len(jobs))
	for _, job := range jobs {
		response = append(response, toJobResponse(job, false))
	}

	writeJSON(w, http.StatusOK, response)
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func parseListFilter(w http.ResponseWriter, r *http.Request) (jobstore.ListFilter, bool) {
	var filter jobstore.ListFilter
	query := r.URL.Query()

	if rawStatus := query.Get("status"); rawStatus != "" {
		status, ok := domain.ParseJobStatus(rawStatus)
		if !ok {
			writeError(w, http.StatusBadRequest, "invalid status filter")
			return jobstore.ListFilter{}, false
		}
		filter.Status = &status
	}

	if rawType := query.Get("type"); rawType != "" {
		filter.Type = &rawType
	}

	rawCreatedFrom := query.Get("created_from")
	if rawCreatedFrom == "" {
		rawCreatedFrom = query.Get("created_at")
	}
	if rawCreatedFrom != "" {
		createdFrom, err := time.Parse(time.RFC3339, rawCreatedFrom)
		if err != nil {
			writeError(w, http.StatusBadRequest, "created_from must be RFC3339")
			return jobstore.ListFilter{}, false
		}
		filter.CreatedFrom = &createdFrom
	}

	if rawCreatedTo := query.Get("created_to"); rawCreatedTo != "" {
		createdTo, err := time.Parse(time.RFC3339, rawCreatedTo)
		if err != nil {
			writeError(w, http.StatusBadRequest, "created_to must be RFC3339")
			return jobstore.ListFilter{}, false
		}
		filter.CreatedTo = &createdTo
	}

	return filter, true
}

func toJobResponse(job domain.Job, includePayload bool) jobResponse {
	response := jobResponse{
		ID:           job.ID,
		Type:         job.Type,
		Priority:     job.Priority,
		Status:       string(job.Status),
		RetryCount:   job.RetryCount,
		MaxRetries:   job.MaxRetries,
		ExecuteAfter: job.ExecuteAfter,
		CreatedAt:    job.CreatedAt,
		UpdatedAt:    job.UpdatedAt,
		LastError:    job.LastError,
		CompletedAt:  job.CompletedAt,
	}

	if includePayload {
		response.Payload = json.RawMessage(job.Payload)
	}

	return response
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}
