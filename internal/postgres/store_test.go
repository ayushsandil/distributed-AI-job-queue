package postgres

import (
	"strings"
	"testing"
	"time"

	"ai-job-queue/internal/domain"
	"ai-job-queue/internal/jobstore"
)

func TestBuildListJobsQuery(t *testing.T) {
	status := domain.JobStatusPending
	jobType := "email.send"
	createdFrom := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	createdTo := createdFrom.Add(time.Hour)

	query, args := buildListJobsQuery(jobstore.ListFilter{
		Status:      &status,
		Type:        &jobType,
		CreatedFrom: &createdFrom,
		CreatedTo:   &createdTo,
	})

	wantParts := []string{
		"FROM jobs",
		"status = $1",
		"type = $2",
		"created_at >= $3",
		"created_at <= $4",
		"ORDER BY created_at DESC",
	}
	for _, part := range wantParts {
		if !strings.Contains(query, part) {
			t.Fatalf("expected query to contain %q, got %s", part, query)
		}
	}
	if len(args) != 4 {
		t.Fatalf("expected 4 args, got %d", len(args))
	}
}
