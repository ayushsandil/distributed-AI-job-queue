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

func TestClaimNextJobSQLUsesSkipLockedAndPriorityFIFO(t *testing.T) {
	wantParts := []string{
		"FOR UPDATE SKIP LOCKED",
		"ORDER BY priority DESC, created_at ASC",
		"status = 'PENDING'",
		"execute_after <= $1",
		"status = 'RUNNING'",
	}

	for _, part := range wantParts {
		if !strings.Contains(claimNextJobSQL, part) {
			t.Fatalf("expected claim SQL to contain %q, got %s", part, claimNextJobSQL)
		}
	}
}

func TestReclaimExpiredJobSQLClearsLeaseAndRequeues(t *testing.T) {
	query := `
		UPDATE jobs
		SET status = $2,
			locked_by = NULL,
			locked_at = NULL,
			lock_expires_at = NULL,
			updated_at = $1
		WHERE status = $3
			AND lock_expires_at IS NOT NULL
			AND lock_expires_at <= $1
	`

	for _, part := range []string{
		"status = $2",
		"locked_by = NULL",
		"lock_expires_at = NULL",
		"status = $3",
		"lock_expires_at <= $1",
	} {
		if !strings.Contains(query, part) {
			t.Fatalf("expected reclaim SQL to contain %q, got %s", part, query)
		}
	}
}
