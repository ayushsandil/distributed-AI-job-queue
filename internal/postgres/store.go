package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"ai-job-queue/internal/domain"
	"ai-job-queue/internal/jobstore"
)

type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

func (s *Store) Create(ctx context.Context, job domain.Job) (domain.Job, error) {
	const query = `
		INSERT INTO jobs (
			id,
			type,
			payload,
			priority,
			status,
			retry_count,
			max_retries,
			execute_after,
			locked_by,
			locked_at,
			lock_expires_at,
			last_error,
			completed_at,
			created_at,
			updated_at
		)
		VALUES (
			$1, $2, $3::jsonb, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
		)
		RETURNING ` + jobColumns

	payload := job.Payload
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}

	row := s.db.QueryRowContext(ctx, query,
		job.ID,
		job.Type,
		string(payload),
		job.Priority,
		string(job.Status),
		job.RetryCount,
		job.MaxRetries,
		job.ExecuteAfter,
		stringValue(job.LockedBy),
		timeValue(job.LockedAt),
		timeValue(job.LockExpiresAt),
		stringValue(job.LastError),
		timeValue(job.CompletedAt),
		job.CreatedAt,
		job.UpdatedAt,
	)

	created, err := scanJob(row)
	if err != nil {
		return domain.Job{}, fmt.Errorf("create job: %w", err)
	}

	return created, nil
}

func (s *Store) GetByID(ctx context.Context, id string) (domain.Job, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id)

	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Job{}, jobstore.ErrJobNotFound
	}
	if err != nil {
		return domain.Job{}, fmt.Errorf("get job: %w", err)
	}

	return job, nil
}

func (s *Store) List(ctx context.Context, filter jobstore.ListFilter) ([]domain.Job, error) {
	query, args := buildListJobsQuery(filter)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]domain.Job, 0)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate jobs: %w", err)
	}

	return jobs, nil
}

const jobColumns = `
	id,
	type,
	payload,
	priority,
	status,
	retry_count,
	max_retries,
	execute_after,
	locked_by,
	locked_at,
	lock_expires_at,
	last_error,
	completed_at,
	created_at,
	updated_at
`

type scanner interface {
	Scan(dest ...any) error
}

func scanJob(row scanner) (domain.Job, error) {
	var job domain.Job
	var payload []byte
	var status string
	var lockedBy sql.NullString
	var lockedAt sql.NullTime
	var lockExpiresAt sql.NullTime
	var lastError sql.NullString
	var completedAt sql.NullTime

	if err := row.Scan(
		&job.ID,
		&job.Type,
		&payload,
		&job.Priority,
		&status,
		&job.RetryCount,
		&job.MaxRetries,
		&job.ExecuteAfter,
		&lockedBy,
		&lockedAt,
		&lockExpiresAt,
		&lastError,
		&completedAt,
		&job.CreatedAt,
		&job.UpdatedAt,
	); err != nil {
		return domain.Job{}, err
	}

	parsedStatus, ok := domain.ParseJobStatus(status)
	if !ok {
		return domain.Job{}, fmt.Errorf("unknown job status %q", status)
	}

	job.Payload = payload
	job.Status = parsedStatus
	job.LockedBy = stringPtrFromNull(lockedBy)
	job.LockedAt = timePtrFromNull(lockedAt)
	job.LockExpiresAt = timePtrFromNull(lockExpiresAt)
	job.LastError = stringPtrFromNull(lastError)
	job.CompletedAt = timePtrFromNull(completedAt)

	return job, nil
}

func buildListJobsQuery(filter jobstore.ListFilter) (string, []any) {
	var builder strings.Builder
	builder.WriteString(`SELECT `)
	builder.WriteString(jobColumns)
	builder.WriteString(` FROM jobs`)

	conditions := make([]string, 0, 4)
	args := make([]any, 0, 4)
	addCondition := func(condition string, value any) {
		args = append(args, value)
		conditions = append(conditions, fmt.Sprintf(condition, len(args)))
	}

	if filter.Status != nil {
		addCondition("status = $%d", string(*filter.Status))
	}
	if filter.Type != nil {
		addCondition("type = $%d", *filter.Type)
	}
	if filter.CreatedFrom != nil {
		addCondition("created_at >= $%d", *filter.CreatedFrom)
	}
	if filter.CreatedTo != nil {
		addCondition("created_at <= $%d", *filter.CreatedTo)
	}

	if len(conditions) > 0 {
		builder.WriteString(` WHERE `)
		builder.WriteString(strings.Join(conditions, ` AND `))
	}

	builder.WriteString(` ORDER BY created_at DESC`)
	return builder.String(), args
}

func stringPtrFromNull(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	return &value.String
}

func timePtrFromNull(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	return &value.Time
}

func stringValue(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func timeValue(value *time.Time) any {
	if value == nil {
		return nil
	}
	return *value
}
