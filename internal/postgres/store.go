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

func (s *Store) ClaimNext(ctx context.Context, workerID string, leaseDuration time.Duration, now time.Time) (domain.Job, error) {
	row := s.db.QueryRowContext(ctx, claimNextJobSQL,
		now,
		workerID,
		now.Add(leaseDuration),
	)

	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Job{}, jobstore.ErrNoJobAvailable
	}
	if err != nil {
		return domain.Job{}, fmt.Errorf("claim next job: %w", err)
	}

	return job, nil
}

func (s *Store) StartJob(ctx context.Context, jobID, workerID string, leaseDuration time.Duration, now time.Time) (domain.Job, error) {
	row := s.db.QueryRowContext(ctx, `
		UPDATE jobs
		SET status = $2,
			locked_by = $3,
			locked_at = $4,
			lock_expires_at = $5,
			updated_at = $4
		WHERE id = $1
			AND status = $6
			AND execute_after <= $4
		RETURNING `+jobColumns,
		jobID,
		string(domain.JobStatusRunning),
		workerID,
		now,
		now.Add(leaseDuration),
		string(domain.JobStatusPending),
	)

	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Job{}, jobstore.ErrJobNotClaimable
	}
	if err != nil {
		return domain.Job{}, fmt.Errorf("start job: %w", err)
	}

	return job, nil
}

func (s *Store) ReclaimExpired(ctx context.Context, now time.Time) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin reclaim expired jobs: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if _, err := tx.ExecContext(ctx, `
		UPDATE job_executions
		SET status = $2,
			ended_at = $1,
			error_message = $3
		WHERE status = $4
			AND job_id IN (
				SELECT id
				FROM jobs
				WHERE status = $5
					AND lock_expires_at IS NOT NULL
					AND lock_expires_at <= $1
			)
	`, now,
		string(domain.ExecutionStatusFailed),
		jobstore.LeaseExpiredError,
		string(domain.ExecutionStatusRunning),
		string(domain.JobStatusRunning),
	); err != nil {
		return 0, fmt.Errorf("fail expired executions: %w", err)
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = $2,
			locked_by = NULL,
			locked_at = NULL,
			lock_expires_at = NULL,
			updated_at = $1
		WHERE status = $3
			AND lock_expires_at IS NOT NULL
			AND lock_expires_at <= $1
	`, now,
		string(domain.JobStatusPending),
		string(domain.JobStatusRunning),
	)
	if err != nil {
		return 0, fmt.Errorf("reclaim expired jobs: %w", err)
	}

	reclaimed, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count reclaimed jobs: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit reclaimed jobs: %w", err)
	}

	return int(reclaimed), nil
}

func (s *Store) CreateExecution(ctx context.Context, execution domain.JobExecution) (domain.JobExecution, error) {
	const query = `
		INSERT INTO job_executions (
			id,
			job_id,
			worker_id,
			started_at,
			ended_at,
			status,
			error_message
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING ` + executionColumns

	row := s.db.QueryRowContext(ctx, query,
		execution.ID,
		execution.JobID,
		execution.WorkerID,
		execution.StartedAt,
		timeValue(execution.EndedAt),
		string(execution.Status),
		stringValue(execution.ErrorMessage),
	)

	created, err := scanExecution(row)
	if err != nil {
		return domain.JobExecution{}, fmt.Errorf("create execution: %w", err)
	}

	return created, nil
}

func (s *Store) CompleteExecution(ctx context.Context, executionID string, endedAt time.Time) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE job_executions
		SET status = $2,
			ended_at = $3
		WHERE id = $1
	`, executionID, string(domain.ExecutionStatusCompleted), endedAt)
	if err != nil {
		return fmt.Errorf("complete execution: %w", err)
	}

	return checkRowsAffected(result, jobstore.ErrExecutionNotFound)
}

func (s *Store) FailExecution(ctx context.Context, executionID string, endedAt time.Time, errorMessage string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE job_executions
		SET status = $2,
			ended_at = $3,
			error_message = $4
		WHERE id = $1
	`, executionID, string(domain.ExecutionStatusFailed), endedAt, errorMessage)
	if err != nil {
		return fmt.Errorf("fail execution: %w", err)
	}

	return checkRowsAffected(result, jobstore.ErrExecutionNotFound)
}

func (s *Store) CompleteJob(ctx context.Context, jobID string, now time.Time) (domain.Job, error) {
	row := s.db.QueryRowContext(ctx, `
		UPDATE jobs
		SET status = $2,
			completed_at = $3,
			updated_at = $3,
			locked_by = NULL,
			locked_at = NULL,
			lock_expires_at = NULL
		WHERE id = $1
		RETURNING `+jobColumns,
		jobID,
		string(domain.JobStatusCompleted),
		now,
	)

	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Job{}, jobstore.ErrJobNotFound
	}
	if err != nil {
		return domain.Job{}, fmt.Errorf("complete job: %w", err)
	}

	return job, nil
}

func (s *Store) FailJob(ctx context.Context, jobID string, errorMessage string, policy domain.RetryPolicy, now time.Time) (domain.Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Job{}, fmt.Errorf("begin fail job: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	row := tx.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1 FOR UPDATE`, jobID)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Job{}, jobstore.ErrJobNotFound
	}
	if err != nil {
		return domain.Job{}, fmt.Errorf("load failed job: %w", err)
	}

	failed := job.Fail(errorMessage, policy, now)
	row = tx.QueryRowContext(ctx, `
		UPDATE jobs
		SET status = $2,
			retry_count = $3,
			execute_after = $4,
			locked_by = NULL,
			locked_at = NULL,
			lock_expires_at = NULL,
			last_error = $5,
			completed_at = $6,
			updated_at = $7
		WHERE id = $1
		RETURNING `+jobColumns,
		failed.ID,
		string(failed.Status),
		failed.RetryCount,
		failed.ExecuteAfter,
		stringValue(failed.LastError),
		timeValue(failed.CompletedAt),
		failed.UpdatedAt,
	)

	updated, err := scanJob(row)
	if err != nil {
		return domain.Job{}, fmt.Errorf("update failed job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return domain.Job{}, fmt.Errorf("commit fail job: %w", err)
	}

	return updated, nil
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

const executionColumns = `
	id,
	job_id,
	worker_id,
	started_at,
	ended_at,
	status,
	error_message
`

const claimNextJobSQL = `
	UPDATE jobs
	SET status = 'RUNNING',
		locked_by = $2,
		locked_at = $1,
		lock_expires_at = $3,
		updated_at = $1
	WHERE id = (
		SELECT id
		FROM jobs
		WHERE status = 'PENDING'
			AND execute_after <= $1
		ORDER BY priority DESC, created_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	)
	RETURNING ` + jobColumns

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

func scanExecution(row scanner) (domain.JobExecution, error) {
	var execution domain.JobExecution
	var endedAt sql.NullTime
	var status string
	var errorMessage sql.NullString

	if err := row.Scan(
		&execution.ID,
		&execution.JobID,
		&execution.WorkerID,
		&execution.StartedAt,
		&endedAt,
		&status,
		&errorMessage,
	); err != nil {
		return domain.JobExecution{}, err
	}

	parsedStatus, ok := parseExecutionStatus(status)
	if !ok {
		return domain.JobExecution{}, fmt.Errorf("unknown execution status %q", status)
	}

	execution.Status = parsedStatus
	execution.EndedAt = timePtrFromNull(endedAt)
	execution.ErrorMessage = stringPtrFromNull(errorMessage)
	return execution, nil
}

func parseExecutionStatus(value string) (domain.ExecutionStatus, bool) {
	switch domain.ExecutionStatus(value) {
	case domain.ExecutionStatusRunning, domain.ExecutionStatusCompleted, domain.ExecutionStatusFailed:
		return domain.ExecutionStatus(value), true
	default:
		return "", false
	}
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

func checkRowsAffected(result sql.Result, notFoundErr error) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return notFoundErr
	}
	return nil
}
