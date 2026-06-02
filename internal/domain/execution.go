package domain

import "time"

type ExecutionStatus string

const (
	ExecutionStatusRunning   ExecutionStatus = "RUNNING"
	ExecutionStatusCompleted ExecutionStatus = "COMPLETED"
	ExecutionStatusFailed    ExecutionStatus = "FAILED"
)

type JobExecution struct {
	ID           string
	JobID        string
	WorkerID     string
	StartedAt    time.Time
	EndedAt      *time.Time
	Status       ExecutionStatus
	ErrorMessage *string
}
