# AI Job Queue

Personal distributed job queue project built in small, testable slices.

## Step 1: Core Model

This first slice defines the job lifecycle, retry policy, execution history model, and PostgreSQL migrations.

Current scope:

- Job statuses: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `DEAD_LETTER`
- Retry/dead-letter decision logic
- Worker lock fields for future crash recovery
- Database schema for `jobs` and `job_executions`

Run tests:

```sh
go test ./...
```
