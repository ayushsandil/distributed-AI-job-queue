# AI Job Queue

Personal distributed job queue project built in small, testable slices.

## Step 1: Core Model

This first slice defines the job lifecycle, retry policy, execution history model, and PostgreSQL migrations.

Current scope:

- Job statuses: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `DEAD_LETTER`
- Retry/dead-letter decision logic
- Worker lock fields for future crash recovery
- Database schema for `jobs` and `job_executions`

## Step 2: REST API

The API currently uses an in-memory repository so behavior can be tested before PostgreSQL is wired in.

Endpoints:

- `POST /jobs`
- `GET /jobs/{id}`
- `GET /jobs`
- `GET /healthz`

Run the API:

```sh
GOCACHE=/private/tmp/ai-job-queue-go-cache go run ./cmd/api
```

Run tests:

```sh
GOCACHE=/private/tmp/ai-job-queue-go-cache go test ./...
```
