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

The API supports an in-memory repository for quick local runs and tests.

Endpoints:

- `POST /jobs`
- `GET /jobs/{id}`
- `GET /jobs`
- `GET /healthz`

## Step 3: PostgreSQL Persistence

Set `DATABASE_URL` to persist API data in PostgreSQL. When `DATABASE_URL` is absent, the API falls back to the in-memory store.

The API runs SQL migrations on startup by default:

```sh
DATABASE_URL='postgres://postgres:postgres@localhost:5432/ai_job_queue?sslmode=disable' \
GOCACHE=/private/tmp/ai-job-queue-go-cache \
go run ./cmd/api
```

To skip startup migrations:

```sh
RUN_MIGRATIONS=false go run ./cmd/api
```

Run the API:

```sh
GOCACHE=/private/tmp/ai-job-queue-go-cache go run ./cmd/api
```

Run tests:

```sh
GOCACHE=/private/tmp/ai-job-queue-go-cache go test ./...
```

Run PostgreSQL integration tests:

```sh
TEST_DATABASE_URL='postgres://postgres:postgres@localhost:5432/ai_job_queue_test?sslmode=disable' \
GOCACHE=/private/tmp/ai-job-queue-go-cache \
go test ./internal/postgres -run Integration
```

The integration test drops and recreates the project tables in the database named by `TEST_DATABASE_URL`, so point it at a disposable test database.
