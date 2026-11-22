# AI Job Queue

Personal distributed job queue project built in small, testable slices.

## Prerequisites

- Go 1.26 or newer
- Docker Desktop with Docker Compose
- Ports `5432`, `6379`, and `8080` available locally

## Quick Start

Start PostgreSQL and Redis from the project root:

```sh
docker compose up -d
docker compose ps
```

Compose exposes PostgreSQL on `localhost:5432`, Redis on `localhost:6379`, and creates the disposable `ai_job_queue_test` database used by the integration tests.

Run the complete test suite:

```sh
go test ./...
```

Run the real PostgreSQL and Redis integration tests:

```sh
TEST_DATABASE_URL='postgres://postgres:postgres@localhost:5432/ai_job_queue_test?sslmode=disable' \
TEST_REDIS_URL='redis://localhost:6379/0' \
go test ./internal/postgres ./internal/redisqueue -run Integration -v
```

Start the API in Redis Stream mode in one terminal:

```sh
QUEUE_BACKEND=redis \
DATABASE_URL='postgres://postgres:postgres@localhost:5432/ai_job_queue?sslmode=disable' \
REDIS_URL='redis://localhost:6379/0' \
go run ./cmd/api
```

Start a worker in a second terminal:

```sh
QUEUE_BACKEND=redis \
DATABASE_URL='postgres://postgres:postgres@localhost:5432/ai_job_queue?sslmode=disable' \
REDIS_URL='redis://localhost:6379/0' \
WORKER_ID=worker-1 \
go run ./cmd/worker
```

Submit a sample job from a third terminal:

```sh
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"noop","payload":{"source":"manual-smoke-test"},"priority":5,"max_retries":2}'
```

Copy the returned `id`, then query its status:

```sh
curl http://localhost:8080/jobs/<job-id>
```

The expected lifecycle is:

```text
PENDING -> RUNNING -> COMPLETED
```

Stop the local infrastructure when finished:

```sh
docker compose down
```

Use `docker compose down -v` only when you want to delete the local PostgreSQL and Redis volumes.

## Architecture

PostgreSQL is the durable source for job metadata and execution history. Redis Streams are an alternate transport for distributing work between workers.

The default worker uses PostgreSQL row-level locking. Redis mode uses a consumer group with `XREADGROUP`, acknowledges completed messages with `XACK`, and recovers stale pending messages with `XAUTOCLAIM`.

Redis mode currently preserves enqueue order rather than PostgreSQL priority ordering. Delayed jobs are held by the worker until `execute_after`; priority lanes and a dedicated delayed-job scheduler are planned improvements.

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
go run ./cmd/api
```

To skip startup migrations:

```sh
RUN_MIGRATIONS=false go run ./cmd/api
```

Run the API:

```sh
go run ./cmd/api
```

Run tests:

```sh
go test ./...
```

Run PostgreSQL integration tests:

```sh
TEST_DATABASE_URL='postgres://postgres:postgres@localhost:5432/ai_job_queue_test?sslmode=disable' \
go test ./internal/postgres -run Integration
```

The integration test drops and recreates the project tables in the database named by `TEST_DATABASE_URL`, so point it at a disposable test database.

## Step 4: Single Worker Processing

The worker claims eligible jobs from PostgreSQL, records execution history, runs a registered handler, and completes or retries the job.

Claiming uses PostgreSQL row-level locking:

```sql
FOR UPDATE SKIP LOCKED
```

Run a worker:

```sh
DATABASE_URL='postgres://postgres:postgres@localhost:5432/ai_job_queue?sslmode=disable' \
go run ./cmd/worker
```

Useful worker environment variables:

- `WORKER_ID`
- `POLL_INTERVAL`, for example `500ms`
- `LEASE_DURATION`, for example `1m`
- `RETRY_INITIAL_DELAY`, for example `1s`
- `RETRY_MAX_DELAY`, for example `1m`

Built-in demo job types:

- `noop`
- `email.send`, currently a successful placeholder handler
- `image.resize`, currently a successful placeholder handler
- `sleep`, with payload such as `{"duration_ms":1000}`
- `always.fail`

These are queue-level job types, not full integrations yet. A real email sender, image processor, or other application-specific handler can be registered by the worker process later.

## Step 5: Multi-Worker Concurrency Tests

The test suite now verifies that multiple workers can process a batch from a shared store without duplicate processing.

Covered by fast unit tests:

- several workers drain one shared in-memory queue
- every job handler runs exactly once
- every processed job reaches `COMPLETED`

Covered by PostgreSQL integration tests when `TEST_DATABASE_URL` is set:

- several workers drain one shared PostgreSQL queue
- each job has exactly one completed execution
- a late worker finds no remaining claimable jobs
- the claim query uses `FOR UPDATE SKIP LOCKED`

## Step 6: Crash Recovery

Workers use a lease for each claimed job. At the start of each polling cycle, expired `RUNNING` jobs are returned to `PENDING`, their lock fields are cleared, and any still-running execution record is marked failed with `worker lease expired`.

This gives the queue at-least-once processing behavior. If a worker crashes after performing external work but before recording completion, the job may run again, so real handlers should be idempotent.

## Step 7: Redis Streams

Redis Streams are available as an alternate queue transport. PostgreSQL remains the durable source for job metadata and execution history.

Run the API in Redis mode:

```sh
QUEUE_BACKEND=redis \
DATABASE_URL='postgres://postgres:postgres@localhost:5432/ai_job_queue?sslmode=disable' \
REDIS_URL='redis://localhost:6379/0' \
go run ./cmd/api
```

Run a Redis Stream worker:

```sh
QUEUE_BACKEND=redis \
DATABASE_URL='postgres://postgres:postgres@localhost:5432/ai_job_queue?sslmode=disable' \
REDIS_URL='redis://localhost:6379/0' \
go run ./cmd/worker
```

The Redis worker uses `XREADGROUP` for work distribution, `XACK` after successful state updates, and `XAUTOCLAIM` to recover messages left pending by crashed workers. Configure `REDIS_STREAM`, `REDIS_GROUP`, and `STREAM_BATCH_SIZE` when needed.

The first Redis transport preserves enqueue order, not PostgreSQL priority ordering. It also waits for a future `execute_after` value inside the worker. Priority lanes and a separate delayed-job scheduler are natural follow-up improvements.

Redis integration tests use:

```sh
TEST_REDIS_URL='redis://localhost:6379/0' \
go test ./internal/redisqueue -run Integration -v
```
