CREATE TABLE job_executions (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    worker_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),
    error_message TEXT
);

CREATE INDEX job_executions_job_started_idx
    ON job_executions (job_id, started_at DESC);

CREATE INDEX job_executions_worker_started_idx
    ON job_executions (worker_id, started_at DESC);
