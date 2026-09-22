-- Combined schema for test database

-- Scheduled jobs table
CREATE TABLE scheduled_jobs (
    id UUID PRIMARY KEY,
    job_key TEXT,
    key_policy TEXT NOT NULL DEFAULT 'QUEUE',
    wait_start TEXT NOT NULL DEFAULT 'SELF',
    wait_duration TEXT,
    wait_repeat INT NOT NULL DEFAULT 1,
    wait_until TIMESTAMPTZ,
    cron_expression TEXT,
    cron_until TIMESTAMPTZ,
    cron_repeat INT,
    cron_run_count INT NOT NULL DEFAULT 0,

    run_at TIMESTAMPTZ NOT NULL,
    effective_run_at TIMESTAMPTZ,
    arrived_at TIMESTAMPTZ NOT NULL,

    destination_topic TEXT NOT NULL,
    message_key BYTEA,
    message_value BYTEA,
    headers JSONB,
    advisory_headers_pattern TEXT,

    state TEXT NOT NULL DEFAULT 'PENDING',
    max_retries INT NOT NULL DEFAULT 3,
    retry_count INT NOT NULL DEFAULT 0,
    version INT NOT NULL DEFAULT 0,

    predecessor_id UUID REFERENCES scheduled_jobs(id),
    sequence_num INT NOT NULL DEFAULT 0,

    acquired_by TEXT,
    acquired_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ,

    last_error TEXT
);

-- Index for polling pending jobs by run time
CREATE INDEX idx_scheduled_jobs_pending_run
    ON scheduled_jobs (effective_run_at)
    WHERE state = 'PENDING';

-- Index for finding jobs by key (for QUEUE/REPLACE/SKIP logic)
CREATE INDEX idx_scheduled_jobs_key_state
    ON scheduled_jobs (job_key, state)
    WHERE job_key IS NOT NULL;

-- Index for finding waiting successors
CREATE INDEX idx_scheduled_jobs_predecessor
    ON scheduled_jobs (predecessor_id)
    WHERE predecessor_id IS NOT NULL;

-- Index for recovery: find jobs that were acquired but not completed
CREATE INDEX idx_scheduled_jobs_acquired
    ON scheduled_jobs (acquired_by, acquired_at)
    WHERE state = 'ACQUIRED' OR state = 'RUNNING';

-- Scheduler instances table for heartbeat-based shard discovery
CREATE TABLE scheduler_instances (
    instance_id TEXT PRIMARY KEY,
    heartbeat_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    version INT NOT NULL DEFAULT 0
);

-- Index for finding live instances (heartbeat within threshold)
CREATE INDEX idx_scheduler_instances_heartbeat
    ON scheduler_instances (heartbeat_at);

-- Index for deterministic ordering (started_at, then instance_id for ties)
CREATE INDEX idx_scheduler_instances_order
    ON scheduler_instances (started_at, instance_id);
