-- CockroachDB schema for test database
-- Note: CockroachDB is PostgreSQL-compatible with minor differences

-- Create user and database (run as root)
CREATE USER IF NOT EXISTS scheduler;
CREATE DATABASE IF NOT EXISTS scheduler;
GRANT ALL ON DATABASE scheduler TO scheduler;

USE scheduler;

-- Scheduled jobs table
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_key STRING,
    key_policy STRING NOT NULL DEFAULT 'QUEUE',
    wait_start STRING NOT NULL DEFAULT 'SELF',
    wait_duration STRING,
    wait_repeat INT NOT NULL DEFAULT 1,
    wait_until TIMESTAMPTZ,
    cron_expression STRING,
    cron_until TIMESTAMPTZ,
    cron_repeat INT,
    cron_run_count INT NOT NULL DEFAULT 0,

    run_at TIMESTAMPTZ NOT NULL,
    effective_run_at TIMESTAMPTZ,
    arrived_at TIMESTAMPTZ NOT NULL,

    destination_topic STRING NOT NULL,
    message_key BYTES,
    message_value BYTES,
    headers JSONB,
    advisory_headers_pattern STRING,

    state STRING NOT NULL DEFAULT 'PENDING',
    max_retries INT NOT NULL DEFAULT 3,
    retry_count INT NOT NULL DEFAULT 0,
    version INT NOT NULL DEFAULT 0,

    predecessor_id UUID REFERENCES scheduled_jobs(id),
    sequence_num INT NOT NULL DEFAULT 0,

    acquired_by STRING,
    acquired_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ,

    last_error STRING,

    INDEX idx_scheduled_jobs_pending_run (effective_run_at) WHERE state = 'PENDING',
    INDEX idx_scheduled_jobs_key_state (job_key, state) WHERE job_key IS NOT NULL,
    INDEX idx_scheduled_jobs_predecessor (predecessor_id) WHERE predecessor_id IS NOT NULL,
    INDEX idx_scheduled_jobs_acquired (acquired_by, acquired_at) WHERE state = 'ACQUIRED' OR state = 'RUNNING'
);

-- Scheduler instances table for heartbeat-based shard discovery
CREATE TABLE IF NOT EXISTS scheduler_instances (
    instance_id STRING PRIMARY KEY,
    heartbeat_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    version INT NOT NULL DEFAULT 0,

    INDEX idx_scheduler_instances_heartbeat (heartbeat_at),
    INDEX idx_scheduler_instances_order (started_at, instance_id)
);

-- Grant permissions
GRANT ALL ON TABLE scheduled_jobs TO scheduler;
GRANT ALL ON TABLE scheduler_instances TO scheduler;
