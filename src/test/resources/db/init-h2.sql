-- H2 schema for unit tests (PostgreSQL compatibility mode)

-- Scheduled jobs table
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    id UUID PRIMARY KEY,
    job_key VARCHAR(255),
    key_policy VARCHAR(50) NOT NULL DEFAULT 'QUEUE',
    sleep_start VARCHAR(50) NOT NULL DEFAULT 'SELF',
    sleep_duration VARCHAR(50),
    sleep_repeat INT NOT NULL DEFAULT 1,
    cron_expression VARCHAR(100),
    cron_end TIMESTAMP WITH TIME ZONE,
    cron_max_count INT,
    cron_fire_count INT NOT NULL DEFAULT 0,
    cron_concurrent BOOLEAN DEFAULT TRUE,
    cron_gap_min VARCHAR(50),
    cron_misfire_policy VARCHAR(50),

    fire_at TIMESTAMP WITH TIME ZONE NOT NULL,
    effective_fire_at TIMESTAMP WITH TIME ZONE,
    arrived_at TIMESTAMP WITH TIME ZONE NOT NULL,

    destination_topic VARCHAR(500) NOT NULL,
    message_key BYTEA,
    message_value BYTEA,
    headers TEXT,
    advisory_headers_pattern VARCHAR(500),

    state VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    max_retries INT NOT NULL DEFAULT 3,
    retry_count INT NOT NULL DEFAULT 0,
    version INT NOT NULL DEFAULT 0,

    predecessor_id UUID,
    sequence_num INT NOT NULL DEFAULT 0,

    acquired_by VARCHAR(255),
    acquired_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE,

    last_error TEXT
);

-- Scheduler instances table for heartbeat-based shard discovery
CREATE TABLE IF NOT EXISTS scheduler_instances (
    instance_id VARCHAR(255) PRIMARY KEY,
    heartbeat_at TIMESTAMP WITH TIME ZONE NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    version INT NOT NULL DEFAULT 0
);

-- Indexes (H2 compatible - no partial indexes)
CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_key_state ON scheduled_jobs (job_key, state);
CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_predecessor ON scheduled_jobs (predecessor_id);
CREATE INDEX IF NOT EXISTS idx_scheduler_instances_heartbeat ON scheduler_instances (heartbeat_at);
