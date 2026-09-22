-- Combined schema for MySQL test database

-- Scheduled jobs table
CREATE TABLE scheduled_jobs (
    id CHAR(36) PRIMARY KEY,
    job_key VARCHAR(255),
    key_policy VARCHAR(50) NOT NULL DEFAULT 'QUEUE',
    wait_start VARCHAR(50) NOT NULL DEFAULT 'SELF',
    wait_duration VARCHAR(50),
    wait_repeat INT NOT NULL DEFAULT 1,
    wait_until TIMESTAMP(6) NULL,
    cron_expression VARCHAR(255),
    cron_until TIMESTAMP(6) NULL,
    cron_repeat INT,
    cron_run_count INT NOT NULL DEFAULT 0,

    run_at TIMESTAMP(6) NOT NULL,
    effective_run_at TIMESTAMP(6) NULL,
    arrived_at TIMESTAMP(6) NOT NULL,

    destination_topic VARCHAR(255) NOT NULL,
    message_key MEDIUMBLOB,
    message_value MEDIUMBLOB,
    headers JSON,
    advisory_headers_pattern VARCHAR(255),

    state VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    max_retries INT NOT NULL DEFAULT 3,
    retry_count INT NOT NULL DEFAULT 0,
    version INT NOT NULL DEFAULT 0,

    predecessor_id CHAR(36),
    sequence_num INT NOT NULL DEFAULT 0,

    acquired_by VARCHAR(255),
    acquired_at TIMESTAMP(6) NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NULL,

    last_error TEXT,

    FOREIGN KEY (predecessor_id) REFERENCES scheduled_jobs(id)
);

-- Index for polling pending jobs by run time
CREATE INDEX idx_scheduled_jobs_pending_run
    ON scheduled_jobs (effective_run_at);

-- Index for finding jobs by key (for QUEUE/REPLACE/SKIP logic)
CREATE INDEX idx_scheduled_jobs_key_state
    ON scheduled_jobs (job_key, state);

-- Index for finding waiting successors
CREATE INDEX idx_scheduled_jobs_predecessor
    ON scheduled_jobs (predecessor_id);

-- Index for recovery: find jobs that were acquired but not completed
CREATE INDEX idx_scheduled_jobs_acquired
    ON scheduled_jobs (acquired_by, acquired_at);

-- Scheduler instances table for heartbeat-based shard discovery
CREATE TABLE scheduler_instances (
    instance_id VARCHAR(255) PRIMARY KEY,
    heartbeat_at TIMESTAMP(6) NOT NULL,
    started_at TIMESTAMP(6) NOT NULL,
    version INT NOT NULL DEFAULT 0
);

-- Index for finding live instances (heartbeat within threshold)
CREATE INDEX idx_scheduler_instances_heartbeat
    ON scheduler_instances (heartbeat_at);

-- Index for deterministic ordering (started_at, then instance_id for ties)
CREATE INDEX idx_scheduler_instances_order
    ON scheduler_instances (started_at, instance_id);
