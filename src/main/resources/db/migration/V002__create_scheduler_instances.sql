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
