# PubSub Scheduler Architecture

A distributed, pub/sub-agnostic message scheduler. Producers publish messages with scheduling headers; the scheduler fires them to destination topics at the specified time.

## Overview

```
┌─────────────┐    ┌─────────────────────────────────┐    ┌─────────────────┐
│  Producer   │───▶│  scheduler.in                   │    │  destination    │
│             │    │  (schedule topic)               │    │  topics         │
└─────────────┘    └───────────────┬─────────────────┘    └────────▲────────┘
                                   │                               │
                                   ▼                               │
                   ┌───────────────────────────────────────────────┴──────┐
                   │                    Scheduler                         │
                   │  ┌─────────┐    ┌─────────────┐    ┌─────────────┐  │
                   │  │ Ingest  │───▶│ DelayQueue  │───▶│    Fire     │  │
                   │  │         │    │ (in-memory) │    │             │  │
                   │  └────┬────┘    └─────────────┘    └─────────────┘  │
                   │       │                                              │
                   │       ▼                                              │
                   │  ┌─────────────┐                                     │
                   │  │  Database   │  (persistence, recovery)            │
                   │  └─────────────┘                                     │
                   └──────────────────────────────────────────────────────┘
                                   │
                                   ▼
                   ┌───────────────────────────────────┐
                   │  scheduler.advisory               │
                   │  (lifecycle events)               │
                   └───────────────────────────────────┘
```

## Topics

| Topic | Purpose |
|-------|---------|
| `scheduler.in` | Inbound schedule requests |
| `scheduler.dlq` | Failed ingest messages |
| `scheduler.advisory` | Job lifecycle events |
| `scheduler.advisory.dlq` | Failed advisory publishes |

## Headers

All scheduler headers are prefixed with `SCHEDULER_`:

| Header | Required | Description | Example |
|--------|----------|-------------|---------|
| `SCHEDULER_DESTINATION` | Yes | Target topic for fired message | `orders.process` |
| `SCHEDULER_FIRE_AT` | No* | Absolute fire time (ISO 8601) | `2026-07-22T15:00:00Z` |
| `SCHEDULER_DELAY` | No* | Relative delay (ISO 8601 duration) | `PT5M` |
| `SCHEDULER_DELAY_START` | No | Reference for delay: `SELF` (arrival, default) or `PREV` (predecessor completion) | `SELF` |
| `SCHEDULER_KEY` | No | Job key for ordering | `order-123` |
| `SCHEDULER_KEY_MODE` | No | `QUEUE` (default), `REPLACE`, or `SKIP` | `QUEUE` |
| `SCHEDULER_RETRIES` | No | Max retries (overrides global default) | `3` |
| `SCHEDULER_ADVISORY_HEADERS` | No | Regex for headers to include in advisory events | `^(requestId\|txnId)$` |

*Either `SCHEDULER_FIRE_AT` or `SCHEDULER_DELAY` should be specified. If neither, fires immediately.

## Key Modes

Jobs can have a **key** for ordering. The mode controls behavior when a new job arrives with the same key:

| Mode | On Arrival | On Predecessor Failure |
|------|------------|------------------------|
| `QUEUE` | Wait for prior jobs with same key to complete, then fire | Cascade failure to all waiting jobs |
| `REPLACE` | Cancel all pending/waiting jobs with same key | N/A |
| `SKIP` | Drop silently if any pending/waiting job exists | N/A |

## Job State Machine

```
WAITING ──▶ PENDING ──▶ ACQUIRED ──▶ FIRING ──▶ COMPLETE
    │          │            │          │
    └──────────┴────────────┴──────────┴──────▶ FAILED
                                                  │
                                                  ▼
                                          (cascade to all
                                           WAITING with same key)
```

| State | Description |
|-------|-------------|
| `WAITING` | Queued behind predecessor (QUEUE mode), not in DelayQueue |
| `PENDING` | In DelayQueue, ready to fire when time comes |
| `ACQUIRED` | Claimed by a scheduler instance |
| `FIRING` | Publishing to destination in progress |
| `COMPLETE` | Successfully published |
| `FAILED` | Failed after retries, triggers cascade for QUEUE mode |

## Advisory Events

Published to `scheduler.advisory`, keyed by job key for partition affinity:

| Event | When |
|-------|------|
| `JOB_SCHEDULED` | Job accepted and queued |
| `JOB_WAITING` | Queued behind predecessor (QUEUE mode) |
| `JOB_SKIPPED` | Dropped due to SKIP mode |
| `JOB_REPLACED` | Cancelled by incoming REPLACE |
| `JOB_PROMOTED` | WAITING → PENDING (predecessor done) |
| `JOB_FIRING` | About to publish to destination |
| `JOB_COMPLETE` | Successfully published |
| `JOB_FAILED` | Failed after retries |
| `JOB_CASCADE_FAILED` | Failed due to predecessor failure |

Advisory events contain metadata only (no original payload). The `SCHEDULER_ADVISORY_HEADERS` regex controls which non-scheduler headers from the original message are included.

## Timing: Push-Based (No Polling)

The scheduler uses `java.util.concurrent.DelayQueue` for precise, push-based timing:

- `DelayQueue.take()` blocks until the next job is due — no polling interval
- Each job implements `Delayed`, returning time remaining until `fire_at`
- Fire loop runs on a virtual thread, spawns a new virtual thread per job
- Database is for persistence/recovery only, not the hot path

## High Availability

### Within Region (Blue/Green)

- Partition sharding: each pod owns specific input partitions and their jobs
- Kafka consumer group handles rebalancing automatically
- On rebalance: reload PENDING jobs from DB into DelayQueue
- No external coordination (ZooKeeper, etcd) required

### Multi-Region (DR)

- Active-passive: Region A processes, Region B is hot standby
- Kafka: MirrorMaker 2 or Confluent Cluster Linking
- Database: CockroachDB with multi-region table localities
- Failover: DNS flip + promote Region B schedulers to active

## Tech Stack

| Component | Choice |
|-----------|--------|
| Runtime | Quarkus |
| Integration | Apache Camel (XML DSL) |
| Build | Gradle |
| Java | 21 LTS |
| Database | PostgreSQL / CockroachDB |
| Messaging | Pub/sub agnostic via Camel endpoints |

## Configuration

```properties
# Endpoints (Camel URIs - swap for any pub/sub system)
scheduler.in=kafka:scheduler.in
scheduler.advisory=kafka:scheduler.advisory
scheduler.dlq=kafka:scheduler.dlq
scheduler.advisory.dlq=kafka:scheduler.advisory.dlq

# Defaults
scheduler.default-retries=3
scheduler.default-advisory-headers=.*
scheduler.consumer-group=scheduler-group
```

## Message Flow

### Ingest
1. Consume from `scheduler.in`
2. Parse `SCHEDULER_*` headers
3. Apply key mode logic (QUEUE/REPLACE/SKIP)
4. Persist to database
5. If PENDING: add to DelayQueue
6. Publish advisory event

### Fire
1. `DelayQueue.take()` returns due job
2. Acquire job (optimistic lock in DB)
3. Publish to destination topic (without `SCHEDULER_*` headers)
4. Mark complete
5. Promote waiting successors (QUEUE mode)
6. Publish advisory event

### Failure
1. Retry up to `max_retries`
2. On final failure: mark FAILED
3. Cascade failure to all WAITING jobs with same key
4. Publish advisory events
