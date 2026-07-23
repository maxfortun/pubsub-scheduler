# PubSub Scheduler

A distributed, pub/sub-agnostic message scheduler. Publish messages with scheduling headers; the scheduler fires them to destination topics at the specified time.

## Quick Start

### Docker (recommended)

```bash
# Start everything: CockroachDB, Kafka, and Scheduler
docker-compose up -d

# Run integration tests
docker-compose up test

# View logs
docker-compose logs -f scheduler

# Stop
docker-compose down
```

### Local Development

```bash
# Build
./gradlew build

# Run with Kafka profile (requires Kafka and PostgreSQL/CockroachDB)
./gradlew quarkusDev -Dquarkus.profile=kafka

# Run with ActiveMQ profile
./gradlew quarkusDev -Dquarkus.profile=activemq
```

## Usage

Publish a message to `scheduler.in` with `SCHEDULER_*` headers:

```
SCHEDULER_DESTINATION: orders.process
SCHEDULER_SLEEP: PT5M
```

The scheduler will deliver the message to `orders.process` after 5 minutes.

## Headers

All headers are prefixed with `SCHEDULER_`.

### Timing (mutually exclusive)

| Header | Description | Example |
|--------|-------------|---------|
| `SCHEDULER_AT` | Absolute fire time (ISO 8601) | `2026-07-22T15:00:00Z` |
| `SCHEDULER_SLEEP` | Relative delay (ISO 8601 duration) | `PT5M`, `PT1H30M` |
| `SCHEDULER_CRON` | Cron expression for recurring jobs | `0 9 * * *` |

If none specified, fires immediately.

### Required

| Header | Description | Example |
|--------|-------------|---------|
| `SCHEDULER_DESTINATION` | Target topic for fired message | `orders.process` |

### Key-Based Ordering

| Header | Description | Example |
|--------|-------------|---------|
| `SCHEDULER_KEY` | Job key for ordering/grouping | `order-123` |
| `SCHEDULER_KEY_POLICY` | Behavior when key exists (default: `QUEUE`) | `QUEUE`, `REPLACE`, `SKIP` |

**Key Policies:**

| Policy | Behavior |
|--------|----------|
| `QUEUE` | Wait for prior jobs with same key to complete, then fire. On predecessor failure, cascade failure to all waiting jobs. |
| `REPLACE` | Cancel all pending/waiting jobs with same key, schedule this one. |
| `SKIP` | Drop silently if any pending/waiting job with same key exists. |

### Sleep Options

| Header | Description | Example |
|--------|-------------|---------|
| `SCHEDULER_SLEEP_START` | Reference point for sleep (default: `SELF`) | `SELF`, `PREV` |

**Sleep Start:**

| Value | Behavior |
|-------|----------|
| `SELF` | Sleep from this job's arrival time |
| `PREV` | Sleep from predecessor's completion (requires `SCHEDULER_KEY` with `QUEUE` policy) |

### Cron Options

| Header | Description | Example |
|--------|-------------|---------|
| `SCHEDULER_CRON_CONCURRENT` | Allow overlapping executions (default: `false`) | `true`, `false` |
| `SCHEDULER_CRON_MIN_GAP` | Minimum delay between completions (ISO 8601 duration) | `PT10M` |
| `SCHEDULER_CRON_GAP_POLICY` | Behavior when gap not met (default: `DELAY`) | `DELAY`, `SKIP` |
| `SCHEDULER_CRON_MISFIRE_POLICY` | Behavior on missed execution (default: `SKIP`) | `SKIP`, `CATCH_UP` |
| `SCHEDULER_CRON_END` | End date for recurring job (ISO 8601) | `2026-12-31T23:59:59Z` |
| `SCHEDULER_CRON_COUNT` | Max number of executions | `100` |

**Cron Gap Policies:**

| Policy | Behavior |
|--------|----------|
| `DELAY` | next_fire = MAX(cron_tick, last_completed + min_gap) |
| `SKIP` | If cron_tick < last_completed + min_gap, skip to following tick |

**Cron Misfire Policies:**

| Policy | Behavior |
|--------|----------|
| `SKIP` | Missed executions are ignored, resume at next scheduled time |
| `CATCH_UP` | Run immediately on recovery, then resume schedule |

### Retry & Error Handling

| Header | Description | Example |
|--------|-------------|---------|
| `SCHEDULER_RETRIES` | Max retry attempts (default: 3) | `5` |

### Advisory Events

| Header | Description | Example |
|--------|-------------|---------|
| `SCHEDULER_ADVISORY_HEADERS` | Regex for headers to include in advisory events | `^(requestId\|txnId)$` |

## Topics

| Topic | Purpose |
|-------|---------|
| `scheduler.in` | Inbound schedule requests |
| `scheduler.dlq` | Failed ingest messages |
| `scheduler.advisory` | Job lifecycle events |
| `scheduler.advisory.dlq` | Failed advisory publishes |

## Advisory Events

Published to `scheduler.advisory` (metadata only, no payload):

| Event | When |
|-------|------|
| `JOB_SCHEDULED` | Job accepted and queued |
| `JOB_WAITING` | Queued behind predecessor (QUEUE policy) |
| `JOB_SKIPPED` | Dropped due to SKIP policy |
| `JOB_REPLACED` | Cancelled by incoming REPLACE |
| `JOB_PROMOTED` | WAITING -> PENDING (predecessor done) |
| `JOB_FIRING` | About to publish to destination |
| `JOB_COMPLETE` | Successfully published |
| `JOB_FAILED` | Failed after retries |
| `JOB_CASCADE_FAILED` | Failed due to predecessor failure |

## Examples

### One-shot delayed message

```
SCHEDULER_DESTINATION: notifications.email
SCHEDULER_SLEEP: PT1H
```

### Scheduled at specific time

```
SCHEDULER_DESTINATION: reports.generate
SCHEDULER_AT: 2026-07-23T09:00:00Z
```

### Daily cron job

```
SCHEDULER_DESTINATION: cleanup.expired
SCHEDULER_CRON: 0 3 * * *
SCHEDULER_CRON_CONCURRENT: false
```

### Ordered job queue

```
SCHEDULER_DESTINATION: orders.process
SCHEDULER_KEY: customer-456
SCHEDULER_KEY_POLICY: QUEUE
SCHEDULER_SLEEP: PT0S
```

### Replace pending job

```
SCHEDULER_DESTINATION: cache.refresh
SCHEDULER_KEY: product-789
SCHEDULER_KEY_POLICY: REPLACE
SCHEDULER_SLEEP: PT30S
```

### Rate-limited cron with minimum gap

```
SCHEDULER_DESTINATION: api.sync
SCHEDULER_CRON: */5 * * * *
SCHEDULER_CRON_CONCURRENT: false
SCHEDULER_CRON_MIN_GAP: PT3M
SCHEDULER_CRON_GAP_POLICY: DELAY
```

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design documentation.

## License

TBD
