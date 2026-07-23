# PubSub Scheduler

A distributed, pub/sub-agnostic message scheduler for Kafka, ActiveMQ, and other messaging systems.

## About

In the world of pub/sub, sometimes messages need to be delayed. "Send this email in 24 hours." "Retry this payment in 5 minutes." "Run this report every morning at 9am."

Some brokers, like ActiveMQ, have scheduling baked in — set `AMQ_SCHEDULED_DELAY` and you're done. But what if you're on Kafka? What if you're migrating between brokers and don't want to rewrite your scheduling logic? What if ActiveMQ's scheduling isn't quite enough — you need job ordering, lifecycle events, or a way to cancel jobs mid-flight?

PubSub Scheduler is a broker-neutral implementation of delayed and scheduled message delivery. It sits between your producers and your messaging infrastructure, accepting messages with `SCHEDULER_*` headers and delivering them to any destination topic at the right time. Built on Apache Camel, it works with Kafka, ActiveMQ, RabbitMQ, and dozens of other transports out of the box. The same scheduling semantics, everywhere.

It's designed for production: CockroachDB-backed persistence for multi-region deployments, Kubernetes-native with blue/green HA, push-based timing (no polling), and a REST API for operational control. When things go wrong, you get advisory events, dead-letter queues, and configurable retry policies.

### Feature Parity with ActiveMQ (and Beyond)

| Feature | ActiveMQ | PubSub Scheduler |
|---------|----------|------------------|
| Delay | `AMQ_SCHEDULED_DELAY` (ms) | `SCHEDULER_SLEEP` (ISO 8601 duration) |
| Absolute time | Not supported | `SCHEDULER_AT` |
| Cron | `AMQ_SCHEDULED_CRON` | `SCHEDULER_CRON` |
| Repeat | `AMQ_SCHEDULED_REPEAT` | `SCHEDULER_SLEEP_REPEAT` |
| Period | `AMQ_SCHEDULED_PERIOD` | `SCHEDULER_SLEEP` (with `SLEEP_REPEAT`) |
| Key-based ordering | Not supported | `SCHEDULER_KEY` + `SCHEDULER_KEY_POLICY` |
| Prevent concurrent runs | Not supported | `SCHEDULER_CRON_CONCURRENT=false` |
| Min gap between runs | Not supported | `SCHEDULER_CRON_GAP_MIN` |
| Misfire handling | Not supported | `SCHEDULER_CRON_MISFIRE_POLICY` |
| Lifecycle events | Not supported | Full advisory topic |
| Cancel/Replace jobs | Not supported | `REPLACE`, `SKIP` policies + REST API |
| REST API | JMX only | Full REST API |
| Broker-agnostic | ActiveMQ only | Kafka, ActiveMQ, RabbitMQ, etc. |
| Multi-region HA | Broker-dependent | Built-in with CockroachDB |

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
| `SCHEDULER_SLEEP_REPEAT` | Number of times to repeat (default: 1, one-shot) | `10`, `-1` (infinite) |

**Sleep Start:**

| Value | Behavior |
|-------|----------|
| `SELF` | Sleep from this job's arrival time |
| `PREV` | Sleep from predecessor's completion (requires `SCHEDULER_KEY` with `QUEUE` policy) |

**Example: Repeat every 5 minutes, 10 times**
```
SCHEDULER_DESTINATION: health.check
SCHEDULER_SLEEP: PT5M
SCHEDULER_SLEEP_REPEAT: 10
```

### Cron Options

| Header | Description | Example |
|--------|-------------|---------|
| `SCHEDULER_CRON_CONCURRENT` | Allow overlapping executions (default: `false`) | `true`, `false` |
| `SCHEDULER_CRON_GAP_MIN` | Minimum delay between completions (ISO 8601 duration) | `PT10M` |
| `SCHEDULER_CRON_GAP_POLICY` | Behavior when gap not met (default: `DELAY`) | `DELAY`, `SKIP` |
| `SCHEDULER_CRON_MISFIRE_POLICY` | Behavior on missed execution (default: `SKIP`) | `SKIP`, `CATCH_UP` |
| `SCHEDULER_CRON_END` | End date for recurring job (ISO 8601)* | `2026-12-31T23:59:59Z` |
| `SCHEDULER_CRON_COUNT` | Max number of executions* | `100` |

*`SCHEDULER_CRON_END` and `SCHEDULER_CRON_COUNT` are mutually exclusive.

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

**Example: Daily job ending on a specific date**
```
SCHEDULER_DESTINATION: reports.daily
SCHEDULER_CRON: 0 9 * * *
SCHEDULER_CRON_END: 2026-12-31T23:59:59Z
```

**Example: Daily job limited to 30 executions**
```
SCHEDULER_DESTINATION: trial.reminder
SCHEDULER_CRON: 0 9 * * *
SCHEDULER_CRON_COUNT: 30
```

### Retry & Error Handling

| Header | Description | Example |
|--------|-------------|---------|
| `SCHEDULER_RETRY_COUNT` | Max retry attempts (default: 3) | `5` |

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
| `JOB_COMPLETE` | Successfully published (one execution) |
| `JOB_EXPIRED` | Recurring job ended (CRON_END/CRON_COUNT/SLEEP_REPEAT limit reached) |
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
SCHEDULER_CRON_GAP_MIN: PT3M
SCHEDULER_CRON_GAP_POLICY: DELAY
```

## Docker

### Build

```bash
# Build image
docker build -t pubsub-scheduler .

# Build with custom tag
docker build -t myregistry/pubsub-scheduler:1.0.0 .
```

### Run Standalone

```bash
docker run -d \
  --name scheduler \
  -p 8080:8080 \
  -e QUARKUS_PROFILE=kafka \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e QUARKUS_DATASOURCE_JDBC_URL=jdbc:postgresql://db:5432/scheduler \
  -e QUARKUS_DATASOURCE_USERNAME=scheduler \
  -e QUARKUS_DATASOURCE_PASSWORD=secret \
  pubsub-scheduler
```

### Docker Compose

The included `docker-compose.yml` runs the full stack:
- **CockroachDB** — database with schema auto-init
- **Kafka** — KRaft mode (no ZooKeeper)
- **Scheduler** — the application
- **Test runner** — integration tests

```bash
# Start all services
docker-compose up -d

# Run integration tests
docker-compose up test

# View scheduler logs
docker-compose logs -f scheduler

# Stop and remove
docker-compose down

# Stop and remove with volumes (clean slate)
docker-compose down -v
```

### Configuration Reference

All configuration can be set via environment variables or `application.properties`.

#### Core Settings

| Variable | Description | Default |
|----------|-------------|---------|
| `QUARKUS_PROFILE` | Config profile (`kafka`, `activemq`) | `kafka` |
| `HOSTNAME` | Instance identifier (auto-detected in containers) | `scheduler-0` |

#### Database

| Variable | Description | Default |
|----------|-------------|---------|
| `QUARKUS_DATASOURCE_JDBC_URL` | JDBC connection URL | `jdbc:postgresql://localhost:5432/scheduler` |
| `QUARKUS_DATASOURCE_USERNAME` | Database username | `scheduler` |
| `QUARKUS_DATASOURCE_PASSWORD` | Database password | `scheduler` |

#### Messaging

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `localhost:9092` |
| `CAMEL_COMPONENT_KAFKA_BROKERS` | Camel Kafka brokers (same as above) | `localhost:9092` |
| `SCHEDULER_IN` | Inbound schedule requests endpoint | `kafka:scheduler.in` |
| `SCHEDULER_ADVISORY` | Advisory events endpoint | `kafka:scheduler.advisory` |
| `SCHEDULER_DLQ` | Dead letter queue for failed ingests | `kafka:scheduler.dlq` |
| `SCHEDULER_ADVISORY_DLQ` | Dead letter queue for failed advisories | `kafka:scheduler.advisory.dlq` |

#### Job Defaults

| Variable | Description | Default |
|----------|-------------|---------|
| `SCHEDULER_DEFAULT_RETRY_COUNT` | Default max retries per job | `3` |
| `SCHEDULER_DEFAULT_ADVISORY_HEADERS` | Regex for headers to include in advisories | `.*` |

#### Scaling Mode

| Variable | Description | Default |
|----------|-------------|---------|
| `SCHEDULER_MODE` | `sharded` or `replicated` (see below) | `sharded` |

**Sharded mode (default):**
- Each instance owns jobs based on `hash(key) % instance_count`
- Memory-efficient: each instance holds only its shard
- Catch-up scan fills gaps from cross-instance ingest
- Best for: high job counts, large payloads, cost-sensitive deployments

**Replicated mode:**
- All instances hold all jobs in memory
- Optimistic locking ensures only one fires
- Zero latency on ingest (no catch-up needed)
- Best for: low-latency requirements, small job counts, small payloads

```bash
# Low-latency mode
SCHEDULER_MODE=replicated

# Memory-efficient mode (default)
SCHEDULER_MODE=sharded
```

#### Instance Discovery

| Variable | Description | Default |
|----------|-------------|---------|
| `SCHEDULER_HEARTBEAT_INTERVAL` | Seconds between heartbeats | `30` |
| `SCHEDULER_HEARTBEAT_STALE` | Seconds before instance considered dead | `120` |

#### Catch-up Scan (Sharded Mode Only)

| Variable | Description | Default |
|----------|-------------|---------|
| `SCHEDULER_CATCHUP_ENABLED` | Enable periodic catch-up scan | `true` |
| `SCHEDULER_CATCHUP_INTERVAL` | Interval between scans (duration) | `60s` |

The catch-up scan queries the database for PENDING jobs that belong to this shard but aren't in the local queue. This handles:
- Jobs ingested on a different instance (Kafka partition != shard owner)
- Jobs orphaned by instance crash
- Jobs dropped during shard rebalance

In replicated mode, catch-up is automatically disabled (not needed).

### Image Details

- **Base image:** `bellsoft/liberica-openjre-alpine:21` (~90MB)
- **Final image size:** ~150MB
- **Exposed port:** 8080
- **Health endpoint:** `/q/health`

## Scaling and Load Balancing

PubSub Scheduler uses **dynamic key-based sharding** for horizontal scaling. Instances discover each other via heartbeats to a shared database table — no manual shard configuration required.

### How It Works

1. **Heartbeat registration** — each instance writes to `scheduler_instances` every 30 seconds
2. **Live instance discovery** — instances with heartbeat > 2 minutes ago are considered dead
3. **Deterministic shard assignment** — live instances are ranked by `(started_at, instance_id)`; position in list = shard index
4. **Key-based ownership** — `hash(job_key ?? job_id) % live_instance_count` determines owner

```
shard = hash(job_key ?? job_id) % live_instance_count
```

**Why this design?**
- **No coordination service** — uses the database you already have
- **Works everywhere** — Docker, K8s, bare metal, mixed deployments
- **Self-healing** — dead instances automatically excluded after 2 minutes
- **Dynamic scaling** — add/remove instances anytime, shards rebalance automatically
- **Key affinity preserved** — jobs with the same key always land on the same instance

**Example:** With 3 live instances and a job keyed `order-123`:
```
hash("order-123") % 3 = 1  →  handled by instance ranked #1
```

### Instance API

```bash
# List all live instances and their shards
curl http://localhost:8080/api/instances

# Get this instance's shard info
curl http://localhost:8080/api/instances/self

# Find which instance owns a specific key
curl http://localhost:8080/api/instances/shard?key=order-123
```

### Kubernetes Deployment

The `k8s/` folder contains production-ready manifests:

```bash
# Deploy with kustomize
kubectl apply -k k8s/

# Scale up — new instances register automatically
kubectl scale statefulset pubsub-scheduler -n pubsub-scheduler --replicas=6

# Scale down — removed instances expire after 2 minutes
kubectl scale statefulset pubsub-scheduler -n pubsub-scheduler --replicas=3
```

**What's included:**
- `namespace.yaml` — dedicated namespace
- `configmap.yaml` — non-sensitive config (Kafka brokers, topics)
- `secret.yaml` — database credentials (customize for your cluster)
- `statefulset.yaml` — scheduler pods (no shard config needed)
- `pdb.yaml` — pod disruption budget (min 2 available)
- `hpa.yaml` — horizontal pod autoscaler (3-12 replicas)
- `kustomization.yaml` — kustomize entrypoint

### Docker Compose Scaling

```bash
# Scale to 4 instances
docker-compose up -d --scale app=4

# Instances auto-register and rebalance shards
```

### Scaling Considerations

- **Scale up:** new instance registers, gets shard assignment on first heartbeat, loads its jobs from DB
- **Scale down:** removed instance's heartbeat expires after 2 min, remaining instances absorb its shards
- **Graceful shutdown:** instance deregisters immediately, instant rebalance
- **Crash:** instance heartbeat expires after 2 min, then rebalance
- **Hot keys:** one key with millions of jobs still bottlenecks on one instance (but those jobs are sequential anyway)

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design documentation.

## License

TBD
