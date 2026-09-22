# Kafka Scheduler Integration Tests

Standalone integration tests for the Kafka Scheduler. Tests run against containerized scheduler instances using HTTP and messaging (Kafka or ActiveMQ) - no embedded runtime.

## Running Tests (Containerized - Recommended)

Run all 4 database/messaging flavors in parallel as containers:

```bash
# Start infrastructure first
./scripts/start-test-infra.sh

# Run containerized integration tests
./scripts/run-integration-tests.sh

# Or via Gradle
./gradlew integrationTest
```

## Running Tests (Local Node.js)

For development/debugging, run tests locally:

```bash
# Start infrastructure
./scripts/start-test-infra.sh

# Run tests locally
cd integration-tests
npm install
npm run test:parallel    # All 4 flavors in parallel
```

### Single Flavor
```bash
npm run test:postgres    # Test PostgreSQL + Kafka scheduler
npm run test:mysql       # Test MySQL + Kafka scheduler
npm run test:cockroach   # Test CockroachDB + Kafka scheduler
npm run test:activemq    # Test MySQL + ActiveMQ scheduler
```

### Watch Mode
```bash
SCHEDULER_FLAVOR=postgres npm run test:watch
```

## Test Coverage

| Category | Tests |
|----------|-------|
| Health & Config | 4 |
| Destination Headers | 3 |
| Timing Headers | 5 |
| Cron Options | 3 |
| Wait Options | 3 |
| Key Policies | 7 |
| Retry Configuration | 2 |
| Advisory Headers | 1 |
| Message Preservation | 2 |
| Invalid Input Handling | 4 |
| Job Lifecycle | 2 |
| Job Cancellation | 2 |
| Job Queries | 3 |
| Complete Scenarios | 2 |
| **Total per flavor** | **43** |
| **Total (4 flavors)** | **172** |

## Configuration

Tests are configured via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `SCHEDULER_FLAVOR` | `postgres` | Scheduler flavor to test (`postgres`, `mysql`, `cockroach`, `activemq`) |
| `SCHEDULER_URL` | (per flavor) | Scheduler HTTP endpoint |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka broker addresses (for Kafka flavors) |
| `ACTIVEMQ_HOST` | `localhost` | ActiveMQ STOMP host (for activemq flavor) |
| `ACTIVEMQ_PORT` | `61613` | ActiveMQ STOMP port (for activemq flavor) |

## Scheduler Endpoints

| Flavor | URL | Messaging | Database |
|--------|-----|-----------|----------|
| PostgreSQL | http://localhost:8091 | Kafka: `scheduler-in-postgres` | PostgreSQL |
| MySQL | http://localhost:8092 | Kafka: `scheduler-in-mysql` | MySQL |
| CockroachDB | http://localhost:8093 | Kafka: `scheduler-in-cockroach` | CockroachDB |
| ActiveMQ | http://localhost:8094 | ActiveMQ: `scheduler-in-activemq` | MySQL |

## Architecture

The test suite uses a polymorphic messaging abstraction to support both Kafka and ActiveMQ:

```
MessagingClient (abstract)
├── KafkaClient     - KafkaJS for Kafka-based schedulers
└── ActiveMQClient  - STOMP protocol for ActiveMQ schedulers
```

The `MessagingClientFactory` creates the appropriate client based on `config.messagingType`:
- `kafka` - PostgreSQL, MySQL, CockroachDB flavors
- `activemq` - ActiveMQ flavor

### Key Files

| File | Purpose |
|------|---------|
| `src/messaging/MessagingClient.js` | Abstract base class defining the interface |
| `src/messaging/KafkaClient.js` | Kafka implementation using KafkaJS |
| `src/messaging/ActiveMQClient.js` | ActiveMQ implementation using STOMP |
| `src/messaging/MessagingClientFactory.js` | Factory for creating messaging clients |
| `src/config.js` | Per-flavor configuration (URLs, topics, queues) |
| `src/scheduler.test.js` | The test suite (43 tests) |
