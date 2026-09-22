# Kafka Scheduler Integration Tests

Standalone integration tests for the Kafka Scheduler. Tests run against containerized scheduler instances using HTTP and Kafka - no embedded runtime.

## Running Tests (Containerized - Recommended)

Run all 3 database flavors in parallel as containers:

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
npm run test:parallel    # All 3 flavors in parallel
```

### Single Flavor
```bash
npm run test:postgres    # Test PostgreSQL scheduler
npm run test:mysql       # Test MySQL scheduler
npm run test:cockroach   # Test CockroachDB scheduler
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
| **Total (3 flavors)** | **129** |

## Configuration

Tests are configured via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `SCHEDULER_FLAVOR` | `postgres` | Scheduler flavor to test (`postgres`, `mysql`, `cockroach`) |

## Scheduler Endpoints

| Flavor | URL | Kafka Topic |
|--------|-----|-------------|
| PostgreSQL | http://localhost:8091 | scheduler-in-postgres |
| MySQL | http://localhost:8092 | scheduler-in-mysql |
| CockroachDB | http://localhost:8093 | scheduler-in-cockroach |
