#!/bin/bash
# Run containerized integration tests against all 3 database flavors in parallel

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Running Integration Tests ==="

# Build test image
echo "Building integration test image..."
docker build -t scheduler-integration-tests "$PROJECT_DIR/integration-tests"

# Ensure infrastructure is running
echo "Checking infrastructure..."
if ! docker inspect scheduler-postgres >/dev/null 2>&1; then
    echo "Infrastructure not running. Start with: ./scripts/start-test-infra.sh"
    exit 1
fi

# Get the docker network
NETWORK=$(docker inspect scheduler-postgres --format='{{range .NetworkSettings.Networks}}{{.NetworkID}}{{end}}' | head -1)
NETWORK_NAME=$(docker network inspect "$NETWORK" --format='{{.Name}}')
echo "Using network: $NETWORK_NAME"

# Clean test databases and Kafka topics
echo "Cleaning test data..."
"$SCRIPT_DIR/clean-test-databases.sh"

# Remove any existing test containers
docker rm -f integration-test-postgres integration-test-mysql integration-test-cockroach 2>/dev/null || true

echo ""
echo "Starting integration tests (all 3 flavors in parallel)..."

# Start all 3 test containers in parallel
docker run -d --name integration-test-postgres \
    --network "$NETWORK_NAME" \
    -e SCHEDULER_FLAVOR=postgres \
    -e SCHEDULER_URL=http://scheduler-postgres:8080 \
    -e KAFKA_BROKERS=pubsub-scheduler-test:29092 \
    scheduler-integration-tests

docker run -d --name integration-test-mysql \
    --network "$NETWORK_NAME" \
    -e SCHEDULER_FLAVOR=mysql \
    -e SCHEDULER_URL=http://scheduler-mysql:8080 \
    -e KAFKA_BROKERS=pubsub-scheduler-test:29092 \
    scheduler-integration-tests

docker run -d --name integration-test-cockroach \
    --network "$NETWORK_NAME" \
    -e SCHEDULER_FLAVOR=cockroach \
    -e SCHEDULER_URL=http://scheduler-cockroach:8080 \
    -e KAFKA_BROKERS=pubsub-scheduler-test:29092 \
    scheduler-integration-tests

echo "Test containers started. Waiting for completion..."

# Wait for all containers to complete
PG_EXIT=0
MYSQL_EXIT=0
CRDB_EXIT=0

while true; do
    PG_STATUS=$(docker inspect integration-test-postgres --format='{{.State.Status}}' 2>/dev/null || echo "removed")
    MYSQL_STATUS=$(docker inspect integration-test-mysql --format='{{.State.Status}}' 2>/dev/null || echo "removed")
    CRDB_STATUS=$(docker inspect integration-test-cockroach --format='{{.State.Status}}' 2>/dev/null || echo "removed")

    if [ "$PG_STATUS" = "exited" ] && [ "$MYSQL_STATUS" = "exited" ] && [ "$CRDB_STATUS" = "exited" ]; then
        break
    fi

    echo "  Status: postgres=$PG_STATUS, mysql=$MYSQL_STATUS, cockroach=$CRDB_STATUS"
    sleep 5
done

# Get exit codes
PG_EXIT=$(docker inspect integration-test-postgres --format='{{.State.ExitCode}}')
MYSQL_EXIT=$(docker inspect integration-test-mysql --format='{{.State.ExitCode}}')
CRDB_EXIT=$(docker inspect integration-test-cockroach --format='{{.State.ExitCode}}')

echo ""
echo "=== Test Results ==="

# Show results with logs for failures
for flavor in postgres mysql cockroach; do
    container="integration-test-$flavor"
    code=$(docker inspect "$container" --format='{{.State.ExitCode}}' 2>/dev/null || echo "N/A")
    if [ "$code" = "0" ]; then
        echo "  $flavor: PASSED"
    else
        echo "  $flavor: FAILED (exit code: $code)"
        echo "  --- Logs for $flavor ---"
        docker logs "$container" 2>&1 | tail -50
        echo "  --- End logs ---"
    fi
done

# Cleanup test containers
echo ""
echo "Cleaning up test containers..."
docker rm -f integration-test-postgres integration-test-mysql integration-test-cockroach 2>/dev/null || true

# Return failure if any test failed
if [ "$PG_EXIT" != "0" ] || [ "$MYSQL_EXIT" != "0" ] || [ "$CRDB_EXIT" != "0" ]; then
    exit 1
fi

echo ""
echo "All tests passed!"
exit 0
