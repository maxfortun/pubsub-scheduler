#!/bin/bash
# Start test infrastructure from scratch (databases, Kafka)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_DIR/src/test/resources/docker-compose-test.yml"

echo "=== Starting Test Infrastructure ==="

# Stop and remove existing containers
echo "Stopping existing containers..."
docker-compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true

# Also stop any scheduler containers
docker rm -f scheduler-postgres scheduler-mysql scheduler-cockroach 2>/dev/null || true

# Start fresh containers
echo "Starting fresh containers..."
docker-compose -f "$COMPOSE_FILE" up -d

# Wait for health checks
echo "Waiting for containers to be healthy..."
MAX_WAIT=120
WAITED=0

while [ $WAITED -lt $MAX_WAIT ]; do
    HEALTHY=0
    TOTAL=4

    docker inspect --format='{{.State.Health.Status}}' postgres-scheduler-test 2>/dev/null | grep -q "healthy" && ((HEALTHY++)) || true
    docker inspect --format='{{.State.Health.Status}}' mysql-scheduler-test 2>/dev/null | grep -q "healthy" && ((HEALTHY++)) || true
    docker inspect --format='{{.State.Health.Status}}' cockroachdb-scheduler-test 2>/dev/null | grep -q "healthy" && ((HEALTHY++)) || true
    docker inspect --format='{{.State.Health.Status}}' pubsub-scheduler-test 2>/dev/null | grep -q "healthy" && ((HEALTHY++)) || true

    if [ $HEALTHY -eq $TOTAL ]; then
        echo "All containers healthy!"
        break
    fi

    echo "  Waiting... ($HEALTHY/$TOTAL healthy)"
    sleep 5
    ((WAITED+=5))
done

if [ $WAITED -ge $MAX_WAIT ]; then
    echo "WARNING: Timeout waiting for containers to be healthy"
    docker ps
    exit 1
fi

# Initialize CockroachDB (create user and database)
echo "Initializing CockroachDB..."
docker exec cockroachdb-scheduler-test /cockroach/cockroach sql --insecure -e "
CREATE DATABASE IF NOT EXISTS scheduler;
CREATE USER IF NOT EXISTS scheduler;
GRANT ALL ON DATABASE scheduler TO scheduler;
" 2>/dev/null || true

# Run CockroachDB init script
docker exec cockroachdb-scheduler-test /cockroach/cockroach sql --insecure --database=scheduler -e "$(cat $PROJECT_DIR/src/test/resources/db/init-cockroachdb.sql)" 2>/dev/null || true

echo ""
echo "=== Test Infrastructure Ready ==="
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "scheduler-test|NAMES"

echo ""
echo "Endpoints:"
echo "  PostgreSQL:  localhost:5433"
echo "  MySQL:       localhost:3307"
echo "  CockroachDB: localhost:26257"
echo "  Kafka:       localhost:9092"
echo "  Kafka UI:    http://localhost:9001"
