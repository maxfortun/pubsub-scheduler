#!/bin/bash
# Start test infrastructure from scratch (databases, Kafka, schedulers)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_DIR/src/test/resources/docker-compose-test.yml"

echo "=== Starting Test Infrastructure ==="

# Complete cleanup - force remove all test containers and volumes
echo "Stopping and removing all test containers..."
docker-compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true

# Force remove any lingering containers
docker rm -f scheduler-postgres scheduler-mysql scheduler-cockroach scheduler-activemq 2>/dev/null || true
docker rm -f postgres-scheduler-test mysql-scheduler-test cockroachdb-scheduler-test activemq-scheduler-test 2>/dev/null || true
docker rm -f pubsub-scheduler-test kafka-ui-scheduler-test 2>/dev/null || true

# Remove any anonymous volumes from these containers
docker volume prune -f 2>/dev/null || true

# Start fresh containers
echo "Starting fresh containers..."
docker-compose -f "$COMPOSE_FILE" up -d

# Wait for infrastructure health checks
echo "Waiting for infrastructure to be healthy..."
MAX_WAIT=120
WAITED=0

while [ $WAITED -lt $MAX_WAIT ]; do
    HEALTHY=0
    TOTAL=5

    docker inspect --format='{{.State.Health.Status}}' postgres-scheduler-test 2>/dev/null | grep -q "healthy" && ((HEALTHY++)) || true
    docker inspect --format='{{.State.Health.Status}}' mysql-scheduler-test 2>/dev/null | grep -q "healthy" && ((HEALTHY++)) || true
    docker inspect --format='{{.State.Health.Status}}' cockroachdb-scheduler-test 2>/dev/null | grep -q "healthy" && ((HEALTHY++)) || true
    docker inspect --format='{{.State.Health.Status}}' pubsub-scheduler-test 2>/dev/null | grep -q "healthy" && ((HEALTHY++)) || true
    docker inspect --format='{{.State.Health.Status}}' activemq-scheduler-test 2>/dev/null | grep -q "healthy" && ((HEALTHY++)) || true

    if [ $HEALTHY -eq $TOTAL ]; then
        echo "Infrastructure healthy!"
        break
    fi

    echo "  Waiting for infrastructure... ($HEALTHY/$TOTAL healthy)"
    sleep 5
    ((WAITED+=5))
done

if [ $WAITED -ge $MAX_WAIT ]; then
    echo "WARNING: Timeout waiting for infrastructure"
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

# Delete all scheduler topics to ensure clean Kafka state
echo "Deleting all scheduler topics..."
for topic in scheduler-in-postgres scheduler-in-mysql scheduler-in-cockroach \
             scheduler-dlq-postgres scheduler-dlq-mysql scheduler-dlq-cockroach \
             scheduler-advisory-postgres scheduler-advisory-mysql scheduler-advisory-cockroach \
             scheduler-in scheduler-dlq scheduler-advisory output-topic; do
    docker exec pubsub-scheduler-test kafka-topics --bootstrap-server localhost:9092 --delete --topic "$topic" 2>/dev/null || true
done
echo "Kafka topics cleaned"

# Wait for scheduler containers to be healthy
echo "Waiting for scheduler containers to be healthy..."
MAX_WAIT=120
WAITED=0

while [ $WAITED -lt $MAX_WAIT ]; do
    HEALTHY=0
    TOTAL=4

    # Check scheduler health via API (more reliable than health endpoint for MySQL)
    curl -sf http://localhost:8091/api/instances >/dev/null 2>&1 && ((HEALTHY++)) || true
    curl -sf http://localhost:8092/api/instances >/dev/null 2>&1 && ((HEALTHY++)) || true
    curl -sf http://localhost:8093/api/instances >/dev/null 2>&1 && ((HEALTHY++)) || true
    curl -sf http://localhost:8094/api/instances >/dev/null 2>&1 && ((HEALTHY++)) || true

    if [ $HEALTHY -eq $TOTAL ]; then
        echo "All schedulers healthy!"
        break
    fi

    echo "  Waiting for schedulers... ($HEALTHY/$TOTAL healthy)"
    sleep 5
    ((WAITED+=5))
done

if [ $WAITED -ge $MAX_WAIT ]; then
    echo "WARNING: Timeout waiting for schedulers"
    echo "Scheduler logs:"
    docker logs scheduler-postgres 2>&1 | tail -20
    docker logs scheduler-mysql 2>&1 | tail -20
    docker logs scheduler-cockroach 2>&1 | tail -20
    docker logs scheduler-activemq 2>&1 | tail -20
    exit 1
fi

echo ""
echo "=== Test Infrastructure Ready ==="
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "scheduler|kafka|postgres|mysql|cockroach|activemq|NAMES"

echo ""
echo "Infrastructure:"
echo "  PostgreSQL:  localhost:5433"
echo "  MySQL:       localhost:3307"
echo "  CockroachDB: localhost:26257"
echo "  Kafka:       localhost:9092"
echo "  ActiveMQ:    localhost:61616 (STOMP: 61613, Web: http://localhost:8161)"
echo "  Kafka UI:    http://localhost:9001"

echo ""
echo "Schedulers:"
echo "  PostgreSQL:  http://localhost:8091 (Kafka topic: scheduler-in-postgres)"
echo "  MySQL:       http://localhost:8092 (Kafka topic: scheduler-in-mysql)"
echo "  CockroachDB: http://localhost:8093 (Kafka topic: scheduler-in-cockroach)"
echo "  ActiveMQ:    http://localhost:8094 (ActiveMQ queue: scheduler-in-activemq)"

echo ""
echo "UI Config:"
curl -sf http://localhost:8091/api/config 2>/dev/null && echo ""
curl -sf http://localhost:8092/api/config 2>/dev/null && echo ""
curl -sf http://localhost:8093/api/config 2>/dev/null && echo ""
curl -sf http://localhost:8094/api/config 2>/dev/null && echo ""
