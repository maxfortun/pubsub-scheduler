#!/bin/bash
# Start scheduler containers for each database flavor with isolated topics and distinct UI

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_DIR="$PROJECT_DIR/docker"

NETWORK="${SCHEDULER_NETWORK:-resources_default}"
IMAGE="${SCHEDULER_IMAGE:-pubsub-scheduler:test}"

# Stop existing scheduler containers
echo "Stopping existing scheduler containers..."
docker rm -f scheduler-postgres scheduler-mysql scheduler-cockroach 2>/dev/null || true

echo "Starting scheduler containers..."

# PostgreSQL Scheduler
docker run -d --name scheduler-postgres \
  --network "$NETWORK" \
  --env-file "$ENV_DIR/scheduler-postgres.env" \
  -p 8091:8080 \
  "$IMAGE"

# MySQL Scheduler
docker run -d --name scheduler-mysql \
  --network "$NETWORK" \
  --env-file "$ENV_DIR/scheduler-mysql.env" \
  -p 8092:8080 \
  "$IMAGE"

# CockroachDB Scheduler
docker run -d --name scheduler-cockroach \
  --network "$NETWORK" \
  --env-file "$ENV_DIR/scheduler-cockroach.env" \
  -p 8093:8080 \
  "$IMAGE"

echo "Waiting for schedulers to start..."
sleep 10

echo ""
echo "=== Scheduler Status ==="
echo "PostgreSQL (8091):  $(curl -sf http://localhost:8091/q/health/ready >/dev/null && echo 'HEALTHY' || echo 'NOT READY')"
echo "MySQL (8092):       $(curl -sf http://localhost:8092/q/health/ready >/dev/null && echo 'HEALTHY' || echo 'NOT READY')"
echo "CockroachDB (8093): $(curl -sf http://localhost:8093/q/health/ready >/dev/null && echo 'HEALTHY' || echo 'NOT READY')"

echo ""
echo "=== UI Configuration ==="
echo "PostgreSQL:  $(curl -sf http://localhost:8091/api/config 2>/dev/null)"
echo "MySQL:       $(curl -sf http://localhost:8092/api/config 2>/dev/null)"
echo "CockroachDB: $(curl -sf http://localhost:8093/api/config 2>/dev/null)"

echo ""
echo "Schedulers started:"
echo "  PostgreSQL:  http://localhost:8091 (topic: scheduler-in-postgres)"
echo "  MySQL:       http://localhost:8092 (topic: scheduler-in-mysql)"
echo "  CockroachDB: http://localhost:8093 (topic: scheduler-in-cockroach)"
