#!/bin/bash
# Stop all test infrastructure containers

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_DIR/src/test/resources/docker-compose-test.yml"

echo "=== Stopping Test Infrastructure ==="

# Stop scheduler containers first
echo "Stopping scheduler containers..."
docker rm -f scheduler-postgres scheduler-mysql scheduler-cockroach 2>/dev/null || true

# Stop and remove infrastructure containers (with volumes)
echo "Stopping infrastructure containers..."
docker-compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true

echo "Test infrastructure stopped"
