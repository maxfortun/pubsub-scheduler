#!/bin/bash
# Stop all scheduler containers

echo "Stopping scheduler containers..."
docker stop scheduler-postgres scheduler-mysql scheduler-cockroach 2>/dev/null || true
docker rm scheduler-postgres scheduler-mysql scheduler-cockroach 2>/dev/null || true
echo "Scheduler containers stopped"
