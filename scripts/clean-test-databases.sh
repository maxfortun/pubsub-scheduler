#!/bin/bash
# Clean all test databases - run before integration tests for a fresh start

set -e

echo "=== Cleaning Test Databases ==="

# PostgreSQL
echo "Cleaning PostgreSQL..."
docker exec postgres-scheduler-test psql -U scheduler -d scheduler -c "
  TRUNCATE TABLE scheduled_jobs CASCADE;
  TRUNCATE TABLE scheduler_instances CASCADE;
" 2>/dev/null && echo "  PostgreSQL cleaned" || echo "  PostgreSQL clean failed (container not running?)"

# MySQL
echo "Cleaning MySQL..."
docker exec mysql-scheduler-test mysql -uscheduler -pscheduler scheduler -e "
  SET FOREIGN_KEY_CHECKS=0;
  TRUNCATE TABLE scheduled_jobs;
  TRUNCATE TABLE scheduler_instances;
  SET FOREIGN_KEY_CHECKS=1;
" 2>/dev/null && echo "  MySQL cleaned" || echo "  MySQL clean failed (container not running?)"

# CockroachDB
echo "Cleaning CockroachDB..."
docker exec cockroachdb-scheduler-test /cockroach/cockroach sql --insecure --database=scheduler -e "
  TRUNCATE TABLE scheduled_jobs CASCADE;
  TRUNCATE TABLE scheduler_instances CASCADE;
" 2>/dev/null && echo "  CockroachDB cleaned" || echo "  CockroachDB clean failed (container not running?)"

echo ""
echo "=== Verifying cleanup ==="
echo "PostgreSQL jobs:  $(docker exec postgres-scheduler-test psql -U scheduler -d scheduler -t -c 'SELECT COUNT(*) FROM scheduled_jobs;' 2>/dev/null | tr -d ' ' || echo 'N/A')"
echo "MySQL jobs:       $(docker exec mysql-scheduler-test mysql -uscheduler -pscheduler -sN scheduler -e 'SELECT COUNT(*) FROM scheduled_jobs;' 2>/dev/null || echo 'N/A')"
echo "CockroachDB jobs: $(docker exec cockroachdb-scheduler-test /cockroach/cockroach sql --insecure --database=scheduler --format=csv -e 'SELECT COUNT(*) FROM scheduled_jobs;' 2>/dev/null | tail -1 || echo 'N/A')"

echo ""
echo "Database cleanup complete"
