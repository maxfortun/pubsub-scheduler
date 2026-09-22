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
echo "=== Cleaning Kafka Topics ==="
for topic in scheduler-in-postgres scheduler-in-mysql scheduler-in-cockroach \
             scheduler-dlq-postgres scheduler-dlq-mysql scheduler-dlq-cockroach \
             scheduler-advisory-postgres scheduler-advisory-mysql scheduler-advisory-cockroach \
             scheduler-in scheduler-dlq scheduler-advisory output-topic; do
    docker exec pubsub-scheduler-test kafka-topics --bootstrap-server localhost:9092 --delete --topic "$topic" 2>/dev/null || true
done
echo "  Kafka topics cleaned"

echo ""
echo "=== Verifying cleanup ==="
echo "PostgreSQL jobs:  $(docker exec postgres-scheduler-test psql -U scheduler -d scheduler -t -c 'SELECT COUNT(*) FROM scheduled_jobs;' 2>/dev/null | tr -d ' ' || echo 'N/A')"
echo "MySQL jobs:       $(docker exec mysql-scheduler-test mysql -uscheduler -pscheduler -sN scheduler -e 'SELECT COUNT(*) FROM scheduled_jobs;' 2>/dev/null || echo 'N/A')"
echo "CockroachDB jobs: $(docker exec cockroachdb-scheduler-test /cockroach/cockroach sql --insecure --database=scheduler --format=csv -e 'SELECT COUNT(*) FROM scheduled_jobs;' 2>/dev/null | tail -1 || echo 'N/A')"
echo "Kafka topics:     $(docker exec pubsub-scheduler-test kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -c scheduler || echo '0')"

echo ""
echo "Cleanup complete"
