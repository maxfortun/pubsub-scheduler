package net.maxf.pubsub.scheduler.processor;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

import java.util.HashMap;
import java.util.Map;

@QuarkusTest
@TestProfile(IngestProcessorCockroachDbIT.CockroachDbTestProfile.class)
class IngestProcessorCockroachDbIT extends AbstractKafkaIT {

    public static class CockroachDbTestProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> config = new HashMap<>();
            // Kafka endpoints - set broker globally, topic in endpoint
            config.put("camel.component.kafka.brokers", BOOTSTRAP_SERVERS);
            config.put("scheduler.in", "kafka:" + SCHEDULER_IN_TOPIC);
            config.put("scheduler.dlq", "kafka:" + SCHEDULER_DLQ_TOPIC);
            config.put("scheduler.advisory", "kafka:scheduler-advisory");
            config.put("scheduler.advisory.dlq", "kafka:scheduler-advisory-dlq");
            config.put("scheduler.consumer-group", "scheduler-cockroachdb-it-group");
            // CockroachDB (uses PostgreSQL JDBC driver)
            config.put("quarkus.datasource.db-kind", "cockroachdb");
            config.put("quarkus.datasource.jdbc.url", "jdbc:postgresql://localhost:26257/scheduler?sslmode=disable");
            config.put("quarkus.datasource.username", "root");
            config.put("quarkus.datasource.password", "");
            // Disable all DevServices
            config.put("quarkus.devservices.enabled", "false");
            config.put("quarkus.datasource.devservices.enabled", "false");
            config.put("quarkus.kafka.devservices.enabled", "false");
            config.put("quarkus.secretsmanager.devservices.enabled", "false");
            config.put("quarkus.ssm.devservices.enabled", "false");
            // Scheduler config
            config.put("scheduler.instance-id", "test-instance-cockroachdb");
            config.put("scheduler.mode", "sharded");
            config.put("scheduler.catchup.enabled", "false");
            config.put("scheduler.default-retries", "3");
            config.put("scheduler.default-advisory-headers", ".*");
            // Enable Camel routes for integration tests
            config.put("camel.main.routes-include-pattern", "classpath:camel/kafka.xml");
            return config;
        }

        @Override
        public String getConfigProfile() {
            return "cockroachdb-it";
        }
    }
}
