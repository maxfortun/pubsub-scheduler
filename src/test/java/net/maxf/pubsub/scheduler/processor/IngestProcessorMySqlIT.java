package net.maxf.pubsub.scheduler.processor;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Tag;

import java.util.HashMap;
import java.util.Map;

@QuarkusTest
@TestProfile(IngestProcessorMySqlIT.MySqlTestProfile.class)
@Tag("database")
@Tag("mysql")
class IngestProcessorMySqlIT extends AbstractKafkaIT {

    static {
        SCHEDULER_IN_TOPIC = "scheduler-in-mysql";
        SCHEDULER_DLQ_TOPIC = "scheduler-dlq-mysql";
        SCHEDULER_ADVISORY_TOPIC = "scheduler-advisory-mysql";
    }

    public static class MySqlTestProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> config = new HashMap<>();
            // Kafka endpoints - set broker globally, topic in endpoint
            config.put("camel.component.kafka.brokers", BOOTSTRAP_SERVERS);
            config.put("scheduler.in", "kafka:scheduler-in-mysql");
            config.put("scheduler.dlq", "kafka:scheduler-dlq-mysql");
            config.put("scheduler.advisory", "kafka:scheduler-advisory-mysql");
            config.put("scheduler.advisory.dlq", "kafka:scheduler-advisory-dlq-mysql");
            config.put("scheduler.consumer-group", "scheduler-mysql-it-group");
            // MySQL
            config.put("quarkus.datasource.db-kind", "mysql");
            config.put("quarkus.datasource.jdbc.url", "jdbc:mysql://localhost:3307/scheduler");
            config.put("quarkus.datasource.username", "scheduler");
            config.put("quarkus.datasource.password", "scheduler");
            // Disable all DevServices
            config.put("quarkus.devservices.enabled", "false");
            config.put("quarkus.datasource.devservices.enabled", "false");
            config.put("quarkus.kafka.devservices.enabled", "false");
            config.put("quarkus.secretsmanager.devservices.enabled", "false");
            config.put("quarkus.ssm.devservices.enabled", "false");
            // Scheduler config
            config.put("scheduler.instance-id", "test-instance-mysql");
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
            return "mysql-it";
        }
    }
}
