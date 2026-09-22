package net.maxf.pubsub.scheduler.processor;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Tag;

import java.util.HashMap;
import java.util.Map;

@QuarkusTest
@TestProfile(IngestProcessorH2IT.H2TestProfile.class)
@Tag("database")
@Tag("h2")
class IngestProcessorH2IT extends AbstractKafkaIT {

    public static class H2TestProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> config = new HashMap<>();
            // Kafka endpoints - set broker globally, topic in endpoint
            config.put("camel.component.kafka.brokers", BOOTSTRAP_SERVERS);
            config.put("scheduler.in", "kafka:" + SCHEDULER_IN_TOPIC);
            config.put("scheduler.dlq", "kafka:" + SCHEDULER_DLQ_TOPIC);
            config.put("scheduler.advisory", "kafka:scheduler-advisory");
            config.put("scheduler.advisory.dlq", "kafka:scheduler-advisory-dlq");
            config.put("scheduler.consumer-group", "scheduler-h2-it-group");
            // H2 in-memory database with PostgreSQL compatibility mode
            config.put("quarkus.datasource.db-kind", "h2");
            config.put("quarkus.datasource.jdbc.url", "jdbc:h2:mem:scheduler-h2-it;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;INIT=RUNSCRIPT FROM 'classpath:db/init-h2.sql'");
            config.put("quarkus.datasource.username", "sa");
            config.put("quarkus.datasource.password", "");
            // Disable all DevServices
            config.put("quarkus.devservices.enabled", "false");
            config.put("quarkus.datasource.devservices.enabled", "false");
            config.put("quarkus.kafka.devservices.enabled", "false");
            config.put("quarkus.secretsmanager.devservices.enabled", "false");
            config.put("quarkus.ssm.devservices.enabled", "false");
            // Disable Flyway - using INIT script instead
            config.put("quarkus.flyway.migrate-at-start", "false");
            // Scheduler config
            config.put("scheduler.instance-id", "test-instance-h2");
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
            return "h2-it";
        }
    }
}
