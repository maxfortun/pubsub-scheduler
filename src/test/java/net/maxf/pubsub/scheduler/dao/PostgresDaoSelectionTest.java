package net.maxf.pubsub.scheduler.dao;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("database")
@QuarkusTest
@TestProfile(PostgresDaoSelectionTest.Profile.class)
class PostgresDaoSelectionTest {

    @Inject
    InstanceDao instanceDao;

    @Inject
    JobDao jobDao;

    @Inject
    @Postgres
    InstanceDao postgresInstanceDao;

    @Inject
    @Postgres
    JobDao postgresJobDao;

    @Test
    void postgresql_selectsPostgresDaos() {
        // Verify qualified injection matches the default injection
        // (proves the producer selected PostgresDao for postgresql db-kind)
        assertSame(postgresInstanceDao, instanceDao);
        assertSame(postgresJobDao, jobDao);
    }

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                "quarkus.http.test-port", "0",
                "quarkus.datasource.db-kind", "postgresql",
                "quarkus.datasource.jdbc.url", "jdbc:postgresql://localhost:5433/scheduler",
                "quarkus.datasource.username", "scheduler",
                "quarkus.datasource.password", "scheduler",
                "quarkus.datasource.devservices.enabled", "false",
                "scheduler.instance-id", "postgres-dao-test",
                "scheduler.heartbeat.interval-seconds", "30",
                "scheduler.heartbeat.stale-threshold-seconds", "120"
            );
        }
    }
}
