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
@TestProfile(CockroachDbDaoSelectionTest.Profile.class)
class CockroachDbDaoSelectionTest {

    @Inject
    InstanceDao instanceDao;

    @Inject
    JobDao jobDao;

    @Inject
    @CockroachDb
    InstanceDao cockroachInstanceDao;

    @Inject
    @CockroachDb
    JobDao cockroachJobDao;

    @Test
    void cockroachdb_selectsCockroachDbDaos() {
        // Verify qualified injection matches the default injection
        assertSame(cockroachInstanceDao, instanceDao);
        assertSame(cockroachJobDao, jobDao);
    }

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                "quarkus.http.test-port", "0",
                "quarkus.datasource.db-kind", "postgresql",
                "quarkus.datasource.jdbc.url", "jdbc:postgresql://localhost:26257/scheduler",
                "quarkus.datasource.username", "root",
                "quarkus.datasource.password", "",
                "quarkus.datasource.devservices.enabled", "false",
                "scheduler.db-dialect", "cockroachdb",
                "scheduler.instance-id", "cockroach-dao-test",
                "scheduler.heartbeat.interval-seconds", "30",
                "scheduler.heartbeat.stale-threshold-seconds", "120"
            );
        }
    }
}
