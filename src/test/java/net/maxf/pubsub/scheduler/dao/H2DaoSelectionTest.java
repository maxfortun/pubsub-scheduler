package net.maxf.pubsub.scheduler.dao;

import io.quarkus.arc.ClientProxy;
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
@TestProfile(H2DaoSelectionTest.Profile.class)
class H2DaoSelectionTest {

    @Inject
    InstanceDao instanceDao;

    @Inject
    JobDao jobDao;

    @Test
    void h2_selectsH2Daos() {
        Object unwrappedInstance = ClientProxy.unwrap(instanceDao);
        Object unwrappedJob = ClientProxy.unwrap(jobDao);
        assertInstanceOf(H2InstanceDao.class, unwrappedInstance);
        assertInstanceOf(H2JobDao.class, unwrappedJob);
    }

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                "quarkus.http.test-port", "0",
                "quarkus.datasource.db-kind", "h2",
                "quarkus.datasource.jdbc.url", "jdbc:h2:mem:scheduler-h2-test;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;INIT=RUNSCRIPT FROM 'classpath:db/init-h2.sql'",
                "quarkus.datasource.username", "sa",
                "quarkus.datasource.password", "",
                "quarkus.datasource.devservices.enabled", "false",
                "quarkus.flyway.migrate-at-start", "false",
                "scheduler.instance-id", "h2-dao-test",
                "scheduler.heartbeat.interval-seconds", "30",
                "scheduler.heartbeat.stale-threshold-seconds", "120"
            );
        }
    }
}
