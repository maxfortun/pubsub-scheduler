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
@TestProfile(MySqlDaoSelectionTest.Profile.class)
class MySqlDaoSelectionTest {

    @Inject
    InstanceDao instanceDao;

    @Inject
    JobDao jobDao;

    @Test
    void mysql_selectsMySqlDaos() {
        Object unwrappedInstance = ClientProxy.unwrap(instanceDao);
        Object unwrappedJob = ClientProxy.unwrap(jobDao);
        assertInstanceOf(MySqlInstanceDao.class, unwrappedInstance);
        assertInstanceOf(MySqlJobDao.class, unwrappedJob);
    }

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                "quarkus.http.test-port", "0",
                "quarkus.datasource.db-kind", "mysql",
                "quarkus.datasource.jdbc.url", "jdbc:mysql://localhost:3307/scheduler",
                "quarkus.datasource.username", "scheduler",
                "quarkus.datasource.password", "scheduler",
                "quarkus.datasource.devservices.enabled", "false",
                "scheduler.instance-id", "mysql-dao-test",
                "scheduler.heartbeat.interval-seconds", "30",
                "scheduler.heartbeat.stale-threshold-seconds", "120"
            );
        }
    }
}
