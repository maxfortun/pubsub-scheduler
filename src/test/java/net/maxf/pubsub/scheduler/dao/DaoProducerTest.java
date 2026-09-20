package net.maxf.pubsub.scheduler.dao;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DaoProducerTest {

    @Nested
    @QuarkusTest
    @TestProfile(PostgresProfile.class)
    class PostgresSelectionTest {

        @Inject
        InstanceDao instanceDao;

        @Inject
        JobDao jobDao;

        @Test
        void postgresql_selectsPostgresDaos() {
            assertInstanceOf(PostgresInstanceDao.class, instanceDao);
            assertInstanceOf(PostgresJobDao.class, jobDao);
        }
    }

    @Nested
    @QuarkusTest
    @TestProfile(MySqlProfile.class)
    class MySqlSelectionTest {

        @Inject
        InstanceDao instanceDao;

        @Inject
        JobDao jobDao;

        @Test
        void mysql_selectsMySqlDaos() {
            assertInstanceOf(MySqlInstanceDao.class, instanceDao);
            assertInstanceOf(MySqlJobDao.class, jobDao);
        }
    }

    @Nested
    @QuarkusTest
    @TestProfile(CockroachDbProfile.class)
    class CockroachDbSelectionTest {

        @Inject
        InstanceDao instanceDao;

        @Inject
        JobDao jobDao;

        @Test
        void cockroachdb_selectsCockroachDbDaos() {
            assertInstanceOf(CockroachDbInstanceDao.class, instanceDao);
            assertInstanceOf(CockroachDbJobDao.class, jobDao);
        }
    }

    @Nested
    @QuarkusTest
    @TestProfile(DialectOverrideProfile.class)
    class DialectOverrideTest {

        @Inject
        InstanceDao instanceDao;

        @Inject
        JobDao jobDao;

        @Test
        void dialectOverride_overridesDbKind() {
            // db-kind is postgresql but dialect is cockroachdb
            assertInstanceOf(CockroachDbInstanceDao.class, instanceDao);
            assertInstanceOf(CockroachDbJobDao.class, jobDao);
        }
    }

    public static class PostgresProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                "quarkus.datasource.db-kind", "postgresql",
                "quarkus.datasource.jdbc.url", "jdbc:postgresql://localhost:5433/scheduler",
                "quarkus.datasource.username", "scheduler",
                "quarkus.datasource.password", "scheduler",
                "quarkus.datasource.devservices.enabled", "false"
            );
        }
    }

    public static class MySqlProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                "quarkus.datasource.db-kind", "mysql",
                "quarkus.datasource.jdbc.url", "jdbc:mysql://localhost:3307/scheduler",
                "quarkus.datasource.username", "scheduler",
                "quarkus.datasource.password", "scheduler",
                "quarkus.datasource.devservices.enabled", "false"
            );
        }
    }

    public static class CockroachDbProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                "quarkus.datasource.db-kind", "postgresql",
                "quarkus.datasource.jdbc.url", "jdbc:postgresql://localhost:26257/scheduler",
                "quarkus.datasource.username", "root",
                "quarkus.datasource.password", "",
                "quarkus.datasource.devservices.enabled", "false",
                "scheduler.db-dialect", "cockroachdb"
            );
        }
    }

    public static class DialectOverrideProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                "quarkus.datasource.db-kind", "postgresql",
                "quarkus.datasource.jdbc.url", "jdbc:postgresql://localhost:26257/scheduler",
                "quarkus.datasource.username", "root",
                "quarkus.datasource.password", "",
                "quarkus.datasource.devservices.enabled", "false",
                "scheduler.db-dialect", "cockroachdb"
            );
        }
    }
}
