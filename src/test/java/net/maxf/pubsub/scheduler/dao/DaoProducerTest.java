package net.maxf.pubsub.scheduler.dao;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.Tag;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("database")
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
            Map<String, String> config = new java.util.HashMap<>();
            config.put("quarkus.http.test-port", "0");
            config.put("quarkus.datasource.db-kind", "postgresql");
            config.put("quarkus.datasource.jdbc.url", "jdbc:postgresql://localhost:5433/scheduler");
            config.put("quarkus.datasource.username", "scheduler");
            config.put("quarkus.datasource.password", "scheduler");
            config.put("quarkus.datasource.devservices.enabled", "false");
            config.put("scheduler.instance-id", "postgres-test-instance");
            config.put("scheduler.heartbeat.interval-seconds", "30");
            config.put("scheduler.heartbeat.stale-threshold-seconds", "120");
            return config;
        }
    }

    public static class MySqlProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> config = new java.util.HashMap<>();
            config.put("quarkus.http.test-port", "0");
            config.put("quarkus.datasource.db-kind", "mysql");
            config.put("quarkus.datasource.jdbc.url", "jdbc:mysql://localhost:3307/scheduler");
            config.put("quarkus.datasource.username", "scheduler");
            config.put("quarkus.datasource.password", "scheduler");
            config.put("quarkus.datasource.devservices.enabled", "false");
            config.put("scheduler.instance-id", "mysql-test-instance");
            config.put("scheduler.heartbeat.interval-seconds", "30");
            config.put("scheduler.heartbeat.stale-threshold-seconds", "120");
            return config;
        }
    }

    public static class CockroachDbProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> config = new java.util.HashMap<>();
            config.put("quarkus.http.test-port", "0");
            config.put("quarkus.datasource.db-kind", "postgresql");
            config.put("quarkus.datasource.jdbc.url", "jdbc:postgresql://localhost:26257/scheduler");
            config.put("quarkus.datasource.username", "root");
            config.put("quarkus.datasource.password", "");
            config.put("quarkus.datasource.devservices.enabled", "false");
            config.put("scheduler.db-dialect", "cockroachdb");
            config.put("scheduler.instance-id", "cockroach-test-instance");
            config.put("scheduler.heartbeat.interval-seconds", "30");
            config.put("scheduler.heartbeat.stale-threshold-seconds", "120");
            return config;
        }
    }

    public static class DialectOverrideProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> config = new java.util.HashMap<>();
            config.put("quarkus.http.test-port", "0");
            config.put("quarkus.datasource.db-kind", "postgresql");
            config.put("quarkus.datasource.jdbc.url", "jdbc:postgresql://localhost:26257/scheduler");
            config.put("quarkus.datasource.username", "root");
            config.put("quarkus.datasource.password", "");
            config.put("quarkus.datasource.devservices.enabled", "false");
            config.put("scheduler.db-dialect", "cockroachdb");
            config.put("scheduler.instance-id", "dialect-test-instance");
            config.put("scheduler.heartbeat.interval-seconds", "30");
            config.put("scheduler.heartbeat.stale-threshold-seconds", "120");
            return config;
        }
    }
}
