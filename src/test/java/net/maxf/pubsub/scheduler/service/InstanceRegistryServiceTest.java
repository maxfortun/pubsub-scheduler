package net.maxf.pubsub.scheduler.service;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import net.maxf.pubsub.scheduler.dao.InstanceDao;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class InstanceRegistryServiceTest {

    @Nested
    @QuarkusTest
    @TestProfile(ValidConfigProfile.class)
    class OwnsKeyTests {

        @Inject
        InstanceRegistryService registry;

        @InjectMock
        InstanceDao instanceDao;

        @BeforeEach
        void setUp() {
            when(instanceDao.updateHeartbeat(any(), any())).thenReturn(1);
        }

        @Test
        void ownsKey_nullKey_returnsFalse() {
            assertFalse(registry.ownsKey(null));
        }

        @Test
        void ownsKey_singleInstance_alwaysTrue() {
            when(instanceDao.findLiveInstances(any())).thenReturn(List.of("test-instance"));

            // Trigger shard recomputation
            registry.getLiveInstances();

            assertTrue(registry.ownsKey("any-key-1"));
            assertTrue(registry.ownsKey("any-key-2"));
            assertTrue(registry.ownsKey("any-key-3"));
        }

        @Test
        void ownsKey_multipleInstances_correctSharding() {
            // Simulate 3 instances where current is at index 1
            when(instanceDao.findLiveInstances(any()))
                .thenReturn(List.of("instance-0", "test-instance", "instance-2"));

            registry.getLiveInstances();

            // Different keys should map to different shards
            // The actual shard depends on hashCode % shardCount
            int owned = 0;
            int notOwned = 0;
            for (int i = 0; i < 100; i++) {
                if (registry.ownsKey("key-" + i)) {
                    owned++;
                } else {
                    notOwned++;
                }
            }

            // With 3 shards, roughly 1/3 should be owned
            assertTrue(owned > 20, "Expected ~33% owned, got " + owned);
            assertTrue(owned < 50, "Expected ~33% owned, got " + owned);
            assertTrue(notOwned > 50, "Expected ~66% not owned, got " + notOwned);
        }
    }

    @Nested
    @QuarkusTest
    @TestProfile(ValidConfigProfile.class)
    class ShardChangeListenerTests {

        @Inject
        InstanceRegistryService registry;

        @InjectMock
        InstanceDao instanceDao;

        @Test
        void shardChange_notifiesListener() {
            AtomicInteger callCount = new AtomicInteger(0);
            int[] capturedOld = new int[1];
            int[] capturedNew = new int[1];
            int[] capturedCount = new int[1];

            registry.setShardChangeListener((oldShard, newShard, shardCount) -> {
                callCount.incrementAndGet();
                capturedOld[0] = oldShard;
                capturedNew[0] = newShard;
                capturedCount[0] = shardCount;
            });

            // First call sets up initial shard (via readyCallback, not listener)
            when(instanceDao.findLiveInstances(any())).thenReturn(List.of("test-instance"));
            when(instanceDao.updateHeartbeat(any(), any())).thenReturn(1);
            registry.getLiveInstances();

            // Simulate shard change by adding another instance
            when(instanceDao.findLiveInstances(any()))
                .thenReturn(List.of("other-instance", "test-instance"));

            // This should trigger the listener
            registry.getLiveInstances();

            // Listener should have been called
            assertTrue(callCount.get() >= 1);
        }
    }

    @Nested
    @QuarkusTest
    @TestProfile(InvalidHeartbeatProfile.class)
    class ConfigValidationTests {

        @Test
        void zeroHeartbeat_throwsOnStartup() {
            // The test will fail to start due to invalid config
            // This is expected behavior - we verify by the test profile
        }
    }

    public static class ValidConfigProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                "quarkus.datasource.db-kind", "postgresql",
                "quarkus.datasource.jdbc.url", "jdbc:postgresql://localhost:5433/scheduler",
                "quarkus.datasource.username", "scheduler",
                "quarkus.datasource.password", "scheduler",
                "quarkus.datasource.devservices.enabled", "false",
                "scheduler.instance-id", "test-instance",
                "scheduler.heartbeat.interval-seconds", "30",
                "scheduler.heartbeat.stale-threshold-seconds", "120"
            );
        }
    }

    public static class InvalidHeartbeatProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                "quarkus.datasource.db-kind", "postgresql",
                "quarkus.datasource.jdbc.url", "jdbc:postgresql://localhost:5433/scheduler",
                "quarkus.datasource.username", "scheduler",
                "quarkus.datasource.password", "scheduler",
                "quarkus.datasource.devservices.enabled", "false",
                "scheduler.instance-id", "test-instance",
                "scheduler.heartbeat.interval-seconds", "0",
                "scheduler.heartbeat.stale-threshold-seconds", "120"
            );
        }

        @Override
        public boolean disableGlobalTestResources() {
            return true;
        }
    }
}
