package net.maxf.pubsub.scheduler.service;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import net.maxf.pubsub.scheduler.dao.InstanceDao;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@Tag("database")
@QuarkusTest
@TestProfile(InstanceRegistryServiceTest.Profile.class)
class InstanceRegistryServiceTest {

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

        // Force shard recomputation with new mock data
        registry.refreshShard();

        assertTrue(registry.ownsKey("any-key-1"));
        assertTrue(registry.ownsKey("any-key-2"));
        assertTrue(registry.ownsKey("any-key-3"));
    }

    @Test
    void ownsKey_multipleInstances_correctSharding() {
        when(instanceDao.findLiveInstances(any()))
            .thenReturn(List.of("instance-0", "test-instance", "instance-2"));

        // Force shard recomputation
        registry.refreshShard();

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

    @Test
    void shardChange_notifiesListener() {
        AtomicInteger callCount = new AtomicInteger(0);

        registry.setShardChangeListener((oldShard, newShard, shardCount) -> {
            callCount.incrementAndGet();
        });

        // First shard computation (triggers readyCallback, not shardChangeListener)
        when(instanceDao.findLiveInstances(any())).thenReturn(List.of("test-instance"));
        registry.refreshShard();

        // Now shard change should trigger listener
        when(instanceDao.findLiveInstances(any()))
            .thenReturn(List.of("other-instance", "test-instance"));
        registry.refreshShard();

        assertTrue(callCount.get() >= 1, "ShardChangeListener should have been called");
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
                "scheduler.instance-id", "test-instance",
                "scheduler.heartbeat.interval-seconds", "30",
                "scheduler.heartbeat.stale-threshold-seconds", "120"
            );
        }
    }
}
