package net.maxf.pubsub.scheduler.service;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class InstanceRegistryService {

    private static final Logger LOG = Logger.getLogger(InstanceRegistryService.class);

    @Inject
    DataSource dataSource;

    @ConfigProperty(name = "scheduler.instance-id", defaultValue = "${HOSTNAME:scheduler-0}")
    String instanceId;

    @ConfigProperty(name = "scheduler.heartbeat.interval-seconds", defaultValue = "30")
    int heartbeatIntervalSeconds;

    @ConfigProperty(name = "scheduler.heartbeat.stale-threshold-seconds", defaultValue = "120")
    int staleThresholdSeconds;

    private ScheduledExecutorService heartbeatExecutor;
    private final AtomicInteger currentShardIndex = new AtomicInteger(-1);
    private final AtomicInteger currentShardCount = new AtomicInteger(1);
    private volatile Instant startedAt;
    private volatile ShardChangeListener shardChangeListener;

    public interface ShardChangeListener {
        void onShardChanged(int oldShard, int newShard, int shardCount);
    }

    void onStart(@Observes StartupEvent ev) {
        startedAt = Instant.now();
        register();

        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat");
            t.setDaemon(true);
            return t;
        });

        heartbeatExecutor.scheduleAtFixedRate(
            this::heartbeatAndRecomputeShard,
            heartbeatIntervalSeconds,
            heartbeatIntervalSeconds,
            TimeUnit.SECONDS
        );

        // Initial shard computation
        heartbeatAndRecomputeShard();

        LOG.infof("Instance %s registered, shard %d/%d",
            instanceId, currentShardIndex.get(), currentShardCount.get());
    }

    void onStop(@Observes ShutdownEvent ev) {
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdownNow();
        }
        deregister();
        LOG.infof("Instance %s deregistered", instanceId);
    }

    public void setShardChangeListener(ShardChangeListener listener) {
        this.shardChangeListener = listener;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public int getShardIndex() {
        return currentShardIndex.get();
    }

    public int getShardCount() {
        return currentShardCount.get();
    }

    public boolean ownsKey(String key) {
        if (key == null) return false;
        int shardCount = currentShardCount.get();
        if (shardCount <= 1) return true;
        int shard = Math.abs(key.hashCode()) % shardCount;
        return shard == currentShardIndex.get();
    }

    public boolean ownsJob(String jobKey, String jobId) {
        String key = jobKey != null ? jobKey : jobId;
        return ownsKey(key);
    }

    private void register() {
        String sql = """
            INSERT INTO scheduler_instances (instance_id, heartbeat_at, started_at, version)
            VALUES (?, ?, ?, 0)
            ON CONFLICT (instance_id) DO UPDATE
            SET heartbeat_at = EXCLUDED.heartbeat_at,
                started_at = EXCLUDED.started_at,
                version = scheduler_instances.version + 1
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            Timestamp now = Timestamp.from(Instant.now());
            ps.setString(1, instanceId);
            ps.setTimestamp(2, now);
            ps.setTimestamp(3, Timestamp.from(startedAt));
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.errorf(e, "Failed to register instance %s", instanceId);
        }
    }

    private void deregister() {
        String sql = "DELETE FROM scheduler_instances WHERE instance_id = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, instanceId);
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.warnf(e, "Failed to deregister instance %s", instanceId);
        }
    }

    private void heartbeatAndRecomputeShard() {
        try {
            updateHeartbeat();
            recomputeShard();
        } catch (Exception e) {
            LOG.errorf(e, "Heartbeat failed for instance %s", instanceId);
        }
    }

    private void updateHeartbeat() {
        String sql = "UPDATE scheduler_instances SET heartbeat_at = ?, version = version + 1 WHERE instance_id = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setTimestamp(1, Timestamp.from(Instant.now()));
            ps.setString(2, instanceId);
            int updated = ps.executeUpdate();
            if (updated == 0) {
                register();
            }
        } catch (SQLException e) {
            LOG.errorf(e, "Failed to update heartbeat for instance %s", instanceId);
        }
    }

    private void recomputeShard() {
        List<String> liveInstances = getLiveInstances();
        int newShardCount = liveInstances.size();
        int newShardIndex = liveInstances.indexOf(instanceId);

        if (newShardIndex < 0) {
            LOG.warnf("Instance %s not found in live instances, re-registering", instanceId);
            register();
            return;
        }

        int oldShardIndex = currentShardIndex.getAndSet(newShardIndex);
        int oldShardCount = currentShardCount.getAndSet(newShardCount);

        if (oldShardIndex != newShardIndex || oldShardCount != newShardCount) {
            LOG.infof("Shard assignment changed: %d/%d -> %d/%d",
                oldShardIndex, oldShardCount, newShardIndex, newShardCount);

            ShardChangeListener listener = this.shardChangeListener;
            if (listener != null && oldShardIndex >= 0) {
                listener.onShardChanged(oldShardIndex, newShardIndex, newShardCount);
            }
        }
    }

    public List<String> getLiveInstances() {
        List<String> instances = new ArrayList<>();
        String sql = """
            SELECT instance_id FROM scheduler_instances
            WHERE heartbeat_at > ?
            ORDER BY started_at, instance_id
            """;

        Instant threshold = Instant.now().minus(Duration.ofSeconds(staleThresholdSeconds));

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setTimestamp(1, Timestamp.from(threshold));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    instances.add(rs.getString("instance_id"));
                }
            }
        } catch (SQLException e) {
            LOG.errorf(e, "Failed to query live instances");
        }

        return instances;
    }

    public Optional<InstanceInfo> getInstanceInfo(String id) {
        String sql = "SELECT * FROM scheduler_instances WHERE instance_id = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new InstanceInfo(
                        rs.getString("instance_id"),
                        rs.getTimestamp("heartbeat_at").toInstant(),
                        rs.getTimestamp("started_at").toInstant(),
                        rs.getInt("version")
                    ));
                }
            }
        } catch (SQLException e) {
            LOG.errorf(e, "Failed to get instance info for %s", id);
        }

        return Optional.empty();
    }

    public record InstanceInfo(String instanceId, Instant heartbeatAt, Instant startedAt, int version) {}
}
