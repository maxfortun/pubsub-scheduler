package net.maxf.pubsub.scheduler.service;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import net.maxf.pubsub.scheduler.dao.DaoException;
import net.maxf.pubsub.scheduler.dao.InstanceDao;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
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
    InstanceDao instanceDao;

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
        try {
            instanceDao.upsert(instanceId, Instant.now(), startedAt);
        } catch (DaoException e) {
            LOG.errorf(e, "Failed to register instance %s", instanceId);
        }
    }

    private void deregister() {
        try {
            instanceDao.delete(instanceId);
        } catch (DaoException e) {
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
        try {
            int updated = instanceDao.updateHeartbeat(instanceId, Instant.now());
            if (updated == 0) {
                register();
            }
        } catch (DaoException e) {
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
        Instant threshold = Instant.now().minus(Duration.ofSeconds(staleThresholdSeconds));
        try {
            return instanceDao.findLiveInstances(threshold);
        } catch (DaoException e) {
            LOG.errorf(e, "Failed to query live instances");
            return List.of();
        }
    }

    public Optional<InstanceInfo> getInstanceInfo(String id) {
        try {
            return instanceDao.findById(id)
                .map(info -> new InstanceInfo(
                    info.instanceId(),
                    info.heartbeatAt(),
                    info.startedAt(),
                    info.version()
                ));
        } catch (DaoException e) {
            LOG.errorf(e, "Failed to get instance info for %s", id);
            return Optional.empty();
        }
    }

    public record InstanceInfo(String instanceId, Instant heartbeatAt, Instant startedAt, int version) {}
}
