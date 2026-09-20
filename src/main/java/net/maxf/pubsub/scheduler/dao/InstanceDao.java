package net.maxf.pubsub.scheduler.dao;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface InstanceDao {

    void upsert(String instanceId, Instant heartbeatAt, Instant startedAt);

    void delete(String instanceId);

    int updateHeartbeat(String instanceId, Instant heartbeatAt);

    List<String> findLiveInstances(Instant heartbeatThreshold);

    Optional<InstanceInfo> findById(String instanceId);

    record InstanceInfo(String instanceId, Instant heartbeatAt, Instant startedAt, int version) {}
}
