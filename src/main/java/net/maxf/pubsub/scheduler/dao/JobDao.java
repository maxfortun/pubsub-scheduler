package net.maxf.pubsub.scheduler.dao;

import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.ScheduledJob;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface JobDao {

    void insert(ScheduledJob job);

    boolean update(ScheduledJob job);

    Optional<ScheduledJob> findById(UUID id);

    List<ScheduledJob> findPendingByKey(String jobKey);

    List<ScheduledJob> findWaitingByPredecessor(UUID predecessorId);

    List<ScheduledJob> findWaitingByKey(String jobKey);

    List<ScheduledJob> findPendingForShard(int shardIndex, int shardCount);

    List<ScheduledJob> findPendingForShardExcluding(int shardIndex, int shardCount, Set<UUID> excludeIds);

    List<ScheduledJob> findAllPending();

    List<ScheduledJob> findJobs(JobState state, String jobKey, int limit);

    boolean acquire(UUID jobId, String acquiredBy, int expectedVersion);

    JobStats getStats();

    record JobStats(long pending, long waiting, long acquired, long firing, long complete, long failed) {}
}
