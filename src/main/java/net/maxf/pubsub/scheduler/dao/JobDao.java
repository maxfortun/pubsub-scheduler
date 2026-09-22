package net.maxf.pubsub.scheduler.dao;

import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.ScheduledJob;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface JobDao {

    void insert(ScheduledJob job);

    /**
     * Atomically inserts the job only if no active job with the same key exists.
     * Active means state in (PENDING, WAITING, ACQUIRED, RUNNING).
     * @return true if inserted, false if a job with the key already exists
     */
    boolean insertIfNotExistsByKey(ScheduledJob job);

    boolean update(ScheduledJob job);

    Optional<ScheduledJob> findById(UUID id);

    List<ScheduledJob> findPendingByKey(String jobKey);

    List<ScheduledJob> findWaitingByPredecessor(UUID predecessorId);

    List<ScheduledJob> findWaitingByKey(String jobKey);

    List<ScheduledJob> findPendingForShard(int shardIndex, int shardCount);

    List<ScheduledJob> findPendingForShardExcluding(int shardIndex, int shardCount, Set<UUID> excludeIds);

    List<ScheduledJob> findAllPending();

    List<ScheduledJob> findJobs(JobState state, String jobKey, int limit);

    List<ScheduledJob> findJobsPaged(JobState state, String jobKey, int offset, int limit);

    List<ScheduledJob> findJobsPaged(List<JobState> states, String jobKey, String destination, int offset, int limit);

    long countJobs(JobState state, String jobKey);

    long countJobs(List<JobState> states, String jobKey, String destination);

    boolean acquire(UUID jobId, String acquiredBy, int expectedVersion);

    JobStats getStats();

    record JobStats(long pending, long waiting, long acquired, long firing, long complete, long failed) {}
}
