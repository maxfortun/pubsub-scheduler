package net.maxf.pubsub.scheduler.service;

import net.maxf.pubsub.scheduler.model.AdvisoryEvent;
import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.KeyPolicy;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@ApplicationScoped
public class JobStoreService {

    private static final Logger LOG = Logger.getLogger(JobStoreService.class);

    @Inject
    DataSource dataSource;

    @Inject
    AdvisoryService advisoryService;

    @Inject
    JobQueueService jobQueue;

    @Inject
    InstanceRegistryService instanceRegistry;

    public void save(ScheduledJob job) {
        // TODO: Implement JDBC insert
        LOG.debugf("Saving job %s", job.getId());
    }

    public void update(ScheduledJob job) {
        job.setVersion(job.getVersion() + 1);
        job.setUpdatedAt(Instant.now());
        // TODO: Implement JDBC update with optimistic locking
        LOG.debugf("Updating job %s to state %s", job.getId(), job.getState());
    }

    public Optional<ScheduledJob> findById(UUID id) {
        // TODO: Implement JDBC select
        return Optional.empty();
    }

    public List<ScheduledJob> findPendingByKey(String jobKey) {
        // TODO: Implement JDBC select for PENDING/WAITING jobs with given key
        return List.of();
    }

    public boolean acquire(ScheduledJob job) {
        // Optimistic lock: UPDATE ... WHERE state = PENDING AND version = ?
        // Set state = ACQUIRED, acquired_by = instanceId, acquired_at = now
        // TODO: Implement
        job.setState(JobState.ACQUIRED);
        job.setAcquiredBy(instanceRegistry.getInstanceId());
        job.setAcquiredAt(Instant.now());
        return true;
    }

    public void handleIncomingJob(ScheduledJob job) {
        // Always save the job - it's persisted regardless of shard ownership
        if (job.getJobKey() == null) {
            // No key - schedule immediately
            job.setState(JobState.PENDING);
            save(job);
            advisoryService.publish(job, AdvisoryEvent.JOB_SCHEDULED);
            // Only enqueue locally if we own this shard
            if (ownsJob(job)) {
                jobQueue.enqueue(job);
            }
            return;
        }

        List<ScheduledJob> existingJobs = findPendingByKey(job.getJobKey());

        switch (job.getKeyPolicy()) {
            case SKIP -> {
                if (!existingJobs.isEmpty()) {
                    advisoryService.publish(job, AdvisoryEvent.JOB_SKIPPED);
                    LOG.debugf("Job %s skipped - existing job with key %s", job.getId(), job.getJobKey());
                    return;
                }
                job.setState(JobState.PENDING);
                save(job);
                advisoryService.publish(job, AdvisoryEvent.JOB_SCHEDULED);
                if (ownsJob(job)) {
                    jobQueue.enqueue(job);
                }
            }
            case REPLACE -> {
                for (ScheduledJob existing : existingJobs) {
                    existing.setState(JobState.FAILED);
                    existing.setLastError("Replaced by job " + job.getId());
                    update(existing);
                    if (ownsJob(existing)) {
                        jobQueue.remove(existing.getId());
                    }
                    advisoryService.publish(existing, AdvisoryEvent.JOB_REPLACED);
                }
                job.setState(JobState.PENDING);
                save(job);
                advisoryService.publish(job, AdvisoryEvent.JOB_SCHEDULED);
                if (ownsJob(job)) {
                    jobQueue.enqueue(job);
                }
            }
            case QUEUE -> {
                if (existingJobs.isEmpty()) {
                    job.setState(JobState.PENDING);
                    save(job);
                    advisoryService.publish(job, AdvisoryEvent.JOB_SCHEDULED);
                    if (ownsJob(job)) {
                        jobQueue.enqueue(job);
                    }
                } else {
                    // Find the last job in queue
                    ScheduledJob predecessor = existingJobs.getLast();
                    job.setPredecessorId(predecessor.getId());
                    job.setSequenceNum(predecessor.getSequenceNum() + 1);
                    job.setState(JobState.WAITING);
                    save(job);
                    advisoryService.publish(job, AdvisoryEvent.JOB_WAITING);
                    LOG.debugf("Job %s waiting behind %s", job.getId(), predecessor.getId());
                }
            }
        }
    }

    public void promoteSuccessors(ScheduledJob completedJob) {
        if (completedJob.getJobKey() == null) {
            return;
        }
        // Find WAITING jobs with this job as predecessor
        // TODO: Implement query
        // For each successor:
        //   - Calculate effective fire time
        //   - Set state = PENDING
        //   - Update in DB
        //   - Enqueue only if we own the shard (ownsJob(successor))
        //   - Publish JOB_PROMOTED advisory
    }

    public void cascadeFailure(ScheduledJob failedJob) {
        if (failedJob.getJobKey() == null) {
            return;
        }
        // Find all WAITING jobs with same key
        // TODO: Implement query
        // For each:
        //   - Set state = FAILED
        //   - Set lastError = "Predecessor failed: " + failedJob.getId()
        //   - Update in DB
        //   - Publish JOB_CASCADE_FAILED advisory
        LOG.infof("Cascading failure from job %s to waiting jobs with key %s",
                failedJob.getId(), failedJob.getJobKey());
    }

    public List<ScheduledJob> loadPendingJobsForCurrentShard() {
        // TODO: Query PENDING jobs for this shard only
        // SQL: SELECT * FROM scheduled_jobs
        //      WHERE state = 'PENDING'
        //        AND mod(abs(hashtext(COALESCE(job_key, id::text))), :shardCount) = :shardIndex
        int shardIndex = instanceRegistry.getShardIndex();
        int shardCount = instanceRegistry.getShardCount();
        LOG.infof("Loading pending jobs for shard %d/%d", shardIndex, shardCount);
        return List.of();
    }

    public boolean ownsJob(ScheduledJob job) {
        String shardKey = job.getJobKey() != null ? job.getJobKey() : job.getId().toString();
        return instanceRegistry.ownsKey(shardKey);
    }

    public List<ScheduledJob> findPendingJobsForShardNotInQueue(Set<UUID> enqueuedIds) {
        // Query PENDING jobs for this shard that aren't already enqueued
        // SQL: SELECT * FROM scheduled_jobs
        //      WHERE state = 'PENDING'
        //        AND mod(abs(hashtext(COALESCE(job_key, id::text))), :shardCount) = :shardIndex
        //        AND id NOT IN (:enqueuedIds)
        // TODO: Implement JDBC query
        int shardIndex = instanceRegistry.getShardIndex();
        int shardCount = instanceRegistry.getShardCount();
        LOG.debugf("Catch-up scan for shard %d/%d, excluding %d enqueued jobs",
            shardIndex, shardCount, enqueuedIds.size());
        return List.of();
    }

    public List<ScheduledJob> findJobs(JobState state, String jobKey, int limit) {
        // TODO: Implement JDBC query with filters
        LOG.debugf("Finding jobs: state=%s, key=%s, limit=%d", state, jobKey, limit);
        return List.of();
    }

    public boolean cancelJob(UUID id) {
        Optional<ScheduledJob> jobOpt = findById(id);
        if (jobOpt.isEmpty()) {
            return false;
        }
        ScheduledJob job = jobOpt.get();
        if (job.getState() == JobState.COMPLETE || job.getState() == JobState.FAILED) {
            return false;
        }
        job.setState(JobState.FAILED);
        job.setLastError("Cancelled via API");
        update(job);
        jobQueue.remove(id);
        advisoryService.publish(job, AdvisoryEvent.JOB_FAILED);
        cascadeFailure(job);
        return true;
    }

    public net.maxf.pubsub.scheduler.rest.JobResource.JobStats getStats() {
        // TODO: Implement count queries
        return new net.maxf.pubsub.scheduler.rest.JobResource.JobStats(0, 0, 0, 0, 0, 0);
    }
}
