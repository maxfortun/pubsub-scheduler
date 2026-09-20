package net.maxf.pubsub.scheduler.service;

import net.maxf.pubsub.scheduler.dao.DaoException;
import net.maxf.pubsub.scheduler.dao.JobDao;
import net.maxf.pubsub.scheduler.model.AdvisoryEvent;
import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import net.maxf.pubsub.scheduler.model.SleepStart;
import net.maxf.pubsub.scheduler.rest.JobResource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@ApplicationScoped
public class JobStoreService {

    private static final Logger LOG = Logger.getLogger(JobStoreService.class);

    @Inject
    JobDao jobDao;

    @Inject
    AdvisoryService advisoryService;

    @Inject
    JobQueueService jobQueue;

    @Inject
    InstanceRegistryService instanceRegistry;

    @ConfigProperty(name = "scheduler.mode", defaultValue = "sharded")
    String schedulerMode;

    private boolean isReplicated() {
        return "replicated".equalsIgnoreCase(schedulerMode);
    }

    public void save(ScheduledJob job) {
        try {
            jobDao.insert(job);
            LOG.debugf("Saved job %s", job.getId());
        } catch (DaoException e) {
            LOG.errorf(e, "Failed to save job %s", job.getId());
            throw e;
        }
    }

    public boolean saveIfNotExistsByKey(ScheduledJob job) {
        try {
            boolean inserted = jobDao.insertIfNotExistsByKey(job);
            if (inserted) {
                LOG.debugf("Saved job %s (key=%s)", job.getId(), job.getJobKey());
            } else {
                LOG.debugf("Job with key %s already exists, skipped", job.getJobKey());
            }
            return inserted;
        } catch (DaoException e) {
            LOG.errorf(e, "Failed to save job %s", job.getId());
            throw e;
        }
    }

    public void update(ScheduledJob job) {
        job.setVersion(job.getVersion() + 1);
        job.setUpdatedAt(Instant.now());
        try {
            boolean updated = jobDao.update(job);
            if (!updated) {
                LOG.warnf("Optimistic lock failed for job %s", job.getId());
                throw new DaoException("Optimistic lock failed - job was modified by another instance");
            }
            LOG.debugf("Updated job %s to state %s", job.getId(), job.getState());
        } catch (DaoException e) {
            LOG.errorf(e, "Failed to update job %s", job.getId());
            throw e;
        }
    }

    public Optional<ScheduledJob> findById(UUID id) {
        try {
            return jobDao.findById(id);
        } catch (DaoException e) {
            LOG.errorf(e, "Failed to find job %s", id);
            return Optional.empty();
        }
    }

    public List<ScheduledJob> findPendingByKey(String jobKey) {
        try {
            return jobDao.findPendingByKey(jobKey);
        } catch (DaoException e) {
            LOG.errorf(e, "Failed to find pending jobs by key %s", jobKey);
            return List.of();
        }
    }

    public boolean acquire(ScheduledJob job) {
        try {
            boolean acquired = jobDao.acquire(job.getId(), instanceRegistry.getInstanceId(), job.getVersion());
            if (acquired) {
                job.setState(JobState.ACQUIRED);
                job.setAcquiredBy(instanceRegistry.getInstanceId());
                job.setAcquiredAt(Instant.now());
                job.setVersion(job.getVersion() + 1);
            }
            return acquired;
        } catch (DaoException e) {
            LOG.errorf(e, "Failed to acquire job %s", job.getId());
            return false;
        }
    }

    @Transactional
    public void handleIncomingJob(ScheduledJob job) {
        if (job.getJobKey() == null) {
            job.setState(JobState.PENDING);
            save(job);
            advisoryService.publish(job, AdvisoryEvent.JOB_QUEUED);
            if (shouldEnqueueLocally(job)) {
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
                advisoryService.publish(job, AdvisoryEvent.JOB_QUEUED);
                if (shouldEnqueueLocally(job)) {
                    jobQueue.enqueue(job);
                }
            }
            case REPLACE -> {
                for (ScheduledJob existing : existingJobs) {
                    existing.setState(JobState.FAILED);
                    existing.setLastError("Replaced by job " + job.getId());
                    update(existing);
                    jobQueue.remove(existing.getId());
                    advisoryService.publish(existing, AdvisoryEvent.JOB_REPLACED);
                }
                job.setState(JobState.PENDING);
                save(job);
                advisoryService.publish(job, AdvisoryEvent.JOB_QUEUED);
                if (shouldEnqueueLocally(job)) {
                    jobQueue.enqueue(job);
                }
            }
            case QUEUE -> {
                if (existingJobs.isEmpty()) {
                    job.setState(JobState.PENDING);
                    save(job);
                    advisoryService.publish(job, AdvisoryEvent.JOB_QUEUED);
                    if (shouldEnqueueLocally(job)) {
                        jobQueue.enqueue(job);
                    }
                } else {
                    ScheduledJob predecessor = existingJobs.getLast();
                    job.setPredecessorId(predecessor.getId());
                    job.setSequenceNum(predecessor.getSequenceNum() + 1);
                    job.setState(JobState.WAITING);
                    save(job);
                    advisoryService.publish(job, AdvisoryEvent.JOB_CHAINED);
                    LOG.debugf("Job %s waiting behind %s", job.getId(), predecessor.getId());
                }
            }
        }
    }

    private boolean shouldEnqueueLocally(ScheduledJob job) {
        return isReplicated() || ownsJob(job);
    }

    public void promoteSuccessors(ScheduledJob completedJob) {
        if (completedJob.getJobKey() == null) {
            return;
        }

        try {
            List<ScheduledJob> successors = jobDao.findWaitingByPredecessor(completedJob.getId());
            for (ScheduledJob successor : successors) {
                if (successor.getSleepStart() == SleepStart.PREV && successor.getSleepDuration() != null) {
                    Duration sleep = Duration.parse(successor.getSleepDuration());
                    successor.setEffectiveFireAt(Instant.now().plus(sleep));
                } else {
                    successor.setEffectiveFireAt(successor.getFireAt());
                }
                successor.setState(JobState.PENDING);
                successor.setPredecessorId(null);
                update(successor);
                advisoryService.publish(successor, AdvisoryEvent.JOB_PROMOTED);

                if (shouldEnqueueLocally(successor)) {
                    jobQueue.enqueue(successor);
                }
                LOG.debugf("Promoted job %s after completion of %s", successor.getId(), completedJob.getId());
            }
        } catch (DaoException e) {
            LOG.errorf(e, "Failed to promote successors for job %s", completedJob.getId());
        }
    }

    public void cascadeFailure(ScheduledJob failedJob) {
        if (failedJob.getJobKey() == null) {
            return;
        }

        try {
            List<ScheduledJob> waitingJobs = jobDao.findWaitingByKey(failedJob.getJobKey());
            for (ScheduledJob waiting : waitingJobs) {
                waiting.setState(JobState.FAILED);
                waiting.setLastError("Predecessor failed: " + failedJob.getId());
                update(waiting);
                advisoryService.publish(waiting, AdvisoryEvent.JOB_CASCADE_FAILED);
            }
            LOG.infof("Cascaded failure from job %s to %d waiting jobs with key %s",
                    failedJob.getId(), waitingJobs.size(), failedJob.getJobKey());
        } catch (DaoException e) {
            LOG.errorf(e, "Failed to cascade failure for job %s", failedJob.getId());
        }
    }

    public List<ScheduledJob> loadPendingJobsForCurrentShard() {
        int shardIndex = instanceRegistry.getShardIndex();
        int shardCount = instanceRegistry.getShardCount();
        LOG.infof("Loading pending jobs for shard %d/%d", shardIndex, shardCount);

        try {
            return jobDao.findPendingForShard(shardIndex, shardCount);
        } catch (DaoException e) {
            LOG.errorf(e, "Failed to load pending jobs for shard %d/%d", shardIndex, shardCount);
            return List.of();
        }
    }

    public List<ScheduledJob> loadAllPendingJobs() {
        LOG.infof("Loading all pending jobs (replicated mode)");
        try {
            return jobDao.findAllPending();
        } catch (DaoException e) {
            LOG.errorf(e, "Failed to load all pending jobs");
            return List.of();
        }
    }

    public boolean ownsJob(ScheduledJob job) {
        String shardKey = job.getJobKey() != null ? job.getJobKey() : job.getId().toString();
        return instanceRegistry.ownsKey(shardKey);
    }

    public List<ScheduledJob> findPendingJobsForShardNotInQueue(Set<UUID> enqueuedIds) {
        int shardIndex = instanceRegistry.getShardIndex();
        int shardCount = instanceRegistry.getShardCount();
        LOG.debugf("Catch-up scan for shard %d/%d, excluding %d enqueued jobs",
            shardIndex, shardCount, enqueuedIds.size());

        try {
            return jobDao.findPendingForShardExcluding(shardIndex, shardCount, enqueuedIds);
        } catch (DaoException e) {
            LOG.errorf(e, "Failed to find pending jobs for shard not in queue");
            return List.of();
        }
    }

    public List<ScheduledJob> findJobs(JobState state, String jobKey, int limit) {
        LOG.debugf("Finding jobs: state=%s, key=%s, limit=%d", state, jobKey, limit);
        try {
            return jobDao.findJobs(state, jobKey, limit);
        } catch (DaoException e) {
            LOG.errorf(e, "Failed to find jobs");
            return List.of();
        }
    }

    public net.maxf.pubsub.scheduler.rest.JobResource.PagedResult<ScheduledJob> findJobsPaged(
            JobState state, String jobKey, int offset, int limit) {
        LOG.debugf("Finding jobs paged: state=%s, key=%s, offset=%d, limit=%d", state, jobKey, offset, limit);
        try {
            List<ScheduledJob> items = jobDao.findJobsPaged(state, jobKey, offset, limit);
            long total = jobDao.countJobs(state, jobKey);
            return net.maxf.pubsub.scheduler.rest.JobResource.PagedResult.of(items, offset, limit, total);
        } catch (DaoException e) {
            LOG.errorf(e, "Failed to find jobs paged");
            return net.maxf.pubsub.scheduler.rest.JobResource.PagedResult.of(List.of(), offset, limit, 0);
        }
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

    public JobResource.JobStats getStats() {
        try {
            JobDao.JobStats stats = jobDao.getStats();
            return new JobResource.JobStats(
                stats.pending(),
                stats.waiting(),
                stats.acquired(),
                stats.firing(),
                stats.complete(),
                stats.failed()
            );
        } catch (DaoException e) {
            LOG.errorf(e, "Failed to get job stats");
            return new JobResource.JobStats(0, 0, 0, 0, 0, 0);
        }
    }
}
