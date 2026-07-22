package net.maxf.pubsub.service;

import net.maxf.pubsub.model.AdvisoryEvent;
import net.maxf.pubsub.model.JobState;
import net.maxf.pubsub.model.KeyMode;
import net.maxf.pubsub.model.ScheduledJob;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
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

    @ConfigProperty(name = "scheduler.instance-id", defaultValue = "scheduler-1")
    String instanceId;

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
        job.setAcquiredBy(instanceId);
        job.setAcquiredAt(Instant.now());
        return true;
    }

    public void handleIncomingJob(ScheduledJob job) {
        if (job.getJobKey() == null) {
            // No key - schedule immediately
            job.setState(JobState.PENDING);
            save(job);
            advisoryService.publish(job, AdvisoryEvent.JOB_SCHEDULED);
            jobQueue.enqueue(job);
            return;
        }

        List<ScheduledJob> existingJobs = findPendingByKey(job.getJobKey());

        switch (job.getKeyMode()) {
            case SKIP -> {
                if (!existingJobs.isEmpty()) {
                    advisoryService.publish(job, AdvisoryEvent.JOB_SKIPPED);
                    LOG.debugf("Job %s skipped - existing job with key %s", job.getId(), job.getJobKey());
                    return;
                }
                job.setState(JobState.PENDING);
                save(job);
                advisoryService.publish(job, AdvisoryEvent.JOB_SCHEDULED);
                jobQueue.enqueue(job);
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
                advisoryService.publish(job, AdvisoryEvent.JOB_SCHEDULED);
                jobQueue.enqueue(job);
            }
            case QUEUE -> {
                if (existingJobs.isEmpty()) {
                    job.setState(JobState.PENDING);
                    save(job);
                    advisoryService.publish(job, AdvisoryEvent.JOB_SCHEDULED);
                    jobQueue.enqueue(job);
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
        //   - Enqueue
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

    public List<ScheduledJob> loadPendingJobsOnStartup() {
        // TODO: Query all PENDING jobs for recovery
        return List.of();
    }
}
