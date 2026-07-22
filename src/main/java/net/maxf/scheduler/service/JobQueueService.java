package net.maxf.scheduler.service;

import net.maxf.scheduler.model.JobState;
import net.maxf.scheduler.model.ScheduledJob;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.DelayQueue;

@ApplicationScoped
public class JobQueueService {

    private static final Logger LOG = Logger.getLogger(JobQueueService.class);

    private final DelayQueue<ScheduledJob> delayQueue = new DelayQueue<>();

    @Inject
    JobStoreService jobStore;

    @Inject
    AdvisoryService advisoryService;

    @PostConstruct
    void init() {
        Thread.ofVirtual().name("job-fire-loop").start(this::fireLoop);
        LOG.info("Job fire loop started on virtual thread");
    }

    public void enqueue(ScheduledJob job) {
        delayQueue.put(job);
        LOG.debugf("Job %s enqueued, fire at %s", job.getId(), job.getEffectiveFireAt());
    }

    public boolean remove(UUID jobId) {
        return delayQueue.removeIf(job -> job.getId().equals(jobId));
    }

    private void fireLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                ScheduledJob job = delayQueue.take();
                Thread.ofVirtual().name("fire-" + job.getId()).start(() -> fireJob(job));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.info("Fire loop interrupted, shutting down");
                break;
            }
        }
    }

    private void fireJob(ScheduledJob job) {
        try {
            if (!jobStore.acquire(job)) {
                LOG.debugf("Job %s already acquired by another instance", job.getId());
                return;
            }

            job.setState(JobState.FIRING);
            job.setUpdatedAt(Instant.now());
            jobStore.update(job);
            advisoryService.publish(job, net.maxf.scheduler.model.AdvisoryEvent.JOB_FIRING);

            // Fire to destination via Camel direct endpoint
            fireToDestination(job);

            job.setState(JobState.COMPLETE);
            job.setUpdatedAt(Instant.now());
            jobStore.update(job);
            advisoryService.publish(job, net.maxf.scheduler.model.AdvisoryEvent.JOB_COMPLETE);

            // Promote waiting successors
            jobStore.promoteSuccessors(job);

        } catch (Exception e) {
            handleFireFailure(job, e);
        }
    }

    private void fireToDestination(ScheduledJob job) {
        // Will be implemented to call Camel ProducerTemplate
        LOG.infof("Firing job %s to %s", job.getId(), job.getDestinationTopic());
    }

    private void handleFireFailure(ScheduledJob job, Exception e) {
        job.setRetryCount(job.getRetryCount() + 1);
        job.setLastError(e.getMessage());
        job.setUpdatedAt(Instant.now());

        if (job.getRetryCount() < job.getMaxRetries()) {
            LOG.warnf("Job %s failed (attempt %d/%d), retrying: %s",
                    job.getId(), job.getRetryCount(), job.getMaxRetries(), e.getMessage());
            job.setState(JobState.PENDING);
            jobStore.update(job);
            enqueue(job);
        } else {
            LOG.errorf("Job %s failed after %d retries: %s",
                    job.getId(), job.getMaxRetries(), e.getMessage());
            job.setState(JobState.FAILED);
            jobStore.update(job);
            advisoryService.publish(job, net.maxf.scheduler.model.AdvisoryEvent.JOB_FAILED);
            jobStore.cascadeFailure(job);
        }
    }
}
