package net.maxf.pubsub.scheduler.service;

import io.quarkus.runtime.StartupEvent;
import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.DelayQueue;

@ApplicationScoped
public class JobQueueService implements InstanceRegistryService.ShardChangeListener {

    private static final Logger LOG = Logger.getLogger(JobQueueService.class);

    private final DelayQueue<ScheduledJob> delayQueue = new DelayQueue<>();

    @Inject
    JobStoreService jobStore;

    @Inject
    AdvisoryService advisoryService;

    @Inject
    InstanceRegistryService instanceRegistry;

    void onStart(@Observes StartupEvent ev) {
        instanceRegistry.setShardChangeListener(this);
        Thread.ofVirtual().name("job-fire-loop").start(this::fireLoop);
        LOG.info("Job fire loop started on virtual thread");

        // Load jobs for our shard after instance registry is ready
        Thread.ofVirtual().name("job-loader").start(this::loadJobsForShard);
    }

    @Override
    public void onShardChanged(int oldShard, int newShard, int shardCount) {
        LOG.infof("Shard changed from %d to %d (of %d), reloading jobs", oldShard, newShard, shardCount);
        reloadJobsForShard();
    }

    private void loadJobsForShard() {
        // Small delay to ensure instance registry has computed initial shard
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        reloadJobsForShard();
    }

    private void reloadJobsForShard() {
        // Clear current queue - jobs we no longer own will be picked up by new owner
        delayQueue.clear();

        // Load jobs we now own
        List<ScheduledJob> jobs = jobStore.loadPendingJobsForCurrentShard();
        for (ScheduledJob job : jobs) {
            enqueue(job);
        }
        LOG.infof("Loaded %d jobs for shard %d/%d",
            jobs.size(), instanceRegistry.getShardIndex(), instanceRegistry.getShardCount());
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
            advisoryService.publish(job, net.maxf.pubsub.scheduler.model.AdvisoryEvent.JOB_FIRING);

            // Fire to destination via Camel direct endpoint
            fireToDestination(job);

            job.setState(JobState.COMPLETE);
            job.setUpdatedAt(Instant.now());
            jobStore.update(job);
            advisoryService.publish(job, net.maxf.pubsub.scheduler.model.AdvisoryEvent.JOB_COMPLETE);

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
            advisoryService.publish(job, net.maxf.pubsub.scheduler.model.AdvisoryEvent.JOB_FAILED);
            jobStore.cascadeFailure(job);
        }
    }
}
