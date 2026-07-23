package net.maxf.pubsub.scheduler.service;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

@ApplicationScoped
public class JobQueueService implements InstanceRegistryService.ShardChangeListener {

    private static final Logger LOG = Logger.getLogger(JobQueueService.class);

    private final DelayQueue<ScheduledJob> delayQueue = new DelayQueue<>();
    private final Set<UUID> enqueuedJobIds = ConcurrentHashMap.newKeySet();

    @Inject
    JobStoreService jobStore;

    @ConfigProperty(name = "scheduler.catchup.enabled", defaultValue = "true")
    boolean catchUpEnabled;

    @ConfigProperty(name = "scheduler.mode", defaultValue = "sharded")
    String schedulerMode;

    @Inject
    AdvisoryService advisoryService;

    @Inject
    InstanceRegistryService instanceRegistry;

    private boolean isReplicated() {
        return "replicated".equalsIgnoreCase(schedulerMode);
    }

    void onStart(@Observes StartupEvent ev) {
        instanceRegistry.setShardChangeListener(this);
        Thread.ofVirtual().name("job-fire-loop").start(this::fireLoop);
        LOG.info("Job fire loop started on virtual thread");

        // Load jobs after instance registry is ready
        Thread.ofVirtual().name("job-loader").start(this::loadJobsOnStartup);
    }

    @Override
    public void onShardChanged(int oldShard, int newShard, int shardCount) {
        if (isReplicated()) {
            // In replicated mode, shard changes don't affect job loading
            LOG.debugf("Shard changed but running in replicated mode, no reload needed");
            return;
        }
        LOG.infof("Shard changed from %d to %d (of %d), reloading jobs", oldShard, newShard, shardCount);
        reloadJobs();
    }

    private void loadJobsOnStartup() {
        // Small delay to ensure instance registry has computed initial shard
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        reloadJobs();
    }

    private void reloadJobs() {
        // Clear current queue
        delayQueue.clear();
        enqueuedJobIds.clear();

        // Load jobs based on mode
        List<ScheduledJob> jobs = isReplicated()
            ? jobStore.loadAllPendingJobs()
            : jobStore.loadPendingJobsForCurrentShard();

        for (ScheduledJob job : jobs) {
            enqueue(job);
        }

        if (isReplicated()) {
            LOG.infof("Loaded %d jobs (replicated mode)", jobs.size());
        } else {
            LOG.infof("Loaded %d jobs for shard %d/%d",
                jobs.size(), instanceRegistry.getShardIndex(), instanceRegistry.getShardCount());
        }
    }

    @Scheduled(every = "${scheduler.catchup.interval:60s}")
    void catchUpScan() {
        // Catch-up not needed in replicated mode (all jobs already enqueued locally)
        if (!catchUpEnabled || isReplicated()) {
            return;
        }

        List<ScheduledJob> missing = jobStore.findPendingJobsForShardNotInQueue(enqueuedJobIds);
        if (!missing.isEmpty()) {
            LOG.infof("Catch-up scan found %d missing jobs", missing.size());
            for (ScheduledJob job : missing) {
                enqueue(job);
            }
        }
    }

    public void enqueue(ScheduledJob job) {
        if (enqueuedJobIds.add(job.getId())) {
            delayQueue.put(job);
            LOG.debugf("Job %s enqueued, fire at %s", job.getId(), job.getEffectiveFireAt());
        }
    }

    public boolean remove(UUID jobId) {
        enqueuedJobIds.remove(jobId);
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
            // Remove from tracking before firing
            enqueuedJobIds.remove(job.getId());

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
