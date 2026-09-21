package net.maxf.pubsub.scheduler.service;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import net.maxf.pubsub.scheduler.model.AdvisoryEvent;
import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class JobQueueService implements InstanceRegistryService.ShardChangeListener {

    private static final Logger LOG = Logger.getLogger(JobQueueService.class);

    private final DelayQueue<ScheduledJob> delayQueue = new DelayQueue<>();
    private final Set<UUID> enqueuedJobIds = ConcurrentHashMap.newKeySet();
    private final Object queueLock = new Object();
    private final CountDownLatch registryReady = new CountDownLatch(1);

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
        instanceRegistry.setReadyCallback(this::signalRegistryReady);
        Thread.ofVirtual().name("job-fire-loop").start(this::fireLoop);
        LOG.info("Job fire loop started on virtual thread");

        // Load jobs after instance registry is ready
        Thread.ofVirtual().name("job-loader").start(this::loadJobsOnStartup);
    }

    private void signalRegistryReady() {
        registryReady.countDown();
    }

    @Override
    public void onShardChanged(int oldShard, int newShard, int shardCount) {
        if (isReplicated()) {
            LOG.debugf("Shard changed but running in replicated mode, no reload needed");
            return;
        }
        LOG.infof("Shard changed from %d to %d (of %d), reloading jobs", oldShard, newShard, shardCount);
        reloadJobs();
    }

    private void loadJobsOnStartup() {
        try {
            if (!registryReady.await(30, TimeUnit.SECONDS)) {
                LOG.warn("Timeout waiting for instance registry, loading jobs anyway");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        reloadJobs();
    }

    private void reloadJobs() {
        synchronized (queueLock) {
            delayQueue.clear();
            enqueuedJobIds.clear();

            List<ScheduledJob> jobs = isReplicated()
                ? jobStore.loadAllPendingJobs()
                : jobStore.loadPendingJobsForCurrentShard();

            for (ScheduledJob job : jobs) {
                enqueueInternal(job);
            }

            if (isReplicated()) {
                LOG.infof("Loaded %d jobs (replicated mode)", jobs.size());
            } else {
                LOG.infof("Loaded %d jobs for shard %d/%d",
                    jobs.size(), instanceRegistry.getShardIndex(), instanceRegistry.getShardCount());
            }
        }
    }

    @Scheduled(every = "${scheduler.catchup.interval:60s}")
    void catchUpScan() {
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
        synchronized (queueLock) {
            enqueueInternal(job);
        }
    }

    private void enqueueInternal(ScheduledJob job) {
        if (enqueuedJobIds.add(job.getId())) {
            delayQueue.put(job);
            LOG.debugf("Job %s enqueued, fire at %s", job.getId(), job.getEffectiveRunAt());
        }
    }

    public boolean remove(UUID jobId) {
        synchronized (queueLock) {
            enqueuedJobIds.remove(jobId);
            return delayQueue.removeIf(job -> job.getId().equals(jobId));
        }
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
            enqueuedJobIds.remove(job.getId());

            if (!jobStore.acquire(job)) {
                LOG.debugf("Job %s already acquired by another instance", job.getId());
                return;
            }

            job.setState(JobState.RUNNING);
            job.setUpdatedAt(Instant.now());
            jobStore.update(job);
            advisoryService.publish(job, AdvisoryEvent.JOB_RUNNING);

            fireToDestination(job);

            if (shouldRepeat(job)) {
                scheduleNextRepetition(job);
            } else {
                job.setState(JobState.DONE);
                job.setUpdatedAt(Instant.now());
                jobStore.update(job);
                advisoryService.publish(job, AdvisoryEvent.JOB_DONE);
                jobStore.promoteSuccessors(job);
            }

        } catch (RuntimeException e) {
            handleFireFailure(job, e);
        }
    }

    private void fireToDestination(ScheduledJob job) {
        LOG.infof("Firing job %s to %s", job.getId(), job.getDestinationTopic());
    }

    private boolean shouldRepeat(ScheduledJob job) {
        // Check CRON-based repeat
        if (job.getCronExpression() != null) {
            return shouldRepeatCron(job);
        }
        // Check SLEEP-based repeat
        if (job.getWaitDuration() != null) {
            int repeat = job.getWaitRepeat();
            return repeat <= 0 || repeat > 1;
        }
        return false;
    }

    private boolean shouldRepeatCron(ScheduledJob job) {
        // Check if past end time
        if (job.getCronUntil() != null && Instant.now().isAfter(job.getCronUntil())) {
            return false;
        }
        // Check if reached max count
        if (job.getCronRepeat() != null && job.getCronRunCount() >= job.getCronRepeat()) {
            return false;
        }
        return true;
    }

    private void scheduleNextRepetition(ScheduledJob job) {
        Instant nextFire;

        if (job.getCronExpression() != null) {
            nextFire = net.maxf.pubsub.scheduler.processor.IngestProcessor
                .calculateNextCronFireFrom(job.getCronExpression(), Instant.now());
            job.setCronRunCount(job.getCronRunCount() + 1);
        } else {
            Duration waitDuration = Duration.parse(job.getWaitDuration());
            nextFire = Instant.now().plus(waitDuration);
            if (job.getWaitRepeat() > 0) {
                job.setWaitRepeat(job.getWaitRepeat() - 1);
            }
        }

        job.setRunAt(nextFire);
        job.setEffectiveRunAt(nextFire);
        job.setState(JobState.PENDING);
        job.setUpdatedAt(Instant.now());
        job.setRetryCount(0);

        jobStore.update(job);
        advisoryService.publish(job, AdvisoryEvent.JOB_DONE);
        enqueue(job);
        LOG.infof("Scheduled next repetition of job %s at %s", job.getId(), nextFire);
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
            advisoryService.publish(job, AdvisoryEvent.JOB_FAILED);
            jobStore.cascadeFailure(job);
        }
    }
}
