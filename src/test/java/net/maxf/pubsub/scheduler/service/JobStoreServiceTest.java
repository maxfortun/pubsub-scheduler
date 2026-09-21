package net.maxf.pubsub.scheduler.service;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import net.maxf.pubsub.scheduler.dao.InstanceDao;
import net.maxf.pubsub.scheduler.dao.JobDao;
import net.maxf.pubsub.scheduler.model.AdvisoryEvent;
import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.KeyPolicy;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doNothing;

@QuarkusTest
class JobStoreServiceTest {

    @Inject
    JobStoreService jobStore;

    @InjectMock
    JobDao jobDao;

    @InjectMock
    AdvisoryService advisoryService;

    @InjectMock
    JobQueueService jobQueue;

    @InjectMock
    InstanceRegistryService instanceRegistry;

    @InjectMock
    InstanceDao instanceDao;

    @BeforeEach
    void setUp() {
        when(instanceRegistry.ownsKey(anyString())).thenReturn(true);
        doNothing().when(instanceDao).upsert(anyString(), any(), any());
        when(instanceDao.updateHeartbeat(anyString(), any())).thenReturn(1);
        when(jobDao.update(any(ScheduledJob.class))).thenReturn(true);
    }

    @Nested
    class HandleIncomingJobTests {

        @Test
        void noKey_savesPending() {
            ScheduledJob job = createJob();
            job.setJobKey(null);

            jobStore.handleIncomingJob(job);

            assertEquals(JobState.PENDING, job.getState());
            verify(jobDao).insert(job);
            verify(advisoryService).publish(job, AdvisoryEvent.JOB_QUEUED);
            verify(jobQueue).enqueue(job);
        }

        @Test
        void skipPolicy_existingJob_skipsWithoutSaving() {
            ScheduledJob job = createJob();
            job.setKeyPolicy(KeyPolicy.SKIP);
            job.setJobKey("existing-key");

            ScheduledJob existing = createJob();
            existing.setJobKey("existing-key");
            when(jobDao.findPendingByKey("existing-key")).thenReturn(List.of(existing));

            jobStore.handleIncomingJob(job);

            verify(jobDao, never()).insert(any());
            verify(advisoryService).publish(job, AdvisoryEvent.JOB_SKIPPED);
            verify(jobQueue, never()).enqueue(any());
        }

        @Test
        void skipPolicy_noExisting_saves() {
            ScheduledJob job = createJob();
            job.setKeyPolicy(KeyPolicy.SKIP);
            job.setJobKey("new-key");

            when(jobDao.findPendingByKey("new-key")).thenReturn(List.of());

            jobStore.handleIncomingJob(job);

            assertEquals(JobState.PENDING, job.getState());
            verify(jobDao).insert(job);
            verify(advisoryService).publish(job, AdvisoryEvent.JOB_QUEUED);
        }

        @Test
        void replacePolicy_replacesExisting() {
            ScheduledJob job = createJob();
            job.setKeyPolicy(KeyPolicy.REPLACE);
            job.setJobKey("replace-key");

            ScheduledJob existing1 = createJob();
            existing1.setJobKey("replace-key");
            ScheduledJob existing2 = createJob();
            existing2.setJobKey("replace-key");
            when(jobDao.findPendingByKey("replace-key")).thenReturn(List.of(existing1, existing2));

            jobStore.handleIncomingJob(job);

            // Existing jobs should be failed
            assertEquals(JobState.FAILED, existing1.getState());
            assertEquals(JobState.FAILED, existing2.getState());
            assertTrue(existing1.getLastError().contains(job.getId().toString()));
            verify(jobDao, times(2)).update(argThat(j -> j.getState() == JobState.FAILED));
            verify(jobQueue).remove(existing1.getId());
            verify(jobQueue).remove(existing2.getId());
            verify(advisoryService).publish(existing1, AdvisoryEvent.JOB_REPLACED);
            verify(advisoryService).publish(existing2, AdvisoryEvent.JOB_REPLACED);

            // New job should be saved
            assertEquals(JobState.PENDING, job.getState());
            verify(jobDao).insert(job);
        }

        @Test
        void queuePolicy_noExisting_savesPending() {
            ScheduledJob job = createJob();
            job.setKeyPolicy(KeyPolicy.QUEUE);
            job.setJobKey("queue-key");

            when(jobDao.findPendingByKey("queue-key")).thenReturn(List.of());

            jobStore.handleIncomingJob(job);

            assertEquals(JobState.PENDING, job.getState());
            assertNull(job.getPredecessorId());
            assertEquals(0, job.getSequenceNum());
            verify(jobDao).insert(job);
            verify(advisoryService).publish(job, AdvisoryEvent.JOB_QUEUED);
        }

        @Test
        void queuePolicy_existingJobs_savesWaiting() {
            ScheduledJob job = createJob();
            job.setKeyPolicy(KeyPolicy.QUEUE);
            job.setJobKey("queue-key");

            ScheduledJob predecessor = createJob();
            predecessor.setJobKey("queue-key");
            predecessor.setSequenceNum(5);
            when(jobDao.findPendingByKey("queue-key")).thenReturn(List.of(predecessor));

            jobStore.handleIncomingJob(job);

            assertEquals(JobState.WAITING, job.getState());
            assertEquals(predecessor.getId(), job.getPredecessorId());
            assertEquals(6, job.getSequenceNum());
            verify(jobDao).insert(job);
            verify(advisoryService).publish(job, AdvisoryEvent.JOB_CHAINED);
            verify(jobQueue, never()).enqueue(any());
        }
    }

    @Nested
    class AcquireTests {

        @Test
        void acquire_success_returnsTrue() {
            ScheduledJob job = createJob();
            when(instanceRegistry.getInstanceId()).thenReturn("instance-1");
            when(jobDao.acquire(job.getId(), "instance-1", 0)).thenReturn(true);

            boolean result = jobStore.acquire(job);

            assertTrue(result);
            assertEquals(JobState.ACQUIRED, job.getState());
            assertEquals("instance-1", job.getAcquiredBy());
            assertNotNull(job.getAcquiredAt());
            assertEquals(1, job.getVersion());
        }

        @Test
        void acquire_alreadyAcquired_returnsFalse() {
            ScheduledJob job = createJob();
            when(instanceRegistry.getInstanceId()).thenReturn("instance-1");
            when(jobDao.acquire(job.getId(), "instance-1", 0)).thenReturn(false);

            boolean result = jobStore.acquire(job);

            assertFalse(result);
            assertEquals(JobState.PENDING, job.getState()); // unchanged
        }
    }

    @Nested
    class PromoteSuccessorsTests {

        @Test
        void promoteSuccessors_noKey_doesNothing() {
            ScheduledJob job = createJob();
            job.setJobKey(null);

            jobStore.promoteSuccessors(job);

            verify(jobDao, never()).findWaitingByPredecessor(any());
        }

        @Test
        void promoteSuccessors_promotesWaitingJobs() {
            ScheduledJob completed = createJob();
            completed.setJobKey("test-key");

            ScheduledJob waiting = createJob();
            waiting.setState(JobState.WAITING);
            waiting.setPredecessorId(completed.getId());
            when(jobDao.findWaitingByPredecessor(completed.getId())).thenReturn(List.of(waiting));

            jobStore.promoteSuccessors(completed);

            assertEquals(JobState.PENDING, waiting.getState());
            assertNull(waiting.getPredecessorId());
            verify(jobDao).update(waiting);
            verify(advisoryService).publish(waiting, AdvisoryEvent.JOB_PROMOTED);
            verify(jobQueue).enqueue(waiting);
        }
    }

    @Nested
    class CascadeFailureTests {

        @Test
        void cascadeFailure_noKey_doesNothing() {
            ScheduledJob job = createJob();
            job.setJobKey(null);

            jobStore.cascadeFailure(job);

            verify(jobDao, never()).findWaitingByKey(any());
        }

        @Test
        void cascadeFailure_failsWaitingJobs() {
            ScheduledJob failed = createJob();
            failed.setJobKey("test-key");

            ScheduledJob waiting1 = createJob();
            waiting1.setState(JobState.WAITING);
            ScheduledJob waiting2 = createJob();
            waiting2.setState(JobState.WAITING);
            when(jobDao.findWaitingByKey("test-key")).thenReturn(List.of(waiting1, waiting2));

            jobStore.cascadeFailure(failed);

            assertEquals(JobState.FAILED, waiting1.getState());
            assertEquals(JobState.FAILED, waiting2.getState());
            assertTrue(waiting1.getLastError().contains(failed.getId().toString()));
            verify(jobDao, times(2)).update(argThat(j -> j.getState() == JobState.FAILED));
            verify(advisoryService).publish(waiting1, AdvisoryEvent.JOB_CASCADE_FAILED);
            verify(advisoryService).publish(waiting2, AdvisoryEvent.JOB_CASCADE_FAILED);
        }
    }

    @Nested
    class CancelJobTests {

        @Test
        void cancelJob_pendingJob_cancels() {
            ScheduledJob job = createJob();
            job.setState(JobState.PENDING);
            when(jobDao.findById(job.getId())).thenReturn(Optional.of(job));
            when(jobDao.findWaitingByKey(anyString())).thenReturn(List.of());

            boolean result = jobStore.cancelJob(job.getId());

            assertTrue(result);
            assertEquals(JobState.FAILED, job.getState());
            assertEquals("Cancelled via API", job.getLastError());
            verify(jobQueue).remove(job.getId());
            verify(advisoryService).publish(job, AdvisoryEvent.JOB_FAILED);
        }

        @Test
        void cancelJob_notFound_returnsFalse() {
            UUID id = UUID.randomUUID();
            when(jobDao.findById(id)).thenReturn(Optional.empty());

            boolean result = jobStore.cancelJob(id);

            assertFalse(result);
        }

        @Test
        void cancelJob_alreadyComplete_returnsFalse() {
            ScheduledJob job = createJob();
            job.setState(JobState.COMPLETE);
            when(jobDao.findById(job.getId())).thenReturn(Optional.of(job));

            boolean result = jobStore.cancelJob(job.getId());

            assertFalse(result);
            verify(jobDao, never()).update(any());
        }

        @Test
        void cancelJob_alreadyFailed_returnsFalse() {
            ScheduledJob job = createJob();
            job.setState(JobState.FAILED);
            when(jobDao.findById(job.getId())).thenReturn(Optional.of(job));

            boolean result = jobStore.cancelJob(job.getId());

            assertFalse(result);
        }
    }

    private ScheduledJob createJob() {
        ScheduledJob job = new ScheduledJob();
        job.setDestinationTopic("output-topic");
        job.setFireAt(Instant.now());
        job.setEffectiveFireAt(Instant.now());
        return job;
    }
}
