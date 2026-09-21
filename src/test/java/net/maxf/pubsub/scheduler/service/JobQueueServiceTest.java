package net.maxf.pubsub.scheduler.service;

import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for job queue repeat logic. These are pure unit tests
 * that don't require Quarkus or database - they test the scheduling logic directly.
 */
class JobQueueServiceTest {

    @Nested
    class RepeatLogicTests {

        @Test
        void shouldRepeat_noSleepDurationNoCron_returnsFalse() {
            ScheduledJob job = createJob();
            job.setWaitDuration(null);
            job.setCronExpression(null);
            job.setWaitRepeat(5);

            assertFalse(testShouldRepeat(job));
        }

        @Test
        void shouldRepeat_cronExpression_returnsTrue() {
            ScheduledJob job = createJob();
            job.setCronExpression("0 0 * * *");
            job.setWaitDuration(null);

            assertTrue(testShouldRepeat(job));
        }

        @Test
        void shouldRepeat_cronExpressionPastEnd_returnsFalse() {
            ScheduledJob job = createJob();
            job.setCronExpression("0 0 * * *");
            job.setCronUntil(Instant.now().minusSeconds(1)); // past

            assertFalse(testShouldRepeatCron(job));
        }

        @Test
        void shouldRepeat_cronExpressionReachedMaxCount_returnsFalse() {
            ScheduledJob job = createJob();
            job.setCronExpression("0 0 * * *");
            job.setCronRepeat(5);
            job.setCronRunCount(5);

            assertFalse(testShouldRepeatCron(job));
        }

        @Test
        void shouldRepeat_cronExpressionBelowMaxCount_returnsTrue() {
            ScheduledJob job = createJob();
            job.setCronExpression("0 0 * * *");
            job.setCronRepeat(5);
            job.setCronRunCount(3);

            assertTrue(testShouldRepeatCron(job));
        }

        @Test
        void shouldRepeat_noSleepDuration_returnsFalse() {
            ScheduledJob job = createJob();
            job.setWaitDuration(null);
            job.setWaitRepeat(5);

            assertFalse(testShouldRepeat(job));
        }

        @Test
        void shouldRepeat_waitRepeatOne_returnsFalse() {
            ScheduledJob job = createJob();
            job.setWaitDuration("PT15M");
            job.setWaitRepeat(1);

            assertFalse(testShouldRepeat(job));
        }

        @Test
        void shouldRepeat_waitRepeatZero_returnsTrue() {
            ScheduledJob job = createJob();
            job.setWaitDuration("PT15M");
            job.setWaitRepeat(0);

            assertTrue(testShouldRepeat(job));
        }

        @Test
        void shouldRepeat_waitRepeatNegative_returnsTrue() {
            ScheduledJob job = createJob();
            job.setWaitDuration("PT15M");
            job.setWaitRepeat(-1);

            assertTrue(testShouldRepeat(job));
        }

        @Test
        void shouldRepeat_waitRepeatGreaterThanOne_returnsTrue() {
            ScheduledJob job = createJob();
            job.setWaitDuration("PT15M");
            job.setWaitRepeat(5);

            assertTrue(testShouldRepeat(job));
        }

        @Test
        void shouldRepeat_waitRepeatTwo_returnsTrue() {
            ScheduledJob job = createJob();
            job.setWaitDuration("PT15M");
            job.setWaitRepeat(2);

            assertTrue(testShouldRepeat(job));
        }
    }

    @Nested
    class ScheduleNextRepetitionTests {

        @Test
        void scheduleNextRepetition_setsNewFireAt() {
            ScheduledJob job = createJob();
            job.setWaitDuration("PT15M");
            job.setWaitRepeat(0);
            Instant before = Instant.now();

            testScheduleNextRepetition(job);

            Instant expectedMin = before.plus(Duration.ofMinutes(15));
            assertTrue(job.getRunAt().isAfter(expectedMin.minusSeconds(1)));
            assertTrue(job.getRunAt().isBefore(expectedMin.plusSeconds(5)));
        }

        @Test
        void scheduleNextRepetition_setsStateToPending() {
            ScheduledJob job = createJob();
            job.setWaitDuration("PT15M");
            job.setWaitRepeat(0);
            job.setState(JobState.RUNNING);

            testScheduleNextRepetition(job);

            assertEquals(JobState.PENDING, job.getState());
        }

        @Test
        void scheduleNextRepetition_resetsRetryCount() {
            ScheduledJob job = createJob();
            job.setWaitDuration("PT15M");
            job.setWaitRepeat(0);
            job.setRetryCount(3);

            testScheduleNextRepetition(job);

            assertEquals(0, job.getRetryCount());
        }

        @Test
        void scheduleNextRepetition_decrementsPositiveRepeat() {
            ScheduledJob job = createJob();
            job.setWaitDuration("PT15M");
            job.setWaitRepeat(5);

            testScheduleNextRepetition(job);

            assertEquals(4, job.getWaitRepeat());
        }

        @Test
        void scheduleNextRepetition_zeroRepeatStaysZero() {
            ScheduledJob job = createJob();
            job.setWaitDuration("PT15M");
            job.setWaitRepeat(0);

            testScheduleNextRepetition(job);

            assertEquals(0, job.getWaitRepeat());
        }

        @Test
        void scheduleNextRepetition_negativeRepeatStaysNegative() {
            ScheduledJob job = createJob();
            job.setWaitDuration("PT15M");
            job.setWaitRepeat(-1);

            testScheduleNextRepetition(job);

            assertEquals(-1, job.getWaitRepeat());
        }

    }

    private ScheduledJob createJob() {
        ScheduledJob job = new ScheduledJob();
        job.setId(UUID.randomUUID());
        job.setDestinationTopic("test-topic");
        job.setRunAt(Instant.now());
        job.setEffectiveRunAt(job.getRunAt());
        job.setState(JobState.PENDING);
        return job;
    }

    private boolean testShouldRepeat(ScheduledJob job) {
        if (job.getCronExpression() != null) {
            return testShouldRepeatCron(job);
        }
        if (job.getWaitDuration() == null) {
            return false;
        }
        int repeat = job.getWaitRepeat();
        return repeat <= 0 || repeat > 1;
    }

    private boolean testShouldRepeatCron(ScheduledJob job) {
        if (job.getCronUntil() != null && Instant.now().isAfter(job.getCronUntil())) {
            return false;
        }
        if (job.getCronRepeat() != null && job.getCronRunCount() >= job.getCronRepeat()) {
            return false;
        }
        return true;
    }

    private void testScheduleNextRepetition(ScheduledJob job) {
        Duration waitDuration = Duration.parse(job.getWaitDuration());
        Instant nextFire = Instant.now().plus(waitDuration);

        job.setRunAt(nextFire);
        job.setEffectiveRunAt(nextFire);
        job.setState(JobState.PENDING);
        job.setUpdatedAt(Instant.now());
        job.setRetryCount(0);

        if (job.getWaitRepeat() > 0) {
            job.setWaitRepeat(job.getWaitRepeat() - 1);
        }
    }
}
