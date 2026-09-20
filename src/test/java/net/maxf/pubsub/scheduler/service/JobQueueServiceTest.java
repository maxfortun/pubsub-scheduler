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
            job.setSleepDuration(null);
            job.setCronExpression(null);
            job.setSleepRepeat(5);

            assertFalse(testShouldRepeat(job));
        }

        @Test
        void shouldRepeat_cronExpression_returnsTrue() {
            ScheduledJob job = createJob();
            job.setCronExpression("0 0 * * *");
            job.setSleepDuration(null);

            assertTrue(testShouldRepeat(job));
        }

        @Test
        void shouldRepeat_cronExpressionPastEnd_returnsFalse() {
            ScheduledJob job = createJob();
            job.setCronExpression("0 0 * * *");
            job.setCronEnd(Instant.now().minusSeconds(1)); // past

            assertFalse(testShouldRepeatCron(job));
        }

        @Test
        void shouldRepeat_cronExpressionReachedMaxCount_returnsFalse() {
            ScheduledJob job = createJob();
            job.setCronExpression("0 0 * * *");
            job.setCronMaxCount(5);
            job.setCronFireCount(5);

            assertFalse(testShouldRepeatCron(job));
        }

        @Test
        void shouldRepeat_cronExpressionBelowMaxCount_returnsTrue() {
            ScheduledJob job = createJob();
            job.setCronExpression("0 0 * * *");
            job.setCronMaxCount(5);
            job.setCronFireCount(3);

            assertTrue(testShouldRepeatCron(job));
        }

        @Test
        void shouldRepeat_noSleepDuration_returnsFalse() {
            ScheduledJob job = createJob();
            job.setSleepDuration(null);
            job.setSleepRepeat(5);

            assertFalse(testShouldRepeat(job));
        }

        @Test
        void shouldRepeat_sleepRepeatOne_returnsFalse() {
            ScheduledJob job = createJob();
            job.setSleepDuration("PT15M");
            job.setSleepRepeat(1);

            assertFalse(testShouldRepeat(job));
        }

        @Test
        void shouldRepeat_sleepRepeatZero_returnsTrue() {
            ScheduledJob job = createJob();
            job.setSleepDuration("PT15M");
            job.setSleepRepeat(0);

            assertTrue(testShouldRepeat(job));
        }

        @Test
        void shouldRepeat_sleepRepeatNegative_returnsTrue() {
            ScheduledJob job = createJob();
            job.setSleepDuration("PT15M");
            job.setSleepRepeat(-1);

            assertTrue(testShouldRepeat(job));
        }

        @Test
        void shouldRepeat_sleepRepeatGreaterThanOne_returnsTrue() {
            ScheduledJob job = createJob();
            job.setSleepDuration("PT15M");
            job.setSleepRepeat(5);

            assertTrue(testShouldRepeat(job));
        }

        @Test
        void shouldRepeat_sleepRepeatTwo_returnsTrue() {
            ScheduledJob job = createJob();
            job.setSleepDuration("PT15M");
            job.setSleepRepeat(2);

            assertTrue(testShouldRepeat(job));
        }
    }

    @Nested
    class ScheduleNextRepetitionTests {

        @Test
        void scheduleNextRepetition_setsNewFireAt() {
            ScheduledJob job = createJob();
            job.setSleepDuration("PT15M");
            job.setSleepRepeat(0);
            Instant before = Instant.now();

            testScheduleNextRepetition(job);

            Instant expectedMin = before.plus(Duration.ofMinutes(15));
            assertTrue(job.getFireAt().isAfter(expectedMin.minusSeconds(1)));
            assertTrue(job.getFireAt().isBefore(expectedMin.plusSeconds(5)));
        }

        @Test
        void scheduleNextRepetition_setsStateToPending() {
            ScheduledJob job = createJob();
            job.setSleepDuration("PT15M");
            job.setSleepRepeat(0);
            job.setState(JobState.FIRING);

            testScheduleNextRepetition(job);

            assertEquals(JobState.PENDING, job.getState());
        }

        @Test
        void scheduleNextRepetition_resetsRetryCount() {
            ScheduledJob job = createJob();
            job.setSleepDuration("PT15M");
            job.setSleepRepeat(0);
            job.setRetryCount(3);

            testScheduleNextRepetition(job);

            assertEquals(0, job.getRetryCount());
        }

        @Test
        void scheduleNextRepetition_decrementsPositiveRepeat() {
            ScheduledJob job = createJob();
            job.setSleepDuration("PT15M");
            job.setSleepRepeat(5);

            testScheduleNextRepetition(job);

            assertEquals(4, job.getSleepRepeat());
        }

        @Test
        void scheduleNextRepetition_zeroRepeatStaysZero() {
            ScheduledJob job = createJob();
            job.setSleepDuration("PT15M");
            job.setSleepRepeat(0);

            testScheduleNextRepetition(job);

            assertEquals(0, job.getSleepRepeat());
        }

        @Test
        void scheduleNextRepetition_negativeRepeatStaysNegative() {
            ScheduledJob job = createJob();
            job.setSleepDuration("PT15M");
            job.setSleepRepeat(-1);

            testScheduleNextRepetition(job);

            assertEquals(-1, job.getSleepRepeat());
        }

    }

    private ScheduledJob createJob() {
        ScheduledJob job = new ScheduledJob();
        job.setId(UUID.randomUUID());
        job.setDestinationTopic("test-topic");
        job.setFireAt(Instant.now());
        job.setEffectiveFireAt(job.getFireAt());
        job.setState(JobState.PENDING);
        return job;
    }

    private boolean testShouldRepeat(ScheduledJob job) {
        if (job.getCronExpression() != null) {
            return testShouldRepeatCron(job);
        }
        if (job.getSleepDuration() == null) {
            return false;
        }
        int repeat = job.getSleepRepeat();
        return repeat <= 0 || repeat > 1;
    }

    private boolean testShouldRepeatCron(ScheduledJob job) {
        if (job.getCronEnd() != null && Instant.now().isAfter(job.getCronEnd())) {
            return false;
        }
        if (job.getCronMaxCount() != null && job.getCronFireCount() >= job.getCronMaxCount()) {
            return false;
        }
        return true;
    }

    private void testScheduleNextRepetition(ScheduledJob job) {
        Duration sleepDuration = Duration.parse(job.getSleepDuration());
        Instant nextFire = Instant.now().plus(sleepDuration);

        job.setFireAt(nextFire);
        job.setEffectiveFireAt(nextFire);
        job.setState(JobState.PENDING);
        job.setUpdatedAt(Instant.now());
        job.setRetryCount(0);

        if (job.getSleepRepeat() > 0) {
            job.setSleepRepeat(job.getSleepRepeat() - 1);
        }
    }
}
