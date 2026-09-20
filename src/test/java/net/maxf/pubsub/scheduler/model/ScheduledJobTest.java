package net.maxf.pubsub.scheduler.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ScheduledJobTest {

    @Test
    void constructor_setsDefaults() {
        ScheduledJob job = new ScheduledJob();

        assertNotNull(job.getId());
        assertEquals(KeyPolicy.QUEUE, job.getKeyPolicy());
        assertEquals(SleepStart.SELF, job.getSleepStart());
        assertEquals(1, job.getSleepRepeat());
        assertEquals(0, job.getCronFireCount());
        assertEquals(JobState.PENDING, job.getState());
        assertEquals(0, job.getMaxRetries()); // Default is 0; IngestProcessor sets from config
        assertEquals(0, job.getRetryCount());
        assertEquals(0, job.getVersion());
        assertEquals(0, job.getSequenceNum());
        assertNotNull(job.getCreatedAt());
        assertNotNull(job.getArrivedAt());
    }

    @Test
    void getDelay_futureFireTime_returnsPositive() {
        ScheduledJob job = new ScheduledJob();
        job.setFireAt(Instant.now().plus(1, ChronoUnit.HOURS));

        long delay = job.getDelay(TimeUnit.SECONDS);

        assertTrue(delay > 3500); // ~1 hour in seconds, with some margin
        assertTrue(delay < 3700);
    }

    @Test
    void getDelay_pastFireTime_returnsNegative() {
        ScheduledJob job = new ScheduledJob();
        job.setFireAt(Instant.now().minus(1, ChronoUnit.HOURS));

        long delay = job.getDelay(TimeUnit.SECONDS);

        assertTrue(delay < 0);
    }

    @Test
    void getDelay_effectiveFireAtTakesPrecedence() {
        ScheduledJob job = new ScheduledJob();
        job.setFireAt(Instant.now().plus(1, ChronoUnit.HOURS));
        job.setEffectiveFireAt(Instant.now().plus(2, ChronoUnit.HOURS));

        long delay = job.getDelay(TimeUnit.SECONDS);

        assertTrue(delay > 7000); // ~2 hours
    }

    @Test
    void getDelay_nullFireTimes_fallsBackToNow() {
        ScheduledJob job = new ScheduledJob();
        // fireAt and effectiveFireAt are both null

        long delay = job.getDelay(TimeUnit.MILLISECONDS);

        assertTrue(Math.abs(delay) < 1000); // Should be close to 0
    }

    @Test
    void compareTo_earlierJob_returnsNegative() {
        ScheduledJob earlier = new ScheduledJob();
        earlier.setFireAt(Instant.now().plus(1, ChronoUnit.HOURS));

        ScheduledJob later = new ScheduledJob();
        later.setFireAt(Instant.now().plus(2, ChronoUnit.HOURS));

        assertTrue(earlier.compareTo(later) < 0);
    }

    @Test
    void compareTo_laterJob_returnsPositive() {
        ScheduledJob earlier = new ScheduledJob();
        earlier.setFireAt(Instant.now().plus(1, ChronoUnit.HOURS));

        ScheduledJob later = new ScheduledJob();
        later.setFireAt(Instant.now().plus(2, ChronoUnit.HOURS));

        assertTrue(later.compareTo(earlier) > 0);
    }

    @Test
    void compareTo_sameTime_returnsZero() {
        Instant fireTime = Instant.now().plus(1, ChronoUnit.HOURS);

        ScheduledJob job1 = new ScheduledJob();
        job1.setFireAt(fireTime);

        ScheduledJob job2 = new ScheduledJob();
        job2.setFireAt(fireTime);

        assertEquals(0, job1.compareTo(job2));
    }

    @Test
    void compareTo_usesEffectiveFireAt() {
        ScheduledJob job1 = new ScheduledJob();
        job1.setFireAt(Instant.now().plus(2, ChronoUnit.HOURS));
        job1.setEffectiveFireAt(Instant.now().plus(1, ChronoUnit.HOURS)); // Earlier effective

        ScheduledJob job2 = new ScheduledJob();
        job2.setFireAt(Instant.now().plus(1, ChronoUnit.HOURS));
        job2.setEffectiveFireAt(Instant.now().plus(2, ChronoUnit.HOURS)); // Later effective

        assertTrue(job1.compareTo(job2) < 0); // job1 should be first (earlier effective)
    }

    @Test
    void setters_updateFields() {
        ScheduledJob job = new ScheduledJob();

        job.setJobKey("test-key");
        job.setKeyPolicy(KeyPolicy.REPLACE);
        job.setSleepStart(SleepStart.PREV);
        job.setSleepDuration("PT1H");
        job.setSleepRepeat(5);
        job.setCronExpression("0 0 * * *");
        job.setDestinationTopic("output-topic");
        job.setState(JobState.ACQUIRED);
        job.setMaxRetries(10);

        assertEquals("test-key", job.getJobKey());
        assertEquals(KeyPolicy.REPLACE, job.getKeyPolicy());
        assertEquals(SleepStart.PREV, job.getSleepStart());
        assertEquals("PT1H", job.getSleepDuration());
        assertEquals(5, job.getSleepRepeat());
        assertEquals("0 0 * * *", job.getCronExpression());
        assertEquals("output-topic", job.getDestinationTopic());
        assertEquals(JobState.ACQUIRED, job.getState());
        assertEquals(10, job.getMaxRetries());
    }

    @Test
    void isRepeating_defaultSleepRepeatOne_returnsFalse() {
        ScheduledJob job = new ScheduledJob();

        assertFalse(job.isRepeating());
    }

    @Test
    void isRepeating_sleepRepeatZero_returnsTrue() {
        ScheduledJob job = new ScheduledJob();
        job.setSleepRepeat(0);

        assertTrue(job.isRepeating());
    }

    @Test
    void isRepeating_sleepRepeatNegative_returnsTrue() {
        ScheduledJob job = new ScheduledJob();
        job.setSleepRepeat(-1);

        assertTrue(job.isRepeating());
    }

    @Test
    void isRepeating_sleepRepeatGreaterThanOne_returnsTrue() {
        ScheduledJob job = new ScheduledJob();
        job.setSleepRepeat(5);

        assertTrue(job.isRepeating());
    }

    @Test
    void isRepeating_sleepRepeatTwo_returnsTrue() {
        ScheduledJob job = new ScheduledJob();
        job.setSleepRepeat(2);

        assertTrue(job.isRepeating());
    }

    @Test
    void isRepeating_withCronExpression_returnsTrue() {
        ScheduledJob job = new ScheduledJob();
        job.setCronExpression("0 0 * * *");

        assertTrue(job.isRepeating());
    }

    @Test
    void isCron_withoutCronExpression_returnsFalse() {
        ScheduledJob job = new ScheduledJob();

        assertFalse(job.isCron());
    }

    @Test
    void isCron_withCronExpression_returnsTrue() {
        ScheduledJob job = new ScheduledJob();
        job.setCronExpression("0 0 * * *");

        assertTrue(job.isCron());
    }
}
