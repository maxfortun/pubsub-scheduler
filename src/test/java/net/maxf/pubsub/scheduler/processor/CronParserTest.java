package net.maxf.pubsub.scheduler.processor;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.*;

class CronParserTest {

    @Test
    void calculateNextCronFireFrom_everyMinute_returnsWithinMinute() {
        Instant from = Instant.now();

        Instant next = IngestProcessor.calculateNextCronFireFrom("* * * * *", from);

        assertTrue(next.isAfter(from));
        assertTrue(next.isBefore(from.plusSeconds(61)));
    }

    @Test
    void calculateNextCronFireFrom_dailyAtMidnight_returnsNextMidnight() {
        Instant from = Instant.parse("2026-01-15T10:30:00Z");

        Instant next = IngestProcessor.calculateNextCronFireFrom("0 0 * * *", from);

        // Next midnight should be the next day at 00:00
        assertTrue(next.isAfter(from));
        // Within 24 hours
        assertTrue(next.isBefore(from.plus(24, ChronoUnit.HOURS)));
    }

    @Test
    void calculateNextCronFireFrom_hourly_returnsNextHour() {
        Instant from = Instant.parse("2026-01-15T10:30:00Z");

        Instant next = IngestProcessor.calculateNextCronFireFrom("0 * * * *", from);

        // Next top of hour should be within 60 minutes
        assertTrue(next.isAfter(from));
        assertTrue(next.isBefore(from.plusSeconds(3600)));
    }

    @Test
    void calculateNextCronFireFrom_specificTime_returnsCorrectTime() {
        // Every day at 14:30
        Instant from = Instant.parse("2026-01-15T10:00:00Z");

        Instant next = IngestProcessor.calculateNextCronFireFrom("30 14 * * *", from);

        // Should fire today at 14:30 (within 24 hours)
        assertTrue(next.isAfter(from));
        assertTrue(next.isBefore(from.plus(24, ChronoUnit.HOURS)));
    }

    @Test
    void calculateNextCronFireFrom_weekly_returnsWithinWeek() {
        // Every Monday at 09:00 (day 1 in UNIX cron)
        Instant from = Instant.now();

        Instant next = IngestProcessor.calculateNextCronFireFrom("0 9 * * 1", from);

        // Should be within a week
        assertTrue(next.isAfter(from));
        assertTrue(next.isBefore(from.plus(7, ChronoUnit.DAYS)));
    }

    @Test
    void calculateNextCronFireFrom_invalidCron_throwsException() {
        Instant from = Instant.now();

        assertThrows(Exception.class, () ->
            IngestProcessor.calculateNextCronFireFrom("invalid", from)
        );
    }
}
