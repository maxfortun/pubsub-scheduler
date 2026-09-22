package net.maxf.pubsub.scheduler.processor;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import jakarta.inject.Inject;
import net.maxf.pubsub.scheduler.model.KeyPolicy;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import net.maxf.pubsub.scheduler.model.WaitStart;
import net.maxf.pubsub.scheduler.service.JobStoreService;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.support.DefaultMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@QuarkusTest
class IngestProcessorTest {

    @Inject
    IngestProcessor processor;

    @InjectMock
    JobStoreService jobStore;

    private Exchange exchange;
    private Message message;
    private ArgumentCaptor<ScheduledJob> jobCaptor;

    @BeforeEach
    void setUp() {
        DefaultCamelContext context = new DefaultCamelContext();
        exchange = new DefaultExchange(context);
        message = new DefaultMessage(context);
        exchange.setIn(message);
        jobCaptor = ArgumentCaptor.forClass(ScheduledJob.class);
    }

    @Nested
    class DestinationHeader {

        @Test
        void missingDestination_throwsException() {
            assertThrows(IllegalArgumentException.class, () -> processor.process(exchange));
        }

        @Test
        void blankDestination_throwsException() {
            message.setHeader("SCHEDULER_DESTINATION", "   ");
            assertThrows(IllegalArgumentException.class, () -> processor.process(exchange));
        }

        @Test
        void emptyDestination_throwsException() {
            message.setHeader("SCHEDULER_DESTINATION", "");
            assertThrows(IllegalArgumentException.class, () -> processor.process(exchange));
        }

        @Test
        void validDestination_setsDestinationTopic() throws Exception {
            message.setHeader("SCHEDULER_DESTINATION", "output-topic");

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals("output-topic", jobCaptor.getValue().getDestinationTopic());
        }
    }

    @Nested
    class TimingHeaders {

        @BeforeEach
        void setRequiredHeaders() {
            message.setHeader("SCHEDULER_DESTINATION", "output-topic");
        }

        @Test
        void noTimingHeader_schedulesImmediately() throws Exception {
            Instant before = Instant.now();

            processor.process(exchange);

            Instant after = Instant.now();
            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            ScheduledJob job = jobCaptor.getValue();
            assertNotNull(job.getRunAt());
            assertTrue(job.getRunAt().compareTo(before) >= 0);
            assertTrue(job.getRunAt().compareTo(after) <= 0);
        }

        @Test
        void schedulerAt_parsesInstantAndSetsFireAt() throws Exception {
            Instant targetTime = Instant.now().plus(1, ChronoUnit.HOURS);
            message.setHeader("SCHEDULER_AT", targetTime.toString());

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals(targetTime, jobCaptor.getValue().getRunAt());
        }

        @Test
        void schedulerSleep_calculatesFireAtFromNow() throws Exception {
            message.setHeader("SCHEDULER_WAIT", "PT30M");
            Instant before = Instant.now().plus(30, ChronoUnit.MINUTES);

            processor.process(exchange);

            Instant after = Instant.now().plus(30, ChronoUnit.MINUTES);
            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            ScheduledJob job = jobCaptor.getValue();
            assertTrue(job.getRunAt().compareTo(before) >= 0);
            assertTrue(job.getRunAt().compareTo(after) <= 0);
            assertEquals("PT30M", job.getWaitDuration());
        }

        @Test
        void schedulerSleep_variousDurationFormats() throws Exception {
            message.setHeader("SCHEDULER_WAIT", "PT1H30M15S");

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals("PT1H30M15S", jobCaptor.getValue().getWaitDuration());
        }

        @Test
        void schedulerCron_setsFireAtToNextExecution() throws Exception {
            message.setHeader("SCHEDULER_CRON", "0 0 * * *"); // daily at midnight
            Instant before = Instant.now();

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            ScheduledJob job = jobCaptor.getValue();
            assertNotNull(job.getRunAt());
            assertTrue(job.getRunAt().isAfter(before));
            assertEquals("0 0 * * *", job.getCronExpression());
        }

        @Test
        void schedulerCron_everyMinute_schedulesWithinMinute() throws Exception {
            message.setHeader("SCHEDULER_CRON", "* * * * *"); // every minute
            Instant before = Instant.now();

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            ScheduledJob job = jobCaptor.getValue();
            // Should fire within the next 60 seconds
            assertTrue(job.getRunAt().isBefore(before.plusSeconds(61)));
        }

        @Test
        void multipleTimingHeaders_atAndSleep_throwsException() {
            message.setHeader("SCHEDULER_AT", Instant.now().toString());
            message.setHeader("SCHEDULER_WAIT", "PT1H");

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> processor.process(exchange));
            assertTrue(ex.getMessage().contains("mutually exclusive"));
        }

        @Test
        void multipleTimingHeaders_atAndCron_throwsException() {
            message.setHeader("SCHEDULER_AT", Instant.now().toString());
            message.setHeader("SCHEDULER_CRON", "0 0 * * *");

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> processor.process(exchange));
            assertTrue(ex.getMessage().contains("mutually exclusive"));
        }

        @Test
        void multipleTimingHeaders_sleepAndCron_throwsException() {
            message.setHeader("SCHEDULER_WAIT", "PT1H");
            message.setHeader("SCHEDULER_CRON", "0 0 * * *");

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> processor.process(exchange));
            assertTrue(ex.getMessage().contains("mutually exclusive"));
        }

        @Test
        void allThreeTimingHeaders_throwsException() {
            message.setHeader("SCHEDULER_AT", Instant.now().toString());
            message.setHeader("SCHEDULER_WAIT", "PT1H");
            message.setHeader("SCHEDULER_CRON", "0 0 * * *");

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> processor.process(exchange));
            assertTrue(ex.getMessage().contains("mutually exclusive"));
        }
    }

    @Nested
    class CronOptions {

        @BeforeEach
        void setRequiredHeaders() {
            message.setHeader("SCHEDULER_DESTINATION", "output-topic");
        }

        @Test
        void cronUntil_setsCronEndTime() throws Exception {
            message.setHeader("SCHEDULER_CRON", "0 0 * * *");
            Instant endTime = Instant.now().plus(30, ChronoUnit.DAYS);
            message.setHeader("SCHEDULER_CRON_UNTIL", endTime.toString());

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            ScheduledJob job = jobCaptor.getValue();
            assertEquals("0 0 * * *", job.getCronExpression());
            assertEquals(endTime, job.getCronUntil());
        }

        @Test
        void cronCount_setsCronMaxCount() throws Exception {
            message.setHeader("SCHEDULER_CRON", "0 0 * * *");
            message.setHeader("SCHEDULER_CRON_REPEAT", 5);

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            ScheduledJob job = jobCaptor.getValue();
            assertEquals("0 0 * * *", job.getCronExpression());
            assertEquals(Integer.valueOf(5), job.getCronRepeat());
        }

        @Test
        void cronUntilAndCronCount_bothPresent_throwsException() {
            message.setHeader("SCHEDULER_CRON", "0 0 * * *");
            message.setHeader("SCHEDULER_CRON_UNTIL", Instant.now().plus(30, ChronoUnit.DAYS).toString());
            message.setHeader("SCHEDULER_CRON_REPEAT", 5);

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> processor.process(exchange));
            assertTrue(ex.getMessage().contains("SCHEDULER_CRON_UNTIL and SCHEDULER_CRON_REPEAT are mutually exclusive"));
        }
    }

    @Nested
    class SleepOptions {

        @BeforeEach
        void setRequiredHeaders() {
            message.setHeader("SCHEDULER_DESTINATION", "output-topic");
            message.setHeader("SCHEDULER_WAIT", "PT1H");
        }

        @Test
        void waitStartSelf_setsWaitStartToSelf() throws Exception {
            message.setHeader("SCHEDULER_WAIT_START", "SELF");

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals(WaitStart.SELF, jobCaptor.getValue().getWaitStart());
        }

        @Test
        void waitStartPrev_setsWaitStartToPrev() throws Exception {
            message.setHeader("SCHEDULER_WAIT_START", "PREV");

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals(WaitStart.PREV, jobCaptor.getValue().getWaitStart());
        }

        @Test
        void waitStartLowercase_caseInsensitive() throws Exception {
            message.setHeader("SCHEDULER_WAIT_START", "prev");

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals(WaitStart.PREV, jobCaptor.getValue().getWaitStart());
        }

        @Test
        void waitStartMixedCase_caseInsensitive() throws Exception {
            message.setHeader("SCHEDULER_WAIT_START", "Self");

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals(WaitStart.SELF, jobCaptor.getValue().getWaitStart());
        }

        @Test
        void waitRepeat_setsSleepRepeatCount() throws Exception {
            message.setHeader("SCHEDULER_WAIT_REPEAT", 5);

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals(5, jobCaptor.getValue().getWaitRepeat());
        }

        @Test
        void waitRepeatWithWaitStart_bothSet() throws Exception {
            message.setHeader("SCHEDULER_WAIT_START", "PREV");
            message.setHeader("SCHEDULER_WAIT_REPEAT", 3);

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            ScheduledJob job = jobCaptor.getValue();
            assertEquals(WaitStart.PREV, job.getWaitStart());
            assertEquals(3, job.getWaitRepeat());
        }

        @Test
        void noWaitStart_defaultsToSelf() throws Exception {
            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals(WaitStart.SELF, jobCaptor.getValue().getWaitStart());
        }

        @Test
        void noSleepRepeat_defaultsToOne() throws Exception {
            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals(1, jobCaptor.getValue().getWaitRepeat());
        }
    }

    @Nested
    class KeyHeaders {

        @BeforeEach
        void setRequiredHeaders() {
            message.setHeader("SCHEDULER_DESTINATION", "output-topic");
        }

        @Test
        void schedulerKey_setsJobKey() throws Exception {
            message.setHeader("SCHEDULER_KEY", "order-123");

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals("order-123", jobCaptor.getValue().getJobKey());
        }

        @Test
        void noSchedulerKey_jobKeyIsNull() throws Exception {
            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertNull(jobCaptor.getValue().getJobKey());
        }

        @Test
        void keyPolicyQueue_setsKeyPolicyToQueue() throws Exception {
            message.setHeader("SCHEDULER_KEY", "order-123");
            message.setHeader("SCHEDULER_KEY_POLICY", "QUEUE");

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals(KeyPolicy.QUEUE, jobCaptor.getValue().getKeyPolicy());
        }

        @Test
        void keyPolicyReplace_setsKeyPolicyToReplace() throws Exception {
            message.setHeader("SCHEDULER_KEY", "order-123");
            message.setHeader("SCHEDULER_KEY_POLICY", "REPLACE");

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals(KeyPolicy.REPLACE, jobCaptor.getValue().getKeyPolicy());
        }

        @Test
        void keyPolicySkip_setsKeyPolicyToSkip() throws Exception {
            message.setHeader("SCHEDULER_KEY", "order-123");
            message.setHeader("SCHEDULER_KEY_POLICY", "SKIP");

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals(KeyPolicy.SKIP, jobCaptor.getValue().getKeyPolicy());
        }

        @Test
        void keyPolicyLowercase_caseInsensitive() throws Exception {
            message.setHeader("SCHEDULER_KEY", "order-123");
            message.setHeader("SCHEDULER_KEY_POLICY", "replace");

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals(KeyPolicy.REPLACE, jobCaptor.getValue().getKeyPolicy());
        }

        @Test
        void keyPolicyMixedCase_caseInsensitive() throws Exception {
            message.setHeader("SCHEDULER_KEY", "order-123");
            message.setHeader("SCHEDULER_KEY_POLICY", "Skip");

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals(KeyPolicy.SKIP, jobCaptor.getValue().getKeyPolicy());
        }

        @Test
        void noKeyPolicy_defaultsToQueue() throws Exception {
            message.setHeader("SCHEDULER_KEY", "order-123");

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals(KeyPolicy.QUEUE, jobCaptor.getValue().getKeyPolicy());
        }
    }

    @Nested
    class RetryHeaders {

        @BeforeEach
        void setRequiredHeaders() {
            message.setHeader("SCHEDULER_DESTINATION", "output-topic");
        }

        @Test
        void retryCount_setsMaxRetries() throws Exception {
            message.setHeader("SCHEDULER_RETRY_COUNT", 10);

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals(10, jobCaptor.getValue().getMaxRetries());
        }

        @Test
        void retryCountZero_allowsZeroRetries() throws Exception {
            message.setHeader("SCHEDULER_RETRY_COUNT", 0);

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals(0, jobCaptor.getValue().getMaxRetries());
        }

        @Test
        void noRetryCount_usesDefaultOfThree() throws Exception {
            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals(3, jobCaptor.getValue().getMaxRetries());
        }
    }

    @Nested
    class AdvisoryHeaders {

        @BeforeEach
        void setRequiredHeaders() {
            message.setHeader("SCHEDULER_DESTINATION", "output-topic");
        }

        @Test
        void advisoryHeadersPattern_setsPattern() throws Exception {
            message.setHeader("SCHEDULER_ADVISORY_HEADERS", "X-.*|Custom-.*");

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertEquals("X-.*|Custom-.*", jobCaptor.getValue().getAdvisoryHeadersPattern());
        }

        @Test
        void noAdvisoryHeaders_patternIsNull() throws Exception {
            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertNull(jobCaptor.getValue().getAdvisoryHeadersPattern());
        }
    }

    @Nested
    class HeaderPreservation {

        @BeforeEach
        void setRequiredHeaders() {
            message.setHeader("SCHEDULER_DESTINATION", "output-topic");
        }

        @Test
        void nonSchedulerHeaders_preserved() throws Exception {
            message.setHeader("X-Custom-Header", "custom-value");
            message.setHeader("Content-Type", "application/json");

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            var headers = jobCaptor.getValue().getHeaders();
            assertEquals("custom-value", headers.get("X-Custom-Header"));
            assertEquals("application/json", headers.get("Content-Type"));
        }

        @Test
        void schedulerHeaders_notPreserved() throws Exception {
            message.setHeader("SCHEDULER_AT", Instant.now().plus(1, ChronoUnit.HOURS).toString());
            message.setHeader("SCHEDULER_KEY", "my-key");
            message.setHeader("X-Custom-Header", "custom-value");

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            var headers = jobCaptor.getValue().getHeaders();
            assertFalse(headers.containsKey("SCHEDULER_AT"));
            assertFalse(headers.containsKey("SCHEDULER_KEY"));
            assertFalse(headers.containsKey("SCHEDULER_DESTINATION"));
            assertTrue(headers.containsKey("X-Custom-Header"));
        }

        @Test
        void nonStringHeaders_notPreserved() throws Exception {
            message.setHeader("Integer-Header", 42);
            message.setHeader("Boolean-Header", true);
            message.setHeader("String-Header", "value");

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            var headers = jobCaptor.getValue().getHeaders();
            assertFalse(headers.containsKey("Integer-Header"));
            assertFalse(headers.containsKey("Boolean-Header"));
            assertTrue(headers.containsKey("String-Header"));
        }
    }

    @Nested
    class MessagePreservation {

        @BeforeEach
        void setRequiredHeaders() {
            message.setHeader("SCHEDULER_DESTINATION", "output-topic");
        }

        @Test
        void messageKey_preserved() throws Exception {
            byte[] key = "message-key".getBytes();
            message.setHeader("kafka.KEY", key);

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertArrayEquals(key, jobCaptor.getValue().getMessageKey());
        }

        @Test
        void messageValue_preserved() throws Exception {
            byte[] body = "{\"data\": \"test\"}".getBytes();
            message.setBody(body);

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertArrayEquals(body, jobCaptor.getValue().getMessageValue());
        }

        @Test
        void nullMessageKey_handledGracefully() throws Exception {
            message.setBody("test".getBytes());

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertNull(jobCaptor.getValue().getMessageKey());
        }

        @Test
        void nullMessageValue_handledGracefully() throws Exception {
            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            assertNull(jobCaptor.getValue().getMessageValue());
        }
    }

    @Nested
    class EffectiveFireAt {

        @BeforeEach
        void setRequiredHeaders() {
            message.setHeader("SCHEDULER_DESTINATION", "output-topic");
        }

        @Test
        void effectiveRunAt_sameAsFireAt() throws Exception {
            Instant targetTime = Instant.now().plus(1, ChronoUnit.HOURS);
            message.setHeader("SCHEDULER_AT", targetTime.toString());

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            ScheduledJob job = jobCaptor.getValue();
            assertEquals(job.getRunAt(), job.getEffectiveRunAt());
        }
    }

    @Nested
    class InvalidInputs {

        @BeforeEach
        void setRequiredHeaders() {
            message.setHeader("SCHEDULER_DESTINATION", "output-topic");
        }

        @Test
        void invalidAtFormat_throwsException() {
            message.setHeader("SCHEDULER_AT", "invalid-timestamp");

            assertThrows(Exception.class, () -> processor.process(exchange));
        }

        @Test
        void invalidSleepFormat_throwsException() {
            message.setHeader("SCHEDULER_WAIT", "invalid-duration");

            assertThrows(Exception.class, () -> processor.process(exchange));
        }

        @Test
        void invalidWaitStart_throwsException() {
            message.setHeader("SCHEDULER_WAIT", "PT1H");
            message.setHeader("SCHEDULER_WAIT_START", "INVALID");

            assertThrows(IllegalArgumentException.class, () -> processor.process(exchange));
        }

        @Test
        void invalidKeyPolicy_throwsException() {
            message.setHeader("SCHEDULER_KEY_POLICY", "INVALID");

            assertThrows(IllegalArgumentException.class, () -> processor.process(exchange));
        }
    }

    @Nested
    class CompleteScenarios {

        @BeforeEach
        void setRequiredHeaders() {
            message.setHeader("SCHEDULER_DESTINATION", "output-topic");
        }

        @Test
        void fullScheduledJob_allHeadersSet() throws Exception {
            Instant targetTime = Instant.now().plus(2, ChronoUnit.HOURS);
            byte[] key = "msg-key".getBytes();
            byte[] body = "{\"order\": 123}".getBytes();

            message.setHeader("SCHEDULER_AT", targetTime.toString());
            message.setHeader("SCHEDULER_KEY", "order-123");
            message.setHeader("SCHEDULER_KEY_POLICY", "REPLACE");
            message.setHeader("SCHEDULER_RETRY_COUNT", 5);
            message.setHeader("SCHEDULER_ADVISORY_HEADERS", "X-.*");
            message.setHeader("kafka.KEY", key);
            message.setHeader("X-Correlation-Id", "corr-456");
            message.setBody(body);

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            ScheduledJob job = jobCaptor.getValue();

            assertEquals("output-topic", job.getDestinationTopic());
            assertEquals(targetTime, job.getRunAt());
            assertEquals("order-123", job.getJobKey());
            assertEquals(KeyPolicy.REPLACE, job.getKeyPolicy());
            assertEquals(5, job.getMaxRetries());
            assertEquals("X-.*", job.getAdvisoryHeadersPattern());
            assertArrayEquals(key, job.getMessageKey());
            assertArrayEquals(body, job.getMessageValue());
            assertEquals("corr-456", job.getHeaders().get("X-Correlation-Id"));
            assertFalse(job.getHeaders().containsKey("SCHEDULER_AT"));
        }

        @Test
        void sleepWithRepeat_allSleepOptionsSet() throws Exception {
            message.setHeader("SCHEDULER_WAIT", "PT15M");
            message.setHeader("SCHEDULER_WAIT_START", "PREV");
            message.setHeader("SCHEDULER_WAIT_REPEAT", 10);
            message.setHeader("SCHEDULER_KEY", "batch-job");
            message.setHeader("SCHEDULER_KEY_POLICY", "QUEUE");

            processor.process(exchange);

            verify(jobStore).handleIncomingJob(jobCaptor.capture());
            ScheduledJob job = jobCaptor.getValue();

            assertEquals("PT15M", job.getWaitDuration());
            assertEquals(WaitStart.PREV, job.getWaitStart());
            assertEquals(10, job.getWaitRepeat());
            assertEquals("batch-job", job.getJobKey());
            assertEquals(KeyPolicy.QUEUE, job.getKeyPolicy());
        }
    }
}
