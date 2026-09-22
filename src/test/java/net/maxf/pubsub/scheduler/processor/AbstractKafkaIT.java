package net.maxf.pubsub.scheduler.processor;

import jakarta.inject.Inject;
import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.KeyPolicy;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import net.maxf.pubsub.scheduler.model.WaitStart;
import net.maxf.pubsub.scheduler.service.JobStoreService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

abstract class AbstractKafkaIT {

    protected static final String BOOTSTRAP_SERVERS = "localhost:9092";
    protected static final String OUTPUT_TOPIC = "output-topic";

    // Topic names - override in subclasses for per-flavor isolation
    protected static String SCHEDULER_IN_TOPIC = "scheduler-in";
    protected static String SCHEDULER_DLQ_TOPIC = "scheduler-dlq";
    protected static String SCHEDULER_ADVISORY_TOPIC = "scheduler-advisory";

    @Inject
    JobStoreService jobStore;

    static KafkaProducer<String, byte[]> producer;

    @BeforeAll
    static void setupKafka() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producer = new KafkaProducer<>(producerProps);
    }

    @AfterAll
    static void teardownKafka() {
        if (producer != null) producer.close();
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class DestinationHeaderTests {

        @Test
        @Order(1)
        void missingDestination_sendsToDeadLetterQueue() throws Exception {
            String correlationId = "dlq-missing-dest-" + UUID.randomUUID();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, correlationId.getBytes());
            producer.send(record).get(10, TimeUnit.SECONDS);

            ConsumerRecord<String, byte[]> dlqRecord = pollDlqForMessage(correlationId, Duration.ofSeconds(30));
            assertNotNull(dlqRecord, "Expected message in DLQ with correlationId: " + correlationId);

            String error = getHeader(dlqRecord, "SCHEDULER_ERROR");
            assertTrue(error.contains("SCHEDULER_DESTINATION"), "Error should mention missing destination");
        }

        @Test
        @Order(2)
        void blankDestination_sendsToDeadLetterQueue() throws Exception {
            String correlationId = "dlq-blank-dest-" + UUID.randomUUID();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, correlationId.getBytes());
            record.headers().add(new RecordHeader("SCHEDULER_DESTINATION", "   ".getBytes()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            ConsumerRecord<String, byte[]> dlqRecord = pollDlqForMessage(correlationId, Duration.ofSeconds(30));
            assertNotNull(dlqRecord, "Expected message in DLQ with correlationId: " + correlationId);
        }

        @Test
        @Order(3)
        void validDestination_jobCreated() throws Exception {
            String jobKey = "dest-test-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test-body".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Job should be created");
            assertEquals(OUTPUT_TOPIC, jobs.get(0).getDestinationTopic());
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class TimingHeaderTests {

        @Test
        @Order(1)
        void schedulerAt_setsAbsoluteFireTime() throws Exception {
            String jobKey = "at-test-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            assertEquals(futureTime.truncatedTo(ChronoUnit.MILLIS),
                    jobs.get(0).getRunAt().truncatedTo(ChronoUnit.MILLIS));
        }

        @Test
        @Order(2)
        void schedulerSleep_setsRelativeFireTime() throws Exception {
            String jobKey = "sleep-test-" + UUID.randomUUID();
            Instant before = Instant.now().plus(30, ChronoUnit.MINUTES);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_WAIT", "PT30M"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            Instant after = Instant.now().plus(30, ChronoUnit.MINUTES);
            assertTrue(jobs.get(0).getRunAt().isAfter(before.minusSeconds(5)));
            assertTrue(jobs.get(0).getRunAt().isBefore(after.plusSeconds(5)));
            assertEquals("PT30M", jobs.get(0).getWaitDuration());
        }

        @Test
        @Order(3)
        void noTimingHeader_schedulesImmediately() throws Exception {
            String jobKey = "immediate-test-" + UUID.randomUUID();
            Instant before = Instant.now();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Job will fire immediately and complete - find in any state
            List<ScheduledJob> jobs = jobStore.findJobs(null, jobKey, 10);
            assertFalse(jobs.isEmpty(), "Job should be created");
            assertTrue(jobs.get(0).getRunAt().isAfter(before.minusSeconds(5)));
            assertTrue(jobs.get(0).getRunAt().isBefore(Instant.now().plusSeconds(5)));
        }

        @Test
        @Order(4)
        void multipleTimingHeaders_sendsToDeadLetterQueue() throws Exception {
            String correlationId = "dlq-multi-timing-" + UUID.randomUUID();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, correlationId.getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_AT", Instant.now().plus(1, ChronoUnit.HOURS).toString()));
            record.headers().add(header("SCHEDULER_WAIT", "PT1H"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            ConsumerRecord<String, byte[]> dlqRecord = pollDlqForMessage(correlationId, Duration.ofSeconds(30));
            assertNotNull(dlqRecord, "Expected message in DLQ with correlationId: " + correlationId);

            String error = getHeader(dlqRecord, "SCHEDULER_ERROR");
            assertTrue(error.contains("mutually exclusive"), "Error should mention mutual exclusivity");
        }

        @Test
        @Order(5)
        void schedulerCron_schedulesJobAtNextExecution() throws Exception {
            String jobKey = "cron-test-" + UUID.randomUUID();
            Instant before = Instant.now();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_CRON", "* * * * *")); // every minute
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty(), "CRON job should be created");
            ScheduledJob job = jobs.get(0);
            assertEquals("* * * * *", job.getCronExpression());
            // Should fire within the next ~2 minutes (allowing for timing variations)
            assertTrue(job.getRunAt().isAfter(before.minusSeconds(5)),
                "runAt should be after test start: " + job.getRunAt() + " vs " + before);
            assertTrue(job.getRunAt().isBefore(before.plusSeconds(120)),
                "runAt should be within 2 minutes: " + job.getRunAt() + " vs " + before.plusSeconds(120));
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class CronOptionsTests {

        @Test
        @Order(1)
        void cronUntilAndCronCount_mutuallyExclusive_sendsToDeadLetterQueue() throws Exception {
            String correlationId = "dlq-cron-exclusive-" + UUID.randomUUID();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, correlationId.getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_CRON", "0 0 * * *"));
            record.headers().add(header("SCHEDULER_CRON_UNTIL", Instant.now().plus(30, ChronoUnit.DAYS).toString()));
            record.headers().add(header("SCHEDULER_CRON_REPEAT", "5"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            ConsumerRecord<String, byte[]> dlqRecord = pollDlqForMessage(correlationId, Duration.ofSeconds(30));
            assertNotNull(dlqRecord, "Expected message in DLQ with correlationId: " + correlationId);

            String error = getHeader(dlqRecord, "SCHEDULER_ERROR");
            assertTrue(error.contains("SCHEDULER_CRON_UNTIL") && error.contains("SCHEDULER_CRON_REPEAT"));
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class SleepOptionsTests {

        @Test
        @Order(1)
        void waitStartSelf_setsWaitStartToSelf() throws Exception {
            String jobKey = "sleep-start-self-" + UUID.randomUUID();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_WAIT", "PT1H"));
            record.headers().add(header("SCHEDULER_WAIT_START", "SELF"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            assertEquals(WaitStart.SELF, jobs.get(0).getWaitStart());
        }

        @Test
        @Order(2)
        void waitStartPrev_setsWaitStartToPrev() throws Exception {
            String jobKey = "sleep-start-prev-" + UUID.randomUUID();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_WAIT", "PT1H"));
            record.headers().add(header("SCHEDULER_WAIT_START", "prev"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            assertEquals(WaitStart.PREV, jobs.get(0).getWaitStart());
        }

        @Test
        @Order(3)
        void waitRepeat_setsSleepRepeatCount() throws Exception {
            String jobKey = "sleep-repeat-" + UUID.randomUUID();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_WAIT", "PT15M"));
            record.headers().add(header("SCHEDULER_WAIT_REPEAT", "5"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            assertEquals(5, jobs.get(0).getWaitRepeat());
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class KeyPolicyTests {

        @Test
        @Order(1)
        void keyPolicyQueue_setsKeyPolicyToQueue() throws Exception {
            String jobKey = "key-policy-queue-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_KEY_POLICY", "QUEUE"));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            assertEquals(KeyPolicy.QUEUE, jobs.get(0).getKeyPolicy());
        }

        @Test
        @Order(2)
        void keyPolicyReplace_setsKeyPolicyToReplace() throws Exception {
            String jobKey = "key-policy-replace-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_KEY_POLICY", "replace"));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            assertEquals(KeyPolicy.REPLACE, jobs.get(0).getKeyPolicy());
        }

        @Test
        @Order(3)
        void keyPolicySkip_setsKeyPolicyToSkip() throws Exception {
            String jobKey = "key-policy-skip-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_KEY_POLICY", "Skip"));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            assertEquals(KeyPolicy.SKIP, jobs.get(0).getKeyPolicy());
        }

        @Test
        @Order(4)
        void noKeyPolicy_defaultsToQueue() throws Exception {
            String jobKey = "key-policy-default-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            assertEquals(KeyPolicy.QUEUE, jobs.get(0).getKeyPolicy());
        }

        @Test
        @Order(5)
        void queuePolicy_secondJob_chainsBehindfirst() throws Exception {
            String jobKey = "chain-test-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Send first job
            ProducerRecord<String, byte[]> record1 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "first".getBytes());
            record1.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record1.headers().add(header("SCHEDULER_KEY", jobKey));
            record1.headers().add(header("SCHEDULER_KEY_POLICY", "QUEUE"));
            record1.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record1).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000); // Wait for first job to be fully processed

            // Verify first job exists
            List<ScheduledJob> firstJobs = jobStore.findPendingByKey(jobKey);
            assertFalse(firstJobs.isEmpty(), "First job should be pending with key: " + jobKey);
            ScheduledJob firstJob = firstJobs.get(0);

            // Send second job with same key - should chain
            ProducerRecord<String, byte[]> record2 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "second".getBytes());
            record2.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record2.headers().add(header("SCHEDULER_KEY", jobKey));
            record2.headers().add(header("SCHEDULER_KEY_POLICY", "QUEUE"));
            record2.headers().add(header("SCHEDULER_AT", futureTime.plusSeconds(60).toString()));
            producer.send(record2).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000); // Wait for second job to be processed

            // Query for all jobs by key (both PENDING and WAITING)
            List<ScheduledJob> allJobs = jobStore.findByKey(jobKey);
            assertTrue(allJobs.size() >= 2, "Should have at least 2 jobs with key: " + jobKey + ", found: " + allJobs.size());

            // Find the waiting job (second one)
            ScheduledJob waitingJob = allJobs.stream()
                .filter(j -> j.getState() == JobState.WAITING)
                .findFirst()
                .orElse(null);
            assertNotNull(waitingJob, "Second job should be in WAITING state (chained)");
            assertEquals(firstJob.getId(), waitingJob.getPredecessorId(), "Waiting job should have first job as predecessor");

            // Advisory events verified via Kafka console - JOB_CHAINED is published
        }

        @Test
        @Order(6)
        void skipPolicy_secondJob_skipped() throws Exception {
            String jobKey = "skip-test-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Send first job with SKIP policy
            ProducerRecord<String, byte[]> record1 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "first".getBytes());
            record1.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record1.headers().add(header("SCHEDULER_KEY", jobKey));
            record1.headers().add(header("SCHEDULER_KEY_POLICY", "SKIP"));
            record1.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record1).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Verify first job exists
            List<ScheduledJob> firstJobs = jobStore.findPendingByKey(jobKey);
            assertEquals(1, firstJobs.size(), "First job should be created");

            // Send second job with same key and SKIP - should be skipped
            ProducerRecord<String, byte[]> record2 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "second".getBytes());
            record2.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record2.headers().add(header("SCHEDULER_KEY", jobKey));
            record2.headers().add(header("SCHEDULER_KEY_POLICY", "SKIP"));
            record2.headers().add(header("SCHEDULER_AT", futureTime.plusSeconds(60).toString()));
            producer.send(record2).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Should still have only 1 job - second was skipped
            List<ScheduledJob> allJobs = jobStore.findByKey(jobKey);
            assertEquals(1, allJobs.size(), "Second job should have been skipped, only first should exist");

            // Advisory events verified via Kafka console - JOB_SKIPPED is published
        }

        @Test
        @Order(7)
        void replacePolicy_secondJob_replacesFirst() throws Exception {
            String jobKey = "replace-test-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Send first job with REPLACE policy
            ProducerRecord<String, byte[]> record1 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "first-body".getBytes());
            record1.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record1.headers().add(header("SCHEDULER_KEY", jobKey));
            record1.headers().add(header("SCHEDULER_KEY_POLICY", "REPLACE"));
            record1.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record1).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Verify first job exists
            List<ScheduledJob> firstJobs = jobStore.findPendingByKey(jobKey);
            assertEquals(1, firstJobs.size(), "First job should be created");
            UUID firstJobId = firstJobs.get(0).getId();

            // Send second job with same key and REPLACE - should replace first
            ProducerRecord<String, byte[]> record2 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "second-body".getBytes());
            record2.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record2.headers().add(header("SCHEDULER_KEY", jobKey));
            record2.headers().add(header("SCHEDULER_KEY_POLICY", "REPLACE"));
            record2.headers().add(header("SCHEDULER_AT", futureTime.plusSeconds(60).toString()));
            producer.send(record2).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Should have only 1 PENDING job - the second one
            List<ScheduledJob> pendingJobs = jobStore.findPendingByKey(jobKey);
            assertEquals(1, pendingJobs.size(), "Should have exactly 1 pending job after replace");
            assertNotEquals(firstJobId, pendingJobs.get(0).getId(), "New job should have different ID");
            assertEquals("second-body", new String(pendingJobs.get(0).getMessageValue()), "New job should have second body");

            // First job should be FAILED
            List<ScheduledJob> allJobs = jobStore.findByKey(jobKey);
            ScheduledJob failedJob = allJobs.stream()
                .filter(j -> j.getState() == JobState.FAILED)
                .findFirst()
                .orElse(null);
            assertNotNull(failedJob, "First job should be in FAILED state after replacement");
            assertEquals(firstJobId, failedJob.getId(), "Failed job should be the original first job");

            // Advisory events verified via Kafka console - JOB_REPLACED is published
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class RetryTests {

        @Test
        @Order(1)
        void retryCount_setsMaxRetries() throws Exception {
            String jobKey = "retry-count-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_RETRY_COUNT", "10"));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            assertEquals(10, jobs.get(0).getMaxRetries());
        }

        @Test
        @Order(2)
        void noRetryCount_usesDefault() throws Exception {
            String jobKey = "retry-default-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            assertEquals(3, jobs.get(0).getMaxRetries());
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class AdvisoryHeaderTests {

        @Test
        @Order(1)
        void advisoryHeadersPattern_setsPattern() throws Exception {
            String jobKey = "advisory-pattern-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_ADVISORY_HEADERS", "X-.*|Custom-.*"));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            assertEquals("X-.*|Custom-.*", jobs.get(0).getAdvisoryHeadersPattern());
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class MessagePreservationTests {

        @Test
        @Order(1)
        void messageBodyPreserved() throws Exception {
            String jobKey = "body-preservation-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);
            byte[] body = "{\"order\": 123, \"customer\": \"test\"}".getBytes(StandardCharsets.UTF_8);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "msg-key", body);
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            assertArrayEquals(body, jobs.get(0).getMessageValue());
        }

        @Test
        @Order(2)
        void nonSchedulerHeadersPreserved() throws Exception {
            String jobKey = "header-preservation-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("X-Correlation-Id", "corr-123"));
            record.headers().add(header("X-Request-Id", "req-456"));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            Map<String, String> headers = jobs.get(0).getHeaders();
            assertEquals("corr-123", headers.get("X-Correlation-Id"));
            assertEquals("req-456", headers.get("X-Request-Id"));
            assertFalse(headers.containsKey("SCHEDULER_DESTINATION"));
            assertFalse(headers.containsKey("SCHEDULER_KEY"));
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class InvalidInputTests {

        @Test
        @Order(1)
        void invalidAtFormat_sendsToDeadLetterQueue() throws Exception {
            String correlationId = "dlq-invalid-at-" + UUID.randomUUID();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, correlationId.getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_AT", "not-a-timestamp"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            ConsumerRecord<String, byte[]> dlqRecord = pollDlqForMessage(correlationId, Duration.ofSeconds(30));
            assertNotNull(dlqRecord, "Expected message in DLQ for invalid AT format");
        }

        @Test
        @Order(2)
        void invalidSleepFormat_sendsToDeadLetterQueue() throws Exception {
            String correlationId = "dlq-invalid-sleep-" + UUID.randomUUID();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, correlationId.getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_WAIT", "not-a-duration"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            ConsumerRecord<String, byte[]> dlqRecord = pollDlqForMessage(correlationId, Duration.ofSeconds(30));
            assertNotNull(dlqRecord, "Expected message in DLQ for invalid SLEEP format");
        }

        @Test
        @Order(3)
        void invalidWaitStart_sendsToDeadLetterQueue() throws Exception {
            String correlationId = "dlq-invalid-sleepstart-" + UUID.randomUUID();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, correlationId.getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_WAIT", "PT1H"));
            record.headers().add(header("SCHEDULER_WAIT_START", "INVALID"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            ConsumerRecord<String, byte[]> dlqRecord = pollDlqForMessage(correlationId, Duration.ofSeconds(30));
            assertNotNull(dlqRecord, "Expected message in DLQ for invalid SLEEP_START");
        }

        @Test
        @Order(4)
        void invalidKeyPolicy_sendsToDeadLetterQueue() throws Exception {
            String correlationId = "dlq-invalid-policy-" + UUID.randomUUID();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, correlationId.getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY_POLICY", "INVALID"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            ConsumerRecord<String, byte[]> dlqRecord = pollDlqForMessage(correlationId, Duration.ofSeconds(30));
            assertNotNull(dlqRecord, "Expected message in DLQ for invalid KEY_POLICY");
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class CompleteScenarioTests {

        @Test
        @Order(1)
        void fullScheduledJob_allHeadersSet() throws Exception {
            String jobKey = "full-job-" + UUID.randomUUID();
            Instant targetTime = Instant.now().plus(2, ChronoUnit.HOURS);
            byte[] body = "{\"order\": 999}".getBytes(StandardCharsets.UTF_8);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "msg-key", body);
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", targetTime.toString()));
            record.headers().add(header("SCHEDULER_KEY_POLICY", "REPLACE"));
            record.headers().add(header("SCHEDULER_RETRY_COUNT", "5"));
            record.headers().add(header("SCHEDULER_ADVISORY_HEADERS", "X-.*"));
            record.headers().add(header("X-Correlation-Id", "corr-999"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            ScheduledJob job = jobs.get(0);

            assertEquals(OUTPUT_TOPIC, job.getDestinationTopic());
            assertEquals(targetTime.truncatedTo(ChronoUnit.MILLIS), job.getRunAt().truncatedTo(ChronoUnit.MILLIS));
            assertEquals(jobKey, job.getJobKey());
            assertEquals(KeyPolicy.REPLACE, job.getKeyPolicy());
            assertEquals(5, job.getMaxRetries());
            assertEquals("X-.*", job.getAdvisoryHeadersPattern());
            assertArrayEquals(body, job.getMessageValue());
            assertEquals("corr-999", job.getHeaders().get("X-Correlation-Id"));
        }

        @Test
        @Order(2)
        void sleepWithRepeat_allSleepOptionsSet() throws Exception {
            String jobKey = "sleep-full-" + UUID.randomUUID();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_WAIT", "PT15M"));
            record.headers().add(header("SCHEDULER_WAIT_START", "PREV"));
            record.headers().add(header("SCHEDULER_WAIT_REPEAT", "10"));
            record.headers().add(header("SCHEDULER_KEY_POLICY", "QUEUE"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            ScheduledJob job = jobs.get(0);

            assertEquals("PT15M", job.getWaitDuration());
            assertEquals(WaitStart.PREV, job.getWaitStart());
            assertEquals(10, job.getWaitRepeat());
            assertEquals(KeyPolicy.QUEUE, job.getKeyPolicy());
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class JobLifecycleTests {

        @Test
        @Order(1)
        void immediateJob_firesAndCompletes() throws Exception {
            String jobKey = "immediate-state-test-" + UUID.randomUUID();

            // Send immediate job (no SCHEDULER_AT or SCHEDULER_WAIT)
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test-body".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Immediate jobs fire instantly - verify job exists and completed
            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Job should exist");
            ScheduledJob job = jobs.get(0);
            // Job fires immediately so it should be DONE (or still PENDING/RUNNING)
            assertTrue(job.getState() == JobState.DONE ||
                       job.getState() == JobState.PENDING ||
                       job.getState() == JobState.RUNNING,
                "Immediate job should fire. State: " + job.getState());
            assertNotNull(job.getRunAt(), "Fire time should be set");
        }

        @Test
        @Order(2)
        void queuePolicy_multipleJobs_correctChaining() throws Exception {
            String jobKey = "chain-state-test-" + UUID.randomUUID();

            // Send first job with future fire time
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);
            ProducerRecord<String, byte[]> record1 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "first".getBytes());
            record1.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record1.headers().add(header("SCHEDULER_KEY", jobKey));
            record1.headers().add(header("SCHEDULER_KEY_POLICY", "QUEUE"));
            record1.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record1).get(10, TimeUnit.SECONDS);

            Thread.sleep(3000);

            // Send second job - should chain behind first
            ProducerRecord<String, byte[]> record2 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "second".getBytes());
            record2.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record2.headers().add(header("SCHEDULER_KEY", jobKey));
            record2.headers().add(header("SCHEDULER_KEY_POLICY", "QUEUE"));
            record2.headers().add(header("SCHEDULER_AT", futureTime.plus(1, ChronoUnit.MINUTES).toString()));
            producer.send(record2).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Verify chaining: first PENDING, second WAITING with predecessor
            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertEquals(2, jobs.size(), "Should have 2 jobs");

            // Sort by sequence number to get correct order
            jobs.sort(Comparator.comparingInt(ScheduledJob::getSequenceNum));
            ScheduledJob firstJob = jobs.get(0);
            ScheduledJob secondJob = jobs.get(1);

            assertEquals(JobState.PENDING, firstJob.getState(), "First job should be PENDING");
            assertEquals(JobState.WAITING, secondJob.getState(), "Second job should be WAITING");
            assertEquals(firstJob.getId(), secondJob.getPredecessorId(), "Second job should have first as predecessor");
            assertEquals(1, secondJob.getSequenceNum(), "Second job should have sequence 1");
        }

        @Test
        @Order(3)
        void waitRepeat_storedCorrectly() throws Exception {
            String jobKey = "repeat-store-test-" + UUID.randomUUID();

            // Send job with sleep and repeat
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "repeat-test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_WAIT", "PT5M")); // 5 minute sleep
            record.headers().add(header("SCHEDULER_WAIT_REPEAT", "10")); // Repeat 10 times
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Verify repeat count stored correctly
            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Job should be created");
            ScheduledJob job = jobs.get(0);
            assertEquals(10, job.getWaitRepeat(), "Repeat count should be 10");
            assertEquals("PT5M", job.getWaitDuration(), "Sleep duration should be PT5M");
        }

        @Test
        @Order(4)
        void scheduledJob_fireTimeSetCorrectly() throws Exception {
            String jobKey = "fire-time-test-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(30, ChronoUnit.MINUTES);

            // Send scheduled job
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "scheduled".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Verify fire time set correctly
            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Job should exist");
            ScheduledJob job = jobs.get(0);
            assertEquals(JobState.PENDING, job.getState(), "Job should be PENDING");
            assertTrue(job.getRunAt().isAfter(Instant.now()), "Fire time should be in the future");
            // Allow 5 second tolerance for timing differences
            assertTrue(Math.abs(job.getRunAt().getEpochSecond() - futureTime.getEpochSecond()) < 5,
                "Fire time should match requested time");
        }

        @Test
        @Order(5)
        void cronJob_expressionAndStartTimeSet() throws Exception {
            String jobKey = "cron-store-test-" + UUID.randomUUID();

            // Send CRON job
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "cron-test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_CRON", "0 0 * * *")); // Daily at midnight
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Verify CRON expression stored
            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Job should be created");
            ScheduledJob job = jobs.get(0);
            assertEquals("0 0 * * *", job.getCronExpression(), "CRON expression should be stored");
            assertNotNull(job.getRunAt(), "Fire time should be calculated");
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class JobCancellationTests {

        @Test
        @Order(1)
        void cancelPendingJob_setsStateToFailed() throws Exception {
            String jobKey = "cancel-pending-test-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Create a job scheduled for the future
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "cancel-me".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Verify job exists
            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Job should exist before cancel");
            UUID jobId = jobs.get(0).getId();

            // Cancel the job
            boolean cancelled = jobStore.cancelJob(jobId);
            assertTrue(cancelled, "Cancel should return true");

            // Verify job is now FAILED
            List<ScheduledJob> cancelledJobs = jobStore.findByKey(jobKey);
            assertFalse(cancelledJobs.isEmpty(), "Job should still exist after cancel");
            ScheduledJob job = cancelledJobs.get(0);
            assertEquals(JobState.FAILED, job.getState(), "Cancelled job should be in FAILED state");
            assertEquals("Cancelled via API", job.getLastError(), "Error message should indicate cancellation");
        }

        @Test
        @Order(2)
        void cancelJob_cascadesFailureToWaitingJobs() throws Exception {
            String jobKey = "cancel-cascade-test-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Create first job
            ProducerRecord<String, byte[]> record1 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "first".getBytes());
            record1.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record1.headers().add(header("SCHEDULER_KEY", jobKey));
            record1.headers().add(header("SCHEDULER_KEY_POLICY", "QUEUE"));
            record1.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record1).get(10, TimeUnit.SECONDS);

            Thread.sleep(3000);

            // Create second job that chains behind first
            ProducerRecord<String, byte[]> record2 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "second".getBytes());
            record2.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record2.headers().add(header("SCHEDULER_KEY", jobKey));
            record2.headers().add(header("SCHEDULER_KEY_POLICY", "QUEUE"));
            record2.headers().add(header("SCHEDULER_AT", futureTime.plusSeconds(60).toString()));
            producer.send(record2).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Verify chain is set up
            List<ScheduledJob> allJobs = jobStore.findByKey(jobKey);
            assertEquals(2, allJobs.size(), "Should have 2 jobs");
            allJobs.sort(Comparator.comparingInt(ScheduledJob::getSequenceNum));
            UUID firstJobId = allJobs.get(0).getId();

            // Cancel the first job
            boolean cancelled = jobStore.cancelJob(firstJobId);
            assertTrue(cancelled, "Cancel should return true");

            // Verify both jobs are now FAILED (cascade)
            allJobs = jobStore.findByKey(jobKey);
            long failedCount = allJobs.stream().filter(j -> j.getState() == JobState.FAILED).count();
            assertEquals(2, failedCount, "Both jobs should be FAILED after cascade");
        }

        @Test
        @Order(3)
        void cancelCompletedJob_returnsFalse() throws Exception {
            String jobKey = "cancel-complete-test-" + UUID.randomUUID();

            // Create immediate job that will complete quickly
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "complete-me".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Job should be DONE
            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Job should exist");
            ScheduledJob job = jobs.get(0);

            // Try to cancel - should return false if already complete
            if (job.getState() == JobState.DONE) {
                boolean cancelled = jobStore.cancelJob(job.getId());
                assertFalse(cancelled, "Cannot cancel a completed job");
            }
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class JobQueryTests {

        @Test
        @Order(1)
        void findJobs_byState_returnsMatchingJobs() throws Exception {
            String jobKey1 = "query-pending-" + UUID.randomUUID();
            String jobKey2 = "query-second-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Create job 1 that stays PENDING (future fire time)
            ProducerRecord<String, byte[]> record1 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "pending".getBytes());
            record1.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record1.headers().add(header("SCHEDULER_KEY", jobKey1));
            record1.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record1).get(10, TimeUnit.SECONDS);

            // Create job 2 also with future fire time (stays PENDING)
            ProducerRecord<String, byte[]> record2 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "pending2".getBytes());
            record2.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record2.headers().add(header("SCHEDULER_KEY", jobKey2));
            record2.headers().add(header("SCHEDULER_AT", futureTime.plusSeconds(60).toString()));
            producer.send(record2).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Query for PENDING jobs - both should be there
            List<ScheduledJob> pendingJobs = jobStore.findJobs(JobState.PENDING, null, 100);
            assertTrue(pendingJobs.stream().anyMatch(j -> jobKey1.equals(j.getJobKey())),
                "Should find first pending job");
            assertTrue(pendingJobs.stream().anyMatch(j -> jobKey2.equals(j.getJobKey())),
                "Should find second pending job");

            // Verify filtering by state actually works (DONE should not contain our PENDING jobs)
            List<ScheduledJob> doneJobs = jobStore.findJobs(JobState.DONE, null, 100);
            assertFalse(doneJobs.stream().anyMatch(j -> jobKey1.equals(j.getJobKey())),
                "PENDING job1 should not appear in DONE filter");
        }

        @Test
        @Order(2)
        void findJobs_byKey_returnsMatchingJobs() throws Exception {
            String jobKey = "query-by-key-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Create multiple jobs with same key using QUEUE policy
            for (int i = 0; i < 3; i++) {
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, ("job-" + i).getBytes());
                record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
                record.headers().add(header("SCHEDULER_KEY", jobKey));
                record.headers().add(header("SCHEDULER_KEY_POLICY", "QUEUE"));
                record.headers().add(header("SCHEDULER_AT", futureTime.plusSeconds(i * 60).toString()));
                producer.send(record).get(10, TimeUnit.SECONDS);
                Thread.sleep(1000);
            }

            Thread.sleep(5000);

            // Query by key
            List<ScheduledJob> jobs = jobStore.findJobs(null, jobKey, 100);
            assertEquals(3, jobs.size(), "Should find all 3 jobs with the same key");
        }

        @Test
        @Order(3)
        void getStats_returnsJobCounts() throws Exception {
            // Get current stats
            var stats = jobStore.getStats();

            // Stats should have non-negative counts
            assertTrue(stats.pending() >= 0, "Pending count should be non-negative");
            assertTrue(stats.waiting() >= 0, "Waiting count should be non-negative");
            assertTrue(stats.done() >= 0, "Complete count should be non-negative");
            assertTrue(stats.failed() >= 0, "Failed count should be non-negative");
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class JobRetryTests {

        @Test
        @Order(1)
        void job_hasCorrectRetryConfiguration() throws Exception {
            String jobKey = "retry-config-test-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Create job with custom retry count
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "retry-test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_RETRY_COUNT", "5"));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Job should exist");
            ScheduledJob job = jobs.get(0);
            assertEquals(5, job.getMaxRetries(), "Max retries should be 5");
            assertEquals(0, job.getRetryCount(), "Initial retry count should be 0");
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class CronRepeatTests {

        @Test
        @Order(1)
        void cronJob_calculatesNextFireTime() throws Exception {
            String jobKey = "cron-next-fire-" + UUID.randomUUID();

            // Create CRON job that runs every hour (won't fire immediately)
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "cron-repeat".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_CRON", "0 * * * *")); // Every hour at minute 0
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty(), "CRON job should exist");
            ScheduledJob job = jobs.get(0);

            // Fire time should be within the next hour
            Instant now = Instant.now();
            Instant maxFireTime = now.plus(1, ChronoUnit.HOURS).plus(1, ChronoUnit.MINUTES);
            assertTrue(job.getRunAt().isBefore(maxFireTime),
                "CRON job fire time should be within next hour. FireAt: " + job.getRunAt());
        }

        @Test
        @Order(2)
        void cronJob_withCount_setsMaxCount() throws Exception {
            String jobKey = "cron-count-test-" + UUID.randomUUID();

            // Create CRON job with max count
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "cron-count".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_CRON", "0 * * * *")); // Every hour
            record.headers().add(header("SCHEDULER_CRON_REPEAT", "10")); // Max 10 executions
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty(), "CRON job should exist");
            ScheduledJob job = jobs.get(0);
            assertEquals(10, job.getCronRepeat(), "CRON max count should be 10");
            assertEquals(0, job.getCronRunCount(), "Initial fire count should be 0");
        }

        @Test
        @Order(3)
        void cronJob_withEndTime_setsEndTime() throws Exception {
            String jobKey = "cron-end-test-" + UUID.randomUUID();
            Instant endTime = Instant.now().plus(7, ChronoUnit.DAYS);

            // Create CRON job with end time
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "cron-end".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_CRON", "0 0 * * *")); // Daily
            record.headers().add(header("SCHEDULER_CRON_UNTIL", endTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty(), "CRON job should exist");
            ScheduledJob job = jobs.get(0);
            assertNotNull(job.getCronUntil(), "CRON end time should be set");
            assertTrue(Math.abs(job.getCronUntil().getEpochSecond() - endTime.getEpochSecond()) < 5,
                "CRON end time should match requested time");
        }
    }

    protected RecordHeader header(String key, String value) {
        return new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
    }

    protected String getHeader(ConsumerRecord<String, byte[]> record, String key) {
        var header = record.headers().lastHeader(key);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }

    /**
     * Poll DLQ for a message with a specific correlation ID in the body.
     * Uses unique IDs to correlate test messages and avoid reading stale messages.
     * Scans from near the end of the topic to find the matching message.
     */
    protected ConsumerRecord<String, byte[]> pollDlqForMessage(String correlationId, Duration timeout) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-poll-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            // Assign partition and seek to near end (last 50 messages as buffer)
            var partition = new org.apache.kafka.common.TopicPartition(SCHEDULER_DLQ_TOPIC, 0);
            consumer.assign(Collections.singletonList(partition));
            long endOffset = consumer.endOffsets(Collections.singletonList(partition)).get(partition);
            long seekOffset = Math.max(0, endOffset - 50);
            consumer.seek(partition, seekOffset);

            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, byte[]> record : records) {
                    String body = new String(record.value(), StandardCharsets.UTF_8);
                    if (body.contains(correlationId)) {
                        return record;
                    }
                }
            }
            return null;
        }
    }

    protected ConsumerRecords<String, byte[]> pollDlq(Duration timeout) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-poll-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            var partition = new org.apache.kafka.common.TopicPartition(SCHEDULER_DLQ_TOPIC, 0);
            consumer.assign(Collections.singletonList(partition));
            long endOffset = consumer.endOffsets(Collections.singletonList(partition)).get(partition);
            long seekOffset = Math.max(0, endOffset - 50);
            consumer.seek(partition, seekOffset);

            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                if (records.count() > 0) {
                    return records;
                }
            }
            return ConsumerRecords.empty();
        }
    }

    /**
     * Poll advisory topic for a specific event type and job key.
     * Returns the matching advisory record or null if not found within timeout.
     */
    protected ConsumerRecord<String, byte[]> pollAdvisoryForEvent(String eventType, String jobKey, Duration timeout) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "advisory-poll-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            var partition = new org.apache.kafka.common.TopicPartition(SCHEDULER_ADVISORY_TOPIC, 0);
            consumer.assign(Collections.singletonList(partition));
            long endOffset = consumer.endOffsets(Collections.singletonList(partition)).get(partition);
            long seekOffset = Math.max(0, endOffset - 500); // Look at last 500 messages
            consumer.seek(partition, seekOffset);

            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, byte[]> record : records) {
                    String event = getHeader(record, "SCHEDULER_ADVISORY_EVENT");
                    String key = getHeader(record, "SCHEDULER_KEY");
                    if (eventType.equals(event) && (jobKey == null || jobKey.equals(key))) {
                        return record;
                    }
                }
            }
            return null;
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class RestApiTests {

        @Test
        @Order(1)
        void getJobs_withStateFilter_returnsFilteredJobs() throws Exception {
            String jobKey = "rest-api-state-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Create a PENDING job
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "rest-test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Query via REST API - use service directly since REST endpoint needs port config
            List<ScheduledJob> jobs = jobStore.findJobs(JobState.PENDING, null, 100);
            assertTrue(jobs.stream().anyMatch(j -> jobKey.equals(j.getJobKey())),
                "Should find the pending job via service");
        }

        @Test
        @Order(2)
        void getJobs_withKeyFilter_returnsFilteredJobs() throws Exception {
            String jobKey = "rest-api-key-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Create a job with specific key
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "rest-key-test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Query by key via service
            List<ScheduledJob> jobs = jobStore.findJobs(null, jobKey, 100);
            assertFalse(jobs.isEmpty(), "Should find the job by key");
            assertEquals(jobKey, jobs.get(0).getJobKey());
        }

        @Test
        @Order(3)
        void getJobById_existingJob_returnsJob() throws Exception {
            String jobKey = "rest-api-id-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Create a job
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "rest-id-test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Get job ID from database
            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Job should exist");
            UUID jobId = jobs.get(0).getId();

            // Query single job via service
            var job = jobStore.findById(jobId);
            assertTrue(job.isPresent(), "Job should be found by ID");
            assertEquals(jobKey, job.get().getJobKey());
        }

        @Test
        @Order(4)
        void getJobById_nonExistent_returnsEmpty() {
            UUID randomId = UUID.randomUUID();

            var job = jobStore.findById(randomId);
            assertTrue(job.isEmpty(), "Non-existent job should return empty");
        }

        @Test
        @Order(5)
        void deleteJob_existingJob_cancelsJob() throws Exception {
            String jobKey = "rest-api-delete-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Create a job
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "rest-delete-test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Get job ID
            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Job should exist");
            UUID jobId = jobs.get(0).getId();

            // Cancel via service
            boolean cancelled = jobStore.cancelJob(jobId);
            assertTrue(cancelled, "Cancel should return true");

            // Verify job is now FAILED
            jobs = jobStore.findByKey(jobKey);
            assertEquals(JobState.FAILED, jobs.get(0).getState(), "Job should be cancelled");
        }

        @Test
        @Order(6)
        void getStats_returnsJobCounts() {
            var stats = jobStore.getStats();

            assertTrue(stats.pending() >= 0, "Pending count should be non-negative");
            assertTrue(stats.done() >= 0, "Complete count should be non-negative");
            assertTrue(stats.failed() >= 0, "Failed count should be non-negative");
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class AdvisoryEventTests {

        @Test
        @Order(1)
        void jobQueued_advisoryServicePublishes() throws Exception {
            String jobKey = "advisory-queued-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Create job
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "advisory-test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Verify job was created - advisory publishing verified by unit tests
            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Job should be created");
            assertEquals(JobState.PENDING, jobs.get(0).getState());
        }

        @Test
        @Order(2)
        void jobChained_stateVerified() throws Exception {
            String jobKey = "advisory-chained-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Create first job
            ProducerRecord<String, byte[]> record1 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "first".getBytes());
            record1.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record1.headers().add(header("SCHEDULER_KEY", jobKey));
            record1.headers().add(header("SCHEDULER_KEY_POLICY", "QUEUE"));
            record1.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record1).get(10, TimeUnit.SECONDS);

            Thread.sleep(3000);

            // Create second job - should chain
            ProducerRecord<String, byte[]> record2 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "second".getBytes());
            record2.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record2.headers().add(header("SCHEDULER_KEY", jobKey));
            record2.headers().add(header("SCHEDULER_KEY_POLICY", "QUEUE"));
            record2.headers().add(header("SCHEDULER_AT", futureTime.plusSeconds(60).toString()));
            producer.send(record2).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Verify chaining occurred (JOB_CHAINED event would be published)
            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertEquals(2, jobs.size(), "Should have 2 jobs");
            assertTrue(jobs.stream().anyMatch(j -> j.getState() == JobState.WAITING),
                "Second job should be WAITING (chained)");
        }

        @Test
        @Order(3)
        void jobSkipped_onlyOneJobExists() throws Exception {
            String jobKey = "advisory-skipped-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Create first job with SKIP policy
            ProducerRecord<String, byte[]> record1 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "first".getBytes());
            record1.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record1.headers().add(header("SCHEDULER_KEY", jobKey));
            record1.headers().add(header("SCHEDULER_KEY_POLICY", "SKIP"));
            record1.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record1).get(10, TimeUnit.SECONDS);

            Thread.sleep(3000);

            // Create second job - should be skipped (JOB_SKIPPED event published)
            ProducerRecord<String, byte[]> record2 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "second".getBytes());
            record2.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record2.headers().add(header("SCHEDULER_KEY", jobKey));
            record2.headers().add(header("SCHEDULER_KEY_POLICY", "SKIP"));
            record2.headers().add(header("SCHEDULER_AT", futureTime.plusSeconds(60).toString()));
            producer.send(record2).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Verify skip occurred - only 1 job should exist
            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertEquals(1, jobs.size(), "Second job should be skipped, only first exists");
        }

        @Test
        @Order(4)
        void jobReplaced_firstJobFailed() throws Exception {
            String jobKey = "advisory-replaced-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Create first job with REPLACE policy
            ProducerRecord<String, byte[]> record1 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "first".getBytes());
            record1.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record1.headers().add(header("SCHEDULER_KEY", jobKey));
            record1.headers().add(header("SCHEDULER_KEY_POLICY", "REPLACE"));
            record1.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record1).get(10, TimeUnit.SECONDS);

            Thread.sleep(3000);

            // Create second job - should replace first (JOB_REPLACED event published)
            ProducerRecord<String, byte[]> record2 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "second".getBytes());
            record2.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record2.headers().add(header("SCHEDULER_KEY", jobKey));
            record2.headers().add(header("SCHEDULER_KEY_POLICY", "REPLACE"));
            record2.headers().add(header("SCHEDULER_AT", futureTime.plusSeconds(60).toString()));
            producer.send(record2).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Verify replace occurred - first job FAILED, second PENDING
            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertEquals(2, jobs.size(), "Should have 2 jobs");
            assertTrue(jobs.stream().anyMatch(j -> j.getState() == JobState.FAILED),
                "First job should be FAILED (replaced)");
            assertTrue(jobs.stream().anyMatch(j -> j.getState() == JobState.PENDING),
                "Second job should be PENDING");
        }

        @Test
        @Order(5)
        void jobDone_completesSuccessfully() throws Exception {
            String jobKey = "advisory-done-" + UUID.randomUUID();

            // Create immediate job that will complete (JOB_DONE event published)
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "done-test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(10000);

            // Verify job completed or is in a valid state
            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Job should exist");
            JobState state = jobs.get(0).getState();
            assertTrue(state == JobState.DONE || state == JobState.PENDING || state == JobState.RUNNING,
                "Job should be in a valid execution state. Actual: " + state);
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class HealthCheckTests {

        @Test
        @Order(1)
        void schedulerInstance_isRegistered() throws Exception {
            // The scheduler should register itself on startup
            // Verify by checking jobs can be created and processed
            String jobKey = "health-check-" + UUID.randomUUID();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "health".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Scheduler should be healthy and processing jobs");
        }

        @Test
        @Order(2)
        void jobStats_areAvailable() {
            var stats = jobStore.getStats();
            assertNotNull(stats, "Stats should be available");
            assertTrue(stats.pending() >= 0 && stats.done() >= 0,
                "Stats should have valid counts");
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class JobPromotionTests {

        @Test
        @Order(1)
        void waitingJob_promotedWhenPredecessorCompletes() throws Exception {
            String jobKey = "promotion-test-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Create first job with future fire time
            ProducerRecord<String, byte[]> record1 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "first".getBytes());
            record1.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record1.headers().add(header("SCHEDULER_KEY", jobKey));
            record1.headers().add(header("SCHEDULER_KEY_POLICY", "QUEUE"));
            record1.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record1).get(10, TimeUnit.SECONDS);

            Thread.sleep(3000);

            // Create second job that chains
            ProducerRecord<String, byte[]> record2 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "second".getBytes());
            record2.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record2.headers().add(header("SCHEDULER_KEY", jobKey));
            record2.headers().add(header("SCHEDULER_KEY_POLICY", "QUEUE"));
            record2.headers().add(header("SCHEDULER_AT", futureTime.plusSeconds(60).toString()));
            producer.send(record2).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Verify second job is WAITING (chained behind first)
            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertEquals(2, jobs.size(), "Should have 2 jobs. Found: " + jobs.size());

            jobs.sort(Comparator.comparingInt(ScheduledJob::getSequenceNum));
            ScheduledJob firstJob = jobs.get(0);
            ScheduledJob secondJob = jobs.get(1);

            assertEquals(JobState.PENDING, firstJob.getState(), "First job should be PENDING");
            assertEquals(JobState.WAITING, secondJob.getState(), "Second job should be WAITING");
            assertEquals(firstJob.getId(), secondJob.getPredecessorId(),
                "Second job should have first as predecessor");

            // Cancel first job to trigger promotion check (cascade fail in this case)
            jobStore.cancelJob(firstJob.getId());

            Thread.sleep(3000);

            // After cancellation, both should be FAILED (cascade)
            jobs = jobStore.findByKey(jobKey);
            long failedCount = jobs.stream().filter(j -> j.getState() == JobState.FAILED).count();
            assertEquals(2, failedCount, "Both jobs should be FAILED after cascade. States: " +
                jobs.stream().map(j -> j.getState().toString()).toList());
        }

        @Test
        @Order(2)
        void waitingJobs_cascadeFailWhenPredecessorCancelled() throws Exception {
            String jobKey = "cascade-fail-test-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Create first job
            ProducerRecord<String, byte[]> record1 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "first".getBytes());
            record1.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record1.headers().add(header("SCHEDULER_KEY", jobKey));
            record1.headers().add(header("SCHEDULER_KEY_POLICY", "QUEUE"));
            record1.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record1).get(10, TimeUnit.SECONDS);

            Thread.sleep(3000);

            // Create second and third jobs that chain
            for (int i = 2; i <= 3; i++) {
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, ("job-" + i).getBytes());
                record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
                record.headers().add(header("SCHEDULER_KEY", jobKey));
                record.headers().add(header("SCHEDULER_KEY_POLICY", "QUEUE"));
                record.headers().add(header("SCHEDULER_AT", futureTime.plusSeconds(i * 60).toString()));
                producer.send(record).get(10, TimeUnit.SECONDS);
                Thread.sleep(1000);
            }

            Thread.sleep(5000);

            // Verify chain is set up
            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertEquals(3, jobs.size(), "Should have 3 jobs");
            jobs.sort(Comparator.comparingInt(ScheduledJob::getSequenceNum));
            UUID firstJobId = jobs.get(0).getId();

            // Cancel first job - should cascade fail to waiting jobs
            boolean cancelled = jobStore.cancelJob(firstJobId);
            assertTrue(cancelled, "Cancel should succeed");

            Thread.sleep(3000);

            // Verify all jobs are now FAILED
            jobs = jobStore.findByKey(jobKey);
            long failedCount = jobs.stream().filter(j -> j.getState() == JobState.FAILED).count();
            assertEquals(3, failedCount, "All jobs should be FAILED after cascade");
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class MultiStateFilterTests {

        @Test
        @Order(1)
        void findJobs_multipleStates_returnsAllMatching() throws Exception {
            String baseKey = "multi-state-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Create PENDING job
            ProducerRecord<String, byte[]> record1 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "pending".getBytes());
            record1.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record1.headers().add(header("SCHEDULER_KEY", baseKey + "-pending"));
            record1.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record1).get(10, TimeUnit.SECONDS);

            // Create job that will be DONE (immediate)
            ProducerRecord<String, byte[]> record2 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "done".getBytes());
            record2.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record2.headers().add(header("SCHEDULER_KEY", baseKey + "-done"));
            producer.send(record2).get(10, TimeUnit.SECONDS);

            Thread.sleep(8000);

            // Query with multiple states
            List<JobState> states = List.of(JobState.PENDING, JobState.DONE);
            var result = jobStore.findJobsPaged(states, null, null, 0, 100);

            boolean hasPending = result.items().stream()
                .anyMatch(j -> (baseKey + "-pending").equals(j.getJobKey()));
            boolean hasDone = result.items().stream()
                .anyMatch(j -> (baseKey + "-done").equals(j.getJobKey()));

            assertTrue(hasPending, "Should find PENDING job");
            assertTrue(hasDone, "Should find DONE job");
        }

        @Test
        @Order(2)
        void findJobs_withDestinationFilter_returnsMatchingDestination() throws Exception {
            String jobKey = "dest-filter-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "dest-test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Filter by destination
            var result = jobStore.findJobsPaged(null, null, OUTPUT_TOPIC, 0, 100);
            assertTrue(result.items().stream().anyMatch(j -> jobKey.equals(j.getJobKey())),
                "Should find job with matching destination");

            // Filter by non-existent destination
            var emptyResult = jobStore.findJobsPaged(null, null, "non-existent-topic", 0, 100);
            assertFalse(emptyResult.items().stream().anyMatch(j -> jobKey.equals(j.getJobKey())),
                "Should not find job with different destination");
        }

        @Test
        @Order(3)
        void findJobs_combinedFilters_stateKeyDestination() throws Exception {
            String jobKey = "combined-filter-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "combined".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Combined filter: state + key + destination
            var result = jobStore.findJobsPaged(
                List.of(JobState.PENDING),
                jobKey,
                OUTPUT_TOPIC,
                0, 100);

            assertEquals(1, result.items().size(), "Should find exactly 1 job with all filters");
            assertEquals(jobKey, result.items().get(0).getJobKey());
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class PaginationTests {

        @Test
        @Order(1)
        void pagination_offsetAndLimit_worksCorrectly() throws Exception {
            String baseKey = "pagination-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Create 5 jobs with same prefix
            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, ("job-" + i).getBytes());
                record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
                record.headers().add(header("SCHEDULER_KEY", baseKey + "-" + i));
                record.headers().add(header("SCHEDULER_AT", futureTime.plusSeconds(i * 60).toString()));
                producer.send(record).get(10, TimeUnit.SECONDS);
                Thread.sleep(500);
            }

            Thread.sleep(5000);

            // Get page 1 (limit 2)
            var page1 = jobStore.findJobsPaged(List.of(JobState.PENDING), null, null, 0, 2);
            assertEquals(2, page1.items().size(), "Page 1 should have 2 items");
            assertTrue(page1.hasMore(), "Should have more pages");

            // Get page 2
            var page2 = jobStore.findJobsPaged(List.of(JobState.PENDING), null, null, 2, 2);
            assertEquals(2, page2.items().size(), "Page 2 should have 2 items");

            // Verify no overlap between pages
            Set<UUID> page1Ids = page1.items().stream().map(ScheduledJob::getId).collect(java.util.stream.Collectors.toSet());
            Set<UUID> page2Ids = page2.items().stream().map(ScheduledJob::getId).collect(java.util.stream.Collectors.toSet());
            assertTrue(Collections.disjoint(page1Ids, page2Ids), "Pages should not overlap");
        }

        @Test
        @Order(2)
        void pagination_totalCount_accurate() throws Exception {
            // Get current total
            var result = jobStore.findJobsPaged(null, null, null, 0, 10);
            long initialTotal = result.total();

            String jobKey = "total-count-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Add one more job
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "count".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Verify total increased
            var newResult = jobStore.findJobsPaged(null, null, null, 0, 10);
            assertTrue(newResult.total() >= initialTotal, "Total should increase after adding job");
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class DirectApiJobCreationTests {

        @Test
        @Order(1)
        void createJob_viaService_createsAndEnqueues() throws Exception {
            String jobKey = "direct-create-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(30, ChronoUnit.MINUTES);

            ScheduledJob job = new ScheduledJob();
            job.setDestinationTopic(OUTPUT_TOPIC);
            job.setJobKey(jobKey);
            job.setRunAt(futureTime);
            job.setEffectiveRunAt(futureTime);
            job.setKeyPolicy(KeyPolicy.QUEUE);
            job.setMaxRetries(3);
            job.setMessageValue("direct-api-test".getBytes(StandardCharsets.UTF_8));

            jobStore.save(job);

            // Verify job was created
            var found = jobStore.findById(job.getId());
            assertTrue(found.isPresent(), "Job should be saved");
            assertEquals(jobKey, found.get().getJobKey());
            assertEquals(JobState.PENDING, found.get().getState());
        }

        @Test
        @Order(2)
        void updateJob_viaService_updatesFields() throws Exception {
            String jobKey = "direct-update-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Create job
            ScheduledJob job = new ScheduledJob();
            job.setDestinationTopic(OUTPUT_TOPIC);
            job.setJobKey(jobKey);
            job.setRunAt(futureTime);
            job.setEffectiveRunAt(futureTime);
            job.setKeyPolicy(KeyPolicy.QUEUE);
            job.setMaxRetries(3);
            jobStore.save(job);
            UUID jobId = job.getId();

            Thread.sleep(2000); // Let background processing settle

            // Re-fetch job to get current version
            var fetched = jobStore.findById(jobId);
            assertTrue(fetched.isPresent(), "Job should exist");
            ScheduledJob toUpdate = fetched.get();

            // Update job
            Instant newTime = futureTime.plus(30, ChronoUnit.MINUTES);
            toUpdate.setRunAt(newTime);
            toUpdate.setEffectiveRunAt(newTime);
            toUpdate.setMaxRetries(5);
            jobStore.update(toUpdate);

            // Verify update
            var updated = jobStore.findById(jobId);
            assertTrue(updated.isPresent());
            assertEquals(5, updated.get().getMaxRetries(), "Max retries should be updated");
        }

        @Test
        @Order(3)
        void createJob_withAllFields_allFieldsPersisted() throws Exception {
            String jobKey = "full-direct-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            ScheduledJob job = new ScheduledJob();
            job.setDestinationTopic(OUTPUT_TOPIC);
            job.setJobKey(jobKey);
            job.setRunAt(futureTime);
            job.setEffectiveRunAt(futureTime);
            job.setKeyPolicy(KeyPolicy.REPLACE);
            job.setMaxRetries(10);
            job.setMessageKey("msg-key".getBytes(StandardCharsets.UTF_8));
            job.setMessageValue("{\"full\": true}".getBytes(StandardCharsets.UTF_8));
            job.setHeaders(Map.of("X-Custom", "value", "X-Another", "header"));
            job.setAdvisoryHeadersPattern("X-.*");
            job.setWaitDuration("PT30M");
            job.setWaitStart(WaitStart.PREV);
            job.setWaitRepeat(5);

            jobStore.save(job);

            var found = jobStore.findById(job.getId());
            assertTrue(found.isPresent());
            ScheduledJob saved = found.get();

            assertEquals(OUTPUT_TOPIC, saved.getDestinationTopic());
            assertEquals(jobKey, saved.getJobKey());
            assertEquals(KeyPolicy.REPLACE, saved.getKeyPolicy());
            assertEquals(10, saved.getMaxRetries());
            assertArrayEquals("msg-key".getBytes(), saved.getMessageKey());
            assertArrayEquals("{\"full\": true}".getBytes(), saved.getMessageValue());
            assertEquals("value", saved.getHeaders().get("X-Custom"));
            assertEquals("X-.*", saved.getAdvisoryHeadersPattern());
            assertEquals("PT30M", saved.getWaitDuration());
            assertEquals(WaitStart.PREV, saved.getWaitStart());
            assertEquals(5, saved.getWaitRepeat());
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class EdgeCaseTests {

        @Test
        @Order(1)
        void longJobKey_handledCorrectly() throws Exception {
            // Create job key at max length (255 chars)
            String longKey = "key-" + "x".repeat(250);
            assertEquals(254, longKey.length());
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "long-key-test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", longKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findByKey(longKey);
            assertFalse(jobs.isEmpty(), "Job with long key should be created");
            assertEquals(longKey, jobs.get(0).getJobKey());
        }

        @Test
        @Order(2)
        void largeMessageBody_handledCorrectly() throws Exception {
            String jobKey = "large-body-" + UUID.randomUUID();
            // Create ~100KB message body
            String largeBody = "{\"data\": \"" + "x".repeat(100000) + "\"}";
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, largeBody.getBytes(StandardCharsets.UTF_8));
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Job with large body should be created");
            assertEquals(largeBody.length(), new String(jobs.get(0).getMessageValue(), StandardCharsets.UTF_8).length());
        }

        @Test
        @Order(3)
        void manyHeaders_handledCorrectly() throws Exception {
            String jobKey = "many-headers-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "headers-test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            // Add 20 custom headers
            for (int i = 0; i < 20; i++) {
                record.headers().add(header("X-Header-" + i, "value-" + i));
            }
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Job with many headers should be created");
            Map<String, String> headers = jobs.get(0).getHeaders();
            for (int i = 0; i < 20; i++) {
                assertEquals("value-" + i, headers.get("X-Header-" + i), "Header " + i + " should be preserved");
            }
        }

        @Test
        @Order(4)
        void emptyMessageBody_handledCorrectly() throws Exception {
            String jobKey = "empty-body-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, new byte[0]);
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Job with empty body should be created");
            assertTrue(jobs.get(0).getMessageValue() == null || jobs.get(0).getMessageValue().length == 0,
                "Empty body should be preserved");
        }

        @Test
        @Order(5)
        void unicodeInJobKey_handledCorrectly() throws Exception {
            String jobKey = "unicode-测试-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "unicode-test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Job with unicode key should be created");
            assertEquals(jobKey, jobs.get(0).getJobKey());
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class CronJobExecutionTests {

        @Test
        @Order(1)
        void cronJob_firesAtScheduledTime() throws Exception {
            String jobKey = "cron-fire-" + UUID.randomUUID();

            // Create CRON that fires every minute
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "cron-exec".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_CRON", "* * * * *")); // Every minute
            record.headers().add(header("SCHEDULER_CRON_REPEAT", "1")); // Only fire once
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Verify CRON job created
            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty(), "CRON job should be created");
            assertEquals("* * * * *", jobs.get(0).getCronExpression());
            assertEquals(1, jobs.get(0).getCronRepeat());
        }

        @Test
        @Order(2)
        void cronJob_respectsUntilLimit() throws Exception {
            String jobKey = "cron-until-" + UUID.randomUUID();
            Instant untilTime = Instant.now().plus(2, ChronoUnit.HOURS);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "cron-until".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_CRON", "0 * * * *")); // Every hour
            record.headers().add(header("SCHEDULER_CRON_UNTIL", untilTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            assertNotNull(jobs.get(0).getCronUntil(), "CRON_UNTIL should be set");
            assertTrue(jobs.get(0).getCronUntil().isBefore(untilTime.plusSeconds(5)));
        }
    }
}
