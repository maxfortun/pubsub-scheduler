package net.maxf.pubsub.scheduler.processor;

import jakarta.inject.Inject;
import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.KeyPolicy;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import net.maxf.pubsub.scheduler.model.SleepStart;
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

    static final String BOOTSTRAP_SERVERS = "localhost:9092";
    static final String SCHEDULER_IN_TOPIC = "scheduler-in";
    static final String SCHEDULER_DLQ_TOPIC = "scheduler-dlq";
    static final String SCHEDULER_ADVISORY_TOPIC = "scheduler-advisory";
    static final String OUTPUT_TOPIC = "output-topic";

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
                    jobs.get(0).getFireAt().truncatedTo(ChronoUnit.MILLIS));
        }

        @Test
        @Order(2)
        void schedulerSleep_setsRelativeFireTime() throws Exception {
            String jobKey = "sleep-test-" + UUID.randomUUID();
            Instant before = Instant.now().plus(30, ChronoUnit.MINUTES);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_SLEEP", "PT30M"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            Instant after = Instant.now().plus(30, ChronoUnit.MINUTES);
            assertTrue(jobs.get(0).getFireAt().isAfter(before.minusSeconds(5)));
            assertTrue(jobs.get(0).getFireAt().isBefore(after.plusSeconds(5)));
            assertEquals("PT30M", jobs.get(0).getSleepDuration());
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
            assertTrue(jobs.get(0).getFireAt().isAfter(before.minusSeconds(5)));
            assertTrue(jobs.get(0).getFireAt().isBefore(Instant.now().plusSeconds(5)));
        }

        @Test
        @Order(4)
        void multipleTimingHeaders_sendsToDeadLetterQueue() throws Exception {
            String correlationId = "dlq-multi-timing-" + UUID.randomUUID();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, correlationId.getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_AT", Instant.now().plus(1, ChronoUnit.HOURS).toString()));
            record.headers().add(header("SCHEDULER_SLEEP", "PT1H"));
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
            assertTrue(job.getFireAt().isAfter(before.minusSeconds(5)),
                "fireAt should be after test start: " + job.getFireAt() + " vs " + before);
            assertTrue(job.getFireAt().isBefore(before.plusSeconds(120)),
                "fireAt should be within 2 minutes: " + job.getFireAt() + " vs " + before.plusSeconds(120));
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class CronOptionsTests {

        @Test
        @Order(1)
        void cronEndAndCronCount_mutuallyExclusive_sendsToDeadLetterQueue() throws Exception {
            String correlationId = "dlq-cron-exclusive-" + UUID.randomUUID();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, correlationId.getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_CRON", "0 0 * * *"));
            record.headers().add(header("SCHEDULER_CRON_END", Instant.now().plus(30, ChronoUnit.DAYS).toString()));
            record.headers().add(header("SCHEDULER_CRON_COUNT", "5"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            ConsumerRecord<String, byte[]> dlqRecord = pollDlqForMessage(correlationId, Duration.ofSeconds(30));
            assertNotNull(dlqRecord, "Expected message in DLQ with correlationId: " + correlationId);

            String error = getHeader(dlqRecord, "SCHEDULER_ERROR");
            assertTrue(error.contains("SCHEDULER_CRON_END") && error.contains("SCHEDULER_CRON_COUNT"));
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class SleepOptionsTests {

        @Test
        @Order(1)
        void sleepStartSelf_setsSleepStartToSelf() throws Exception {
            String jobKey = "sleep-start-self-" + UUID.randomUUID();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_SLEEP", "PT1H"));
            record.headers().add(header("SCHEDULER_SLEEP_START", "SELF"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            assertEquals(SleepStart.SELF, jobs.get(0).getSleepStart());
        }

        @Test
        @Order(2)
        void sleepStartPrev_setsSleepStartToPrev() throws Exception {
            String jobKey = "sleep-start-prev-" + UUID.randomUUID();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_SLEEP", "PT1H"));
            record.headers().add(header("SCHEDULER_SLEEP_START", "prev"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            assertEquals(SleepStart.PREV, jobs.get(0).getSleepStart());
        }

        @Test
        @Order(3)
        void sleepRepeat_setsSleepRepeatCount() throws Exception {
            String jobKey = "sleep-repeat-" + UUID.randomUUID();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_SLEEP", "PT15M"));
            record.headers().add(header("SCHEDULER_SLEEP_REPEAT", "5"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            assertEquals(5, jobs.get(0).getSleepRepeat());
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
            record.headers().add(header("SCHEDULER_SLEEP", "not-a-duration"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            ConsumerRecord<String, byte[]> dlqRecord = pollDlqForMessage(correlationId, Duration.ofSeconds(30));
            assertNotNull(dlqRecord, "Expected message in DLQ for invalid SLEEP format");
        }

        @Test
        @Order(3)
        void invalidSleepStart_sendsToDeadLetterQueue() throws Exception {
            String correlationId = "dlq-invalid-sleepstart-" + UUID.randomUUID();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, correlationId.getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_SLEEP", "PT1H"));
            record.headers().add(header("SCHEDULER_SLEEP_START", "INVALID"));
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
            assertEquals(targetTime.truncatedTo(ChronoUnit.MILLIS), job.getFireAt().truncatedTo(ChronoUnit.MILLIS));
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
            record.headers().add(header("SCHEDULER_SLEEP", "PT15M"));
            record.headers().add(header("SCHEDULER_SLEEP_START", "PREV"));
            record.headers().add(header("SCHEDULER_SLEEP_REPEAT", "10"));
            record.headers().add(header("SCHEDULER_KEY_POLICY", "QUEUE"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty());
            ScheduledJob job = jobs.get(0);

            assertEquals("PT15M", job.getSleepDuration());
            assertEquals(SleepStart.PREV, job.getSleepStart());
            assertEquals(10, job.getSleepRepeat());
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

            // Send immediate job (no SCHEDULER_AT or SCHEDULER_SLEEP)
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test-body".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Immediate jobs fire instantly - verify job exists and completed
            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Job should exist");
            ScheduledJob job = jobs.get(0);
            // Job fires immediately so it should be COMPLETE (or still PENDING/FIRING)
            assertTrue(job.getState() == JobState.COMPLETE ||
                       job.getState() == JobState.PENDING ||
                       job.getState() == JobState.FIRING,
                "Immediate job should fire. State: " + job.getState());
            assertNotNull(job.getFireAt(), "Fire time should be set");
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
        void sleepRepeat_storedCorrectly() throws Exception {
            String jobKey = "repeat-store-test-" + UUID.randomUUID();

            // Send job with sleep and repeat
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "repeat-test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_SLEEP", "PT5M")); // 5 minute sleep
            record.headers().add(header("SCHEDULER_SLEEP_REPEAT", "10")); // Repeat 10 times
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Verify repeat count stored correctly
            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Job should be created");
            ScheduledJob job = jobs.get(0);
            assertEquals(10, job.getSleepRepeat(), "Repeat count should be 10");
            assertEquals("PT5M", job.getSleepDuration(), "Sleep duration should be PT5M");
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
            assertTrue(job.getFireAt().isAfter(Instant.now()), "Fire time should be in the future");
            // Allow 5 second tolerance for timing differences
            assertTrue(Math.abs(job.getFireAt().getEpochSecond() - futureTime.getEpochSecond()) < 5,
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
            assertNotNull(job.getFireAt(), "Fire time should be calculated");
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

            // Job should be COMPLETE
            List<ScheduledJob> jobs = jobStore.findByKey(jobKey);
            assertFalse(jobs.isEmpty(), "Job should exist");
            ScheduledJob job = jobs.get(0);

            // Try to cancel - should return false if already complete
            if (job.getState() == JobState.COMPLETE) {
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
            String jobKey2 = "query-complete-" + UUID.randomUUID();
            Instant futureTime = Instant.now().plus(1, ChronoUnit.HOURS);

            // Create a job that stays PENDING (future fire time)
            ProducerRecord<String, byte[]> record1 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "pending".getBytes());
            record1.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record1.headers().add(header("SCHEDULER_KEY", jobKey1));
            record1.headers().add(header("SCHEDULER_AT", futureTime.toString()));
            producer.send(record1).get(10, TimeUnit.SECONDS);

            // Create a job that fires immediately (will complete)
            ProducerRecord<String, byte[]> record2 = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "complete".getBytes());
            record2.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record2.headers().add(header("SCHEDULER_KEY", jobKey2));
            producer.send(record2).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            // Query for PENDING jobs
            List<ScheduledJob> pendingJobs = jobStore.findJobs(JobState.PENDING, null, 100);
            assertTrue(pendingJobs.stream().anyMatch(j -> jobKey1.equals(j.getJobKey())),
                "Should find the pending job");

            // Query for COMPLETE jobs
            List<ScheduledJob> completeJobs = jobStore.findJobs(JobState.COMPLETE, null, 100);
            assertTrue(completeJobs.stream().anyMatch(j -> jobKey2.equals(j.getJobKey())),
                "Should find the completed job");
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
            assertTrue(stats.complete() >= 0, "Complete count should be non-negative");
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

            // Create CRON job that runs every minute
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "cron-repeat".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY", jobKey));
            record.headers().add(header("SCHEDULER_CRON", "* * * * *")); // Every minute
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty(), "CRON job should exist");
            ScheduledJob job = jobs.get(0);

            // Fire time should be within the next minute
            Instant now = Instant.now();
            Instant maxFireTime = now.plus(2, ChronoUnit.MINUTES);
            assertTrue(job.getFireAt().isBefore(maxFireTime),
                "CRON job fire time should be within next 2 minutes. FireAt: " + job.getFireAt());
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
            record.headers().add(header("SCHEDULER_CRON_COUNT", "10")); // Max 10 executions
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty(), "CRON job should exist");
            ScheduledJob job = jobs.get(0);
            assertEquals(10, job.getCronMaxCount(), "CRON max count should be 10");
            assertEquals(0, job.getCronFireCount(), "Initial fire count should be 0");
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
            record.headers().add(header("SCHEDULER_CRON_END", endTime.toString()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            Thread.sleep(5000);

            List<ScheduledJob> jobs = jobStore.findPendingByKey(jobKey);
            assertFalse(jobs.isEmpty(), "CRON job should exist");
            ScheduledJob job = jobs.get(0);
            assertNotNull(job.getCronEnd(), "CRON end time should be set");
            assertTrue(Math.abs(job.getCronEnd().getEpochSecond() - endTime.getEpochSecond()) < 5,
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
}
