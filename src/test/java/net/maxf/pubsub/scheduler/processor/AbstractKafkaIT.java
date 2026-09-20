package net.maxf.pubsub.scheduler.processor;

import jakarta.inject.Inject;
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
    static final String OUTPUT_TOPIC = "output-topic";

    @Inject
    JobStoreService jobStore;

    static KafkaProducer<String, byte[]> producer;
    static KafkaConsumer<String, byte[]> dlqConsumer;

    @BeforeAll
    static void setupKafka() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producer = new KafkaProducer<>(producerProps);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-dlq-consumer-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        dlqConsumer = new KafkaConsumer<>(consumerProps);
        dlqConsumer.subscribe(Collections.singletonList(SCHEDULER_DLQ_TOPIC));
    }

    @AfterAll
    static void teardownKafka() {
        if (producer != null) producer.close();
        if (dlqConsumer != null) dlqConsumer.close();
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class DestinationHeaderTests {

        @Test
        @Order(1)
        void missingDestination_sendsToDeadLetterQueue() throws Exception {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            producer.send(record).get(10, TimeUnit.SECONDS);

            ConsumerRecords<String, byte[]> records = pollDlq(Duration.ofSeconds(30));
            assertTrue(records.count() > 0, "Expected message in DLQ");

            ConsumerRecord<String, byte[]> dlqRecord = records.iterator().next();
            String error = getHeader(dlqRecord, "SCHEDULER_ERROR");
            assertTrue(error.contains("SCHEDULER_DESTINATION"), "Error should mention missing destination");
        }

        @Test
        @Order(2)
        void blankDestination_sendsToDeadLetterQueue() throws Exception {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(new RecordHeader("SCHEDULER_DESTINATION", "   ".getBytes()));
            producer.send(record).get(10, TimeUnit.SECONDS);

            ConsumerRecords<String, byte[]> records = pollDlq(Duration.ofSeconds(30));
            assertTrue(records.count() > 0, "Expected message in DLQ");
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
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_AT", Instant.now().plus(1, ChronoUnit.HOURS).toString()));
            record.headers().add(header("SCHEDULER_SLEEP", "PT1H"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            ConsumerRecords<String, byte[]> records = pollDlq(Duration.ofSeconds(10));
            assertTrue(records.count() > 0, "Expected message in DLQ for mutually exclusive timing headers");

            ConsumerRecord<String, byte[]> dlqRecord = records.iterator().next();
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
            // Should fire within the next 60 seconds
            assertTrue(job.getFireAt().isAfter(before.minusSeconds(1)));
            assertTrue(job.getFireAt().isBefore(before.plusSeconds(61)));
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class CronOptionsTests {

        @Test
        @Order(1)
        void cronEndAndCronCount_mutuallyExclusive_sendsToDeadLetterQueue() throws Exception {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_CRON", "0 0 * * *"));
            record.headers().add(header("SCHEDULER_CRON_END", Instant.now().plus(30, ChronoUnit.DAYS).toString()));
            record.headers().add(header("SCHEDULER_CRON_COUNT", "5"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            ConsumerRecords<String, byte[]> records = pollDlq(Duration.ofSeconds(30));
            assertTrue(records.count() > 0);

            ConsumerRecord<String, byte[]> dlqRecord = records.iterator().next();
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
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_AT", "not-a-timestamp"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            ConsumerRecords<String, byte[]> records = pollDlq(Duration.ofSeconds(10));
            assertTrue(records.count() > 0, "Expected message in DLQ for invalid AT format");
        }

        @Test
        @Order(2)
        void invalidSleepFormat_sendsToDeadLetterQueue() throws Exception {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_SLEEP", "not-a-duration"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            ConsumerRecords<String, byte[]> records = pollDlq(Duration.ofSeconds(10));
            assertTrue(records.count() > 0, "Expected message in DLQ for invalid SLEEP format");
        }

        @Test
        @Order(3)
        void invalidSleepStart_sendsToDeadLetterQueue() throws Exception {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_SLEEP", "PT1H"));
            record.headers().add(header("SCHEDULER_SLEEP_START", "INVALID"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            ConsumerRecords<String, byte[]> records = pollDlq(Duration.ofSeconds(10));
            assertTrue(records.count() > 0, "Expected message in DLQ for invalid SLEEP_START");
        }

        @Test
        @Order(4)
        void invalidKeyPolicy_sendsToDeadLetterQueue() throws Exception {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(SCHEDULER_IN_TOPIC, "test".getBytes());
            record.headers().add(header("SCHEDULER_DESTINATION", OUTPUT_TOPIC));
            record.headers().add(header("SCHEDULER_KEY_POLICY", "INVALID"));
            producer.send(record).get(10, TimeUnit.SECONDS);

            ConsumerRecords<String, byte[]> records = pollDlq(Duration.ofSeconds(10));
            assertTrue(records.count() > 0, "Expected message in DLQ for invalid KEY_POLICY");
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

    protected RecordHeader header(String key, String value) {
        return new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
    }

    protected String getHeader(ConsumerRecord<String, byte[]> record, String key) {
        var header = record.headers().lastHeader(key);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }

    protected ConsumerRecords<String, byte[]> pollDlq(Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, byte[]> records = dlqConsumer.poll(Duration.ofMillis(500));
            if (records.count() > 0) {
                return records;
            }
        }
        return ConsumerRecords.empty();
    }
}
