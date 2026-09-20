package net.maxf.pubsub.scheduler.service;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import net.maxf.pubsub.scheduler.dao.InstanceDao;
import net.maxf.pubsub.scheduler.dao.JobDao;
import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.KeyPolicy;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@QuarkusTest
@Tag("database")
@TestProfile(JobPreloadServiceTest.Profile.class)
class JobPreloadServiceTest {

    @Inject
    JobStoreService jobStore;

    @InjectMock
    JobDao jobDao;

    @InjectMock
    InstanceDao instanceDao;

    @BeforeEach
    void setUp() {
        when(instanceDao.updateHeartbeat(any(), any())).thenReturn(1);
        when(instanceDao.findLiveInstances(any())).thenReturn(List.of("test-instance"));
    }

    @Nested
    class SaveIfNotExistsByKeyTests {

        @Test
        void saveIfNotExistsByKey_whenInserted_returnsTrue() {
            ScheduledJob job = createJob("test-key");
            when(jobDao.insertIfNotExistsByKey(any())).thenReturn(true);

            boolean result = jobStore.saveIfNotExistsByKey(job);

            assertTrue(result);
            verify(jobDao).insertIfNotExistsByKey(job);
        }

        @Test
        void saveIfNotExistsByKey_whenExists_returnsFalse() {
            ScheduledJob job = createJob("existing-key");
            when(jobDao.insertIfNotExistsByKey(any())).thenReturn(false);

            boolean result = jobStore.saveIfNotExistsByKey(job);

            assertFalse(result);
            verify(jobDao).insertIfNotExistsByKey(job);
        }

        @Test
        void saveIfNotExistsByKey_passesJobToDao() {
            ScheduledJob job = createJob("my-job-key");
            job.setDestinationTopic("my-topic");
            job.setSleepDuration("PT30M");
            job.setSleepRepeat(0);
            when(jobDao.insertIfNotExistsByKey(any())).thenReturn(true);

            jobStore.saveIfNotExistsByKey(job);

            ArgumentCaptor<ScheduledJob> captor = ArgumentCaptor.forClass(ScheduledJob.class);
            verify(jobDao).insertIfNotExistsByKey(captor.capture());
            ScheduledJob captured = captor.getValue();
            assertEquals("my-job-key", captured.getJobKey());
            assertEquals("my-topic", captured.getDestinationTopic());
            assertEquals("PT30M", captured.getSleepDuration());
            assertEquals(0, captured.getSleepRepeat());
        }
    }

    @Nested
    class JobDefinitionParsingTests {

        @Test
        void jobDefinition_heartbeatConfig_parsesCorrectly() {
            String json = """
                [{
                  "jobKey": "scheduler-heartbeat",
                  "destination": "scheduler-advisory",
                  "keyPolicy": "SKIP",
                  "sleepDuration": "PT15M",
                  "sleepRepeat": 0,
                  "headers": {"SCHEDULER_INTERNAL_JOB": "true"},
                  "body": "{\\"type\\":\\"SCHEDULER_HEARTBEAT\\"}",
                  "maxRetries": 3
                }]
                """;

            JobPreloadService.JobDefinition def = parseFirstDefinition(json);

            assertEquals("scheduler-heartbeat", def.jobKey());
            assertEquals("scheduler-advisory", def.destination());
            assertEquals("SKIP", def.keyPolicy());
            assertEquals("PT15M", def.sleepDuration());
            assertEquals(0, def.sleepRepeat());
            assertEquals("true", def.headers().get("SCHEDULER_INTERNAL_JOB"));
            assertEquals(3, def.maxRetries());
        }

        @Test
        void jobDefinition_minimalConfig_parsesCorrectly() {
            String json = """
                [{
                  "jobKey": "simple-job",
                  "destination": "output-topic"
                }]
                """;

            JobPreloadService.JobDefinition def = parseFirstDefinition(json);

            assertEquals("simple-job", def.jobKey());
            assertEquals("output-topic", def.destination());
            assertNull(def.keyPolicy());
            assertNull(def.sleepDuration());
            assertNull(def.sleepRepeat());
            assertNull(def.headers());
            assertNull(def.maxRetries());
        }

        @Test
        void jobDefinition_infiniteRepeat_parsesZero() {
            String json = """
                [{
                  "jobKey": "infinite-job",
                  "destination": "topic",
                  "sleepDuration": "PT1H",
                  "sleepRepeat": 0
                }]
                """;

            JobPreloadService.JobDefinition def = parseFirstDefinition(json);

            assertEquals(0, def.sleepRepeat());
        }

        @Test
        void jobDefinition_finiteRepeat_parsesCount() {
            String json = """
                [{
                  "jobKey": "finite-job",
                  "destination": "topic",
                  "sleepDuration": "PT1H",
                  "sleepRepeat": 10
                }]
                """;

            JobPreloadService.JobDefinition def = parseFirstDefinition(json);

            assertEquals(10, def.sleepRepeat());
        }

        private JobPreloadService.JobDefinition parseFirstDefinition(String json) {
            try {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                java.util.List<JobPreloadService.JobDefinition> defs = mapper.readValue(json,
                    new com.fasterxml.jackson.core.type.TypeReference<>() {});
                return defs.get(0);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private ScheduledJob createJob(String jobKey) {
        ScheduledJob job = new ScheduledJob();
        job.setJobKey(jobKey);
        job.setDestinationTopic("test-topic");
        job.setKeyPolicy(KeyPolicy.SKIP);
        job.setState(JobState.PENDING);
        return job;
    }

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                "quarkus.http.test-port", "0",
                "quarkus.datasource.db-kind", "postgresql",
                "quarkus.datasource.jdbc.url", "jdbc:postgresql://localhost:5433/scheduler",
                "quarkus.datasource.username", "scheduler",
                "quarkus.datasource.password", "scheduler",
                "quarkus.datasource.devservices.enabled", "false",
                "scheduler.instance-id", "test-instance",
                "scheduler.heartbeat.interval-seconds", "30",
                "scheduler.heartbeat.stale-threshold-seconds", "120"
            );
        }
    }
}
