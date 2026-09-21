package net.maxf.pubsub.scheduler.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for job preload JSON parsing. These are pure unit tests
 * that don't require Quarkus or database.
 */
class JobPreloadServiceTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Nested
    class JobDefinitionParsingTests {

        @Test
        void jobDefinition_heartbeatConfig_parsesCorrectly() throws Exception {
            String json = """
                [{
                  "jobKey": "scheduler-heartbeat",
                  "destination": "scheduler-advisory",
                  "keyPolicy": "SKIP",
                  "waitDuration": "PT15M",
                  "waitRepeat": 0,
                  "headers": {"SCHEDULER_INTERNAL_JOB": "true"},
                  "body": "{\\"type\\":\\"SCHEDULER_HEARTBEAT\\"}",
                  "maxRetries": 3
                }]
                """;

            JobPreloadService.JobDefinition def = parseFirstDefinition(json);

            assertEquals("scheduler-heartbeat", def.jobKey());
            assertEquals("scheduler-advisory", def.destination());
            assertEquals("SKIP", def.keyPolicy());
            assertEquals("PT15M", def.waitDuration());
            assertEquals(0, def.waitRepeat());
            assertEquals("true", def.headers().get("SCHEDULER_INTERNAL_JOB"));
            assertEquals(3, def.maxRetries());
        }

        @Test
        void jobDefinition_minimalConfig_parsesCorrectly() throws Exception {
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
            assertNull(def.waitDuration());
            assertNull(def.waitRepeat());
            assertNull(def.headers());
            assertNull(def.maxRetries());
        }

        @Test
        void jobDefinition_infiniteRepeat_parsesZero() throws Exception {
            String json = """
                [{
                  "jobKey": "infinite-job",
                  "destination": "topic",
                  "waitDuration": "PT1H",
                  "waitRepeat": 0
                }]
                """;

            JobPreloadService.JobDefinition def = parseFirstDefinition(json);

            assertEquals(0, def.waitRepeat());
        }

        @Test
        void jobDefinition_finiteRepeat_parsesCount() throws Exception {
            String json = """
                [{
                  "jobKey": "finite-job",
                  "destination": "topic",
                  "waitDuration": "PT1H",
                  "waitRepeat": 10
                }]
                """;

            JobPreloadService.JobDefinition def = parseFirstDefinition(json);

            assertEquals(10, def.waitRepeat());
        }

        @Test
        void jobDefinition_cronJob_parsesCorrectly() throws Exception {
            String json = """
                [{
                  "jobKey": "cron-job",
                  "destination": "topic",
                  "waitDuration": "PT1H",
                  "waitRepeat": 0,
                  "headers": {"SCHEDULER_CRON": "0 0 * * *"}
                }]
                """;

            JobPreloadService.JobDefinition def = parseFirstDefinition(json);

            assertEquals("cron-job", def.jobKey());
            assertEquals("0 0 * * *", def.headers().get("SCHEDULER_CRON"));
        }

        @Test
        void jobDefinition_multipleJobs_parsesAll() throws Exception {
            String json = """
                [
                  {"jobKey": "job1", "destination": "topic1"},
                  {"jobKey": "job2", "destination": "topic2"},
                  {"jobKey": "job3", "destination": "topic3"}
                ]
                """;

            List<JobPreloadService.JobDefinition> defs = mapper.readValue(json, new TypeReference<>() {});

            assertEquals(3, defs.size());
            assertEquals("job1", defs.get(0).jobKey());
            assertEquals("job2", defs.get(1).jobKey());
            assertEquals("job3", defs.get(2).jobKey());
        }

        private JobPreloadService.JobDefinition parseFirstDefinition(String json) throws Exception {
            List<JobPreloadService.JobDefinition> defs = mapper.readValue(json, new TypeReference<>() {});
            return defs.get(0);
        }
    }
}
