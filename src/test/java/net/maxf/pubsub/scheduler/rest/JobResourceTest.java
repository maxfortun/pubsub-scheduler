package net.maxf.pubsub.scheduler.rest;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.KeyPolicy;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import net.maxf.pubsub.scheduler.service.JobQueueService;
import net.maxf.pubsub.scheduler.service.JobStoreService;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@QuarkusTest
class JobResourceTest {

    @InjectMock
    JobStoreService jobStore;

    @InjectMock
    JobQueueService jobQueue;

    @Test
    void listJobs_returnsJobList() {
        ScheduledJob job1 = createJob("key-1");
        ScheduledJob job2 = createJob("key-2");
        when(jobStore.findJobsPaged(anyList(), isNull(), isNull(), eq(0), eq(100)))
            .thenReturn(JobResource.PagedResult.of(List.of(job1, job2), 0, 100, 2));

        given()
            .when().get("/api/jobs")
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .body("items.size()", equalTo(2))
            .body("items[0].jobKey", equalTo("key-1"))
            .body("items[1].jobKey", equalTo("key-2"))
            .body("offset", equalTo(0))
            .body("limit", equalTo(100))
            .body("total", equalTo(2))
            .body("hasMore", equalTo(false));
    }

    @Test
    void listJobs_withStateFilter_filtersJobs() {
        ScheduledJob job = createJob("key-1");
        job.setState(JobState.PENDING);
        when(jobStore.findJobsPaged(eq(List.of(JobState.PENDING)), isNull(), isNull(), eq(0), eq(100)))
            .thenReturn(JobResource.PagedResult.of(List.of(job), 0, 100, 1));

        given()
            .queryParam("state", "PENDING")
            .when().get("/api/jobs")
            .then()
            .statusCode(200)
            .body("items.size()", equalTo(1))
            .body("items[0].state", equalTo("PENDING"));
    }

    @Test
    void listJobs_withKeyFilter_filtersJobs() {
        ScheduledJob job = createJob("specific-key");
        when(jobStore.findJobsPaged(anyList(), eq("specific-key"), isNull(), eq(0), eq(100)))
            .thenReturn(JobResource.PagedResult.of(List.of(job), 0, 100, 1));

        given()
            .queryParam("key", "specific-key")
            .when().get("/api/jobs")
            .then()
            .statusCode(200)
            .body("items.size()", equalTo(1))
            .body("items[0].jobKey", equalTo("specific-key"));
    }

    @Test
    void listJobs_withLimit_limitsResults() {
        ScheduledJob job = createJob("key-1");
        when(jobStore.findJobsPaged(anyList(), isNull(), isNull(), eq(0), eq(10)))
            .thenReturn(JobResource.PagedResult.of(List.of(job), 0, 10, 1));

        given()
            .queryParam("limit", 10)
            .when().get("/api/jobs")
            .then()
            .statusCode(200)
            .body("limit", equalTo(10));
    }

    @Test
    void listJobs_withOffsetAndLimit_paginates() {
        ScheduledJob job = createJob("key-3");
        when(jobStore.findJobsPaged(anyList(), isNull(), isNull(), eq(20), eq(10)))
            .thenReturn(JobResource.PagedResult.of(List.of(job), 20, 10, 50));

        given()
            .queryParam("offset", 20)
            .queryParam("limit", 10)
            .when().get("/api/jobs")
            .then()
            .statusCode(200)
            .body("items.size()", equalTo(1))
            .body("offset", equalTo(20))
            .body("limit", equalTo(10))
            .body("total", equalTo(50))
            .body("hasMore", equalTo(true));
    }

    @Test
    void listJobs_negativeOffset_clampsToZero() {
        when(jobStore.findJobsPaged(anyList(), isNull(), isNull(), eq(0), eq(100)))
            .thenReturn(JobResource.PagedResult.of(List.of(), 0, 100, 0));

        given()
            .queryParam("offset", -10)
            .when().get("/api/jobs")
            .then()
            .statusCode(200)
            .body("offset", equalTo(0));
    }

    @Test
    void listJobs_zeroLimit_clampsToOne() {
        when(jobStore.findJobsPaged(anyList(), isNull(), isNull(), eq(0), eq(1)))
            .thenReturn(JobResource.PagedResult.of(List.of(), 0, 1, 0));

        given()
            .queryParam("limit", 0)
            .when().get("/api/jobs")
            .then()
            .statusCode(200)
            .body("limit", equalTo(1));
    }

    @Test
    void listJobs_negativeLimit_clampsToOne() {
        when(jobStore.findJobsPaged(anyList(), isNull(), isNull(), eq(0), eq(1)))
            .thenReturn(JobResource.PagedResult.of(List.of(), 0, 1, 0));

        given()
            .queryParam("limit", -5)
            .when().get("/api/jobs")
            .then()
            .statusCode(200)
            .body("limit", equalTo(1));
    }

    @Test
    void listJobs_limitExceedsMax_cappedToMax() {
        when(jobStore.findJobsPaged(anyList(), isNull(), isNull(), eq(0), eq(1000)))
            .thenReturn(JobResource.PagedResult.of(List.of(), 0, 1000, 0));

        given()
            .queryParam("limit", 5000)
            .when().get("/api/jobs")
            .then()
            .statusCode(200)
            .body("limit", equalTo(1000));
    }

    @Test
    void listJobs_emptyResults_returnsEmptyPage() {
        when(jobStore.findJobsPaged(eq(List.of(JobState.DONE)), isNull(), isNull(), eq(0), eq(100)))
            .thenReturn(JobResource.PagedResult.of(List.of(), 0, 100, 0));

        given()
            .queryParam("state", "DONE")
            .when().get("/api/jobs")
            .then()
            .statusCode(200)
            .body("items.size()", equalTo(0))
            .body("total", equalTo(0))
            .body("hasMore", equalTo(false));
    }

    @Test
    void listJobs_lastPage_hasMoreFalse() {
        ScheduledJob job = createJob("last-job");
        when(jobStore.findJobsPaged(anyList(), isNull(), isNull(), eq(90), eq(10)))
            .thenReturn(JobResource.PagedResult.of(List.of(job), 90, 10, 91));

        given()
            .queryParam("offset", 90)
            .queryParam("limit", 10)
            .when().get("/api/jobs")
            .then()
            .statusCode(200)
            .body("items.size()", equalTo(1))
            .body("offset", equalTo(90))
            .body("total", equalTo(91))
            .body("hasMore", equalTo(false));
    }

    @Test
    void listJobs_exactBoundary_hasMoreFalse() {
        ScheduledJob job1 = createJob("job-1");
        ScheduledJob job2 = createJob("job-2");
        when(jobStore.findJobsPaged(anyList(), isNull(), isNull(), eq(8), eq(2)))
            .thenReturn(JobResource.PagedResult.of(List.of(job1, job2), 8, 2, 10));

        given()
            .queryParam("offset", 8)
            .queryParam("limit", 2)
            .when().get("/api/jobs")
            .then()
            .statusCode(200)
            .body("items.size()", equalTo(2))
            .body("total", equalTo(10))
            .body("hasMore", equalTo(false));
    }

    @Test
    void listJobs_combinedFiltersWithPagination() {
        ScheduledJob job = createJob("order-123");
        job.setState(JobState.PENDING);
        when(jobStore.findJobsPaged(eq(List.of(JobState.PENDING)), eq("order-123"), isNull(), eq(5), eq(25)))
            .thenReturn(JobResource.PagedResult.of(List.of(job), 5, 25, 15));

        given()
            .queryParam("state", "PENDING")
            .queryParam("key", "order-123")
            .queryParam("offset", 5)
            .queryParam("limit", 25)
            .when().get("/api/jobs")
            .then()
            .statusCode(200)
            .body("items.size()", equalTo(1))
            .body("items[0].state", equalTo("PENDING"))
            .body("items[0].jobKey", equalTo("order-123"))
            .body("offset", equalTo(5))
            .body("limit", equalTo(25))
            .body("total", equalTo(15))
            .body("hasMore", equalTo(true));
    }

    @Test
    void getJob_exists_returnsJob() {
        ScheduledJob job = createJob("test-key");
        when(jobStore.findById(job.getId())).thenReturn(Optional.of(job));

        given()
            .when().get("/api/jobs/" + job.getId())
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .body("id", equalTo(job.getId().toString()))
            .body("jobKey", equalTo("test-key"));
    }

    @Test
    void getJob_notExists_returns404() {
        UUID id = UUID.randomUUID();
        when(jobStore.findById(id)).thenReturn(Optional.empty());

        given()
            .when().get("/api/jobs/" + id)
            .then()
            .statusCode(404);
    }

    @Test
    void cancelJob_exists_returns204() {
        UUID id = UUID.randomUUID();
        when(jobStore.cancelJob(id)).thenReturn(true);

        given()
            .when().delete("/api/jobs/" + id)
            .then()
            .statusCode(204);
    }

    @Test
    void cancelJob_notExists_returns404() {
        UUID id = UUID.randomUUID();
        when(jobStore.cancelJob(id)).thenReturn(false);

        given()
            .when().delete("/api/jobs/" + id)
            .then()
            .statusCode(404);
    }

    @Test
    void getStats_returnsStats() {
        JobResource.JobStats stats = new JobResource.JobStats(10, 5, 2, 1, 100, 3);
        when(jobStore.getStats()).thenReturn(stats);

        given()
            .when().get("/api/jobs/stats")
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .body("pending", equalTo(10))
            .body("waiting", equalTo(5))
            .body("acquired", equalTo(2))
            .body("running", equalTo(1))
            .body("done", equalTo(100))
            .body("failed", equalTo(3));
    }

    private ScheduledJob createJob(String key) {
        ScheduledJob job = new ScheduledJob();
        job.setJobKey(key);
        job.setDestinationTopic("output-topic");
        job.setRunAt(Instant.now());
        job.setEffectiveRunAt(Instant.now());
        return job;
    }

    @Nested
    class UpdateJobTests {

        @Test
        void updateJob_pendingJob_succeeds() {
            ScheduledJob job = createJob("test-key");
            job.setState(JobState.PENDING);
            when(jobStore.findById(job.getId())).thenReturn(Optional.of(job));

            given()
                .contentType(ContentType.JSON)
                .body("""
                    {
                        "destinationTopic": "new-topic",
                        "jobKey": "updated-key",
                        "runAt": "2099-12-31T23:59:59Z"
                    }
                    """)
                .when().put("/api/jobs/" + job.getId())
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("destinationTopic", equalTo("new-topic"))
                .body("jobKey", equalTo("updated-key"));

            verify(jobStore).update(any(ScheduledJob.class));
            verify(jobQueue).requeue(any(ScheduledJob.class));
        }

        @Test
        void updateJob_waitingJob_succeeds() {
            ScheduledJob job = createJob("waiting-key");
            job.setState(JobState.WAITING);
            when(jobStore.findById(job.getId())).thenReturn(Optional.of(job));

            given()
                .contentType(ContentType.JSON)
                .body("""
                    {
                        "destinationTopic": "updated-topic",
                        "waitDuration": "PT30M"
                    }
                    """)
                .when().put("/api/jobs/" + job.getId())
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("destinationTopic", equalTo("updated-topic"));

            verify(jobStore).update(any(ScheduledJob.class));
            verify(jobQueue).requeue(any(ScheduledJob.class));
        }

        @Test
        void updateJob_notFound_returns404() {
            UUID id = UUID.randomUUID();
            when(jobStore.findById(id)).thenReturn(Optional.empty());

            given()
                .contentType(ContentType.JSON)
                .body("""
                    {
                        "destinationTopic": "some-topic"
                    }
                    """)
                .when().put("/api/jobs/" + id)
                .then()
                .statusCode(404)
                .body("error", equalTo("Job not found"));
        }

        @Test
        void updateJob_acquiredJob_returns409() {
            ScheduledJob job = createJob("acquired-key");
            job.setState(JobState.ACQUIRED);
            when(jobStore.findById(job.getId())).thenReturn(Optional.of(job));

            given()
                .contentType(ContentType.JSON)
                .body("""
                    {
                        "destinationTopic": "some-topic"
                    }
                    """)
                .when().put("/api/jobs/" + job.getId())
                .then()
                .statusCode(409)
                .body("error", containsString("ACQUIRED"));
        }

        @Test
        void updateJob_runningJob_returns409() {
            ScheduledJob job = createJob("running-key");
            job.setState(JobState.RUNNING);
            when(jobStore.findById(job.getId())).thenReturn(Optional.of(job));

            given()
                .contentType(ContentType.JSON)
                .body("""
                    {
                        "destinationTopic": "some-topic"
                    }
                    """)
                .when().put("/api/jobs/" + job.getId())
                .then()
                .statusCode(409)
                .body("error", containsString("RUNNING"));
        }

        @Test
        void updateJob_doneJob_returns409() {
            ScheduledJob job = createJob("done-key");
            job.setState(JobState.DONE);
            when(jobStore.findById(job.getId())).thenReturn(Optional.of(job));

            given()
                .contentType(ContentType.JSON)
                .body("""
                    {
                        "destinationTopic": "some-topic"
                    }
                    """)
                .when().put("/api/jobs/" + job.getId())
                .then()
                .statusCode(409)
                .body("error", containsString("DONE"));
        }

        @Test
        void updateJob_failedJob_returns409() {
            ScheduledJob job = createJob("failed-key");
            job.setState(JobState.FAILED);
            when(jobStore.findById(job.getId())).thenReturn(Optional.of(job));

            given()
                .contentType(ContentType.JSON)
                .body("""
                    {
                        "destinationTopic": "some-topic"
                    }
                    """)
                .when().put("/api/jobs/" + job.getId())
                .then()
                .statusCode(409)
                .body("error", containsString("FAILED"));
        }

        @Test
        void updateJob_missingDestination_returns400() {
            ScheduledJob job = createJob("test-key");
            job.setState(JobState.PENDING);
            when(jobStore.findById(job.getId())).thenReturn(Optional.of(job));

            given()
                .contentType(ContentType.JSON)
                .body("""
                    {
                        "jobKey": "some-key"
                    }
                    """)
                .when().put("/api/jobs/" + job.getId())
                .then()
                .statusCode(400)
                .body("error", containsString("destinationTopic is required"));
        }

        @Test
        void updateJob_blankDestination_returns400() {
            ScheduledJob job = createJob("test-key");
            job.setState(JobState.PENDING);
            when(jobStore.findById(job.getId())).thenReturn(Optional.of(job));

            given()
                .contentType(ContentType.JSON)
                .body("""
                    {
                        "destinationTopic": "   "
                    }
                    """)
                .when().put("/api/jobs/" + job.getId())
                .then()
                .statusCode(400)
                .body("error", containsString("destinationTopic is required"));
        }

        @Test
        void updateJob_invalidWaitDuration_returns400() {
            ScheduledJob job = createJob("test-key");
            job.setState(JobState.PENDING);
            when(jobStore.findById(job.getId())).thenReturn(Optional.of(job));

            given()
                .contentType(ContentType.JSON)
                .body("""
                    {
                        "destinationTopic": "valid-topic",
                        "waitDuration": "invalid-duration"
                    }
                    """)
                .when().put("/api/jobs/" + job.getId())
                .then()
                .statusCode(400)
                .body("error", containsString("waitDuration is not a valid ISO-8601 duration"));
        }

        @Test
        void updateJob_invalidTopicName_returns400() {
            ScheduledJob job = createJob("test-key");
            job.setState(JobState.PENDING);
            when(jobStore.findById(job.getId())).thenReturn(Optional.of(job));

            given()
                .contentType(ContentType.JSON)
                .body("""
                    {
                        "destinationTopic": "invalid topic with spaces!"
                    }
                    """)
                .when().put("/api/jobs/" + job.getId())
                .then()
                .statusCode(400)
                .body("error", containsString("invalid characters"));
        }

        @Test
        void updateJob_negativeMaxRetries_returns400() {
            ScheduledJob job = createJob("test-key");
            job.setState(JobState.PENDING);
            when(jobStore.findById(job.getId())).thenReturn(Optional.of(job));

            given()
                .contentType(ContentType.JSON)
                .body("""
                    {
                        "destinationTopic": "valid-topic",
                        "maxRetries": -1
                    }
                    """)
                .when().put("/api/jobs/" + job.getId())
                .then()
                .statusCode(400)
                .body("error", containsString("maxRetries must be non-negative"));
        }

        @Test
        void updateJob_withKeyPolicy_setsKeyPolicy() {
            ScheduledJob job = createJob("test-key");
            job.setState(JobState.PENDING);
            when(jobStore.findById(job.getId())).thenReturn(Optional.of(job));

            given()
                .contentType(ContentType.JSON)
                .body("""
                    {
                        "destinationTopic": "valid-topic",
                        "keyPolicy": "REPLACE"
                    }
                    """)
                .when().put("/api/jobs/" + job.getId())
                .then()
                .statusCode(200)
                .body("keyPolicy", equalTo("REPLACE"));
        }

        @Test
        void updateJob_withCronExpression_setsCronFields() {
            ScheduledJob job = createJob("cron-key");
            job.setState(JobState.PENDING);
            when(jobStore.findById(job.getId())).thenReturn(Optional.of(job));

            given()
                .contentType(ContentType.JSON)
                .body("""
                    {
                        "destinationTopic": "valid-topic",
                        "cronExpression": "0 0 * * *",
                        "cronRepeat": 10
                    }
                    """)
                .when().put("/api/jobs/" + job.getId())
                .then()
                .statusCode(200)
                .body("cronExpression", equalTo("0 0 * * *"))
                .body("cronRepeat", equalTo(10));
        }

        @Test
        void updateJob_withWaitDuration_setsWaitFields() {
            ScheduledJob job = createJob("wait-key");
            job.setState(JobState.PENDING);
            when(jobStore.findById(job.getId())).thenReturn(Optional.of(job));

            given()
                .contentType(ContentType.JSON)
                .body("""
                    {
                        "destinationTopic": "valid-topic",
                        "waitDuration": "PT1H",
                        "waitStart": "PREV",
                        "waitRepeat": 5
                    }
                    """)
                .when().put("/api/jobs/" + job.getId())
                .then()
                .statusCode(200)
                .body("waitDuration", equalTo("PT1H"))
                .body("waitStart", equalTo("PREV"))
                .body("waitRepeat", equalTo(5));
        }
    }
}
