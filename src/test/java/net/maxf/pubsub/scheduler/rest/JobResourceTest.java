package net.maxf.pubsub.scheduler.rest;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import net.maxf.pubsub.scheduler.service.JobStoreService;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@QuarkusTest
class JobResourceTest {

    @InjectMock
    JobStoreService jobStore;

    @Test
    void listJobs_returnsJobList() {
        ScheduledJob job1 = createJob("key-1");
        ScheduledJob job2 = createJob("key-2");
        when(jobStore.findJobsPaged(null, null, 0, 100))
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
        when(jobStore.findJobsPaged(JobState.PENDING, null, 0, 100))
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
        when(jobStore.findJobsPaged(null, "specific-key", 0, 100))
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
        when(jobStore.findJobsPaged(null, null, 0, 10))
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
        when(jobStore.findJobsPaged(null, null, 20, 10))
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
        when(jobStore.findJobsPaged(null, null, 0, 100))
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
        when(jobStore.findJobsPaged(null, null, 0, 1))
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
        when(jobStore.findJobsPaged(null, null, 0, 1))
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
        when(jobStore.findJobsPaged(null, null, 0, 1000))
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
        when(jobStore.findJobsPaged(JobState.DONE, null, 0, 100))
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
        when(jobStore.findJobsPaged(null, null, 90, 10))
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
        when(jobStore.findJobsPaged(null, null, 8, 2))
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
        when(jobStore.findJobsPaged(JobState.PENDING, "order-123", 5, 25))
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
            .body("firing", equalTo(1))
            .body("complete", equalTo(100))
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
}
