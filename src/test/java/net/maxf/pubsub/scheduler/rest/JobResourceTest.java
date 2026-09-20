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
        when(jobStore.findJobs(null, null, 100)).thenReturn(List.of(job1, job2));

        given()
            .when().get("/api/jobs")
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .body("$.size()", equalTo(2))
            .body("[0].jobKey", equalTo("key-1"))
            .body("[1].jobKey", equalTo("key-2"));
    }

    @Test
    void listJobs_withStateFilter_filtersJobs() {
        ScheduledJob job = createJob("key-1");
        job.setState(JobState.PENDING);
        when(jobStore.findJobs(JobState.PENDING, null, 100)).thenReturn(List.of(job));

        given()
            .queryParam("state", "PENDING")
            .when().get("/api/jobs")
            .then()
            .statusCode(200)
            .body("$.size()", equalTo(1))
            .body("[0].state", equalTo("PENDING"));
    }

    @Test
    void listJobs_withKeyFilter_filtersJobs() {
        ScheduledJob job = createJob("specific-key");
        when(jobStore.findJobs(null, "specific-key", 100)).thenReturn(List.of(job));

        given()
            .queryParam("key", "specific-key")
            .when().get("/api/jobs")
            .then()
            .statusCode(200)
            .body("$.size()", equalTo(1))
            .body("[0].jobKey", equalTo("specific-key"));
    }

    @Test
    void listJobs_withLimit_limitsResults() {
        ScheduledJob job = createJob("key-1");
        when(jobStore.findJobs(null, null, 10)).thenReturn(List.of(job));

        given()
            .queryParam("limit", 10)
            .when().get("/api/jobs")
            .then()
            .statusCode(200);
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
        job.setFireAt(Instant.now());
        job.setEffectiveFireAt(Instant.now());
        return job;
    }
}
