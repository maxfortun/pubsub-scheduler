package net.maxf.pubsub.scheduler.integration;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

/**
 * Integration tests that build and run the scheduler in Docker.
 * These tests verify the full container lifecycle and API endpoints.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("docker")
class DockerIntegrationTest {

    private static final String COMPOSE_FILE = "src/test/resources/docker-compose-integration.yml";
    private static final String SCHEDULER_URL = "http://localhost:8085";
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(3);

    @BeforeAll
    static void buildAndStartContainers() throws Exception {
        RestAssured.baseURI = SCHEDULER_URL;

        // Build the Docker image
        System.out.println("Building Docker image...");
        runCommand("./gradlew build -x test --quiet");
        runCommand("docker build -t pubsub-scheduler:test .");

        // Start containers
        System.out.println("Starting containers...");
        runCommand("docker compose -f " + COMPOSE_FILE + " up -d");

        // Wait for scheduler to be healthy
        System.out.println("Waiting for scheduler to be healthy...");
        waitForHealth(STARTUP_TIMEOUT);
    }

    @AfterAll
    static void stopContainers() throws Exception {
        System.out.println("Stopping containers...");
        runCommand("docker compose -f " + COMPOSE_FILE + " down -v");
    }

    @Test
    @Order(1)
    void healthEndpointReturnsUp() {
        given()
            .when()
                .get("/q/health")
            .then()
                .statusCode(200)
                .body("status", equalTo("UP"));
    }

    @Test
    @Order(2)
    void swaggerUiIsAccessible() {
        given()
            .when()
                .get("/swagger-ui")
            .then()
                .statusCode(200)
                .contentType(ContentType.HTML);
    }

    @Test
    @Order(3)
    void openApiSpecIsAccessible() {
        given()
            .when()
                .get("/q/openapi")
            .then()
                .statusCode(200)
                .body(containsString("PubSub Scheduler API"));
    }

    @Test
    @Order(4)
    void openApiSpecContainsPaginationDocs() {
        given()
            .when()
                .get("/q/openapi")
            .then()
                .statusCode(200)
                .body(containsString("offset"))
                .body(containsString("limit"))
                .body(containsString("hasMore"))
                .body(containsString("PagedResult"));
    }

    // ==================== Jobs API Tests ====================

    @Test
    @Order(10)
    void listJobs_returnsPagedResult() {
        given()
            .when()
                .get("/api/jobs")
            .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("items", hasSize(0))
                .body("offset", equalTo(0))
                .body("limit", equalTo(100))
                .body("total", equalTo(0))
                .body("hasMore", equalTo(false));
    }

    @Test
    @Order(11)
    void listJobs_withCustomPagination() {
        given()
            .queryParam("offset", 0)
            .queryParam("limit", 50)
            .when()
                .get("/api/jobs")
            .then()
                .statusCode(200)
                .body("offset", equalTo(0))
                .body("limit", equalTo(50))
                .body("items", notNullValue());
    }

    @Test
    @Order(12)
    void listJobs_negativeOffsetClampedToZero() {
        given()
            .queryParam("offset", -10)
            .when()
                .get("/api/jobs")
            .then()
                .statusCode(200)
                .body("offset", equalTo(0));
    }

    @Test
    @Order(13)
    void listJobs_zeroLimitClampedToOne() {
        given()
            .queryParam("limit", 0)
            .when()
                .get("/api/jobs")
            .then()
                .statusCode(200)
                .body("limit", equalTo(1));
    }

    @Test
    @Order(14)
    void listJobs_exceedingLimitCapped() {
        given()
            .queryParam("limit", 5000)
            .when()
                .get("/api/jobs")
            .then()
                .statusCode(200)
                .body("limit", equalTo(1000));
    }

    @Test
    @Order(15)
    void listJobs_withStateFilter() {
        given()
            .queryParam("state", "PENDING")
            .when()
                .get("/api/jobs")
            .then()
                .statusCode(200)
                .body("items", notNullValue());
    }

    @Test
    @Order(16)
    void listJobs_withKeyFilter() {
        given()
            .queryParam("key", "some-job-key")
            .when()
                .get("/api/jobs")
            .then()
                .statusCode(200)
                .body("items", hasSize(0));
    }

    @Test
    @Order(17)
    void listJobs_combinedFilters() {
        given()
            .queryParam("state", "PENDING")
            .queryParam("key", "test-key")
            .queryParam("offset", 0)
            .queryParam("limit", 25)
            .when()
                .get("/api/jobs")
            .then()
                .statusCode(200)
                .body("offset", equalTo(0))
                .body("limit", equalTo(25))
                .body("items", notNullValue());
    }

    @Test
    @Order(20)
    void getJobStats_returnsZeroCounts() {
        given()
            .when()
                .get("/api/jobs/stats")
            .then()
                .statusCode(200)
                .body("pending", equalTo(0))
                .body("waiting", equalTo(0))
                .body("acquired", equalTo(0))
                .body("firing", equalTo(0))
                .body("complete", equalTo(0))
                .body("failed", equalTo(0));
    }

    @Test
    @Order(21)
    void getNonExistentJob_returns404() {
        given()
            .when()
                .get("/api/jobs/00000000-0000-0000-0000-000000000000")
            .then()
                .statusCode(404);
    }

    @Test
    @Order(22)
    void cancelNonExistentJob_returns404() {
        given()
            .when()
                .delete("/api/jobs/00000000-0000-0000-0000-000000000000")
            .then()
                .statusCode(404);
    }

    // ==================== Instances API Tests ====================

    @Test
    @Order(30)
    void listInstances_returnsCurrentInstance() {
        given()
            .when()
                .get("/api/instances")
            .then()
                .statusCode(200)
                .body("$", hasSize(greaterThanOrEqualTo(1)))
                .body("[0].self", equalTo(true));
    }

    @Test
    @Order(31)
    void getSelfInstance_returnsInfo() {
        given()
            .when()
                .get("/api/instances/self")
            .then()
                .statusCode(200)
                .body("self", equalTo(true))
                .body("shardIndex", greaterThanOrEqualTo(0))
                .body("shardCount", greaterThanOrEqualTo(1));
    }

    @Test
    @Order(32)
    void getShardForKey_returnsOwner() {
        given()
            .queryParam("key", "test-key-123")
            .when()
                .get("/api/instances/shard")
            .then()
                .statusCode(200)
                .body("key", equalTo("test-key-123"))
                .body("shard", greaterThanOrEqualTo(0))
                .body("owner", notNullValue());
    }

    // ==================== Infrastructure Tests ====================

    @Test
    @Order(40)
    void prometheusMetrics_areExposed() {
        given()
            .when()
                .get("/q/metrics")
            .then()
                .statusCode(200)
                .body(containsString("jvm_memory"));
    }

    @Test
    @Order(41)
    void livenessEndpoint_returnsUp() {
        given()
            .when()
                .get("/q/health/live")
            .then()
                .statusCode(200)
                .body("status", equalTo("UP"));
    }

    @Test
    @Order(42)
    void readinessEndpoint_returnsUp() {
        given()
            .when()
                .get("/q/health/ready")
            .then()
                .statusCode(200)
                .body("status", equalTo("UP"));
    }

    // ==================== Helper Methods ====================

    private static void waitForHealth(Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                int status = given()
                    .when()
                        .get("/q/health/ready")
                    .then()
                        .extract().statusCode();
                if (status == 200) {
                    System.out.println("Scheduler is healthy!");
                    return;
                }
            } catch (Exception e) {
                // Ignore connection errors during startup
            }
            Thread.sleep(2000);
        }
        throw new RuntimeException("Scheduler did not become healthy within " + timeout);
    }

    private static void runCommand(String command) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("sh", "-c", command);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }

        boolean finished = process.waitFor(5, TimeUnit.MINUTES);
        if (!finished) {
            process.destroyForcibly();
            throw new RuntimeException("Command timed out: " + command);
        }

        int exitCode = process.exitValue();
        if (exitCode != 0) {
            throw new RuntimeException("Command failed with exit code " + exitCode + ": " + command);
        }
    }
}
