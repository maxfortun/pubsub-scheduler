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
    void listJobsReturnsEmptyList() {
        given()
            .when()
                .get("/api/jobs")
            .then()
                .statusCode(200)
                .body("$", hasSize(0));
    }

    @Test
    @Order(5)
    void getJobStatsReturnsZeroCounts() {
        given()
            .when()
                .get("/api/jobs/stats")
            .then()
                .statusCode(200)
                .body("pending", equalTo(0))
                .body("waiting", equalTo(0))
                .body("failed", equalTo(0));
    }

    @Test
    @Order(6)
    void listInstancesReturnsCurrentInstance() {
        given()
            .when()
                .get("/api/instances")
            .then()
                .statusCode(200)
                .body("$", hasSize(greaterThanOrEqualTo(1)))
                .body("[0].self", equalTo(true));
    }

    @Test
    @Order(7)
    void getSelfInstanceReturnsInfo() {
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
    @Order(8)
    void getShardForKeyReturnsOwner() {
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

    @Test
    @Order(9)
    void getNonExistentJobReturns404() {
        given()
            .when()
                .get("/api/jobs/00000000-0000-0000-0000-000000000000")
            .then()
                .statusCode(404);
    }

    @Test
    @Order(10)
    void prometheusMetricsAreExposed() {
        given()
            .when()
                .get("/q/metrics")
            .then()
                .statusCode(200)
                .body(containsString("jvm_memory"));
    }

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
