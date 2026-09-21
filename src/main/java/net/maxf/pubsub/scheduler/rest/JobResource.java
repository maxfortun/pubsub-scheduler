package net.maxf.pubsub.scheduler.rest;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.KeyPolicy;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import net.maxf.pubsub.scheduler.model.WaitStart;
import net.maxf.pubsub.scheduler.service.JobQueueService;
import net.maxf.pubsub.scheduler.service.JobStoreService;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

@Path("/api/jobs")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "Jobs", description = "Manage scheduled jobs")
public class JobResource {

    private static final Pattern KAFKA_TOPIC_PATTERN = Pattern.compile("^[a-zA-Z0-9._-]+$");
    private static final int MAX_TOPIC_LENGTH = 249;
    private static final int MAX_JOB_KEY_LENGTH = 255;
    private static final int MAX_MESSAGE_KEY_SIZE = 1024 * 1024;
    private static final int MAX_MESSAGE_VALUE_SIZE = 10 * 1024 * 1024;
    private static final int MAX_HEADER_KEY_LENGTH = 255;
    private static final int MAX_HEADER_VALUE_LENGTH = 10000;
    private static final int MAX_RETRIES_LIMIT = 100;

    @Inject
    JobStoreService jobStore;

    @Inject
    JobQueueService jobQueue;

    @ConfigProperty(name = "scheduler.api.max-limit", defaultValue = "1000")
    int maxLimit;

    @GET
    @Operation(
        summary = "List jobs with pagination",
        description = "Returns a paginated list of scheduled jobs. Results can be filtered by state and job key. " +
            "The limit parameter is capped by scheduler.api.max-limit configuration."
    )
    @APIResponses({
        @APIResponse(
            responseCode = "200",
            description = "Paginated list of jobs",
            content = @Content(
                mediaType = MediaType.APPLICATION_JSON,
                schema = @Schema(implementation = JobPagedResult.class)
            )
        )
    })
    public PagedResult<ScheduledJob> listJobs(
            @Parameter(
                description = "Filter by job state",
                schema = @Schema(enumeration = {"PENDING", "WAITING", "ACQUIRED", "RUNNING", "DONE", "FAILED"})
            )
            @QueryParam("state") JobState state,
            @Parameter(description = "Filter by job key (exact match)")
            @QueryParam("key") String jobKey,
            @Parameter(
                description = "Number of results to skip (0-indexed). Negative values are clamped to 0.",
                schema = @Schema(minimum = "0", defaultValue = "0")
            )
            @QueryParam("offset") @DefaultValue("0") int offset,
            @Parameter(
                description = "Maximum number of results per page. Values are clamped to [1, scheduler.api.max-limit].",
                schema = @Schema(minimum = "1", maximum = "1000", defaultValue = "100")
            )
            @QueryParam("limit") @DefaultValue("100") int limit) {
        int effectiveOffset = Math.max(0, offset);
        int effectiveLimit = Math.min(Math.max(1, limit), maxLimit);
        String sanitizedKey = jobKey != null ? jobKey.trim() : null;
        if (sanitizedKey != null && sanitizedKey.length() > MAX_JOB_KEY_LENGTH) {
            sanitizedKey = sanitizedKey.substring(0, MAX_JOB_KEY_LENGTH);
        }
        return jobStore.findJobsPaged(state, sanitizedKey, effectiveOffset, effectiveLimit);
    }

    @GET
    @Path("/{id}")
    @Operation(summary = "Get job by ID", description = "Retrieve a specific scheduled job by its unique identifier")
    @APIResponses({
        @APIResponse(
            responseCode = "200",
            description = "Job found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ScheduledJob.class))
        ),
        @APIResponse(responseCode = "404", description = "Job not found")
    })
    public Response getJob(
            @Parameter(description = "Job UUID", required = true)
            @PathParam("id") UUID id) {
        return jobStore.findById(id)
                .map(job -> Response.ok(job).build())
                .orElse(Response.status(Response.Status.NOT_FOUND).build());
    }

    @DELETE
    @Path("/{id}")
    @Operation(
        summary = "Cancel job",
        description = "Cancel a pending or waiting scheduled job. Jobs that are already acquired, running, done, or failed cannot be cancelled."
    )
    @APIResponses({
        @APIResponse(responseCode = "204", description = "Job cancelled successfully"),
        @APIResponse(responseCode = "404", description = "Job not found or not in cancellable state")
    })
    public Response cancelJob(
            @Parameter(description = "Job UUID", required = true)
            @PathParam("id") UUID id) {
        boolean cancelled = jobStore.cancelJob(id);
        if (cancelled) {
            return Response.noContent().build();
        }
        return Response.status(Response.Status.NOT_FOUND).build();
    }

    @GET
    @Path("/stats")
    @Operation(summary = "Get job statistics", description = "Returns aggregate counts of jobs grouped by state")
    @APIResponse(
        responseCode = "200",
        description = "Job statistics",
        content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = JobStats.class))
    )
    public JobStats getStats() {
        return jobStore.getStats();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(
        summary = "Create a scheduled job",
        description = "Creates a new scheduled job. Specify timing using one of: runAt (absolute ISO-8601 timestamp), " +
            "waitDuration (ISO-8601 duration like PT30S, PT5M, PT1H, P1D), or cronExpression (5-part cron)."
    )
    @APIResponses({
        @APIResponse(
            responseCode = "201",
            description = "Job created successfully",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ScheduledJob.class))
        ),
        @APIResponse(responseCode = "400", description = "Invalid request")
    })
    public Response createJob(CreateJobRequest request) {
        List<String> errors = validateCreateRequest(request);
        if (!errors.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", String.join("; ", errors)))
                    .build();
        }

        ScheduledJob job = new ScheduledJob();
        job.setDestinationTopic(request.destinationTopic.trim());
        job.setJobKey(request.jobKey != null ? request.jobKey.trim() : null);
        job.setKeyPolicy(request.keyPolicy != null ? request.keyPolicy : KeyPolicy.QUEUE);
        job.setMaxRetries(request.maxRetries != null ? Math.min(request.maxRetries, MAX_RETRIES_LIMIT) : 3);

        if (request.messageKey != null) {
            job.setMessageKey(request.messageKey.getBytes(StandardCharsets.UTF_8));
        }
        if (request.messageValue != null) {
            job.setMessageValue(request.messageValue.getBytes(StandardCharsets.UTF_8));
        }
        if (request.headers != null) {
            job.setHeaders(request.headers);
        }

        Instant now = Instant.now();
        if (request.cronExpression != null) {
            job.setCronExpression(request.cronExpression.trim());
            job.setCronRepeat(request.cronRepeat != null ? Math.max(0, request.cronRepeat) : null);
            job.setCronUntil(request.cronUntil);
        } else if (request.waitDuration != null) {
            job.setWaitDuration(request.waitDuration.trim());
            job.setWaitStart(request.waitStart != null ? request.waitStart : WaitStart.SELF);
            job.setWaitRepeat(request.waitRepeat != null ? Math.max(0, request.waitRepeat) : 1);
            job.setWaitUntil(request.waitUntil);
            Duration d = Duration.parse(request.waitDuration.trim());
            job.setRunAt(now.plus(d));
            job.setEffectiveRunAt(job.getRunAt());
        } else if (request.runAt != null) {
            job.setRunAt(request.runAt);
            job.setEffectiveRunAt(request.runAt);
        } else {
            job.setRunAt(now);
            job.setEffectiveRunAt(now);
        }

        jobStore.save(job);
        jobQueue.enqueue(job);

        return Response.status(Response.Status.CREATED).entity(job).build();
    }

    private List<String> validateCreateRequest(CreateJobRequest request) {
        List<String> errors = new ArrayList<>();

        if (request.destinationTopic == null || request.destinationTopic.isBlank()) {
            errors.add("destinationTopic is required");
        } else {
            String topic = request.destinationTopic.trim();
            if (topic.length() > MAX_TOPIC_LENGTH) {
                errors.add("destinationTopic exceeds maximum length of " + MAX_TOPIC_LENGTH);
            }
            if (!KAFKA_TOPIC_PATTERN.matcher(topic).matches()) {
                errors.add("destinationTopic contains invalid characters (allowed: alphanumeric, dots, dashes, underscores)");
            }
        }

        if (request.jobKey != null && request.jobKey.trim().length() > MAX_JOB_KEY_LENGTH) {
            errors.add("jobKey exceeds maximum length of " + MAX_JOB_KEY_LENGTH);
        }

        if (request.messageKey != null && request.messageKey.getBytes(StandardCharsets.UTF_8).length > MAX_MESSAGE_KEY_SIZE) {
            errors.add("messageKey exceeds maximum size of " + (MAX_MESSAGE_KEY_SIZE / 1024) + "KB");
        }

        if (request.messageValue != null && request.messageValue.getBytes(StandardCharsets.UTF_8).length > MAX_MESSAGE_VALUE_SIZE) {
            errors.add("messageValue exceeds maximum size of " + (MAX_MESSAGE_VALUE_SIZE / 1024 / 1024) + "MB");
        }

        if (request.headers != null) {
            for (Map.Entry<String, String> entry : request.headers.entrySet()) {
                if (entry.getKey() == null || entry.getKey().length() > MAX_HEADER_KEY_LENGTH) {
                    errors.add("header key exceeds maximum length of " + MAX_HEADER_KEY_LENGTH);
                }
                if (entry.getValue() != null && entry.getValue().length() > MAX_HEADER_VALUE_LENGTH) {
                    errors.add("header value for '" + entry.getKey() + "' exceeds maximum length of " + MAX_HEADER_VALUE_LENGTH);
                }
            }
        }

        if (request.maxRetries != null && request.maxRetries < 0) {
            errors.add("maxRetries must be non-negative");
        }

        if (request.waitDuration != null) {
            try {
                Duration.parse(request.waitDuration.trim());
            } catch (DateTimeParseException e) {
                errors.add("waitDuration is not a valid ISO-8601 duration (e.g., PT1H, PT30M, P1D)");
            }
        }

        if (request.waitRepeat != null && request.waitRepeat < 0) {
            errors.add("waitRepeat must be non-negative (0 for infinite)");
        }

        if (request.cronRepeat != null && request.cronRepeat < 0) {
            errors.add("cronRepeat must be non-negative");
        }

        return errors;
    }

    @Schema(description = "Request to create a new scheduled job")
    public static class CreateJobRequest {
        @Schema(description = "Destination topic for the message", required = true, example = "my-output-topic")
        public String destinationTopic;

        @Schema(description = "Job key for deduplication/chaining", example = "order-123")
        public String jobKey;

        @Schema(description = "Policy when job key already exists", example = "QUEUE")
        public KeyPolicy keyPolicy;

        @Schema(description = "Absolute fire time (ISO-8601)", example = "2024-12-31T23:59:59Z")
        public Instant runAt;

        @Schema(description = "ISO-8601 duration (PT30S=30sec, PT5M=5min, PT1H=1hr, P1D=1day)", example = "PT1H")
        public String waitDuration;

        @Schema(description = "Sleep start reference", example = "SELF")
        public WaitStart waitStart;

        @Schema(description = "Number of times to repeat (0 = infinite)", example = "1")
        public Integer waitRepeat;

        @Schema(description = "End time for duration-based repeating jobs")
        public Instant waitUntil;

        @Schema(description = "Cron expression for recurring jobs", example = "0 0 * * *")
        public String cronExpression;

        @Schema(description = "End time for cron jobs")
        public Instant cronUntil;

        @Schema(description = "Maximum fire count for cron jobs", example = "10")
        public Integer cronRepeat;

        @Schema(description = "Max retry attempts on failure", example = "3")
        public Integer maxRetries;

        @Schema(description = "Kafka message key", example = "my-key")
        public String messageKey;

        @Schema(description = "Message payload", example = "{\"orderId\": 123}")
        public String messageValue;

        @Schema(description = "Additional message headers")
        public Map<String, String> headers;
    }

    @Schema(description = "Aggregate counts of jobs by state")
    public record JobStats(
            @Schema(description = "Jobs pending delivery", example = "42")
            long pending,
            @Schema(description = "Jobs waiting for predecessor to done", example = "10")
            long waiting,
            @Schema(description = "Jobs acquired by an instance for processing", example = "3")
            long acquired,
            @Schema(description = "Jobs currently being delivered", example = "1")
            long running,
            @Schema(description = "Jobs successfully delivered", example = "1523")
            long done,
            @Schema(description = "Jobs that failed after all retries", example = "7")
            long failed
    ) {}

    @Schema(description = "Paginated result containing a list of items with metadata")
    public record PagedResult<T>(
            @Schema(description = "Items in the current page")
            List<T> items,
            @Schema(description = "Starting position (0-indexed)", example = "0")
            int offset,
            @Schema(description = "Page size (capped by scheduler.api.max-limit)", example = "100")
            int limit,
            @Schema(description = "Total count of items matching the filters", example = "1523")
            long total,
            @Schema(description = "True if more results exist beyond this page", example = "true")
            boolean hasMore
    ) {
        public static <T> PagedResult<T> of(List<T> items, int offset, int limit, long total) {
            return new PagedResult<>(items, offset, limit, total, offset + items.size() < total);
        }
    }

    @Schema(name = "JobPagedResult", description = "Paginated list of scheduled jobs")
    public static class JobPagedResult {
        @Schema(description = "Jobs in the current page")
        public List<ScheduledJob> items;
        @Schema(description = "Starting position (0-indexed)", example = "0")
        public int offset;
        @Schema(description = "Page size", example = "100")
        public int limit;
        @Schema(description = "Total count matching filters", example = "1523")
        public long total;
        @Schema(description = "True if more results exist", example = "true")
        public boolean hasMore;
    }
}
