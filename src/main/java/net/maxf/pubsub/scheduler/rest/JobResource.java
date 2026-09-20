package net.maxf.pubsub.scheduler.rest;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
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

import java.util.List;
import java.util.UUID;

@Path("/api/jobs")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "Jobs", description = "Manage scheduled jobs")
public class JobResource {

    @Inject
    JobStoreService jobStore;

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
                schema = @Schema(enumeration = {"PENDING", "WAITING", "ACQUIRED", "FIRING", "COMPLETE", "FAILED"})
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
        return jobStore.findJobsPaged(state, jobKey, effectiveOffset, effectiveLimit);
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
        description = "Cancel a pending or waiting scheduled job. Jobs that are already acquired, firing, complete, or failed cannot be cancelled."
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

    @Schema(description = "Aggregate counts of jobs by state")
    public record JobStats(
            @Schema(description = "Jobs pending delivery", example = "42")
            long pending,
            @Schema(description = "Jobs waiting for predecessor to complete", example = "10")
            long waiting,
            @Schema(description = "Jobs acquired by an instance for processing", example = "3")
            long acquired,
            @Schema(description = "Jobs currently being delivered", example = "1")
            long firing,
            @Schema(description = "Jobs successfully delivered", example = "1523")
            long complete,
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
