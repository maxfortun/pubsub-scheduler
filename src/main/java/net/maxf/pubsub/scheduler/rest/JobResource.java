package net.maxf.pubsub.scheduler.rest;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import net.maxf.pubsub.scheduler.service.JobStoreService;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.util.List;
import java.util.UUID;

@Path("/api/jobs")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "Jobs", description = "Manage scheduled jobs")
public class JobResource {

    @Inject
    JobStoreService jobStore;

    @GET
    @Operation(summary = "List jobs", description = "List scheduled jobs with optional filters")
    public List<ScheduledJob> listJobs(
            @Parameter(description = "Filter by job state") @QueryParam("state") JobState state,
            @Parameter(description = "Filter by job key") @QueryParam("key") String jobKey,
            @Parameter(description = "Maximum results") @QueryParam("limit") @DefaultValue("100") int limit) {
        return jobStore.findJobs(state, jobKey, limit);
    }

    @GET
    @Path("/{id}")
    @Operation(summary = "Get job", description = "Get a scheduled job by ID")
    @APIResponse(responseCode = "200", description = "Job found")
    @APIResponse(responseCode = "404", description = "Job not found")
    public Response getJob(@Parameter(description = "Job ID") @PathParam("id") UUID id) {
        return jobStore.findById(id)
                .map(job -> Response.ok(job).build())
                .orElse(Response.status(Response.Status.NOT_FOUND).build());
    }

    @DELETE
    @Path("/{id}")
    @Operation(summary = "Cancel job", description = "Cancel a pending scheduled job")
    @APIResponse(responseCode = "204", description = "Job cancelled")
    @APIResponse(responseCode = "404", description = "Job not found")
    public Response cancelJob(@Parameter(description = "Job ID") @PathParam("id") UUID id) {
        boolean cancelled = jobStore.cancelJob(id);
        if (cancelled) {
            return Response.noContent().build();
        }
        return Response.status(Response.Status.NOT_FOUND).build();
    }

    @GET
    @Path("/stats")
    @Operation(summary = "Get job statistics", description = "Get counts of jobs by state")
    public JobStats getStats() {
        return jobStore.getStats();
    }

    public record JobStats(
            long pending,
            long waiting,
            long acquired,
            long firing,
            long complete,
            long failed
    ) {}
}
