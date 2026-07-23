package net.maxf.pubsub.scheduler.rest;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import net.maxf.pubsub.scheduler.service.JobStoreService;

import java.util.List;
import java.util.UUID;

@Path("/api/jobs")
@Produces(MediaType.APPLICATION_JSON)
public class JobResource {

    @Inject
    JobStoreService jobStore;

    @GET
    public List<ScheduledJob> listJobs(
            @QueryParam("state") JobState state,
            @QueryParam("key") String jobKey,
            @QueryParam("limit") @DefaultValue("100") int limit) {
        return jobStore.findJobs(state, jobKey, limit);
    }

    @GET
    @Path("/{id}")
    public Response getJob(@PathParam("id") UUID id) {
        return jobStore.findById(id)
                .map(job -> Response.ok(job).build())
                .orElse(Response.status(Response.Status.NOT_FOUND).build());
    }

    @DELETE
    @Path("/{id}")
    public Response cancelJob(@PathParam("id") UUID id) {
        boolean cancelled = jobStore.cancelJob(id);
        if (cancelled) {
            return Response.noContent().build();
        }
        return Response.status(Response.Status.NOT_FOUND).build();
    }

    @GET
    @Path("/stats")
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
