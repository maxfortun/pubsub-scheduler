package net.maxf.pubsub.scheduler.rest;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.util.Map;
import java.util.Optional;

@Path("/api/config")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "Config", description = "Scheduler configuration")
public class ConfigResource {

    @ConfigProperty(name = "scheduler.ui.name", defaultValue = "Scheduler")
    String uiName;

    @ConfigProperty(name = "scheduler.ui.color")
    Optional<String> uiColor;

    @ConfigProperty(name = "scheduler.instance-id")
    String instanceId;

    @GET
    @Operation(summary = "Get scheduler UI configuration")
    public Map<String, Object> getConfig() {
        return Map.of(
            "name", uiName,
            "color", uiColor.orElse(null) == null ? "" : uiColor.get(),
            "instanceId", instanceId
        );
    }
}
