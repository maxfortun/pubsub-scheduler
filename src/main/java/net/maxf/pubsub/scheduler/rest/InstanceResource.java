package net.maxf.pubsub.scheduler.rest;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import net.maxf.pubsub.scheduler.service.InstanceRegistryService;

import java.time.Instant;
import java.util.List;

@Path("/api/instances")
@Produces(MediaType.APPLICATION_JSON)
public class InstanceResource {

    @Inject
    InstanceRegistryService instanceRegistry;

    @GET
    public List<InstanceView> listInstances() {
        List<String> liveIds = instanceRegistry.getLiveInstances();
        int shardCount = liveIds.size();

        return liveIds.stream()
            .map(id -> {
                int shardIndex = liveIds.indexOf(id);
                var info = instanceRegistry.getInstanceInfo(id).orElse(null);
                return new InstanceView(
                    id,
                    shardIndex,
                    shardCount,
                    info != null ? info.heartbeatAt() : null,
                    info != null ? info.startedAt() : null,
                    id.equals(instanceRegistry.getInstanceId())
                );
            })
            .toList();
    }

    @GET
    @Path("/self")
    public InstanceView getSelf() {
        String id = instanceRegistry.getInstanceId();
        var info = instanceRegistry.getInstanceInfo(id).orElse(null);
        return new InstanceView(
            id,
            instanceRegistry.getShardIndex(),
            instanceRegistry.getShardCount(),
            info != null ? info.heartbeatAt() : null,
            info != null ? info.startedAt() : null,
            true
        );
    }

    @GET
    @Path("/shard")
    public ShardInfo getShardForKey(@QueryParam("key") String key) {
        if (key == null || key.isBlank()) {
            throw new BadRequestException("key parameter is required");
        }
        List<String> liveIds = instanceRegistry.getLiveInstances();
        int shardCount = liveIds.size();
        int shard = Math.abs(key.hashCode()) % shardCount;
        String owner = shard < liveIds.size() ? liveIds.get(shard) : null;

        return new ShardInfo(key, shard, shardCount, owner,
            owner != null && owner.equals(instanceRegistry.getInstanceId()));
    }

    public record InstanceView(
        String instanceId,
        int shardIndex,
        int shardCount,
        Instant heartbeatAt,
        Instant startedAt,
        boolean self
    ) {}

    public record ShardInfo(
        String key,
        int shard,
        int shardCount,
        String owner,
        boolean ownedBySelf
    ) {}
}
