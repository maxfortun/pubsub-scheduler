package net.maxf.pubsub.scheduler.transform;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import org.jboss.logging.Logger;

/**
 * Produces transformer beans based on configuration.
 * Reads scheduler.transform.pre.type and scheduler.transform.post.type
 * to determine which transformer implementations to create.
 */
@ApplicationScoped
public class TransformProducer {

    private static final Logger LOG = Logger.getLogger(TransformProducer.class);

    @Inject
    TransformConfig config;

    @Produces
    @PreSchedulingTransformer
    @ApplicationScoped
    public MessageTransformer producePreTransformer() {
        String type = config.pre().type().orElse(null);
        if (type == null || type.isBlank()) {
            LOG.debug("No pre-scheduling transformer configured");
            return new NoOpTransformer();
        }

        LOG.infof("Creating pre-scheduling transformer: %s", type);
        return createTransformer(type, true);
    }

    @Produces
    @PostSchedulingTransformer
    @ApplicationScoped
    public MessageTransformer producePostTransformer() {
        String type = config.post().type().orElse(null);
        if (type == null || type.isBlank()) {
            LOG.debug("No post-scheduling transformer configured");
            return new NoOpTransformer();
        }

        LOG.infof("Creating post-scheduling transformer: %s", type);
        return createTransformer(type, false);
    }

    private MessageTransformer createTransformer(String type, boolean isPre) {
        return switch (type.toLowerCase()) {
            case "http" -> {
                TransformConfig.HttpConfig httpConfig = isPre
                        ? config.pre().http()
                        : config.post().http();
                yield new HttpTransformer(httpConfig);
            }
            default -> throw new IllegalArgumentException("Unknown transformer type: " + type);
        };
    }

    /**
     * No-op transformer that accepts nothing and does nothing.
     * Used when no transformer is configured.
     */
    private static class NoOpTransformer implements MessageTransformer {
        @Override
        public boolean accepts(ScheduledJob job) {
            return false;
        }

        @Override
        public ScheduledJob transform(ScheduledJob job) {
            return job;
        }
    }
}
