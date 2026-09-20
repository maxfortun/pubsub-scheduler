package net.maxf.pubsub.scheduler.transform;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TransformService {

    private static final Logger LOG = Logger.getLogger(TransformService.class);

    @Inject
    @PreSchedulingTransformer
    Instance<MessageTransformer> preTransformers;

    @Inject
    @PostSchedulingTransformer
    Instance<MessageTransformer> postTransformers;

    /**
     * Apply all pre-scheduling transformers to the job.
     * Called after job is populated but before it is stored.
     */
    public ScheduledJob applyPreSchedulingTransforms(ScheduledJob job) throws TransformException {
        ScheduledJob result = job;
        for (MessageTransformer transformer : preTransformers) {
            if (transformer.accepts(result)) {
                LOG.debugf("Applying pre-scheduling transformer %s to job %s",
                        transformer.getClass().getSimpleName(), job.getId());
                result = transformer.transform(result);
            }
        }
        return result;
    }

    /**
     * Apply all post-scheduling transformers to the job.
     * Called after job is retrieved but before it is fired.
     */
    public ScheduledJob applyPostSchedulingTransforms(ScheduledJob job) throws TransformException {
        ScheduledJob result = job;
        for (MessageTransformer transformer : postTransformers) {
            if (transformer.accepts(result)) {
                LOG.debugf("Applying post-scheduling transformer %s to job %s",
                        transformer.getClass().getSimpleName(), job.getId());
                result = transformer.transform(result);
            }
        }
        return result;
    }

    /**
     * Check if any pre-scheduling transformers are configured.
     */
    public boolean hasPreTransformers() {
        return !preTransformers.isUnsatisfied();
    }

    /**
     * Check if any post-scheduling transformers are configured.
     */
    public boolean hasPostTransformers() {
        return !postTransformers.isUnsatisfied();
    }
}
