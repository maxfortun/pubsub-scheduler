package net.maxf.pubsub.scheduler.transform;

import net.maxf.pubsub.scheduler.model.ScheduledJob;

/**
 * Base interface for message transformers.
 * Transformers can modify the job's message payload and headers.
 */
public interface MessageTransformer {

    /**
     * Transform the job's message content.
     *
     * @param job the scheduled job to transform
     * @return the transformed job (may be the same instance, modified in place)
     * @throws TransformException if transformation fails
     */
    ScheduledJob transform(ScheduledJob job) throws TransformException;

    /**
     * Check if this transformer should process the given job.
     * Allows transformers to be selective based on job properties.
     *
     * @param job the job to check
     * @return true if this transformer should process the job
     */
    default boolean accepts(ScheduledJob job) {
        return true;
    }
}
