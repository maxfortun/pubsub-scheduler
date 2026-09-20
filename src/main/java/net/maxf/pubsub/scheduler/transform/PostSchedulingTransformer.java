package net.maxf.pubsub.scheduler.transform;

import jakarta.inject.Qualifier;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import static java.lang.annotation.ElementType.*;

/**
 * Qualifier for transformers that run before a job is fired.
 * Use for claim-check retrieval, decompression, decryption, etc.
 */
@Qualifier
@Retention(RetentionPolicy.RUNTIME)
@Target({TYPE, METHOD, FIELD, PARAMETER})
public @interface PostSchedulingTransformer {
}
