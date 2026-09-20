package net.maxf.pubsub.scheduler.transform;

import jakarta.enterprise.inject.Stereotype;
import jakarta.inject.Qualifier;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import static java.lang.annotation.ElementType.*;

/**
 * Qualifier for transformers that run before a job is stored.
 * Use for claim-check externalization, compression, encryption, etc.
 */
@Qualifier
@Retention(RetentionPolicy.RUNTIME)
@Target({TYPE, METHOD, FIELD, PARAMETER})
public @interface PreSchedulingTransformer {
}
