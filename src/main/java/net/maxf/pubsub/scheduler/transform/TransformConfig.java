package net.maxf.pubsub.scheduler.transform;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.util.Optional;

@ConfigMapping(prefix = "scheduler.transform")
public interface TransformConfig {

    PreTransformConfig pre();

    PostTransformConfig post();

    interface PreTransformConfig {
        /**
         * Transformer type (e.g., "http"). Empty means disabled.
         */
        Optional<String> type();

        HttpConfig http();
    }

    interface PostTransformConfig {
        /**
         * Transformer type (e.g., "http"). Empty means disabled.
         */
        Optional<String> type();

        HttpConfig http();
    }

    interface HttpConfig {
        /**
         * URL for the HTTP request. For post-transform, use {ref} placeholder.
         */
        Optional<String> url();

        /**
         * HTTP method (GET, POST, PUT).
         */
        @WithDefault("POST")
        String method();

        /**
         * Minimum payload size (bytes) to trigger transform.
         */
        @WithDefault("0")
        int thresholdBytes();

        /**
         * Optional Authorization header value.
         */
        Optional<String> authorization();

        /**
         * Connection timeout in milliseconds.
         */
        @WithDefault("5000")
        int connectTimeoutMs();

        /**
         * Read timeout in milliseconds.
         */
        @WithDefault("30000")
        int readTimeoutMs();
    }
}
