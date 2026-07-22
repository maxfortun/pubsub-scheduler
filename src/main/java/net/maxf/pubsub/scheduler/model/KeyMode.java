package net.maxf.pubsub.scheduler.model;

public enum KeyMode {
    QUEUE,      // Wait for prior jobs with same key to complete
    REPLACE,    // Cancel all PENDING/WAITING jobs with same key
    SKIP        // Drop if any PENDING/WAITING job with same key exists
}
