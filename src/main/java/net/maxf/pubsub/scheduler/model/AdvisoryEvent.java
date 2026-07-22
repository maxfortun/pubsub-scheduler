package net.maxf.pubsub.scheduler.model;

public enum AdvisoryEvent {
    JOB_SCHEDULED,      // Job accepted and queued
    JOB_WAITING,        // Queued behind predecessor (QUEUE mode)
    JOB_SKIPPED,        // Dropped due to SKIP mode
    JOB_REPLACED,       // Cancelled by incoming REPLACE
    JOB_PROMOTED,       // WAITING -> PENDING (predecessor done)
    JOB_FIRING,         // About to publish to destination
    JOB_COMPLETE,       // Successfully published
    JOB_FAILED,         // Failed after retries
    JOB_CASCADE_FAILED  // Failed due to predecessor failure
}
