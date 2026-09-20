package net.maxf.pubsub.scheduler.model;

public enum AdvisoryEvent {
    JOB_QUEUED,         // Job accepted, pending delivery
    JOB_WAITING,        // Blocked behind predecessor (QUEUE policy)
    JOB_SKIPPED,        // Dropped due to SKIP policy
    JOB_REPLACED,       // Cancelled by incoming REPLACE
    JOB_PROMOTED,       // WAITING -> PENDING (predecessor done)
    JOB_FIRING,         // Delivering to destination
    JOB_COMPLETE,       // Delivered successfully
    JOB_EXPIRED,        // Recurring job finished
    JOB_FAILED,         // Failed after retries
    JOB_CASCADE_FAILED  // Failed due to predecessor failure
}
