package net.maxf.pubsub.scheduler.model;

public enum AdvisoryEvent {
    // Entry states
    JOB_QUEUED,         // Job accepted, pending delivery
    JOB_CHAINED,        // Linked behind predecessor (QUEUE policy)

    // Transitions
    JOB_PROMOTED,       // Predecessor done, ready for delivery
    JOB_RUNNING,        // Delivering to destination

    // Successful exits
    JOB_COMPLETE,       // Delivered successfully
    JOB_EXPIRED,        // Recurring job finished

    // Cancelled exits
    JOB_SKIPPED,        // Dropped (SKIP policy)
    JOB_REPLACED,       // Cancelled by incoming REPLACE

    // Failed exits
    JOB_FAILED,         // Failed after retries
    JOB_CASCADE_FAILED  // Failed due to predecessor failure
}
