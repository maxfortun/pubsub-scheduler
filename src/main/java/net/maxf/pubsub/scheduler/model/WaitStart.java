package net.maxf.pubsub.scheduler.model;

public enum WaitStart {
    SELF,   // Sleep from this job's arrival time (default)
    PREV    // Sleep from predecessor's completion (QUEUE mode only)
}
