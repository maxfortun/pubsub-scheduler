package net.maxf.pubsub.model;

public enum SleepStart {
    SELF,   // Sleep from this job's arrival time (default)
    PREV    // Sleep from predecessor's completion (QUEUE mode only)
}
