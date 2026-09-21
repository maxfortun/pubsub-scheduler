package net.maxf.pubsub.scheduler.model;

public enum JobState {
    WAITING,    // Queued behind predecessor (QUEUE mode)
    PENDING,    // In DelayQueue, ready to fire when time comes
    ACQUIRED,   // Claimed by scheduler instance
    RUNNING,    // Publishing to destination in progress
    DONE,       // Successfully published
    FAILED      // Failed after retries (triggers cascade for QUEUE mode)
}
