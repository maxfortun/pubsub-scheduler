package net.maxf.pubsub.model;

public enum JobState {
    WAITING,    // Queued behind predecessor (QUEUE mode)
    PENDING,    // In DelayQueue, ready to fire when time comes
    ACQUIRED,   // Claimed by scheduler instance
    FIRING,     // Publishing to destination in progress
    COMPLETE,   // Successfully published
    FAILED      // Failed after retries (triggers cascade for QUEUE mode)
}
