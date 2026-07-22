package com.maxfortun.scheduler.model;

public enum DelayStart {
    SELF,   // Delay from this job's arrival time (default)
    PREV    // Delay from predecessor's completion (QUEUE mode only)
}
