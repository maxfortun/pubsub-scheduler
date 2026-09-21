package net.maxf.pubsub.scheduler.model;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class ScheduledJob implements Delayed {

    private UUID id;
    private String jobKey;
    private KeyPolicy keyPolicy;
    private WaitStart waitStart;
    private String waitDuration;
    private int waitRepeat;
    private Instant waitUntil;
    private String cronExpression;
    private Instant cronUntil;
    private Integer cronRepeat;
    private int cronRunCount;

    private Instant runAt;
    private Instant effectiveRunAt;
    private Instant arrivedAt;

    private String destinationTopic;
    private byte[] messageKey;
    private byte[] messageValue;
    private Map<String, String> headers;
    private String advisoryHeadersPattern;

    private JobState state;
    private int maxRetries;
    private int retryCount;
    private int version;

    private UUID predecessorId;
    private int sequenceNum;

    private String acquiredBy;
    private Instant acquiredAt;
    private Instant createdAt;
    private Instant updatedAt;

    private String lastError;

    public ScheduledJob() {
        this.id = UUID.randomUUID();
        this.state = JobState.PENDING;
        this.keyPolicy = KeyPolicy.QUEUE;
        this.waitStart = WaitStart.SELF;
        this.waitRepeat = 1;
        this.retryCount = 0;
        this.version = 0;
        this.createdAt = Instant.now();
        this.arrivedAt = Instant.now();
    }

    @Override
    public long getDelay(TimeUnit unit) {
        Instant fireTime = getFireTimeOrNow();
        long delayMillis = fireTime.toEpochMilli() - System.currentTimeMillis();
        return unit.convert(delayMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        if (other instanceof ScheduledJob otherJob) {
            return getFireTimeOrNow().compareTo(otherJob.getFireTimeOrNow());
        }
        return Long.compare(getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
    }

    private Instant getFireTimeOrNow() {
        if (effectiveRunAt != null) return effectiveRunAt;
        if (runAt != null) return runAt;
        return Instant.now();
    }

    // Getters and setters

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public String getJobKey() { return jobKey; }
    public void setJobKey(String jobKey) { this.jobKey = jobKey; }

    public KeyPolicy getKeyPolicy() { return keyPolicy; }
    public void setKeyPolicy(KeyPolicy keyPolicy) { this.keyPolicy = keyPolicy; }

    public WaitStart getWaitStart() { return waitStart; }
    public void setWaitStart(WaitStart waitStart) { this.waitStart = waitStart; }

    public String getCronExpression() { return cronExpression; }
    public void setCronExpression(String cronExpression) { this.cronExpression = cronExpression; }

    public String getWaitDuration() { return waitDuration; }
    public void setWaitDuration(String waitDuration) { this.waitDuration = waitDuration; }

    public int getWaitRepeat() { return waitRepeat; }
    public void setWaitRepeat(int waitRepeat) { this.waitRepeat = waitRepeat; }

    public Instant getWaitUntil() { return waitUntil; }
    public void setWaitUntil(Instant waitUntil) { this.waitUntil = waitUntil; }

    public boolean isRepeating() { return waitRepeat != 1 || waitUntil != null || cronExpression != null; }

    public boolean isCron() { return cronExpression != null; }

    public Instant getCronUntil() { return cronUntil; }
    public void setCronUntil(Instant cronUntil) { this.cronUntil = cronUntil; }

    public Integer getCronRepeat() { return cronRepeat; }
    public void setCronRepeat(Integer cronRepeat) { this.cronRepeat = cronRepeat; }

    public int getCronRunCount() { return cronRunCount; }
    public void setCronRunCount(int cronRunCount) { this.cronRunCount = cronRunCount; }

    public Instant getRunAt() { return runAt; }
    public void setRunAt(Instant runAt) { this.runAt = runAt; }

    public Instant getEffectiveRunAt() { return effectiveRunAt; }
    public void setEffectiveRunAt(Instant effectiveRunAt) { this.effectiveRunAt = effectiveRunAt; }

    public Instant getArrivedAt() { return arrivedAt; }
    public void setArrivedAt(Instant arrivedAt) { this.arrivedAt = arrivedAt; }

    public String getDestinationTopic() { return destinationTopic; }
    public void setDestinationTopic(String destinationTopic) { this.destinationTopic = destinationTopic; }

    public byte[] getMessageKey() { return messageKey; }
    public void setMessageKey(byte[] messageKey) { this.messageKey = messageKey; }

    public byte[] getMessageValue() { return messageValue; }
    public void setMessageValue(byte[] messageValue) { this.messageValue = messageValue; }

    public Map<String, String> getHeaders() { return headers; }
    public void setHeaders(Map<String, String> headers) { this.headers = headers; }

    public String getAdvisoryHeadersPattern() { return advisoryHeadersPattern; }
    public void setAdvisoryHeadersPattern(String advisoryHeadersPattern) { this.advisoryHeadersPattern = advisoryHeadersPattern; }

    public JobState getState() { return state; }
    public void setState(JobState state) { this.state = state; }

    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }

    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }

    public int getVersion() { return version; }
    public void setVersion(int version) { this.version = version; }

    public UUID getPredecessorId() { return predecessorId; }
    public void setPredecessorId(UUID predecessorId) { this.predecessorId = predecessorId; }

    public int getSequenceNum() { return sequenceNum; }
    public void setSequenceNum(int sequenceNum) { this.sequenceNum = sequenceNum; }

    public String getAcquiredBy() { return acquiredBy; }
    public void setAcquiredBy(String acquiredBy) { this.acquiredBy = acquiredBy; }

    public Instant getAcquiredAt() { return acquiredAt; }
    public void setAcquiredAt(Instant acquiredAt) { this.acquiredAt = acquiredAt; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }

    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }
}
