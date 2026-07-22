package com.maxfortun.scheduler.service;

import com.maxfortun.scheduler.model.AdvisoryEvent;
import com.maxfortun.scheduler.model.ScheduledJob;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.camel.ProducerTemplate;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

@ApplicationScoped
public class AdvisoryService {

    private static final Logger LOG = Logger.getLogger(AdvisoryService.class);

    @Inject
    ProducerTemplate producerTemplate;

    @ConfigProperty(name = "scheduler.advisory")
    String advisoryEndpoint;

    @ConfigProperty(name = "scheduler.default-advisory-headers", defaultValue = ".*")
    String defaultAdvisoryHeadersPattern;

    public void publish(ScheduledJob job, AdvisoryEvent event) {
        try {
            Map<String, Object> headers = buildAdvisoryHeaders(job, event);
            String body = buildAdvisoryBody(job, event);

            producerTemplate.sendBodyAndHeaders(advisoryEndpoint, body, headers);
            LOG.debugf("Published advisory %s for job %s", event, job.getId());

        } catch (Exception e) {
            // Fire and forget - log warning but don't block
            LOG.warnf("Failed to publish advisory %s for job %s: %s", event, job.getId(), e.getMessage());
        }
    }

    private Map<String, Object> buildAdvisoryHeaders(ScheduledJob job, AdvisoryEvent event) {
        Map<String, Object> headers = new HashMap<>();

        // Standard advisory headers
        headers.put("SCHEDULER_ADVISORY_EVENT", event.name());
        headers.put("SCHEDULER_ADVISORY_TIMESTAMP", Instant.now().toString());
        headers.put("SCHEDULER_JOB_ID", job.getId().toString());

        if (job.getJobKey() != null) {
            headers.put("SCHEDULER_JOB_KEY", job.getJobKey());
            // Use job key as Kafka message key for partition affinity
            headers.put("kafka.KEY", job.getJobKey());
        }

        headers.put("SCHEDULER_JOB_STATE", job.getState().name());
        headers.put("SCHEDULER_DESTINATION", job.getDestinationTopic());

        if (job.getFireAt() != null) {
            headers.put("SCHEDULER_FIRE_AT", job.getFireAt().toString());
        }
        if (job.getEffectiveFireAt() != null) {
            headers.put("SCHEDULER_EFFECTIVE_FIRE_AT", job.getEffectiveFireAt().toString());
        }
        if (job.getPredecessorId() != null) {
            headers.put("SCHEDULER_PREDECESSOR_ID", job.getPredecessorId().toString());
        }
        if (job.getLastError() != null) {
            headers.put("SCHEDULER_ERROR", job.getLastError());
        }

        headers.put("SCHEDULER_RETRY_COUNT", String.valueOf(job.getRetryCount()));
        headers.put("SCHEDULER_MAX_RETRIES", String.valueOf(job.getMaxRetries()));

        // Include filtered original headers
        addFilteredHeaders(job, headers);

        return headers;
    }

    private void addFilteredHeaders(ScheduledJob job, Map<String, Object> advisoryHeaders) {
        if (job.getHeaders() == null || job.getHeaders().isEmpty()) {
            return;
        }

        String patternStr = job.getAdvisoryHeadersPattern() != null
                ? job.getAdvisoryHeadersPattern()
                : defaultAdvisoryHeadersPattern;

        Pattern pattern = Pattern.compile(patternStr);

        for (Map.Entry<String, String> entry : job.getHeaders().entrySet()) {
            String headerName = entry.getKey();
            // Skip SCHEDULER_ headers - they're internal
            if (headerName.startsWith("SCHEDULER_")) {
                continue;
            }
            if (pattern.matcher(headerName).matches()) {
                advisoryHeaders.put(headerName, entry.getValue());
            }
        }
    }

    private String buildAdvisoryBody(ScheduledJob job, AdvisoryEvent event) {
        // Simple JSON body - metadata only, no payload
        return """
                {
                  "event": "%s",
                  "timestamp": "%s",
                  "jobId": "%s",
                  "jobKey": %s,
                  "state": "%s",
                  "destination": "%s",
                  "fireAt": %s,
                  "effectiveFireAt": %s,
                  "retryCount": %d,
                  "maxRetries": %d,
                  "error": %s
                }
                """.formatted(
                event.name(),
                Instant.now(),
                job.getId(),
                job.getJobKey() != null ? "\"" + job.getJobKey() + "\"" : "null",
                job.getState().name(),
                job.getDestinationTopic(),
                job.getFireAt() != null ? "\"" + job.getFireAt() + "\"" : "null",
                job.getEffectiveFireAt() != null ? "\"" + job.getEffectiveFireAt() + "\"" : "null",
                job.getRetryCount(),
                job.getMaxRetries(),
                job.getLastError() != null ? "\"" + job.getLastError() + "\"" : "null"
        );
    }
}
