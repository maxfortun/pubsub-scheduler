package net.maxf.pubsub.scheduler.service;

import net.maxf.pubsub.scheduler.model.AdvisoryEvent;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
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
        headers.put("scheduler_advisory_event", event.name());
        headers.put("scheduler_advisory_timestamp", Instant.now().toString());
        headers.put("scheduler_job_id", job.getId().toString());

        if (job.getJobKey() != null) {
            headers.put("scheduler_job_key", job.getJobKey());
            // Use job key as Kafka message key for partition affinity
            headers.put("kafka.KEY", job.getJobKey());
        }

        headers.put("scheduler_job_state", job.getState().name());
        headers.put("scheduler_destination", job.getDestinationTopic());

        if (job.getRunAt() != null) {
            headers.put("scheduler_fire_at", job.getRunAt().toString());
        }
        if (job.getEffectiveRunAt() != null) {
            headers.put("scheduler_effective_fire_at", job.getEffectiveRunAt().toString());
        }
        if (job.getPredecessorId() != null) {
            headers.put("scheduler_predecessor_id", job.getPredecessorId().toString());
        }
        if (job.getLastError() != null) {
            headers.put("scheduler_error", job.getLastError());
        }

        headers.put("scheduler_retry_count", String.valueOf(job.getRetryCount()));
        headers.put("scheduler_max_retries", String.valueOf(job.getMaxRetries()));

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
            // Skip scheduler_ headers (case-insensitive) - they are internal
            if (headerName.toLowerCase().startsWith("scheduler_")) {
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
                  "runAt": %s,
                  "effectiveRunAt": %s,
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
                job.getRunAt() != null ? "\"" + job.getRunAt() + "\"" : "null",
                job.getEffectiveRunAt() != null ? "\"" + job.getEffectiveRunAt() + "\"" : "null",
                job.getRetryCount(),
                job.getMaxRetries(),
                job.getLastError() != null ? "\"" + job.getLastError() + "\"" : "null"
        );
    }
}
