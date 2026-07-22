package com.maxfortun.scheduler.processor;

import com.maxfortun.scheduler.model.DelayStart;
import com.maxfortun.scheduler.model.KeyMode;
import com.maxfortun.scheduler.model.ScheduledJob;
import com.maxfortun.scheduler.service.JobStoreService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@ApplicationScoped
@Named("ingestProcessor")
public class IngestProcessor implements Processor {

    private static final Logger LOG = Logger.getLogger(IngestProcessor.class);

    private static final String HEADER_PREFIX = "SCHEDULER_";
    private static final String HEADER_FIRE_AT = HEADER_PREFIX + "FIRE_AT";
    private static final String HEADER_DELAY = HEADER_PREFIX + "DELAY";
    private static final String HEADER_DELAY_START = HEADER_PREFIX + "DELAY_START";
    private static final String HEADER_DESTINATION = HEADER_PREFIX + "DESTINATION";
    private static final String HEADER_KEY = HEADER_PREFIX + "KEY";
    private static final String HEADER_KEY_MODE = HEADER_PREFIX + "KEY_MODE";
    private static final String HEADER_RETRIES = HEADER_PREFIX + "RETRIES";
    private static final String HEADER_ADVISORY_HEADERS = HEADER_PREFIX + "ADVISORY_HEADERS";

    @Inject
    JobStoreService jobStore;

    @ConfigProperty(name = "scheduler.default-retries", defaultValue = "3")
    int defaultRetries;

    @Override
    public void process(Exchange exchange) throws Exception {
        Message message = exchange.getIn();
        ScheduledJob job = new ScheduledJob();

        // Required: destination topic
        String destination = message.getHeader(HEADER_DESTINATION, String.class);
        if (destination == null || destination.isBlank()) {
            throw new IllegalArgumentException("Missing required header: " + HEADER_DESTINATION);
        }
        job.setDestinationTopic(destination);

        // Timing: either FIRE_AT or DELAY
        String fireAtStr = message.getHeader(HEADER_FIRE_AT, String.class);
        String delayStr = message.getHeader(HEADER_DELAY, String.class);

        if (fireAtStr != null) {
            job.setFireAt(Instant.parse(fireAtStr));
        } else if (delayStr != null) {
            Duration delay = Duration.parse(delayStr);
            job.setFireAt(Instant.now().plus(delay));
        } else {
            // Immediate
            job.setFireAt(Instant.now());
        }

        // Delay start reference
        String delayStartStr = message.getHeader(HEADER_DELAY_START, String.class);
        if (delayStartStr != null) {
            job.setDelayStart(DelayStart.valueOf(delayStartStr.toUpperCase()));
        }

        // Key and mode
        job.setJobKey(message.getHeader(HEADER_KEY, String.class));
        String keyModeStr = message.getHeader(HEADER_KEY_MODE, String.class);
        if (keyModeStr != null) {
            job.setKeyMode(KeyMode.valueOf(keyModeStr.toUpperCase()));
        }

        // Retries
        Integer retries = message.getHeader(HEADER_RETRIES, Integer.class);
        job.setMaxRetries(retries != null ? retries : defaultRetries);

        // Advisory header filter
        job.setAdvisoryHeadersPattern(message.getHeader(HEADER_ADVISORY_HEADERS, String.class));

        // Preserve message key and value
        job.setMessageKey(message.getHeader("kafka.KEY", byte[].class));
        job.setMessageValue(message.getBody(byte[].class));

        // Preserve non-scheduler headers for forwarding
        Map<String, String> headers = new HashMap<>();
        for (Map.Entry<String, Object> entry : message.getHeaders().entrySet()) {
            if (!entry.getKey().startsWith(HEADER_PREFIX) && entry.getValue() instanceof String) {
                headers.put(entry.getKey(), (String) entry.getValue());
            }
        }
        job.setHeaders(headers);

        // Calculate effective fire time (will be adjusted for QUEUE mode if needed)
        job.setEffectiveFireAt(job.getFireAt());

        LOG.infof("Ingested job %s: destination=%s, fireAt=%s, key=%s, mode=%s",
                job.getId(), job.getDestinationTopic(), job.getFireAt(),
                job.getJobKey(), job.getKeyMode());

        // Handle according to key mode
        jobStore.handleIncomingJob(job);
    }
}
