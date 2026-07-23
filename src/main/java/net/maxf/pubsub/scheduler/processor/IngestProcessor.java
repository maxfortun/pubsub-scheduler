package net.maxf.pubsub.scheduler.processor;

import net.maxf.pubsub.scheduler.model.SleepStart;
import net.maxf.pubsub.scheduler.model.KeyPolicy;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import net.maxf.pubsub.scheduler.service.JobStoreService;
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
    private static final String HEADER_AT = HEADER_PREFIX + "AT";
    private static final String HEADER_SLEEP = HEADER_PREFIX + "SLEEP";
    private static final String HEADER_CRON = HEADER_PREFIX + "CRON";
    private static final String HEADER_SLEEP_START = HEADER_PREFIX + "SLEEP_START";
    private static final String HEADER_SLEEP_REPEAT = HEADER_PREFIX + "SLEEP_REPEAT";
    private static final String HEADER_DESTINATION = HEADER_PREFIX + "DESTINATION";
    private static final String HEADER_KEY = HEADER_PREFIX + "KEY";
    private static final String HEADER_KEY_POLICY = HEADER_PREFIX + "KEY_POLICY";
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

        // Timing: AT, SLEEP, or CRON (mutually exclusive)
        String atStr = message.getHeader(HEADER_AT, String.class);
        String sleepStr = message.getHeader(HEADER_SLEEP, String.class);
        String cronStr = message.getHeader(HEADER_CRON, String.class);

        int timingCount = (atStr != null ? 1 : 0) + (sleepStr != null ? 1 : 0) + (cronStr != null ? 1 : 0);
        if (timingCount > 1) {
            throw new IllegalArgumentException("SCHEDULER_AT, SCHEDULER_SLEEP, and SCHEDULER_CRON are mutually exclusive");
        }

        if (atStr != null) {
            job.setFireAt(Instant.parse(atStr));
        } else if (sleepStr != null) {
            Duration sleep = Duration.parse(sleepStr);
            job.setFireAt(Instant.now().plus(sleep));
            job.setSleepDuration(sleepStr);
        } else if (cronStr != null) {
            job.setCronExpression(cronStr);
            job.setFireAt(calculateNextCronFire(cronStr));
        } else {
            // Immediate
            job.setFireAt(Instant.now());
        }

        // Sleep options (only applies to SLEEP)
        String sleepStartStr = message.getHeader(HEADER_SLEEP_START, String.class);
        if (sleepStartStr != null) {
            job.setSleepStart(SleepStart.valueOf(sleepStartStr.toUpperCase()));
        }
        Integer sleepRepeat = message.getHeader(HEADER_SLEEP_REPEAT, Integer.class);
        if (sleepRepeat != null) {
            job.setSleepRepeat(sleepRepeat);
        }

        // Key and mode
        job.setJobKey(message.getHeader(HEADER_KEY, String.class));
        String keyPolicyStr = message.getHeader(HEADER_KEY_POLICY, String.class);
        if (keyPolicyStr != null) {
            job.setKeyPolicy(KeyPolicy.valueOf(keyPolicyStr.toUpperCase()));
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
                job.getJobKey(), job.getKeyPolicy());

        // Handle according to key mode
        jobStore.handleIncomingJob(job);
    }

    private Instant calculateNextCronFire(String cronExpression) {
        // TODO: Use cron-utils or similar library to calculate next fire time
        throw new UnsupportedOperationException("CRON support not yet implemented");
    }
}
