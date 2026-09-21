package net.maxf.pubsub.scheduler.processor;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import net.maxf.pubsub.scheduler.model.WaitStart;
import net.maxf.pubsub.scheduler.model.KeyPolicy;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import net.maxf.pubsub.scheduler.service.JobStoreService;
import net.maxf.pubsub.scheduler.transform.TransformException;
import net.maxf.pubsub.scheduler.transform.TransformService;
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
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
    private static final String HEADER_RETRY_COUNT = HEADER_PREFIX + "RETRY_COUNT";
    private static final String HEADER_ADVISORY_HEADERS = HEADER_PREFIX + "ADVISORY_HEADERS";
    private static final String HEADER_CRON_END = HEADER_PREFIX + "CRON_END";
    private static final String HEADER_CRON_COUNT = HEADER_PREFIX + "CRON_COUNT";

    @Inject
    JobStoreService jobStore;

    @Inject
    TransformService transformService;

    @ConfigProperty(name = "scheduler.default-retries", defaultValue = "3")
    int defaultRetries;

    @Override
    public void process(Exchange exchange) throws Exception {
        Message message = exchange.getIn();
        ScheduledJob job = new ScheduledJob();
        job.setArrivedAt(Instant.now());

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
            job.setRunAt(Instant.parse(atStr));
        } else if (sleepStr != null) {
            Duration sleep = Duration.parse(sleepStr);
            job.setRunAt(Instant.now().plus(sleep));
            job.setWaitDuration(sleepStr);
        } else if (cronStr != null) {
            // Cron end conditions (mutually exclusive) - validate first
            String cronUntilStr = message.getHeader(HEADER_CRON_END, String.class);
            Integer cronCount = message.getHeader(HEADER_CRON_COUNT, Integer.class);
            if (cronUntilStr != null && cronCount != null) {
                throw new IllegalArgumentException("SCHEDULER_CRON_END and SCHEDULER_CRON_COUNT are mutually exclusive");
            }

            job.setCronExpression(cronStr);
            job.setRunAt(calculateNextCronFire(cronStr));

            if (cronUntilStr != null) {
                job.setCronUntil(Instant.parse(cronUntilStr));
            }
            if (cronCount != null) {
                job.setCronRepeat(cronCount);
            }
        } else {
            // Immediate
            job.setRunAt(Instant.now());
        }

        // Sleep options (only applies to SLEEP)
        String waitStartStr = message.getHeader(HEADER_SLEEP_START, String.class);
        if (waitStartStr != null) {
            job.setWaitStart(WaitStart.valueOf(waitStartStr.toUpperCase()));
        }
        Integer waitRepeat = message.getHeader(HEADER_SLEEP_REPEAT, Integer.class);
        if (waitRepeat != null) {
            job.setWaitRepeat(waitRepeat);
        }

        // Key and mode
        job.setJobKey(message.getHeader(HEADER_KEY, String.class));
        String keyPolicyStr = message.getHeader(HEADER_KEY_POLICY, String.class);
        if (keyPolicyStr != null) {
            job.setKeyPolicy(KeyPolicy.valueOf(keyPolicyStr.toUpperCase()));
        }

        // Retries
        Integer retries = message.getHeader(HEADER_RETRY_COUNT, Integer.class);
        job.setMaxRetries(retries != null ? retries : defaultRetries);

        // Advisory header filter
        job.setAdvisoryHeadersPattern(message.getHeader(HEADER_ADVISORY_HEADERS, String.class));

        // Preserve message key and value
        job.setMessageKey(message.getHeader("kafka.KEY", byte[].class));
        job.setMessageValue(message.getBody(byte[].class));

        // Preserve non-scheduler headers for forwarding
        Map<String, String> headers = new HashMap<>();
        for (Map.Entry<String, Object> entry : message.getHeaders().entrySet()) {
            String key = entry.getKey();
            String keyLower = key.toLowerCase();
            if (!key.startsWith(HEADER_PREFIX) && !keyLower.startsWith("kafka.") && !keyLower.startsWith("camel")) {
                Object value = entry.getValue();
                if (value instanceof String) {
                    headers.put(key, (String) value);
                } else if (value instanceof byte[]) {
                    headers.put(key, new String((byte[]) value, java.nio.charset.StandardCharsets.UTF_8));
                }
            }
        }
        job.setHeaders(headers);

        // Calculate effective fire time (will be adjusted for QUEUE mode if needed)
        job.setEffectiveRunAt(job.getRunAt());

        LOG.infof("Ingested job %s: destination=%s, runAt=%s, key=%s, mode=%s",
                job.getId(), job.getDestinationTopic(), job.getRunAt(),
                job.getJobKey(), job.getKeyPolicy());

        // Apply pre-scheduling transforms (e.g., claim check externalization)
        try {
            job = transformService.applyPreSchedulingTransforms(job);
        } catch (TransformException e) {
            throw new RuntimeException("Pre-scheduling transform failed for job " + job.getId(), e);
        }

        // Handle according to key mode
        jobStore.handleIncomingJob(job);
    }

    private static final CronParser CRON_PARSER = new CronParser(
        CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX)
    );

    private Instant calculateNextCronFire(String cronExpression) {
        Cron cron = CRON_PARSER.parse(cronExpression);
        ExecutionTime executionTime = ExecutionTime.forCron(cron);
        Optional<ZonedDateTime> nextExecution = executionTime.nextExecution(ZonedDateTime.now());
        return nextExecution
            .map(ZonedDateTime::toInstant)
            .orElseThrow(() -> new IllegalArgumentException("Cannot calculate next execution for cron: " + cronExpression));
    }

    public static Instant calculateNextCronFireFrom(String cronExpression, Instant from) {
        Cron cron = CRON_PARSER.parse(cronExpression);
        ExecutionTime executionTime = ExecutionTime.forCron(cron);
        Optional<ZonedDateTime> nextExecution = executionTime.nextExecution(
            ZonedDateTime.ofInstant(from, java.time.ZoneId.systemDefault())
        );
        return nextExecution
            .map(ZonedDateTime::toInstant)
            .orElseThrow(() -> new IllegalArgumentException("Cannot calculate next execution for cron: " + cronExpression));
    }
}
