package net.maxf.pubsub.scheduler.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.KeyPolicy;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import net.maxf.pubsub.scheduler.model.SleepStart;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@ApplicationScoped
public class JobPreloadService {

    private static final Logger LOG = Logger.getLogger(JobPreloadService.class);

    @Inject
    JobStoreService jobStore;

    @Inject
    ObjectMapper objectMapper;

    @ConfigProperty(name = "scheduler.preload.file")
    Optional<String> preloadFile;

    @ConfigProperty(name = "scheduler.preload.classpath")
    Optional<String> preloadClasspath;

    void onStart(@Observes StartupEvent ev) {
        if (preloadFile.isPresent()) {
            loadFromFile(preloadFile.get());
        } else if (preloadClasspath.isPresent()) {
            loadFromClasspath(preloadClasspath.get());
        }
    }

    private void loadFromFile(String filePath) {
        try {
            Path path = Path.of(filePath);
            if (!Files.exists(path)) {
                LOG.warnf("Preload file not found: %s", filePath);
                return;
            }
            String content = Files.readString(path);
            loadJobs(content);
        } catch (IOException e) {
            LOG.errorf(e, "Failed to read preload file: %s", filePath);
        }
    }

    private void loadFromClasspath(String resourcePath) {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                LOG.warnf("Preload resource not found: %s", resourcePath);
                return;
            }
            String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            loadJobs(content);
        } catch (IOException e) {
            LOG.errorf(e, "Failed to read preload resource: %s", resourcePath);
        }
    }

    private void loadJobs(String jsonContent) {
        try {
            List<JobDefinition> definitions = objectMapper.readValue(jsonContent, new TypeReference<>() {});
            LOG.infof("Loading %d preload job definitions", definitions.size());

            for (JobDefinition def : definitions) {
                createJobIfNotExists(def);
            }
        } catch (IOException e) {
            LOG.errorf(e, "Failed to parse preload jobs JSON");
        }
    }

    private void createJobIfNotExists(JobDefinition def) {
        if (def.jobKey == null || def.jobKey.isBlank()) {
            LOG.warnf("Skipping preload job without jobKey: %s", def);
            return;
        }

        ScheduledJob job = new ScheduledJob();
        job.setJobKey(def.jobKey);
        job.setDestinationTopic(def.destination);
        job.setKeyPolicy(def.keyPolicy != null ? KeyPolicy.valueOf(def.keyPolicy.toUpperCase()) : KeyPolicy.SKIP);
        job.setSleepStart(def.sleepStart != null ? SleepStart.valueOf(def.sleepStart.toUpperCase()) : SleepStart.SELF);

        if (def.sleepDuration != null) {
            Duration interval = Duration.parse(def.sleepDuration);
            job.setFireAt(Instant.now().plus(interval));
            job.setSleepDuration(def.sleepDuration);
            job.setSleepRepeat(def.sleepRepeat != null ? def.sleepRepeat : 1);
        } else {
            job.setFireAt(Instant.now());
        }
        job.setEffectiveFireAt(job.getFireAt());

        if (def.headers != null) {
            job.setHeaders(def.headers);
        }

        if (def.body != null) {
            job.setMessageValue(def.body.getBytes(StandardCharsets.UTF_8));
        }

        job.setState(JobState.PENDING);
        job.setMaxRetries(def.maxRetries != null ? def.maxRetries : 3);

        boolean created = jobStore.saveIfNotExistsByKey(job);
        if (created) {
            LOG.infof("Created preload job %s (key=%s, destination=%s, interval=%s, repeat=%s)",
                job.getId(), job.getJobKey(), job.getDestinationTopic(), def.sleepDuration, def.sleepRepeat);
        } else {
            LOG.debugf("Preload job already exists: %s", def.jobKey);
        }
    }

    public record JobDefinition(
        String jobKey,
        String destination,
        String keyPolicy,
        String sleepDuration,
        Integer sleepRepeat,
        String sleepStart,
        Map<String, String> headers,
        String body,
        Integer maxRetries
    ) {}
}
