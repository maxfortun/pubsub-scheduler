package net.maxf.pubsub.scheduler.transform;

import net.maxf.pubsub.scheduler.model.ScheduledJob;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * HTTP-based message transformer.
 * POSTs payload to configured URL, response replaces the payload.
 */
public class HttpTransformer implements MessageTransformer {

    private static final Logger LOG = Logger.getLogger(HttpTransformer.class);

    private final String url;
    private final String method;
    private final int thresholdBytes;
    private final String authorization;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;

    private HttpClient httpClient;

    public HttpTransformer(TransformConfig.HttpConfig config) {
        this.url = config.url().orElse(null);
        this.method = config.method();
        this.thresholdBytes = config.thresholdBytes();
        this.authorization = config.authorization().orElse(null);
        this.connectTimeoutMs = config.connectTimeoutMs();
        this.readTimeoutMs = config.readTimeoutMs();
    }

    @Override
    public boolean accepts(ScheduledJob job) {
        byte[] payload = job.getMessageValue();
        return payload != null && (thresholdBytes == 0 || payload.length >= thresholdBytes);
    }

    @Override
    public ScheduledJob transform(ScheduledJob job) throws TransformException {
        if (url == null) {
            throw new TransformException("HTTP transform URL not configured");
        }

        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMillis(readTimeoutMs));

            if (authorization != null) {
                builder.header("Authorization", authorization);
            }

            byte[] payload = job.getMessageValue();

            switch (method.toUpperCase()) {
                case "GET" -> builder.GET();
                case "PUT" -> builder.PUT(HttpRequest.BodyPublishers.ofByteArray(payload));
                default -> builder.POST(HttpRequest.BodyPublishers.ofByteArray(payload));
            }

            HttpResponse<byte[]> response = getHttpClient().send(
                    builder.build(), HttpResponse.BodyHandlers.ofByteArray());

            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new TransformException("HTTP transform failed with status " + response.statusCode());
            }

            LOG.infof("HTTP transform: %d bytes -> %d bytes for job %s",
                    payload.length, response.body().length, job.getId());

            job.setMessageValue(response.body());
            return job;

        } catch (IOException | InterruptedException e) {
            throw new TransformException("HTTP transform request failed", e);
        }
    }

    private synchronized HttpClient getHttpClient() {
        if (httpClient == null) {
            httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                    .build();
        }
        return httpClient;
    }
}
