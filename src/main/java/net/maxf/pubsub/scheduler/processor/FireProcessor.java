package net.maxf.pubsub.scheduler.processor;

import net.maxf.pubsub.scheduler.model.ScheduledJob;
import net.maxf.pubsub.scheduler.transform.TransformException;
import net.maxf.pubsub.scheduler.transform.TransformService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.jboss.logging.Logger;

import java.util.Map;

@ApplicationScoped
@Named("fireProcessor")
public class FireProcessor implements Processor {

    private static final Logger LOG = Logger.getLogger(FireProcessor.class);

    @Inject
    TransformService transformService;

    @Override
    public void process(Exchange exchange) throws Exception {
        ScheduledJob job = exchange.getIn().getBody(ScheduledJob.class);

        // Apply post-scheduling transforms (e.g., claim check retrieval)
        try {
            job = transformService.applyPostSchedulingTransforms(job);
        } catch (TransformException e) {
            throw new RuntimeException("Post-scheduling transform failed for job " + job.getId(), e);
        }

        // Clear all existing headers
        exchange.getIn().getHeaders().clear();

        // Set the destination dynamically
        exchange.getIn().setHeader("CamelOverrideEndpointUri",
                "kafka:" + job.getDestinationTopic());

        // Set message key if present
        if (job.getMessageKey() != null) {
            exchange.getIn().setHeader("kafka.KEY", job.getMessageKey());
        }

        // Forward only non-SCHEDULER_ headers
        if (job.getHeaders() != null) {
            for (Map.Entry<String, String> entry : job.getHeaders().entrySet()) {
                if (!entry.getKey().startsWith("SCHEDULER_")) {
                    exchange.getIn().setHeader(entry.getKey(), entry.getValue());
                }
            }
        }

        // Set the original message body
        exchange.getIn().setBody(job.getMessageValue());

        LOG.debugf("Prepared job %s for firing to %s", job.getId(), job.getDestinationTopic());
    }
}
