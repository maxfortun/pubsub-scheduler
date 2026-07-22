package net.maxf.pubsub.scheduler.processor;

import net.maxf.pubsub.scheduler.model.ScheduledJob;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Named;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.jboss.logging.Logger;

import java.util.Map;

@ApplicationScoped
@Named("fireProcessor")
public class FireProcessor implements Processor {

    private static final Logger LOG = Logger.getLogger(FireProcessor.class);

    @Override
    public void process(Exchange exchange) throws Exception {
        ScheduledJob job = exchange.getIn().getBody(ScheduledJob.class);

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
