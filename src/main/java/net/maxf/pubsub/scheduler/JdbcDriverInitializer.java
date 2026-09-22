package net.maxf.pubsub.scheduler;

import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@Startup
@ApplicationScoped
public class JdbcDriverInitializer {

    private static final Logger LOG = Logger.getLogger(JdbcDriverInitializer.class);

    @ConfigProperty(name = "quarkus.datasource.db-kind")
    String dbKind;

    @PostConstruct
    void init() {
        if ("mysql".equalsIgnoreCase(dbKind)) {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                LOG.info("Explicitly loaded MySQL JDBC driver");
            } catch (ClassNotFoundException e) {
                LOG.warn("MySQL JDBC driver not found on classpath", e);
            }
        }
    }
}
