package net.maxf.pubsub.scheduler.dao;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;

@ApplicationScoped
public class RuntimeDataSourceProducer {

    private static final Logger LOG = Logger.getLogger(RuntimeDataSourceProducer.class);

    @ConfigProperty(name = "quarkus.datasource.jdbc.url")
    String jdbcUrl;

    @ConfigProperty(name = "quarkus.datasource.username", defaultValue = "")
    String username;

    @ConfigProperty(name = "quarkus.datasource.password", defaultValue = "")
    String password;

    @ConfigProperty(name = "quarkus.datasource.jdbc.max-size", defaultValue = "20")
    int maxSize;

    @Produces
    @Singleton
    public DataSource createDataSource() throws SQLException {
        String driverClass = detectDriverClass(jdbcUrl);
        LOG.infof("Creating DataSource with driver %s for URL %s", driverClass, jdbcUrl);

        AgroalDataSourceConfigurationSupplier config = new AgroalDataSourceConfigurationSupplier()
            .connectionPoolConfiguration(cp -> cp
                .maxSize(maxSize)
                .acquisitionTimeout(Duration.ofSeconds(30))
                .connectionFactoryConfiguration(cf -> cf
                    .jdbcUrl(jdbcUrl)
                    .connectionProviderClassName(driverClass)
                    .principal(new NamePrincipal(username))
                    .credential(new SimplePassword(password))
                )
            );

        return AgroalDataSource.from(config);
    }

    private String detectDriverClass(String url) {
        if (url.startsWith("jdbc:mysql:")) {
            return "com.mysql.cj.jdbc.Driver";
        } else if (url.startsWith("jdbc:postgresql:")) {
            return "org.postgresql.Driver";
        } else if (url.startsWith("jdbc:h2:")) {
            return "org.h2.Driver";
        }
        throw new IllegalArgumentException("Unsupported JDBC URL: " + url);
    }
}
