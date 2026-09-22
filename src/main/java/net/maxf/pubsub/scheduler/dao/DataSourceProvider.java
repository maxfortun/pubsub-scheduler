package net.maxf.pubsub.scheduler.dao;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;

import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ConnectionValidator.defaultValidator;

@ApplicationScoped
public class DataSourceProvider {

    private static final Logger LOG = Logger.getLogger(DataSourceProvider.class);

    @ConfigProperty(name = "quarkus.datasource.db-kind", defaultValue = "postgresql")
    String dbKind;

    @ConfigProperty(name = "quarkus.datasource.jdbc.url")
    String jdbcUrl;

    @ConfigProperty(name = "quarkus.datasource.username", defaultValue = "scheduler")
    String username;

    @ConfigProperty(name = "quarkus.datasource.password")
    java.util.Optional<String> password;

    @ConfigProperty(name = "quarkus.datasource.jdbc.max-size", defaultValue = "20")
    int maxSize;

    @Inject
    DataSource quarkusDataSource;

    private AgroalDataSource mysqlDataSource;

    @PostConstruct
    void init() {
        if ("mysql".equalsIgnoreCase(dbKind)) {
            try {
                initMySqlDataSource();
            } catch (SQLException e) {
                LOG.error("Failed to initialize MySQL DataSource", e);
                throw new RuntimeException("Failed to initialize MySQL DataSource", e);
            }
        }
    }

    private void initMySqlDataSource() throws SQLException {
        LOG.info("Creating custom MySQL DataSource with explicit driver");

        AgroalDataSourceConfigurationSupplier configSupplier = new AgroalDataSourceConfigurationSupplier()
            .connectionPoolConfiguration(cp -> cp
                .maxSize(maxSize)
                .minSize(2)
                .initialSize(2)
                .acquisitionTimeout(Duration.ofSeconds(5))
                .validationTimeout(Duration.ofSeconds(5))
                .connectionValidator(defaultValidator())
                .connectionFactoryConfiguration(cf -> cf
                    .jdbcUrl(jdbcUrl)
                    .principal(new NamePrincipal(username))
                    .credential(new SimplePassword(password.orElse("")))
                    .connectionProviderClassName("com.mysql.cj.jdbc.Driver")
                )
            );

        mysqlDataSource = AgroalDataSource.from(configSupplier);
        LOG.info("Custom MySQL DataSource created successfully");
    }

    public DataSource getDataSource() {
        if ("mysql".equalsIgnoreCase(dbKind) && mysqlDataSource != null) {
            return mysqlDataSource;
        }
        return quarkusDataSource;
    }

    public DataSource getMySqlDataSource() {
        return mysqlDataSource != null ? mysqlDataSource : quarkusDataSource;
    }

    @PreDestroy
    void cleanup() {
        if (mysqlDataSource != null) {
            try {
                mysqlDataSource.close();
            } catch (Exception e) {
                LOG.warn("Error closing MySQL DataSource", e);
            }
        }
    }
}
