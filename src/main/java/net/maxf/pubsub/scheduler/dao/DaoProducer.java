package net.maxf.pubsub.scheduler.dao;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class DaoProducer {

    private static final Logger LOG = Logger.getLogger(DaoProducer.class);

    @ConfigProperty(name = "quarkus.datasource.db-kind", defaultValue = "postgresql")
    String dbKind;

    @Inject
    @Postgres
    PostgresInstanceDao postgresInstanceDao;

    @Inject
    @MySql
    MySqlInstanceDao mySqlInstanceDao;

    @Inject
    @Postgres
    PostgresJobDao postgresJobDao;

    @Inject
    @MySql
    MySqlJobDao mySqlJobDao;

    @Produces
    @ApplicationScoped
    public InstanceDao instanceDao() {
        LOG.infof("Selecting InstanceDao for database type: %s", dbKind);
        return selectByDbKind(postgresInstanceDao, mySqlInstanceDao);
    }

    @Produces
    @ApplicationScoped
    public JobDao jobDao() {
        LOG.infof("Selecting JobDao for database type: %s", dbKind);
        return selectByDbKind(postgresJobDao, mySqlJobDao);
    }

    private <T> T selectByDbKind(T postgresImpl, T mySqlImpl) {
        return switch (dbKind.toLowerCase()) {
            case "mysql", "mariadb" -> mySqlImpl;
            case "postgresql", "postgres", "h2" -> postgresImpl;
            default -> {
                LOG.warnf("Unknown database type '%s', defaulting to PostgreSQL DAO", dbKind);
                yield postgresImpl;
            }
        };
    }
}
