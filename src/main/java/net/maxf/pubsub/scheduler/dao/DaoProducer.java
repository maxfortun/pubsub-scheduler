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

    @ConfigProperty(name = "scheduler.db-dialect")
    java.util.Optional<String> dbDialect;

    @Inject
    @Postgres
    PostgresInstanceDao postgresInstanceDao;

    @Inject
    @MySql
    MySqlInstanceDao mySqlInstanceDao;

    @Inject
    @CockroachDb
    CockroachDbInstanceDao cockroachDbInstanceDao;

    @Inject
    @H2
    H2InstanceDao h2InstanceDao;

    @Inject
    @Postgres
    PostgresJobDao postgresJobDao;

    @Inject
    @MySql
    MySqlJobDao mySqlJobDao;

    @Inject
    @CockroachDb
    CockroachDbJobDao cockroachDbJobDao;

    @Inject
    @H2
    H2JobDao h2JobDao;

    @Produces
    @ApplicationScoped
    public InstanceDao instanceDao() {
        String dialect = getEffectiveDialect();
        LOG.infof("Selecting InstanceDao for database dialect: %s", dialect);
        return selectByDialect(dialect, postgresInstanceDao, mySqlInstanceDao, cockroachDbInstanceDao, h2InstanceDao);
    }

    @Produces
    @ApplicationScoped
    public JobDao jobDao() {
        String dialect = getEffectiveDialect();
        LOG.infof("Selecting JobDao for database dialect: %s", dialect);
        return selectByDialect(dialect, postgresJobDao, mySqlJobDao, cockroachDbJobDao, h2JobDao);
    }

    private String getEffectiveDialect() {
        return dbDialect.orElse(dbKind);
    }

    private <T> T selectByDialect(String dialect, T postgresImpl, T mySqlImpl, T cockroachDbImpl, T h2Impl) {
        return switch (dialect.toLowerCase()) {
            case "mysql", "mariadb" -> mySqlImpl;
            case "cockroachdb", "cockroach" -> cockroachDbImpl;
            case "h2" -> h2Impl;
            case "postgresql", "postgres" -> postgresImpl;
            default -> {
                LOG.warnf("Unknown database dialect '%s', defaulting to PostgreSQL DAO", dialect);
                yield postgresImpl;
            }
        };
    }
}
