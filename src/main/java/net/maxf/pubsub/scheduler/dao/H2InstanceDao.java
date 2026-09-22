package net.maxf.pubsub.scheduler.dao;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import javax.sql.DataSource;

@ApplicationScoped
@H2
public class H2InstanceDao extends AbstractInstanceDao {

    private static final Logger LOG = Logger.getLogger(H2InstanceDao.class);

    @Inject
    DataSource dataSource;

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    protected DataSource getDataSource() {
        return dataSource;
    }

    @Override
    protected String getUpsertSql() {
        return """
            MERGE INTO scheduler_instances (instance_id, heartbeat_at, started_at, version)
            KEY (instance_id)
            VALUES (?, ?, ?, 0)
            """;
    }
}
