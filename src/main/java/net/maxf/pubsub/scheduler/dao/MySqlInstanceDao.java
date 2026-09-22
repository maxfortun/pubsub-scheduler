package net.maxf.pubsub.scheduler.dao;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import javax.sql.DataSource;

@ApplicationScoped
@MySql
public class MySqlInstanceDao extends AbstractInstanceDao {

    private static final Logger LOG = Logger.getLogger(MySqlInstanceDao.class);

    @Inject
    DataSourceProvider dataSourceProvider;

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    protected DataSource getDataSource() {
        return dataSourceProvider.getMySqlDataSource();
    }

    @Override
    protected String getUpsertSql() {
        return """
            INSERT INTO scheduler_instances (instance_id, heartbeat_at, started_at, version)
            VALUES (?, ?, ?, 0)
            ON DUPLICATE KEY UPDATE
                heartbeat_at = VALUES(heartbeat_at),
                started_at = VALUES(started_at),
                version = version + 1
            """;
    }
}
