package net.maxf.pubsub.scheduler.dao;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import javax.sql.DataSource;

@ApplicationScoped
@Postgres
public class PostgresInstanceDao extends AbstractInstanceDao {

    private static final Logger LOG = Logger.getLogger(PostgresInstanceDao.class);

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
            INSERT INTO scheduler_instances (instance_id, heartbeat_at, started_at, version)
            VALUES (?, ?, ?, 0)
            ON CONFLICT (instance_id) DO UPDATE
            SET heartbeat_at = EXCLUDED.heartbeat_at,
                started_at = EXCLUDED.started_at,
                version = scheduler_instances.version + 1
            """;
    }
}
