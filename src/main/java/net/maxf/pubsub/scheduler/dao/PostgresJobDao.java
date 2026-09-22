package net.maxf.pubsub.scheduler.dao;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.UUID;

@ApplicationScoped
@Postgres
public class PostgresJobDao extends AbstractJobDao {

    private static final Logger LOG = Logger.getLogger(PostgresJobDao.class);

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
    protected String getShardHashExpression() {
        return "hashtext(COALESCE(job_key, id::text))";
    }

    @Override
    protected void setUuidParam(PreparedStatement ps, int idx, UUID value) throws SQLException {
        if (value != null) {
            ps.setObject(idx, value);
        } else {
            ps.setNull(idx, Types.OTHER);
        }
    }

    @Override
    protected UUID getUuidFromResultSet(ResultSet rs, String column) throws SQLException {
        return rs.getObject(column, UUID.class);
    }

    @Override
    protected String getJsonCast() {
        return "::jsonb";
    }
}
