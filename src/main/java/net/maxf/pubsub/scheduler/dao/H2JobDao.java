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
@H2
public class H2JobDao extends AbstractJobDao {

    private static final Logger LOG = Logger.getLogger(H2JobDao.class);

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
        return "HASH('SHA-256', COALESCE(job_key, CAST(id AS VARCHAR)), 1)";
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
        return "";
    }
}
