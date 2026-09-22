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
@MySql
public class MySqlJobDao extends AbstractJobDao {

    private static final Logger LOG = Logger.getLogger(MySqlJobDao.class);

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
    protected String getShardHashExpression() {
        return "CRC32(COALESCE(job_key, id))";
    }

    @Override
    protected void setUuidParam(PreparedStatement ps, int idx, UUID value) throws SQLException {
        if (value != null) {
            ps.setString(idx, value.toString());
        } else {
            ps.setNull(idx, Types.VARCHAR);
        }
    }

    @Override
    protected UUID getUuidFromResultSet(ResultSet rs, String column) throws SQLException {
        String value = rs.getString(column);
        return value != null ? UUID.fromString(value) : null;
    }

    @Override
    protected String getJsonCast() {
        return "";
    }
}
