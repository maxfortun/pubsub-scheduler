package net.maxf.pubsub.scheduler.dao;

import org.jboss.logging.Logger;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public abstract class AbstractInstanceDao implements InstanceDao {

    protected abstract Logger getLogger();
    protected abstract DataSource getDataSource();
    protected abstract String getUpsertSql();

    @Override
    public void upsert(String instanceId, Instant heartbeatAt, Instant startedAt) {
        try (Connection conn = getDataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(getUpsertSql())) {
            ps.setString(1, instanceId);
            ps.setTimestamp(2, Timestamp.from(heartbeatAt));
            ps.setTimestamp(3, Timestamp.from(startedAt));
            ps.executeUpdate();
        } catch (SQLException e) {
            getLogger().errorf(e, "Failed to upsert instance %s", instanceId);
            throw new DaoException("Failed to upsert instance", e);
        }
    }

    @Override
    public void delete(String instanceId) {
        String sql = "DELETE FROM scheduler_instances WHERE instance_id = ?";

        try (Connection conn = getDataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, instanceId);
            ps.executeUpdate();
        } catch (SQLException e) {
            getLogger().warnf(e, "Failed to delete instance %s", instanceId);
            throw new DaoException("Failed to delete instance", e);
        }
    }

    @Override
    public int updateHeartbeat(String instanceId, Instant heartbeatAt) {
        String sql = "UPDATE scheduler_instances SET heartbeat_at = ?, version = version + 1 WHERE instance_id = ?";

        try (Connection conn = getDataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setTimestamp(1, Timestamp.from(heartbeatAt));
            ps.setString(2, instanceId);
            return ps.executeUpdate();
        } catch (SQLException e) {
            getLogger().errorf(e, "Failed to update heartbeat for instance %s", instanceId);
            throw new DaoException("Failed to update heartbeat", e);
        }
    }

    @Override
    public List<String> findLiveInstances(Instant heartbeatThreshold) {
        List<String> instances = new ArrayList<>();
        String sql = """
            SELECT instance_id FROM scheduler_instances
            WHERE heartbeat_at > ?
            ORDER BY started_at, instance_id
            """;

        try (Connection conn = getDataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setTimestamp(1, Timestamp.from(heartbeatThreshold));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    instances.add(rs.getString("instance_id"));
                }
            }
        } catch (SQLException e) {
            getLogger().errorf(e, "Failed to query live instances");
            throw new DaoException("Failed to query live instances", e);
        }

        return instances;
    }

    @Override
    public Optional<InstanceInfo> findById(String instanceId) {
        String sql = "SELECT * FROM scheduler_instances WHERE instance_id = ?";

        try (Connection conn = getDataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, instanceId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new InstanceInfo(
                        rs.getString("instance_id"),
                        rs.getTimestamp("heartbeat_at").toInstant(),
                        rs.getTimestamp("started_at").toInstant(),
                        rs.getInt("version")
                    ));
                }
            }
        } catch (SQLException e) {
            getLogger().errorf(e, "Failed to find instance %s", instanceId);
            throw new DaoException("Failed to find instance", e);
        }

        return Optional.empty();
    }
}
