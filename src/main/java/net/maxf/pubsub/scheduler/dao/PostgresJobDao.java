package net.maxf.pubsub.scheduler.dao;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import net.maxf.pubsub.scheduler.model.JobState;
import net.maxf.pubsub.scheduler.model.KeyPolicy;
import net.maxf.pubsub.scheduler.model.ScheduledJob;
import net.maxf.pubsub.scheduler.model.SleepStart;
import org.jboss.logging.Logger;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.*;

@ApplicationScoped
@Postgres
public class PostgresJobDao implements JobDao {

    private static final Logger LOG = Logger.getLogger(PostgresJobDao.class);

    @Inject
    DataSource dataSource;

    @Override
    public void insert(ScheduledJob job) {
        String sql = """
            INSERT INTO scheduled_jobs (
                id, job_key, key_policy, sleep_start, sleep_duration, sleep_repeat,
                cron_expression, cron_end, cron_max_count, cron_fire_count,
                fire_at, effective_fire_at, arrived_at,
                destination_topic, message_key, message_value, headers, advisory_headers_pattern,
                state, max_retries, retry_count, version,
                predecessor_id, sequence_num,
                acquired_by, acquired_at, created_at, updated_at, last_error
            ) VALUES (
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?::jsonb, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?, ?
            )
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            setJobParameters(ps, job);
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.errorf(e, "Failed to insert job %s", job.getId());
            throw new DaoException("Failed to insert job", e);
        }
    }

    @Override
    public boolean update(ScheduledJob job) {
        String sql = """
            UPDATE scheduled_jobs SET
                job_key = ?, key_policy = ?, sleep_start = ?, sleep_duration = ?, sleep_repeat = ?,
                cron_expression = ?, cron_end = ?, cron_max_count = ?, cron_fire_count = ?,
                fire_at = ?, effective_fire_at = ?, arrived_at = ?,
                destination_topic = ?, message_key = ?, message_value = ?, headers = ?::jsonb, advisory_headers_pattern = ?,
                state = ?, max_retries = ?, retry_count = ?, version = ?,
                predecessor_id = ?, sequence_num = ?,
                acquired_by = ?, acquired_at = ?, updated_at = ?, last_error = ?
            WHERE id = ? AND version = ?
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            ps.setString(idx++, job.getJobKey());
            ps.setString(idx++, job.getKeyPolicy().name());
            ps.setString(idx++, job.getSleepStart().name());
            ps.setString(idx++, job.getSleepDuration());
            ps.setInt(idx++, job.getSleepRepeat());
            ps.setString(idx++, job.getCronExpression());
            ps.setTimestamp(idx++, toTimestamp(job.getCronEnd()));
            setNullableInt(ps, idx++, job.getCronMaxCount());
            ps.setInt(idx++, job.getCronFireCount());
            ps.setTimestamp(idx++, toTimestamp(job.getFireAt()));
            ps.setTimestamp(idx++, toTimestamp(job.getEffectiveFireAt()));
            ps.setTimestamp(idx++, toTimestamp(job.getArrivedAt()));
            ps.setString(idx++, job.getDestinationTopic());
            ps.setBytes(idx++, job.getMessageKey());
            ps.setBytes(idx++, job.getMessageValue());
            ps.setString(idx++, JsonUtil.toJson(job.getHeaders()));
            ps.setString(idx++, job.getAdvisoryHeadersPattern());
            ps.setString(idx++, job.getState().name());
            ps.setInt(idx++, job.getMaxRetries());
            ps.setInt(idx++, job.getRetryCount());
            ps.setInt(idx++, job.getVersion());
            setNullableUuid(ps, idx++, job.getPredecessorId());
            ps.setInt(idx++, job.getSequenceNum());
            ps.setString(idx++, job.getAcquiredBy());
            ps.setTimestamp(idx++, toTimestamp(job.getAcquiredAt()));
            ps.setTimestamp(idx++, toTimestamp(job.getUpdatedAt()));
            ps.setString(idx++, job.getLastError());
            ps.setObject(idx++, job.getId());
            ps.setInt(idx++, job.getVersion() - 1);

            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            LOG.errorf(e, "Failed to update job %s", job.getId());
            throw new DaoException("Failed to update job", e);
        }
    }

    @Override
    public Optional<ScheduledJob> findById(UUID id) {
        String sql = "SELECT * FROM scheduled_jobs WHERE id = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapJob(rs));
                }
            }
        } catch (SQLException e) {
            LOG.errorf(e, "Failed to find job %s", id);
            throw new DaoException("Failed to find job", e);
        }
        return Optional.empty();
    }

    @Override
    public List<ScheduledJob> findPendingByKey(String jobKey) {
        String sql = """
            SELECT * FROM scheduled_jobs
            WHERE job_key = ? AND state IN ('PENDING', 'WAITING', 'ACQUIRED', 'FIRING')
            ORDER BY sequence_num
            """;

        return queryJobs(sql, ps -> ps.setString(1, jobKey));
    }

    @Override
    public List<ScheduledJob> findWaitingByPredecessor(UUID predecessorId) {
        String sql = "SELECT * FROM scheduled_jobs WHERE predecessor_id = ? AND state = 'WAITING'";
        return queryJobs(sql, ps -> ps.setObject(1, predecessorId));
    }

    @Override
    public List<ScheduledJob> findWaitingByKey(String jobKey) {
        String sql = "SELECT * FROM scheduled_jobs WHERE job_key = ? AND state = 'WAITING' ORDER BY sequence_num";
        return queryJobs(sql, ps -> ps.setString(1, jobKey));
    }

    @Override
    public List<ScheduledJob> findPendingForShard(int shardIndex, int shardCount) {
        String sql = """
            SELECT * FROM scheduled_jobs
            WHERE state = 'PENDING'
              AND mod(abs(hashtext(COALESCE(job_key, id::text))), ?) = ?
            ORDER BY effective_fire_at
            """;

        return queryJobs(sql, ps -> {
            ps.setInt(1, shardCount);
            ps.setInt(2, shardIndex);
        });
    }

    @Override
    public List<ScheduledJob> findPendingForShardExcluding(int shardIndex, int shardCount, Set<UUID> excludeIds) {
        if (excludeIds.isEmpty()) {
            return findPendingForShard(shardIndex, shardCount);
        }

        String placeholders = String.join(",", Collections.nCopies(excludeIds.size(), "?"));
        String sql = """
            SELECT * FROM scheduled_jobs
            WHERE state = 'PENDING'
              AND mod(abs(hashtext(COALESCE(job_key, id::text))), ?) = ?
              AND id NOT IN (%s)
            ORDER BY effective_fire_at
            """.formatted(placeholders);

        return queryJobs(sql, ps -> {
            ps.setInt(1, shardCount);
            ps.setInt(2, shardIndex);
            int idx = 3;
            for (UUID id : excludeIds) {
                ps.setObject(idx++, id);
            }
        });
    }

    @Override
    public List<ScheduledJob> findAllPending() {
        String sql = "SELECT * FROM scheduled_jobs WHERE state = 'PENDING' ORDER BY effective_fire_at";
        return queryJobs(sql, ps -> {});
    }

    @Override
    public List<ScheduledJob> findJobs(JobState state, String jobKey, int limit) {
        StringBuilder sql = new StringBuilder("SELECT * FROM scheduled_jobs WHERE 1=1");
        List<Object> params = new ArrayList<>();

        if (state != null) {
            sql.append(" AND state = ?");
            params.add(state.name());
        }
        if (jobKey != null) {
            sql.append(" AND job_key = ?");
            params.add(jobKey);
        }
        sql.append(" ORDER BY created_at DESC LIMIT ?");
        params.add(limit);

        return queryJobs(sql.toString(), ps -> {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
        });
    }

    @Override
    public boolean acquire(UUID jobId, String acquiredBy, int expectedVersion) {
        String sql = """
            UPDATE scheduled_jobs SET
                state = 'ACQUIRED',
                acquired_by = ?,
                acquired_at = ?,
                version = version + 1
            WHERE id = ? AND state = 'PENDING' AND version = ?
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, acquiredBy);
            ps.setTimestamp(2, Timestamp.from(Instant.now()));
            ps.setObject(3, jobId);
            ps.setInt(4, expectedVersion);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            LOG.errorf(e, "Failed to acquire job %s", jobId);
            throw new DaoException("Failed to acquire job", e);
        }
    }

    @Override
    public JobStats getStats() {
        String sql = """
            SELECT state, COUNT(*) as cnt FROM scheduled_jobs
            GROUP BY state
            """;

        Map<String, Long> counts = new HashMap<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                counts.put(rs.getString("state"), rs.getLong("cnt"));
            }
        } catch (SQLException e) {
            LOG.errorf(e, "Failed to get job stats");
            throw new DaoException("Failed to get job stats", e);
        }

        return new JobStats(
            counts.getOrDefault("PENDING", 0L),
            counts.getOrDefault("WAITING", 0L),
            counts.getOrDefault("ACQUIRED", 0L),
            counts.getOrDefault("FIRING", 0L),
            counts.getOrDefault("COMPLETE", 0L),
            counts.getOrDefault("FAILED", 0L)
        );
    }

    private void setJobParameters(PreparedStatement ps, ScheduledJob job) throws SQLException {
        int idx = 1;
        ps.setObject(idx++, job.getId());
        ps.setString(idx++, job.getJobKey());
        ps.setString(idx++, job.getKeyPolicy().name());
        ps.setString(idx++, job.getSleepStart().name());
        ps.setString(idx++, job.getSleepDuration());
        ps.setInt(idx++, job.getSleepRepeat());
        ps.setString(idx++, job.getCronExpression());
        ps.setTimestamp(idx++, toTimestamp(job.getCronEnd()));
        setNullableInt(ps, idx++, job.getCronMaxCount());
        ps.setInt(idx++, job.getCronFireCount());
        ps.setTimestamp(idx++, toTimestamp(job.getFireAt()));
        ps.setTimestamp(idx++, toTimestamp(job.getEffectiveFireAt()));
        ps.setTimestamp(idx++, toTimestamp(job.getArrivedAt()));
        ps.setString(idx++, job.getDestinationTopic());
        ps.setBytes(idx++, job.getMessageKey());
        ps.setBytes(idx++, job.getMessageValue());
        ps.setString(idx++, JsonUtil.toJson(job.getHeaders()));
        ps.setString(idx++, job.getAdvisoryHeadersPattern());
        ps.setString(idx++, job.getState().name());
        ps.setInt(idx++, job.getMaxRetries());
        ps.setInt(idx++, job.getRetryCount());
        ps.setInt(idx++, job.getVersion());
        setNullableUuid(ps, idx++, job.getPredecessorId());
        ps.setInt(idx++, job.getSequenceNum());
        ps.setString(idx++, job.getAcquiredBy());
        ps.setTimestamp(idx++, toTimestamp(job.getAcquiredAt()));
        ps.setTimestamp(idx++, toTimestamp(job.getCreatedAt()));
        ps.setTimestamp(idx++, toTimestamp(job.getUpdatedAt()));
        ps.setString(idx++, job.getLastError());
    }

    private ScheduledJob mapJob(ResultSet rs) throws SQLException {
        ScheduledJob job = new ScheduledJob();
        job.setId(rs.getObject("id", UUID.class));
        job.setJobKey(rs.getString("job_key"));
        job.setKeyPolicy(KeyPolicy.valueOf(rs.getString("key_policy")));
        job.setSleepStart(SleepStart.valueOf(rs.getString("sleep_start")));
        job.setSleepDuration(rs.getString("sleep_duration"));
        job.setSleepRepeat(rs.getInt("sleep_repeat"));
        job.setCronExpression(rs.getString("cron_expression"));
        job.setCronEnd(toInstant(rs.getTimestamp("cron_end")));
        job.setCronMaxCount(getNullableInt(rs, "cron_max_count"));
        job.setCronFireCount(rs.getInt("cron_fire_count"));
        job.setFireAt(toInstant(rs.getTimestamp("fire_at")));
        job.setEffectiveFireAt(toInstant(rs.getTimestamp("effective_fire_at")));
        job.setArrivedAt(toInstant(rs.getTimestamp("arrived_at")));
        job.setDestinationTopic(rs.getString("destination_topic"));
        job.setMessageKey(rs.getBytes("message_key"));
        job.setMessageValue(rs.getBytes("message_value"));
        job.setHeaders(JsonUtil.fromJson(rs.getString("headers")));
        job.setAdvisoryHeadersPattern(rs.getString("advisory_headers_pattern"));
        job.setState(JobState.valueOf(rs.getString("state")));
        job.setMaxRetries(rs.getInt("max_retries"));
        job.setRetryCount(rs.getInt("retry_count"));
        job.setVersion(rs.getInt("version"));
        job.setPredecessorId(rs.getObject("predecessor_id", UUID.class));
        job.setSequenceNum(rs.getInt("sequence_num"));
        job.setAcquiredBy(rs.getString("acquired_by"));
        job.setAcquiredAt(toInstant(rs.getTimestamp("acquired_at")));
        job.setCreatedAt(toInstant(rs.getTimestamp("created_at")));
        job.setUpdatedAt(toInstant(rs.getTimestamp("updated_at")));
        job.setLastError(rs.getString("last_error"));
        return job;
    }

    private List<ScheduledJob> queryJobs(String sql, SqlConsumer<PreparedStatement> paramSetter) {
        List<ScheduledJob> jobs = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            paramSetter.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    jobs.add(mapJob(rs));
                }
            }
        } catch (SQLException e) {
            LOG.errorf(e, "Failed to query jobs");
            throw new DaoException("Failed to query jobs", e);
        }
        return jobs;
    }

    @FunctionalInterface
    interface SqlConsumer<T> {
        void accept(T t) throws SQLException;
    }

    private Timestamp toTimestamp(Instant instant) {
        return instant != null ? Timestamp.from(instant) : null;
    }

    private Instant toInstant(Timestamp ts) {
        return ts != null ? ts.toInstant() : null;
    }

    private void setNullableInt(PreparedStatement ps, int idx, Integer value) throws SQLException {
        if (value != null) {
            ps.setInt(idx, value);
        } else {
            ps.setNull(idx, Types.INTEGER);
        }
    }

    private Integer getNullableInt(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private void setNullableUuid(PreparedStatement ps, int idx, UUID value) throws SQLException {
        if (value != null) {
            ps.setObject(idx, value);
        } else {
            ps.setNull(idx, Types.OTHER);
        }
    }

}
