import { useState, useEffect, useCallback } from 'react';
import type { ScheduledJob, JobStats, JobState, JobFilters } from './types';
import { fetchJobs, fetchStats, cancelJob } from './api';
import './App.css';

function App() {
  const [jobs, setJobs] = useState<ScheduledJob[]>([]);
  const [stats, setStats] = useState<JobStats | null>(null);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedJob, setSelectedJob] = useState<ScheduledJob | null>(null);
  const [filters, setFilters] = useState<JobFilters>({
    offset: 0,
    limit: 20,
  });

  const loadData = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const [jobsResult, statsResult] = await Promise.all([
        fetchJobs(filters),
        fetchStats(),
      ]);
      setJobs(jobsResult.items);
      setTotal(jobsResult.total);
      setStats(statsResult);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  }, [filters]);

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, 5000);
    return () => clearInterval(interval);
  }, [loadData]);

  const handleCancel = async (id: string) => {
    if (!confirm('Are you sure you want to cancel this job?')) return;
    try {
      await cancelJob(id);
      loadData();
      setSelectedJob(null);
    } catch (err) {
      alert('Failed to cancel job');
    }
  };

  const handleStateFilter = (state: JobState | '') => {
    setFilters(f => ({ ...f, state: state || undefined, offset: 0 }));
  };

  const handleKeyFilter = (key: string) => {
    setFilters(f => ({ ...f, key: key || undefined, offset: 0 }));
  };

  const handlePageChange = (newOffset: number) => {
    setFilters(f => ({ ...f, offset: Math.max(0, newOffset) }));
  };

  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleString();
  };

  const getStateColor = (state: JobState) => {
    switch (state) {
      case 'PENDING': return '#3498db';
      case 'WAITING': return '#9b59b6';
      case 'ACQUIRED': return '#f39c12';
      case 'FIRING': return '#e67e22';
      case 'COMPLETE': return '#27ae60';
      case 'FAILED': return '#e74c3c';
      default: return '#95a5a6';
    }
  };

  return (
    <div className="app">
      <header className="header">
        <h1>Kafka Scheduler</h1>
        <button onClick={loadData} disabled={loading}>
          {loading ? 'Loading...' : 'Refresh'}
        </button>
      </header>

      {error && <div className="error">{error}</div>}

      {stats && (
        <div className="stats">
          <div className="stat-card" style={{ borderColor: getStateColor('PENDING') }}>
            <div className="stat-value">{stats.pending}</div>
            <div className="stat-label">Pending</div>
          </div>
          <div className="stat-card" style={{ borderColor: getStateColor('WAITING') }}>
            <div className="stat-value">{stats.waiting}</div>
            <div className="stat-label">Waiting</div>
          </div>
          <div className="stat-card" style={{ borderColor: getStateColor('ACQUIRED') }}>
            <div className="stat-value">{stats.acquired}</div>
            <div className="stat-label">Acquired</div>
          </div>
          <div className="stat-card" style={{ borderColor: getStateColor('FIRING') }}>
            <div className="stat-value">{stats.firing}</div>
            <div className="stat-label">Firing</div>
          </div>
          <div className="stat-card" style={{ borderColor: getStateColor('COMPLETE') }}>
            <div className="stat-value">{stats.complete}</div>
            <div className="stat-label">Complete</div>
          </div>
          <div className="stat-card" style={{ borderColor: getStateColor('FAILED') }}>
            <div className="stat-value">{stats.failed}</div>
            <div className="stat-label">Failed</div>
          </div>
        </div>
      )}

      <div className="filters">
        <select
          value={filters.state || ''}
          onChange={e => handleStateFilter(e.target.value as JobState | '')}
        >
          <option value="">All States</option>
          <option value="PENDING">Pending</option>
          <option value="WAITING">Waiting</option>
          <option value="ACQUIRED">Acquired</option>
          <option value="FIRING">Firing</option>
          <option value="COMPLETE">Complete</option>
          <option value="FAILED">Failed</option>
        </select>
        <input
          type="text"
          placeholder="Filter by job key..."
          value={filters.key || ''}
          onChange={e => handleKeyFilter(e.target.value)}
        />
        <span className="total-count">{total} jobs</span>
      </div>

      <div className="main-content">
        <div className="jobs-list">
          <table>
            <thead>
              <tr>
                <th>State</th>
                <th>Job Key</th>
                <th>Destination</th>
                <th>Fire At</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map(job => (
                <tr
                  key={job.id}
                  onClick={() => setSelectedJob(job)}
                  className={selectedJob?.id === job.id ? 'selected' : ''}
                >
                  <td>
                    <span
                      className="state-badge"
                      style={{ backgroundColor: getStateColor(job.state) }}
                    >
                      {job.state}
                    </span>
                  </td>
                  <td className="job-key">{job.jobKey || '-'}</td>
                  <td className="destination">{job.destinationTopic}</td>
                  <td>{formatDate(job.effectiveFireAt)}</td>
                  <td>{formatDate(job.createdAt)}</td>
                </tr>
              ))}
              {jobs.length === 0 && (
                <tr>
                  <td colSpan={5} className="no-data">No jobs found</td>
                </tr>
              )}
            </tbody>
          </table>

          <div className="pagination">
            <button
              disabled={filters.offset === 0}
              onClick={() => handlePageChange(filters.offset - filters.limit)}
            >
              Previous
            </button>
            <span>
              {filters.offset + 1} - {Math.min(filters.offset + filters.limit, total)} of {total}
            </span>
            <button
              disabled={filters.offset + filters.limit >= total}
              onClick={() => handlePageChange(filters.offset + filters.limit)}
            >
              Next
            </button>
          </div>
        </div>

        {selectedJob && (
          <div className="job-details">
            <div className="details-header">
              <h2>Job Details</h2>
              <button className="close-btn" onClick={() => setSelectedJob(null)}>x</button>
            </div>
            <div className="details-content">
              <div className="detail-row">
                <label>ID:</label>
                <span className="monospace">{selectedJob.id}</span>
              </div>
              <div className="detail-row">
                <label>State:</label>
                <span
                  className="state-badge"
                  style={{ backgroundColor: getStateColor(selectedJob.state) }}
                >
                  {selectedJob.state}
                </span>
              </div>
              <div className="detail-row">
                <label>Job Key:</label>
                <span>{selectedJob.jobKey || '-'}</span>
              </div>
              <div className="detail-row">
                <label>Destination:</label>
                <span>{selectedJob.destinationTopic}</span>
              </div>
              <div className="detail-row">
                <label>Fire At:</label>
                <span>{formatDate(selectedJob.fireAt)}</span>
              </div>
              <div className="detail-row">
                <label>Effective Fire At:</label>
                <span>{formatDate(selectedJob.effectiveFireAt)}</span>
              </div>
              <div className="detail-row">
                <label>Key Policy:</label>
                <span>{selectedJob.keyPolicy}</span>
              </div>
              {selectedJob.predecessorId && (
                <div className="detail-row">
                  <label>Predecessor:</label>
                  <span className="monospace">{selectedJob.predecessorId}</span>
                </div>
              )}
              {selectedJob.sleepDuration && (
                <>
                  <div className="detail-row">
                    <label>Sleep Duration:</label>
                    <span>{selectedJob.sleepDuration}</span>
                  </div>
                  <div className="detail-row">
                    <label>Sleep Start:</label>
                    <span>{selectedJob.sleepStart}</span>
                  </div>
                  <div className="detail-row">
                    <label>Sleep Repeat:</label>
                    <span>{selectedJob.sleepRepeat}</span>
                  </div>
                </>
              )}
              {selectedJob.cronExpression && (
                <>
                  <div className="detail-row">
                    <label>Cron:</label>
                    <span className="monospace">{selectedJob.cronExpression}</span>
                  </div>
                  <div className="detail-row">
                    <label>Fire Count:</label>
                    <span>{selectedJob.cronFireCount} / {selectedJob.cronMaxCount || 'unlimited'}</span>
                  </div>
                </>
              )}
              <div className="detail-row">
                <label>Retries:</label>
                <span>{selectedJob.retryCount} / {selectedJob.maxRetries}</span>
              </div>
              {selectedJob.lastError && (
                <div className="detail-row error-row">
                  <label>Last Error:</label>
                  <span>{selectedJob.lastError}</span>
                </div>
              )}
              <div className="detail-row">
                <label>Created:</label>
                <span>{formatDate(selectedJob.createdAt)}</span>
              </div>
              <div className="detail-row">
                <label>Updated:</label>
                <span>{formatDate(selectedJob.updatedAt)}</span>
              </div>
              {(selectedJob.state === 'PENDING' || selectedJob.state === 'WAITING') && (
                <div className="actions">
                  <button
                    className="cancel-btn"
                    onClick={() => handleCancel(selectedJob.id)}
                  >
                    Cancel Job
                  </button>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
