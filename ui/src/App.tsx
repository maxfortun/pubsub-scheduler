import { useState, useEffect, useCallback } from 'react';
import type { ScheduledJob, JobStats, JobState, JobFilters, CreateJobRequest, TimingType } from './types';
import { fetchJobs, fetchStats, cancelJob, createJob, updateJob } from './api';
import { AtTiming, DurationTiming, CronTiming } from './components';
import './App.css';

interface UIConfig {
  name: string;
  color: string;
  instanceId: string;
}

function App() {
  const [jobs, setJobs] = useState<ScheduledJob[]>([]);
  const [stats, setStats] = useState<JobStats | null>(null);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedJob, setSelectedJob] = useState<ScheduledJob | null>(null);
  const [config, setConfig] = useState<UIConfig>({ name: 'Scheduler', color: '', instanceId: '' });
  const [filters, setFilters] = useState<JobFilters>({
    offset: 0,
    limit: 20,
  });
  const [columnFilters, setColumnFilters] = useState({
    states: [] as JobState[],
    key: '',
    destination: '',
  });
  const [activeFilter, setActiveFilter] = useState<string | null>(null);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [editingJob, setEditingJob] = useState<ScheduledJob | null>(null);
  const [timingType, setTimingType] = useState<TimingType>('DURATION');
  const [createForm, setCreateForm] = useState<CreateJobRequest>({
    destinationTopic: '',
    waitDuration: 'PT1M',
    messageValue: '',
  });
  const [saving, setSaving] = useState(false);

  const loadData = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const apiFilters = {
        ...filters,
        states: columnFilters.states,
        key: columnFilters.key || undefined,
        destination: columnFilters.destination || undefined,
      };
      const [jobsResult, statsResult] = await Promise.all([
        fetchJobs(apiFilters),
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
  }, [filters, columnFilters]);

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, 5000);
    return () => clearInterval(interval);
  }, [loadData]);

  useEffect(() => {
    fetch('/api/config')
      .then(res => res.json())
      .then(setConfig)
      .catch(() => {});
  }, []);

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

  const handleColumnFilter = (column: string, value: string) => {
    setColumnFilters(f => ({ ...f, [column]: value }));
    setFilters(f => ({ ...f, offset: 0 }));
    setActiveFilter(null);
  };

  const handleStateToggle = (state: JobState) => {
    setColumnFilters(f => {
      const newStates = f.states.includes(state)
        ? f.states.filter(s => s !== state)
        : [...f.states, state];
      return { ...f, states: newStates };
    });
    setFilters(f => ({ ...f, offset: 0 }));
  };

  const clearColumnFilter = (column: string) => {
    if (column === 'states') {
      setColumnFilters(f => ({ ...f, states: [] }));
    } else {
      setColumnFilters(f => ({ ...f, [column]: '' }));
    }
    setFilters(f => ({ ...f, offset: 0 }));
  };

  const getUniqueDestinations = () => {
    const destinations = new Set(jobs.map(j => j.destinationTopic));
    return Array.from(destinations).sort();
  };

  const filteredJobs = jobs;

  const handlePageChange = (newOffset: number) => {
    setFilters(f => ({ ...f, offset: Math.max(0, newOffset) }));
  };

  const validateForm = (): string[] => {
    const errors: string[] = [];
    const topicPattern = /^[a-zA-Z0-9._-]+$/;

    if (!createForm.destinationTopic?.trim()) {
      errors.push('Destination topic is required');
    } else {
      const topic = createForm.destinationTopic.trim();
      if (topic.length > 249) {
        errors.push('Topic name too long (max 249 characters)');
      }
      if (!topicPattern.test(topic)) {
        errors.push('Topic contains invalid characters (use alphanumeric, dots, dashes, underscores)');
      }
    }

    if (createForm.jobKey && createForm.jobKey.length > 255) {
      errors.push('Job key too long (max 255 characters)');
    }

    if (timingType === 'AT') {
      if (!createForm.runAt) {
        errors.push('Fire time is required for AT timing');
      }
    } else if (timingType === 'DURATION') {
      if (!createForm.waitDuration) {
        errors.push('Duration is required');
      } else {
        const durationPattern = /^P(?:\d+D)?(?:T(?:\d+H)?(?:\d+M)?(?:\d+S)?)?$/i;
        if (!durationPattern.test(createForm.waitDuration.trim())) {
          errors.push('Duration must be ISO 8601 format (e.g., PT30S, PT5M, PT1H, P1D)');
        }
      }
    } else if (timingType === 'CRON') {
      if (!createForm.cronExpression) {
        errors.push('Cron expression is required');
      } else {
        const parts = createForm.cronExpression.trim().split(/\s+/);
        if (parts.length < 5 || parts.length > 6) {
          errors.push('Cron expression should have 5 or 6 parts (e.g., "0 * * * *")');
        }
      }
    }

    return errors;
  };

  const handleTimingTypeChange = (type: TimingType) => {
    setTimingType(type);
    setCreateForm(f => ({
      ...f,
      runAt: undefined,
      waitDuration: type === 'DURATION' ? 'PT1M' : undefined,
      waitRepeat: undefined,
      waitUntil: undefined,
      cronExpression: undefined,
      cronUntil: undefined,
      cronRepeat: undefined,
    }));
  };

  const handleFormUpdate = (updates: Partial<CreateJobRequest>) => {
    setCreateForm(f => ({ ...f, ...updates }));
  };

  const handleEdit = (job: ScheduledJob) => {
    setEditingJob(job);
    setShowCreateForm(true);

    // Determine timing type
    let type: TimingType = 'DURATION';
    if (job.cronExpression) {
      type = 'CRON';
    } else if (job.runAt && !job.waitDuration) {
      type = 'AT';
    }
    setTimingType(type);

    // Populate form
    setCreateForm({
      destinationTopic: job.destinationTopic,
      jobKey: job.jobKey || undefined,
      keyPolicy: job.keyPolicy,
      runAt: job.runAt,
      waitDuration: job.waitDuration || undefined,
      waitStart: job.waitStart || undefined,
      waitRepeat: job.waitRepeat || undefined,
      waitUntil: job.waitUntil || undefined,
      cronExpression: job.cronExpression || undefined,
      cronUntil: job.cronUntil || undefined,
      cronRepeat: job.cronRepeat || undefined,
      maxRetries: job.maxRetries,
      messageKey: job.messageKey || undefined,
      messageValue: job.messageValue || undefined,
    });
  };

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault();
    const errors = validateForm();
    if (errors.length > 0) {
      alert(errors.join('\n'));
      return;
    }
    try {
      setSaving(true);
      if (editingJob) {
        await updateJob(editingJob.id, createForm);
        setSelectedJob(null);
      } else {
        await createJob(createForm);
      }
      setShowCreateForm(false);
      setEditingJob(null);
      setTimingType('DURATION');
      setCreateForm({ destinationTopic: '', waitDuration: 'PT1M', messageValue: '' });
      loadData();
    } catch (err) {
      alert(err instanceof Error ? err.message : 'Failed to save job');
    } finally {
      setSaving(false);
    }
  };

  const handleCloseForm = () => {
    setShowCreateForm(false);
    setEditingJob(null);
    setTimingType('DURATION');
    setCreateForm({ destinationTopic: '', waitDuration: 'PT1M', messageValue: '' });
  };

  const formatDate = (dateStr: string | null | undefined) => {
    if (!dateStr) return '-';
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) return '-';
    return date.toLocaleString();
  };

  const getStateColor = (state: JobState) => {
    switch (state) {
      case 'PENDING': return '#3498db';
      case 'WAITING': return '#9b59b6';
      case 'ACQUIRED': return '#f39c12';
      case 'RUNNING': return '#e67e22';
      case 'DONE': return '#27ae60';
      case 'FAILED': return '#e74c3c';
      default: return '#95a5a6';
    }
  };

  const isCancellable = (state: JobState) => state === 'PENDING' || state === 'WAITING';

  const handleCancelClick = (e: React.MouseEvent, jobId: string) => {
    e.stopPropagation();
    handleCancel(jobId);
  };

  return (
    <div className="app">
      <header className="header">
        <h1 style={config.color ? { color: config.color } : undefined}>{config.name}</h1>
        <div className="header-actions">
          <button className="create-btn" onClick={() => setShowCreateForm(true)}>
            + New Job
          </button>
          <button onClick={loadData} disabled={loading}>
            {loading ? 'Loading...' : 'Refresh'}
          </button>
        </div>
      </header>

      {error && <div className="error">{error}</div>}

      {showCreateForm && (
        <div className="modal-overlay" onClick={handleCloseForm}>
          <div className="modal" onClick={e => e.stopPropagation()}>
            <div className="modal-header">
              <h2>{editingJob ? 'Edit Job' : 'Create New Job'}</h2>
              <button className="close-btn" onClick={handleCloseForm}>x</button>
            </div>
            <form onSubmit={handleSave} className="create-form">
              <div className="form-group">
                <label>Destination Topic *</label>
                <input
                  type="text"
                  value={createForm.destinationTopic}
                  onChange={e => setCreateForm(f => ({ ...f, destinationTopic: e.target.value }))}
                  placeholder="my-output-topic"
                  required
                />
              </div>
              <div className="form-row">
                <div className="form-group">
                  <label>Job Key</label>
                  <input
                    type="text"
                    value={createForm.jobKey || ''}
                    onChange={e => setCreateForm(f => ({ ...f, jobKey: e.target.value || undefined }))}
                    placeholder="Optional key for chaining"
                  />
                </div>
                <div className="form-group">
                  <label>Key Policy</label>
                  <select
                    value={createForm.keyPolicy || 'QUEUE'}
                    onChange={e => setCreateForm(f => ({ ...f, keyPolicy: e.target.value as 'QUEUE' | 'SKIP' | 'REPLACE' }))}
                  >
                    <option value="QUEUE">Queue</option>
                    <option value="SKIP">Skip</option>
                    <option value="REPLACE">Replace</option>
                  </select>
                </div>
              </div>
              <div className="form-group">
                <label>Timing Type</label>
                <select
                  value={timingType}
                  onChange={e => handleTimingTypeChange(e.target.value as TimingType)}
                  className="timing-select"
                >
                  <option value="AT">AT - Fire at specific time</option>
                  <option value="DURATION">DURATION - Fire after delay</option>
                  <option value="CRON">CRON - Recurring schedule</option>
                </select>
              </div>

              {timingType === 'AT' && (
                <AtTiming form={createForm} onChange={handleFormUpdate} />
              )}
              {timingType === 'DURATION' && (
                <DurationTiming form={createForm} onChange={handleFormUpdate} />
              )}
              {timingType === 'CRON' && (
                <CronTiming form={createForm} onChange={handleFormUpdate} />
              )}
              <div className="form-group">
                <label>Message Key</label>
                <input
                  type="text"
                  value={createForm.messageKey || ''}
                  onChange={e => setCreateForm(f => ({ ...f, messageKey: e.target.value || undefined }))}
                  placeholder="Kafka message key"
                />
              </div>
              <div className="form-group">
                <label>Message Payload</label>
                <textarea
                  value={createForm.messageValue || ''}
                  onChange={e => setCreateForm(f => ({ ...f, messageValue: e.target.value || undefined }))}
                  placeholder='{"orderId": 123}'
                  rows={4}
                />
              </div>
              <div className="form-actions">
                <button type="button" onClick={handleCloseForm}>Cancel</button>
                <button type="submit" className="create-btn" disabled={saving}>
                  {saving ? 'Saving...' : (editingJob ? 'Save Changes' : 'Create Job')}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

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
          <div className="stat-card" style={{ borderColor: getStateColor('RUNNING') }}>
            <div className="stat-value">{stats.running}</div>
            <div className="stat-label">Running</div>
          </div>
          <div className="stat-card" style={{ borderColor: getStateColor('DONE') }}>
            <div className="stat-value">{stats.done}</div>
            <div className="stat-label">Done</div>
          </div>
          <div className="stat-card" style={{ borderColor: getStateColor('FAILED') }}>
            <div className="stat-value">{stats.failed}</div>
            <div className="stat-label">Failed</div>
          </div>
        </div>
      )}

      <div className="filters-bar">
        <span className="total-count">{total} jobs total, {filteredJobs.length} shown</span>
        {(columnFilters.states.length > 0 || columnFilters.key || columnFilters.destination) && (
          <button
            className="clear-filters-btn"
            onClick={() => {
              setColumnFilters({ states: [], key: '', destination: '' });
              setFilters(f => ({ ...f, state: undefined, key: undefined, offset: 0 }));
            }}
          >
            Clear All Filters
          </button>
        )}
      </div>

      <div className="main-content">
        <div className="jobs-list">
          <table>
            <thead>
              <tr>
                <th className="filterable-header">
                  <div className="header-content">
                    <span>State {columnFilters.states.length > 0 && `(${columnFilters.states.length})`}</span>
                    <button
                      className={`filter-btn ${columnFilters.states.length > 0 ? 'active' : ''}`}
                      onClick={() => setActiveFilter(activeFilter === 'state' ? null : 'state')}
                    >
                      ▼
                    </button>
                  </div>
                  {activeFilter === 'state' && (
                    <div className="filter-dropdown checkbox-dropdown">
                      {(['PENDING', 'WAITING', 'ACQUIRED', 'RUNNING', 'DONE', 'FAILED'] as JobState[]).map(state => (
                        <label key={state} className="checkbox-label">
                          <input
                            type="checkbox"
                            checked={columnFilters.states.includes(state)}
                            onChange={() => handleStateToggle(state)}
                          />
                          <span
                            className="state-badge-small"
                            style={{ backgroundColor: getStateColor(state) }}
                          >
                            {state}
                          </span>
                        </label>
                      ))}
                      {columnFilters.states.length > 0 && (
                        <button className="clear-btn" onClick={() => clearColumnFilter('states')}>Clear All</button>
                      )}
                    </div>
                  )}
                </th>
                <th className="filterable-header">
                  <div className="header-content">
                    <span>Job Key</span>
                    <button
                      className={`filter-btn ${columnFilters.key ? 'active' : ''}`}
                      onClick={() => setActiveFilter(activeFilter === 'key' ? null : 'key')}
                    >
                      ▼
                    </button>
                  </div>
                  {activeFilter === 'key' && (
                    <div className="filter-dropdown">
                      <input
                        type="text"
                        placeholder="Filter by key..."
                        value={columnFilters.key}
                        onChange={e => handleColumnFilter('key', e.target.value)}
                        autoFocus
                      />
                      {columnFilters.key && (
                        <button className="clear-btn" onClick={() => clearColumnFilter('key')}>Clear</button>
                      )}
                    </div>
                  )}
                </th>
                <th className="filterable-header">
                  <div className="header-content">
                    <span>Destination</span>
                    <button
                      className={`filter-btn ${columnFilters.destination ? 'active' : ''}`}
                      onClick={() => setActiveFilter(activeFilter === 'destination' ? null : 'destination')}
                    >
                      ▼
                    </button>
                  </div>
                  {activeFilter === 'destination' && (
                    <div className="filter-dropdown">
                      <select
                        value={columnFilters.destination}
                        onChange={e => handleColumnFilter('destination', e.target.value)}
                        autoFocus
                      >
                        <option value="">All Destinations</option>
                        {getUniqueDestinations().map(dest => (
                          <option key={dest} value={dest}>{dest}</option>
                        ))}
                      </select>
                      {columnFilters.destination && (
                        <button className="clear-btn" onClick={() => clearColumnFilter('destination')}>Clear</button>
                      )}
                    </div>
                  )}
                </th>
                <th>Run At</th>
                <th>Created</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {filteredJobs.map(job => (
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
                  <td>{formatDate(job.effectiveRunAt || job.runAt)}</td>
                  <td>{formatDate(job.createdAt)}</td>
                  <td className="actions-cell">
                    {isCancellable(job.state) && (
                      <button
                        className="cancel-btn-small"
                        onClick={(e) => handleCancelClick(e, job.id)}
                        title="Cancel job"
                      >
                        Cancel
                      </button>
                    )}
                  </td>
                </tr>
              ))}
              {filteredJobs.length === 0 && (
                <tr>
                  <td colSpan={6} className="no-data">No jobs found</td>
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
                <span>{formatDate(selectedJob.runAt)}</span>
              </div>
              <div className="detail-row">
                <label>Effective Fire At:</label>
                <span>{formatDate(selectedJob.effectiveRunAt)}</span>
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
              {selectedJob.waitDuration && (
                <>
                  <div className="detail-row">
                    <label>Wait Duration:</label>
                    <span>{selectedJob.waitDuration}</span>
                  </div>
                  <div className="detail-row">
                    <label>Wait Start:</label>
                    <span>{selectedJob.waitStart}</span>
                  </div>
                  <div className="detail-row">
                    <label>Wait Repeat:</label>
                    <span>{selectedJob.waitRepeat}</span>
                  </div>
                  {selectedJob.waitUntil && (
                    <div className="detail-row">
                      <label>Wait Until:</label>
                      <span>{formatDate(selectedJob.waitUntil)}</span>
                    </div>
                  )}
                </>
              )}
              {selectedJob.cronExpression && (
                <>
                  <div className="detail-row">
                    <label>Cron:</label>
                    <span className="monospace">{selectedJob.cronExpression}</span>
                  </div>
                  <div className="detail-row">
                    <label>Run Count:</label>
                    <span>{selectedJob.cronRunCount} / {selectedJob.cronRepeat || 'unlimited'}</span>
                  </div>
                  {selectedJob.cronUntil && (
                    <div className="detail-row">
                      <label>Cron Until:</label>
                      <span>{formatDate(selectedJob.cronUntil)}</span>
                    </div>
                  )}
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
                    className="edit-btn"
                    onClick={() => handleEdit(selectedJob)}
                    style={{
                      background: '#3498db',
                      border: 'none',
                      color: 'white',
                      padding: '8px 16px',
                      borderRadius: '4px',
                      cursor: 'pointer',
                      marginRight: '8px'
                    }}
                  >
                    Edit Job
                  </button>
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
