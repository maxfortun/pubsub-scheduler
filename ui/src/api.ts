import type { ScheduledJob, JobStats, PagedResult, JobFilters, CreateJobRequest } from './types';

const API_BASE = '/api';

export async function fetchJobs(filters: JobFilters): Promise<PagedResult<ScheduledJob>> {
  const params = new URLSearchParams();
  if (filters.state) params.set('state', filters.state);
  if (filters.key) params.set('key', filters.key);
  params.set('offset', filters.offset.toString());
  params.set('limit', filters.limit.toString());

  const response = await fetch(`${API_BASE}/jobs?${params}`);
  if (!response.ok) throw new Error('Failed to fetch jobs');
  return response.json();
}

export async function fetchJob(id: string): Promise<ScheduledJob> {
  const response = await fetch(`${API_BASE}/jobs/${id}`);
  if (!response.ok) throw new Error('Job not found');
  return response.json();
}

export async function cancelJob(id: string): Promise<void> {
  const response = await fetch(`${API_BASE}/jobs/${id}`, { method: 'DELETE' });
  if (!response.ok) throw new Error('Failed to cancel job');
}

export async function fetchStats(): Promise<JobStats> {
  const response = await fetch(`${API_BASE}/jobs/stats`);
  if (!response.ok) throw new Error('Failed to fetch stats');
  return response.json();
}

export async function createJob(request: CreateJobRequest): Promise<ScheduledJob> {
  const response = await fetch(`${API_BASE}/jobs`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(request),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Failed to create job' }));
    throw new Error(error.error || 'Failed to create job');
  }
  return response.json();
}

export async function updateJob(id: string, request: CreateJobRequest): Promise<ScheduledJob> {
  const response = await fetch(`${API_BASE}/jobs/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(request),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Failed to update job' }));
    throw new Error(error.error || 'Failed to update job');
  }
  return response.json();
}
