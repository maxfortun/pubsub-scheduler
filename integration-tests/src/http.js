import { getConfig } from './config.js';

const config = getConfig();

async function request(path, options = {}) {
  const url = `${config.url}${path}`;
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
  });
  return response;
}

export async function getJson(path) {
  const response = await request(path);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${path}`);
  }
  return response.json();
}

export async function getHealth() {
  // Use /api/instances as health check - works for all flavors including MySQL
  const response = await request('/api/instances');
  return response.ok;
}

export async function getConfig() {
  return getJson('/api/config');
}

export async function getInstances() {
  return getJson('/api/instances');
}

export async function getStats() {
  return getJson('/api/jobs/stats');
}

export async function getJobs(params = {}) {
  const searchParams = new URLSearchParams();
  if (params.key) searchParams.set('key', params.key);
  if (params.state) searchParams.set('state', params.state);
  if (params.destination) searchParams.set('destination', params.destination);
  if (params.limit) searchParams.set('limit', String(params.limit));
  if (params.offset) searchParams.set('offset', String(params.offset));

  const query = searchParams.toString();
  return getJson(`/api/jobs${query ? '?' + query : ''}`);
}

export async function getJobsByKey(key) {
  const result = await getJobs({ key, limit: 100 });
  return result.items || [];
}

export async function getJobById(id) {
  return getJson(`/api/jobs/${id}`);
}

export async function cancelJob(id) {
  const response = await request(`/api/jobs/${id}`, { method: 'DELETE' });
  return response.ok;
}

export function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export function futureTime(hours = 1) {
  return new Date(Date.now() + hours * 60 * 60 * 1000);
}

export function uuid() {
  return crypto.randomUUID();
}
