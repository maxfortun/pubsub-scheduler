export type JobState = 'PENDING' | 'WAITING' | 'ACQUIRED' | 'RUNNING' | 'DONE' | 'FAILED';

export type TimingType = 'AT' | 'DURATION' | 'CRON';

export interface ScheduledJob {
  id: string;
  jobKey: string | null;
  destinationTopic: string;
  runAt: string;
  effectiveRunAt: string;
  state: JobState;
  keyPolicy: 'QUEUE' | 'SKIP' | 'REPLACE';
  predecessorId: string | null;
  sequenceNum: number;
  waitDuration: string | null;
  waitStart: 'SELF' | 'PREV' | null;
  waitRepeat: number;
  waitUntil: string | null;
  cronExpression: string | null;
  cronUntil: string | null;
  cronRepeat: number | null;
  cronRunCount: number;
  retryCount: number;
  maxRetries: number;
  lastError: string | null;
  acquiredBy: string | null;
  acquiredAt: string | null;
  createdAt: string;
  updatedAt: string;
  version: number;
  messageKey: string | null;
  messageValue: string | null;
}

export interface JobStats {
  pending: number;
  waiting: number;
  acquired: number;
  running: number;
  done: number;
  failed: number;
}

export interface PagedResult<T> {
  items: T[];
  offset: number;
  limit: number;
  total: number;
  hasMore: boolean;
}

export interface JobFilters {
  state?: JobState;
  states?: JobState[];
  key?: string;
  destination?: string;
  offset: number;
  limit: number;
}

export interface CreateJobRequest {
  destinationTopic: string;
  jobKey?: string;
  keyPolicy?: 'QUEUE' | 'SKIP' | 'REPLACE';
  runAt?: string;
  waitDuration?: string;
  waitStart?: 'SELF' | 'PREV';
  waitRepeat?: number;
  waitUntil?: string;
  cronExpression?: string;
  cronUntil?: string;
  cronRepeat?: number;
  maxRetries?: number;
  messageKey?: string;
  messageValue?: string;
  headers?: Record<string, string>;
}
