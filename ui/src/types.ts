export type JobState = 'PENDING' | 'WAITING' | 'ACQUIRED' | 'FIRING' | 'COMPLETE' | 'FAILED';

export type TimingType = 'AT' | 'DURATION' | 'CRON';

export interface ScheduledJob {
  id: string;
  jobKey: string | null;
  destinationTopic: string;
  fireAt: string;
  effectiveFireAt: string;
  state: JobState;
  keyPolicy: 'QUEUE' | 'SKIP' | 'REPLACE';
  predecessorId: string | null;
  sequenceNum: number;
  sleepDuration: string | null;
  sleepStart: 'SELF' | 'PREV' | null;
  sleepRepeat: number;
  cronExpression: string | null;
  cronEnd: string | null;
  cronMaxCount: number | null;
  cronFireCount: number;
  retryCount: number;
  maxRetries: number;
  lastError: string | null;
  acquiredBy: string | null;
  acquiredAt: string | null;
  createdAt: string;
  updatedAt: string;
  version: number;
}

export interface JobStats {
  pending: number;
  waiting: number;
  acquired: number;
  firing: number;
  complete: number;
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
  key?: string;
  offset: number;
  limit: number;
}

export interface CreateJobRequest {
  destinationTopic: string;
  jobKey?: string;
  keyPolicy?: 'QUEUE' | 'SKIP' | 'REPLACE';
  fireAt?: string;
  sleepDuration?: string;
  sleepStart?: 'SELF' | 'PREV';
  sleepRepeat?: number;
  cronExpression?: string;
  cronEnd?: string;
  cronMaxCount?: number;
  maxRetries?: number;
  messageKey?: string;
  messageValue?: string;
  headers?: Record<string, string>;
}
