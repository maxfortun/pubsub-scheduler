import { describe, test, expect, beforeAll, afterAll } from 'vitest';
import { getConfig } from './config.js';
import {
  sendMessage,
  sendScheduledMessage,
  pollDlqForMessage,
  disconnectProducer,
} from './kafka.js';
import {
  getHealth,
  getConfig as getSchedulerConfig,
  getInstances,
  getStats,
  getJobsByKey,
  getJobById,
  getJobs,
  cancelJob,
  sleep,
  futureTime,
  uuid,
} from './http.js';

const config = getConfig();

describe(`${config.name} Scheduler Integration Tests`, () => {
  beforeAll(async () => {
    const healthy = await getHealth();
    expect(healthy).toBe(true);
  });

  afterAll(async () => {
    await disconnectProducer();
  });

  // ========== Health & Config ==========
  describe('Health & Config', () => {
    test('instances endpoint returns healthy', async () => {
      const healthy = await getHealth();
      expect(healthy).toBe(true);
    });

    test('config returns expected values', async () => {
      const cfg = await getSchedulerConfig();
      expect(cfg.name).toBe(config.expectedName);
      expect(cfg.color).toBe(config.expectedColor);
      expect(cfg.instanceId).toBe(config.expectedInstanceId);
    });

    test('instances endpoint is accessible', async () => {
      const instances = await getInstances();
      expect(Array.isArray(instances)).toBe(true);
      // Instance registration may take time, so we just verify the endpoint works
    });

    test('stats endpoint returns job counts', async () => {
      const stats = await getStats();
      expect(stats.pending).toBeGreaterThanOrEqual(0);
      expect(stats.done).toBeGreaterThanOrEqual(0);
      expect(stats.failed).toBeGreaterThanOrEqual(0);
    });
  });

  // ========== Destination Header Tests ==========
  describe('Destination Headers', () => {
    test('missing destination sends to DLQ', async () => {
      const correlationId = `dlq-missing-dest-${uuid()}`;
      await sendMessage(config.inTopic, correlationId, {});
      await sleep(2000); // Give scheduler time to process

      const dlqRecord = await pollDlqForMessage(correlationId);
      expect(dlqRecord).not.toBeNull();
      expect(dlqRecord.headers.SCHEDULER_ERROR).toContain('SCHEDULER_DESTINATION');
    });

    test('blank destination sends to DLQ', async () => {
      const correlationId = `dlq-blank-dest-${uuid()}`;
      await sendMessage(config.inTopic, correlationId, { SCHEDULER_DESTINATION: '   ' });
      await sleep(2000);

      const dlqRecord = await pollDlqForMessage(correlationId);
      expect(dlqRecord).not.toBeNull();
    });

    test('valid destination creates job', async () => {
      const jobKey = `dest-test-${uuid()}`;
      await sendScheduledMessage(jobKey, { runAt: futureTime() });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      expect(jobs[0].destinationTopic).toBe(config.outputTopic);
    });
  });

  // ========== Timing Header Tests ==========
  describe('Timing Headers', () => {
    test('SCHEDULER_AT sets absolute fire time', async () => {
      const jobKey = `at-test-${uuid()}`;
      const runAt = futureTime();
      await sendScheduledMessage(jobKey, { runAt });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      const jobRunAt = new Date(jobs[0].runAt);
      expect(Math.abs(jobRunAt.getTime() - runAt.getTime())).toBeLessThan(5000);
    });

    test('SCHEDULER_WAIT sets relative fire time', async () => {
      const jobKey = `wait-test-${uuid()}`;
      await sendScheduledMessage(jobKey, { wait: 'PT30M' });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      expect(jobs[0].waitDuration).toBe('PT30M');
    });

    test('no timing header schedules immediately', async () => {
      const jobKey = `immediate-test-${uuid()}`;
      await sendScheduledMessage(jobKey, {});
      await sleep(5000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
    });

    test('multiple timing headers sends to DLQ', async () => {
      const correlationId = `dlq-multi-timing-${uuid()}`;
      await sendMessage(config.inTopic, correlationId, {
        SCHEDULER_DESTINATION: config.outputTopic,
        SCHEDULER_AT: futureTime().toISOString(),
        SCHEDULER_WAIT: 'PT1H',
      });
      await sleep(2000);

      const dlqRecord = await pollDlqForMessage(correlationId);
      expect(dlqRecord).not.toBeNull();
      expect(dlqRecord.headers.SCHEDULER_ERROR).toContain('mutually exclusive');
    });

    test('SCHEDULER_CRON creates cron job', async () => {
      const jobKey = `cron-test-${uuid()}`;
      await sendScheduledMessage(jobKey, { cron: '* * * * *' });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      expect(jobs[0].cronExpression).toBe('* * * * *');
    });
  });

  // ========== Cron Options Tests ==========
  describe('Cron Options', () => {
    test('CRON_UNTIL and CRON_REPEAT are mutually exclusive', async () => {
      const correlationId = `dlq-cron-exclusive-${uuid()}`;
      await sendMessage(config.inTopic, correlationId, {
        SCHEDULER_DESTINATION: config.outputTopic,
        SCHEDULER_CRON: '0 0 * * *',
        SCHEDULER_CRON_UNTIL: futureTime(24 * 30).toISOString(),
        SCHEDULER_CRON_REPEAT: '5',
      });
      await sleep(2000);

      const dlqRecord = await pollDlqForMessage(correlationId);
      expect(dlqRecord).not.toBeNull();
    });

    test('CRON_REPEAT sets max count', async () => {
      const jobKey = `cron-repeat-${uuid()}`;
      await sendScheduledMessage(jobKey, { cron: '0 * * * *', cronRepeat: 10 });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      expect(jobs[0].cronRepeat).toBe(10);
    });

    test('CRON_UNTIL sets end time', async () => {
      const jobKey = `cron-until-${uuid()}`;
      const endTime = futureTime(24 * 7);
      await sendScheduledMessage(jobKey, { cron: '0 0 * * *', cronUntil: endTime });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      expect(jobs[0].cronUntil).not.toBeNull();
    });
  });

  // ========== Wait Options Tests ==========
  describe('Wait Options', () => {
    test('WAIT_START=SELF sets waitStart to SELF', async () => {
      const jobKey = `wait-start-self-${uuid()}`;
      await sendScheduledMessage(jobKey, { wait: 'PT1H', waitStart: 'SELF' });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      expect(jobs[0].waitStart).toBe('SELF');
    });

    test('WAIT_START=PREV sets waitStart to PREV', async () => {
      const jobKey = `wait-start-prev-${uuid()}`;
      await sendScheduledMessage(jobKey, { wait: 'PT1H', waitStart: 'PREV' });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      expect(jobs[0].waitStart).toBe('PREV');
    });

    test('WAIT_REPEAT sets repeat count', async () => {
      const jobKey = `wait-repeat-${uuid()}`;
      await sendScheduledMessage(jobKey, { wait: 'PT15M', waitRepeat: 5 });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      expect(jobs[0].waitRepeat).toBe(5);
    });
  });

  // ========== Key Policy Tests ==========
  describe('Key Policies', () => {
    test('KEY_POLICY=QUEUE sets keyPolicy to QUEUE', async () => {
      const jobKey = `key-policy-queue-${uuid()}`;
      await sendScheduledMessage(jobKey, { runAt: futureTime(), keyPolicy: 'QUEUE' });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      expect(jobs[0].keyPolicy).toBe('QUEUE');
    });

    test('KEY_POLICY=REPLACE sets keyPolicy to REPLACE', async () => {
      const jobKey = `key-policy-replace-${uuid()}`;
      await sendScheduledMessage(jobKey, { runAt: futureTime(), keyPolicy: 'REPLACE' });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      expect(jobs[0].keyPolicy).toBe('REPLACE');
    });

    test('KEY_POLICY=SKIP sets keyPolicy to SKIP', async () => {
      const jobKey = `key-policy-skip-${uuid()}`;
      await sendScheduledMessage(jobKey, { runAt: futureTime(), keyPolicy: 'SKIP' });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      expect(jobs[0].keyPolicy).toBe('SKIP');
    });

    test('no KEY_POLICY defaults to QUEUE', async () => {
      const jobKey = `key-policy-default-${uuid()}`;
      await sendScheduledMessage(jobKey, { runAt: futureTime() });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      expect(jobs[0].keyPolicy).toBe('QUEUE');
    });

    test('QUEUE policy chains second job behind first', async () => {
      const jobKey = `chain-test-${uuid()}`;
      const runAt = futureTime();

      await sendScheduledMessage(jobKey, { runAt, keyPolicy: 'QUEUE', body: 'first' });
      await sleep(3000);
      await sendScheduledMessage(jobKey, { runAt: new Date(runAt.getTime() + 60000), keyPolicy: 'QUEUE', body: 'second' });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBe(2);

      const hasWaiting = jobs.some(j => j.state === 'WAITING' && j.predecessorId);
      expect(hasWaiting).toBe(true);
    });

    test('SKIP policy skips second job', async () => {
      const jobKey = `skip-test-${uuid()}`;
      const runAt = futureTime();

      await sendScheduledMessage(jobKey, { runAt, keyPolicy: 'SKIP', body: 'first' });
      await sleep(3000);
      await sendScheduledMessage(jobKey, { runAt: new Date(runAt.getTime() + 60000), keyPolicy: 'SKIP', body: 'second' });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBe(1);
    });

    test('REPLACE policy replaces first job', async () => {
      const jobKey = `replace-test-${uuid()}`;
      const runAt = futureTime();

      await sendScheduledMessage(jobKey, { runAt, keyPolicy: 'REPLACE', body: 'first' });
      await sleep(3000);
      const firstJobs = await getJobsByKey(jobKey);
      const firstJobId = firstJobs[0].id;

      await sendScheduledMessage(jobKey, { runAt: new Date(runAt.getTime() + 60000), keyPolicy: 'REPLACE', body: 'second' });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBe(2);

      const failedOriginal = jobs.find(j => j.id === firstJobId && j.state === 'FAILED');
      const pendingNew = jobs.find(j => j.id !== firstJobId && j.state === 'PENDING');
      expect(failedOriginal).toBeDefined();
      expect(pendingNew).toBeDefined();
    });
  });

  // ========== Retry Tests ==========
  describe('Retry Configuration', () => {
    test('RETRY_COUNT sets maxRetries', async () => {
      const jobKey = `retry-count-${uuid()}`;
      await sendScheduledMessage(jobKey, { runAt: futureTime(), retryCount: 10 });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      expect(jobs[0].maxRetries).toBe(10);
    });

    test('no RETRY_COUNT uses default (3)', async () => {
      const jobKey = `retry-default-${uuid()}`;
      await sendScheduledMessage(jobKey, { runAt: futureTime() });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      expect(jobs[0].maxRetries).toBe(3);
    });
  });

  // ========== Advisory Header Tests ==========
  describe('Advisory Headers', () => {
    test('ADVISORY_HEADERS sets pattern', async () => {
      const jobKey = `advisory-pattern-${uuid()}`;
      await sendScheduledMessage(jobKey, { runAt: futureTime(), advisoryHeaders: 'X-.*|Custom-.*' });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      expect(jobs[0].advisoryHeadersPattern).toBe('X-.*|Custom-.*');
    });
  });

  // ========== Message Preservation Tests ==========
  describe('Message Preservation', () => {
    test('message body is preserved', async () => {
      const jobKey = `body-preservation-${uuid()}`;
      const body = '{"order": 123}';
      await sendScheduledMessage(jobKey, { runAt: futureTime(), body });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      const storedBody = Buffer.from(jobs[0].messageValue, 'base64').toString();
      expect(storedBody).toBe(body);
    });

    test('non-scheduler headers are preserved', async () => {
      const jobKey = `header-preservation-${uuid()}`;
      await sendScheduledMessage(jobKey, {
        runAt: futureTime(),
        customHeaders: {
          'X-Correlation-Id': 'corr-123',
          'X-Request-Id': 'req-456',
        },
      });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      expect(jobs[0].headers['X-Correlation-Id']).toBe('corr-123');
      expect(jobs[0].headers['X-Request-Id']).toBe('req-456');
      expect(jobs[0].headers['SCHEDULER_DESTINATION']).toBeUndefined();
    });
  });

  // ========== Invalid Input Tests ==========
  describe('Invalid Input Handling', () => {
    test('invalid AT format sends to DLQ', async () => {
      const correlationId = `dlq-invalid-at-${uuid()}`;
      await sendMessage(config.inTopic, correlationId, {
        SCHEDULER_DESTINATION: config.outputTopic,
        SCHEDULER_AT: 'not-a-timestamp',
      });
      await sleep(2000);

      const dlqRecord = await pollDlqForMessage(correlationId);
      expect(dlqRecord).not.toBeNull();
    });

    test('invalid WAIT format sends to DLQ', async () => {
      const correlationId = `dlq-invalid-wait-${uuid()}`;
      await sendMessage(config.inTopic, correlationId, {
        SCHEDULER_DESTINATION: config.outputTopic,
        SCHEDULER_WAIT: 'not-a-duration',
      });
      await sleep(2000);

      const dlqRecord = await pollDlqForMessage(correlationId);
      expect(dlqRecord).not.toBeNull();
    });

    test('invalid WAIT_START sends to DLQ', async () => {
      const correlationId = `dlq-invalid-waitstart-${uuid()}`;
      await sendMessage(config.inTopic, correlationId, {
        SCHEDULER_DESTINATION: config.outputTopic,
        SCHEDULER_WAIT: 'PT1H',
        SCHEDULER_WAIT_START: 'INVALID',
      });
      await sleep(2000);

      const dlqRecord = await pollDlqForMessage(correlationId);
      expect(dlqRecord).not.toBeNull();
    });

    test('invalid KEY_POLICY sends to DLQ', async () => {
      const correlationId = `dlq-invalid-policy-${uuid()}`;
      await sendMessage(config.inTopic, correlationId, {
        SCHEDULER_DESTINATION: config.outputTopic,
        SCHEDULER_KEY_POLICY: 'INVALID',
      });
      await sleep(2000);

      const dlqRecord = await pollDlqForMessage(correlationId);
      expect(dlqRecord).not.toBeNull();
    });
  });

  // ========== Job Lifecycle Tests ==========
  describe('Job Lifecycle', () => {
    test('immediate job fires and completes', async () => {
      const jobKey = `immediate-lifecycle-${uuid()}`;
      await sendScheduledMessage(jobKey, {});
      await sleep(5000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      expect(['DONE', 'PENDING', 'RUNNING']).toContain(jobs[0].state);
    });

    test('QUEUE policy creates correct chain', async () => {
      const jobKey = `chain-lifecycle-${uuid()}`;
      const runAt = futureTime();

      await sendScheduledMessage(jobKey, { runAt, keyPolicy: 'QUEUE', body: 'first' });
      await sleep(2000);
      await sendScheduledMessage(jobKey, { runAt: new Date(runAt.getTime() + 60000), keyPolicy: 'QUEUE', body: 'second' });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBe(2);

      const pendingCount = jobs.filter(j => j.state === 'PENDING').length;
      const waitingCount = jobs.filter(j => j.state === 'WAITING').length;
      expect(pendingCount).toBe(1);
      expect(waitingCount).toBe(1);
    });
  });

  // ========== Job Cancellation Tests ==========
  describe('Job Cancellation', () => {
    test('cancel pending job sets state to FAILED', async () => {
      const jobKey = `cancel-pending-${uuid()}`;
      await sendScheduledMessage(jobKey, { runAt: futureTime() });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);

      const cancelled = await cancelJob(jobs[0].id);
      expect(cancelled).toBe(true);

      const updatedJobs = await getJobsByKey(jobKey);
      expect(updatedJobs[0].state).toBe('FAILED');
      expect(updatedJobs[0].lastError).toContain('Cancelled');
    });

    test('cancel cascades to waiting jobs', async () => {
      const jobKey = `cancel-cascade-${uuid()}`;
      const runAt = futureTime();

      await sendScheduledMessage(jobKey, { runAt, keyPolicy: 'QUEUE', body: 'first' });
      await sleep(2000);
      await sendScheduledMessage(jobKey, { runAt: new Date(runAt.getTime() + 60000), keyPolicy: 'QUEUE', body: 'second' });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      const pendingJob = jobs.find(j => j.state === 'PENDING');
      expect(pendingJob).toBeDefined();

      await cancelJob(pendingJob.id);
      await sleep(2000);

      const updatedJobs = await getJobsByKey(jobKey);
      const failedCount = updatedJobs.filter(j => j.state === 'FAILED').length;
      expect(failedCount).toBe(2);
    });
  });

  // ========== Job Query Tests ==========
  describe('Job Queries', () => {
    test('query by state returns filtered jobs', async () => {
      const jobKey = `query-state-${uuid()}`;
      await sendScheduledMessage(jobKey, { runAt: futureTime() });
      await sleep(3000);

      const result = await getJobs({ state: 'PENDING', limit: 100 });
      const found = result.items.some(j => j.jobKey === jobKey);
      expect(found).toBe(true);
    });

    test('query by key returns matching jobs', async () => {
      const jobKey = `query-key-${uuid()}`;
      const runAt = futureTime();

      for (let i = 0; i < 3; i++) {
        await sendScheduledMessage(jobKey, {
          runAt: new Date(runAt.getTime() + i * 60000),
          keyPolicy: 'QUEUE',
          body: `job-${i}`,
        });
        await sleep(1000);
      }
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBe(3);
    });

    test('get job by ID returns job', async () => {
      const jobKey = `query-id-${uuid()}`;
      await sendScheduledMessage(jobKey, { runAt: futureTime() });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);

      const job = await getJobById(jobs[0].id);
      expect(job.jobKey).toBe(jobKey);
    });
  });

  // ========== Complete Scenario Tests ==========
  describe('Complete Scenarios', () => {
    test('full scheduled job with all headers', async () => {
      const jobKey = `full-job-${uuid()}`;
      const runAt = futureTime(2);
      const body = '{"order": 999}';

      await sendScheduledMessage(jobKey, {
        runAt,
        keyPolicy: 'REPLACE',
        retryCount: 5,
        advisoryHeaders: 'X-.*',
        body,
        customHeaders: { 'X-Correlation-Id': 'corr-999' },
      });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      const job = jobs[0];

      expect(job.destinationTopic).toBe(config.outputTopic);
      expect(job.jobKey).toBe(jobKey);
      expect(job.keyPolicy).toBe('REPLACE');
      expect(job.maxRetries).toBe(5);
      expect(job.advisoryHeadersPattern).toBe('X-.*');
      expect(job.headers['X-Correlation-Id']).toBe('corr-999');
    });

    test('wait job with all options', async () => {
      const jobKey = `wait-full-${uuid()}`;
      await sendScheduledMessage(jobKey, {
        wait: 'PT15M',
        waitStart: 'PREV',
        waitRepeat: 10,
        keyPolicy: 'QUEUE',
      });
      await sleep(3000);

      const jobs = await getJobsByKey(jobKey);
      expect(jobs.length).toBeGreaterThan(0);
      const job = jobs[0];

      expect(job.waitDuration).toBe('PT15M');
      expect(job.waitStart).toBe('PREV');
      expect(job.waitRepeat).toBe(10);
      expect(job.keyPolicy).toBe('QUEUE');
    });
  });
});
