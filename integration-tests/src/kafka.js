import { Kafka } from 'kafkajs';
import { getConfig } from './config.js';

const config = getConfig();
const kafka = new Kafka({
  clientId: `integration-test-${config.flavor}`,
  brokers: config.kafkaBrokers,
});

let producer = null;

export async function getProducer() {
  if (!producer) {
    producer = kafka.producer();
    await producer.connect();
  }
  return producer;
}

export async function disconnectProducer() {
  if (producer) {
    await producer.disconnect();
    producer = null;
  }
}

export async function sendMessage(topic, value, headers = {}) {
  const prod = await getProducer();
  const kafkaHeaders = {};
  for (const [key, val] of Object.entries(headers)) {
    kafkaHeaders[key] = Buffer.from(String(val));
  }
  await prod.send({
    topic,
    messages: [{ value: Buffer.from(value), headers: kafkaHeaders }],
  });
}

export async function sendScheduledMessage(jobKey, options = {}) {
  const headers = {
    SCHEDULER_DESTINATION: options.destination || config.outputTopic,
    SCHEDULER_KEY: jobKey,
  };

  if (options.runAt) {
    headers.SCHEDULER_AT = options.runAt.toISOString();
  }
  if (options.wait) {
    headers.SCHEDULER_WAIT = options.wait;
  }
  if (options.waitStart) {
    headers.SCHEDULER_WAIT_START = options.waitStart;
  }
  if (options.waitRepeat) {
    headers.SCHEDULER_WAIT_REPEAT = String(options.waitRepeat);
  }
  if (options.keyPolicy) {
    headers.SCHEDULER_KEY_POLICY = options.keyPolicy;
  }
  if (options.retryCount !== undefined) {
    headers.SCHEDULER_RETRY_COUNT = String(options.retryCount);
  }
  if (options.cron) {
    headers.SCHEDULER_CRON = options.cron;
  }
  if (options.cronRepeat) {
    headers.SCHEDULER_CRON_REPEAT = String(options.cronRepeat);
  }
  if (options.cronUntil) {
    headers.SCHEDULER_CRON_UNTIL = options.cronUntil.toISOString();
  }
  if (options.advisoryHeaders) {
    headers.SCHEDULER_ADVISORY_HEADERS = options.advisoryHeaders;
  }
  if (options.customHeaders) {
    Object.assign(headers, options.customHeaders);
  }

  await sendMessage(config.inTopic, options.body || 'test', headers);
}

export async function pollDlqForMessage(correlationId, timeoutMs = 30000) {
  // Use admin to get offsets, then consumer without group to read messages
  const admin = kafka.admin();
  await admin.connect();

  try {
    // Check if topic exists and get offsets
    const topics = await admin.listTopics();
    if (!topics.includes(config.dlqTopic)) {
      await admin.disconnect();
      return null;
    }

    const offsets = await admin.fetchTopicOffsets(config.dlqTopic);
    const endOffset = parseInt(offsets[0]?.offset || '0');
    await admin.disconnect();

    if (endOffset === 0) {
      return null; // No messages in topic
    }

    // Now use consumer to read all messages from beginning
    const consumer = kafka.consumer({
      groupId: `dlq-reader-${Date.now()}-${Math.random()}`,
    });
    await consumer.connect();
    await consumer.subscribe({ topic: config.dlqTopic, fromBeginning: true });

    let found = null;
    const startTime = Date.now();

    await new Promise((resolve) => {
      const checkTimeout = setInterval(() => {
        if (found || (Date.now() - startTime) > timeoutMs) {
          clearInterval(checkTimeout);
          resolve();
        }
      }, 100);

      consumer.run({
        eachMessage: async ({ message }) => {
          const body = message.value.toString();
          if (body.includes(correlationId)) {
            const headers = {};
            for (const [key, value] of Object.entries(message.headers || {})) {
              headers[key] = value.toString();
            }
            found = { body, headers };
          }
        },
      });
    });

    await consumer.stop();
    await consumer.disconnect();
    return found;
  } catch (e) {
    console.error('DLQ poll error:', e.message);
    await admin.disconnect().catch(() => {});
    return null;
  }
}

export async function pollAdvisoryForEvent(eventType, jobKey, timeoutMs = 30000) {
  const consumer = kafka.consumer({ groupId: `advisory-poll-${Date.now()}-${Math.random()}` });
  await consumer.connect();
  await consumer.subscribe({ topic: config.advisoryTopic, fromBeginning: true });

  return new Promise((resolve) => {
    const timeout = setTimeout(async () => {
      await consumer.disconnect();
      resolve(null);
    }, timeoutMs);

    consumer.run({
      eachMessage: async ({ message }) => {
        const headers = {};
        for (const [key, value] of Object.entries(message.headers || {})) {
          headers[key] = value.toString();
        }
        if (headers.SCHEDULER_ADVISORY_EVENT === eventType &&
            (!jobKey || headers.SCHEDULER_KEY === jobKey)) {
          clearTimeout(timeout);
          await consumer.disconnect();
          resolve({ body: message.value.toString(), headers });
        }
      },
    });
  });
}
