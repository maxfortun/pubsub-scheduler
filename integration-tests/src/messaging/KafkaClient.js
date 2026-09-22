import { Kafka } from 'kafkajs';
import { MessagingClient } from './MessagingClient.js';

export class KafkaClient extends MessagingClient {
  constructor(config) {
    super(config);
    this.kafka = new Kafka({
      clientId: `test-client-${config.flavor}`,
      brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
    });
    this.producer = null;
  }

  async sendMessage(topic, value, headers = {}) {
    if (!this.producer) {
      this.producer = this.kafka.producer();
      await this.producer.connect();
    }

    const kafkaHeaders = {};
    for (const [key, val] of Object.entries(headers)) {
      kafkaHeaders[key] = String(val);
    }

    await this.producer.send({
      topic,
      messages: [{ value, headers: kafkaHeaders }],
    });
  }

  pollDlqForMessage(correlationId, timeoutMs = 30000) {
    return new Promise(async (resolve) => {
      const consumer = this.kafka.consumer({
        groupId: `test-dlq-consumer-${this.config.flavor}-${Date.now()}`,
      });

      let done = false;
      let timer;

      const finish = async (result) => {
        if (done) return;
        done = true;
        clearTimeout(timer);
        try {
          await consumer.disconnect();
        } catch (e) {
          // Ignore
        }
        resolve(result);
      };

      try {
        await consumer.connect();

        consumer.on(consumer.events.GROUP_JOIN, () => {
          timer = setTimeout(() => finish(null), timeoutMs);
        });

        await consumer.subscribe({ topic: this.config.dlqTopic, fromBeginning: true });

        await consumer.run({
          eachMessage: async ({ message }) => {
            if (done) return;
            const body = message.value?.toString() || '';
            if (body.includes(correlationId)) {
              const headers = {};
              if (message.headers) {
                for (const [key, val] of Object.entries(message.headers)) {
                  headers[key] = val?.toString();
                }
              }
              finish({ body, headers });
            }
          },
        });
      } catch (err) {
        finish(null);
      }
    });
  }

  getInDestination() {
    return this.config.inTopic;
  }

  async disconnect() {
    if (this.producer) {
      await this.producer.disconnect();
      this.producer = null;
    }
  }
}
