import stompit from 'stompit';
import { MessagingClient } from './MessagingClient.js';

export class ActiveMQClient extends MessagingClient {
  constructor(config) {
    super(config);
    this.connectionOptions = {
      host: process.env.ACTIVEMQ_HOST || 'localhost',
      port: parseInt(process.env.ACTIVEMQ_PORT || '61613'),
      connectHeaders: {
        host: '/',
        login: 'admin',
        passcode: 'admin',
        'heart-beat': '0,0',
      },
    };
  }

  async sendMessage(queue, value, headers = {}) {
    return new Promise((resolve, reject) => {
      stompit.connect(this.connectionOptions, (err, client) => {
        if (err) {
          reject(err);
          return;
        }

        // Use queue name directly without /queue/ prefix - Artemis maps destinations by address name
        const sendHeaders = {
          destination: queue,
          'content-type': 'text/plain',
        };

        for (const [key, val] of Object.entries(headers)) {
          sendHeaders[key] = String(val);
        }

        const frame = client.send(sendHeaders);
        frame.write(value);
        frame.end();

        client.disconnect((disconnectErr) => {
          if (disconnectErr) reject(disconnectErr);
          else resolve();
        });
      });
    });
  }

  async pollDlqForMessage(correlationId, timeoutMs = 30000) {
    return new Promise((resolve, reject) => {
      stompit.connect(this.connectionOptions, (err, client) => {
        if (err) {
          reject(err);
          return;
        }

        const timer = setTimeout(() => {
          client.disconnect(() => resolve(null));
        }, timeoutMs);

        // Use queue name directly without /queue/ prefix - Artemis maps destinations by address name
        const subscribeHeaders = {
          destination: this.config.dlqQueue,
          ack: 'client-individual',
        };

        client.subscribe(subscribeHeaders, (subscribeErr, message) => {
          if (subscribeErr) {
            clearTimeout(timer);
            client.disconnect(() => reject(subscribeErr));
            return;
          }

          let body = '';
          message.on('data', (chunk) => {
            body += chunk.toString();
          });

          message.on('end', () => {
            if (body.includes(correlationId)) {
              clearTimeout(timer);
              const headers = {};
              for (const key of Object.keys(message.headers)) {
                headers[key] = message.headers[key];
              }
              client.ack(message);
              client.disconnect(() => resolve({ body, headers }));
            } else {
              client.nack(message);
            }
          });

          message.on('error', (msgErr) => {
            clearTimeout(timer);
            client.disconnect(() => reject(msgErr));
          });
        });
      });
    });
  }

  getInDestination() {
    return this.config.inQueue;
  }

  async disconnect() {
    // STOMP connections are per-operation, no persistent connection to close
  }
}
