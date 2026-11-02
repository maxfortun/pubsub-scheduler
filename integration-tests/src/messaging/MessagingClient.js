/**
 * Abstract messaging client interface.
 * Implementations must provide methods for sending messages and polling DLQ.
 */
export class MessagingClient {
  constructor(config) {
    if (new.target === MessagingClient) {
      throw new Error('MessagingClient is abstract and cannot be instantiated directly');
    }
    this.config = config;
  }

  /**
   * Send a raw message to a destination.
   * @param {string} destination - Queue/topic name
   * @param {string} value - Message body
   * @param {Object} headers - Message headers
   */
  async sendMessage(destination, value, headers = {}) {
    throw new Error('sendMessage must be implemented');
  }

  /**
   * Send a scheduled message with scheduler headers.
   * @param {string} jobKey - Job key
   * @param {Object} options - Scheduling options
   */
  async sendScheduledMessage(jobKey, options = {}) {
    const headers = {
      scheduler_destination: options.destination || this.config.outputTopic,
      scheduler_key: jobKey,
    };

    if (options.runAt) headers.scheduler_at = options.runAt.toISOString();
    if (options.wait) headers.scheduler_wait = options.wait;
    if (options.waitStart) headers.scheduler_wait_start = options.waitStart;
    if (options.waitRepeat) headers.scheduler_wait_repeat = String(options.waitRepeat);
    if (options.keyPolicy) headers.scheduler_key_policy = options.keyPolicy;
    if (options.retryCount !== undefined) headers.scheduler_retry_count = String(options.retryCount);
    if (options.cron) headers.scheduler_cron = options.cron;
    if (options.cronRepeat) headers.scheduler_cron_repeat = String(options.cronRepeat);
    if (options.cronUntil) headers.scheduler_cron_until = options.cronUntil.toISOString();
    if (options.advisoryHeaders) headers.scheduler_advisory_headers = options.advisoryHeaders;
    if (options.customHeaders) Object.assign(headers, options.customHeaders);

    return this.sendMessage(this.getInDestination(), options.body || 'test', headers);
  }

  /**
   * Poll DLQ for a message containing the correlation ID.
   * @param {string} correlationId - ID to search for in message body
   * @param {number} timeoutMs - Timeout in milliseconds
   * @returns {Object|null} - Message with body and headers, or null if not found
   */
  async pollDlqForMessage(correlationId, timeoutMs = 30000) {
    throw new Error('pollDlqForMessage must be implemented');
  }

  /**
   * Get the input destination name for this messaging system.
   * @returns {string}
   */
  getInDestination() {
    throw new Error('getInDestination must be implemented');
  }

  /**
   * Disconnect and cleanup resources.
   */
  async disconnect() {
    throw new Error('disconnect must be implemented');
  }
}
