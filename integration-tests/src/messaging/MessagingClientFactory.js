import { KafkaClient } from './KafkaClient.js';
import { ActiveMQClient } from './ActiveMQClient.js';

const clientRegistry = {
  kafka: KafkaClient,
  activemq: ActiveMQClient,
};

export function createMessagingClient(config) {
  const type = config.messagingType || 'kafka';
  const ClientClass = clientRegistry[type];

  if (!ClientClass) {
    throw new Error(`Unknown messaging type: ${type}. Available: ${Object.keys(clientRegistry).join(', ')}`);
  }

  return new ClientClass(config);
}

export function registerMessagingClient(type, ClientClass) {
  clientRegistry[type] = ClientClass;
}
