const FLAVORS = {
  postgres: {
    name: 'PostgreSQL',
    url: process.env.SCHEDULER_URL || 'http://localhost:8091',
    messagingType: 'kafka',
    inTopic: 'scheduler-in-postgres',
    dlqTopic: 'scheduler-dlq-postgres',
    advisoryTopic: 'scheduler-advisory-postgres',
    expectedName: 'PostgreSQL Scheduler',
    expectedColor: '#336791',
    expectedInstanceId: 'scheduler-postgres',
  },
  mysql: {
    name: 'MySQL',
    url: process.env.SCHEDULER_URL || 'http://localhost:8092',
    messagingType: 'kafka',
    inTopic: 'scheduler-in-mysql',
    dlqTopic: 'scheduler-dlq-mysql',
    advisoryTopic: 'scheduler-advisory-mysql',
    expectedName: 'MySQL Scheduler',
    expectedColor: '#F29111',
    expectedInstanceId: 'scheduler-mysql',
  },
  cockroach: {
    name: 'CockroachDB',
    url: process.env.SCHEDULER_URL || 'http://localhost:8093',
    messagingType: 'kafka',
    inTopic: 'scheduler-in-cockroach',
    dlqTopic: 'scheduler-dlq-cockroach',
    advisoryTopic: 'scheduler-advisory-cockroach',
    expectedName: 'CockroachDB Scheduler',
    expectedColor: '#6933FF',
    expectedInstanceId: 'scheduler-cockroach',
  },
  activemq: {
    name: 'ActiveMQ',
    url: process.env.SCHEDULER_URL || 'http://localhost:8094',
    messagingType: 'activemq',
    inQueue: 'scheduler-in-activemq',
    dlqQueue: 'scheduler-dlq-activemq',
    advisoryTopic: 'scheduler-advisory-activemq',
    expectedName: 'ActiveMQ Scheduler',
    expectedColor: '#D6242D',
    expectedInstanceId: 'scheduler-activemq',
  },
};

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const ACTIVEMQ_HOST = process.env.ACTIVEMQ_HOST || 'localhost';
const ACTIVEMQ_PORT = parseInt(process.env.ACTIVEMQ_PORT || '61613'); // STOMP port
const OUTPUT_TOPIC = 'output-topic';

export function getConfig() {
  const flavor = process.env.SCHEDULER_FLAVOR || 'postgres';
  const config = FLAVORS[flavor];
  if (!config) {
    throw new Error(`Unknown flavor: ${flavor}. Valid options: ${Object.keys(FLAVORS).join(', ')}`);
  }
  return {
    ...config,
    flavor,
    kafkaBrokers: KAFKA_BROKERS,
    activemqHost: ACTIVEMQ_HOST,
    activemqPort: ACTIVEMQ_PORT,
    outputTopic: OUTPUT_TOPIC,
  };
}

export { FLAVORS, KAFKA_BROKERS, ACTIVEMQ_HOST, ACTIVEMQ_PORT, OUTPUT_TOPIC };
