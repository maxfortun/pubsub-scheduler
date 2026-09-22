const FLAVORS = {
  postgres: {
    name: 'PostgreSQL',
    url: process.env.SCHEDULER_URL || 'http://localhost:8091',
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
    inTopic: 'scheduler-in-cockroach',
    dlqTopic: 'scheduler-dlq-cockroach',
    advisoryTopic: 'scheduler-advisory-cockroach',
    expectedName: 'CockroachDB Scheduler',
    expectedColor: '#6933FF',
    expectedInstanceId: 'scheduler-cockroach',
  },
};

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
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
    outputTopic: OUTPUT_TOPIC,
  };
}

export { FLAVORS, KAFKA_BROKERS, OUTPUT_TOPIC };
