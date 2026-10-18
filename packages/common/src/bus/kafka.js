import { Kafka, Partitioners, logLevel } from 'kafkajs';
import { parseMessage } from '../message.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Thin wrapper around kafkajs with the semantics the services rely on:
 * idempotent producer, at-least-once consumption (offsets are committed only
 * after the handler resolves), bounded retries and a dead-letter topic.
 */
export class KafkaBus {
  constructor({ brokers, clientId, logger, maxAttempts = 5 }) {
    this.kafka = new Kafka({
      clientId,
      brokers,
      logLevel: logLevel.WARN,
      retry: { initialRetryTime: 300, retries: 10 },
    });
    this.logger = logger;
    this.maxAttempts = maxAttempts;
    // Keyed by order id with the murmur2 partitioner, so every message for one
    // order lands on the same partition and is consumed in order.
    this.producer = this.kafka.producer({
      idempotent: true,
      maxInFlightRequests: 1,
      createPartitioner: Partitioners.DefaultPartitioner,
    });
    this.consumers = [];
  }

  async connect() {
    await this.producer.connect();
  }

  async ensureTopics(topics, numPartitions = 3) {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      const existing = new Set(await admin.listTopics());
      const missing = topics.filter((t) => !existing.has(t));
      if (missing.length) {
        await admin.createTopics({
          waitForLeaders: true,
          topics: missing.map((topic) => ({ topic, numPartitions })),
        });
      }
    } finally {
      await admin.disconnect();
    }
  }

  async publish(topic, messages) {
    await this.producer.send({ topic, messages });
  }

  async subscribe({ groupId, topics, handler }) {
    const consumer = this.kafka.consumer({ groupId });
    await consumer.connect();
    for (const topic of topics) {
      await consumer.subscribe({ topic, fromBeginning: true });
    }
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        let parsed;
        try {
          parsed = parseMessage(message.value);
        } catch (err) {
          this.logger.error({ err, topic, partition, offset: message.offset }, 'dropping unparseable message');
          await this.deadLetter(topic, message, err);
          return;
        }
        for (let attempt = 1; ; attempt++) {
          try {
            await handler(parsed, { topic, partition, offset: message.offset });
            return;
          } catch (err) {
            if (attempt >= this.maxAttempts) {
              this.logger.error({ err, type: parsed.type, id: parsed.id }, 'handler failed, sending to DLQ');
              await this.deadLetter(topic, message, err);
              return;
            }
            this.logger.warn({ err, attempt, type: parsed.type }, 'handler failed, retrying');
            await sleep(Math.min(200 * 2 ** attempt, 5000));
          }
        }
      },
    });
    this.consumers.push(consumer);
  }

  async deadLetter(topic, message, err) {
    await this.producer.send({
      topic: `${topic}.dlq`,
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: { error: String(err?.message ?? err) },
        },
      ],
    });
  }

  async disconnect() {
    await Promise.allSettled(this.consumers.map((c) => c.disconnect()));
    await this.producer.disconnect();
  }
}
