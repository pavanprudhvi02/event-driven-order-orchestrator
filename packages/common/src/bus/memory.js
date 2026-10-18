import { parseMessage } from '../message.js';

/**
 * In-process bus with the same interface as KafkaBus. Used by unit and
 * integration tests so they run without a broker. Each consumer group gets
 * its own ordered queue per topic, like a single-partition Kafka topic.
 *
 * `duplicateDelivery: true` delivers every message twice, which is how the
 * tests prove that consumers are idempotent.
 */
export class MemoryBus {
  constructor({ duplicateDelivery = false } = {}) {
    this.duplicateDelivery = duplicateDelivery;
    this.groups = new Map(); // groupId -> { topics:Set, handler, queue:[], busy:boolean }
    this.published = [];
  }

  async connect() {}
  async ensureTopics() {}

  async publish(topic, messages) {
    for (const m of messages) {
      this.published.push({ topic, key: m.key, value: m.value });
      for (const group of this.groups.values()) {
        if (!group.topics.has(topic)) continue;
        group.queue.push({ topic, value: m.value });
        if (this.duplicateDelivery) group.queue.push({ topic, value: m.value });
        this.#drain(group);
      }
    }
  }

  async subscribe({ groupId, topics, handler }) {
    this.groups.set(groupId, { topics: new Set(topics), handler, queue: [], busy: false });
  }

  #drain(group) {
    if (group.busy) return;
    group.busy = true;
    setImmediate(async () => {
      while (group.queue.length) {
        const { topic, value } = group.queue.shift();
        try {
          await group.handler(parseMessage(value), { topic });
        } catch {
          // Kafka would retry; the tests that care assert on final state instead.
        }
      }
      group.busy = false;
    });
  }

  async idle() {
    // Resolves once every queue is empty and nothing is being handled.
    for (;;) {
      const busy = [...this.groups.values()].some((g) => g.busy || g.queue.length);
      if (!busy) return;
      await new Promise((r) => setTimeout(r, 5));
    }
  }

  async disconnect() {
    this.groups.clear();
  }
}
