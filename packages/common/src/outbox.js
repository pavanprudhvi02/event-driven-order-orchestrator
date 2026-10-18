/**
 * Adds a message to the outbox inside the caller's transaction.
 * The message is only visible to the relay once that transaction commits,
 * so a business change and its event are never out of sync.
 */
export async function enqueue(client, topic, message) {
  await client.query(
    `INSERT INTO outbox (id, topic, msg_key, type, payload)
     VALUES ($1, $2, $3, $4, $5)`,
    [message.id, topic, message.orderId, message.type, message],
  );
}

/**
 * Records that a message was handled. Returns false when the message id was
 * already processed, which means the caller should skip it (Kafka delivery is
 * at-least-once, so redeliveries are expected).
 */
export async function markProcessed(client, message) {
  const { rowCount } = await client.query(
    `INSERT INTO processed_messages (message_id, message_type)
     VALUES ($1, $2)
     ON CONFLICT (message_id) DO NOTHING`,
    [message.id, message.type],
  );
  return rowCount === 1;
}

/**
 * Polls the outbox and publishes pending rows in creation order.
 * FOR UPDATE SKIP LOCKED lets several replicas run the relay without
 * publishing the same row twice in the normal case.
 */
export class OutboxRelay {
  constructor({ pool, bus, logger, batchSize = 100, intervalMs = 200 }) {
    this.pool = pool;
    this.bus = bus;
    this.logger = logger;
    this.batchSize = batchSize;
    this.intervalMs = intervalMs;
    this.timer = null;
    this.running = false;
    this.stopped = true;
  }

  start() {
    this.stopped = false;
    const loop = async () => {
      if (this.stopped) return;
      try {
        const published = await this.flush();
        // Drain quickly while there is a backlog, otherwise wait for the interval.
        this.timer = setTimeout(loop, published === this.batchSize ? 0 : this.intervalMs);
      } catch (err) {
        this.logger.error({ err }, 'outbox relay failed, retrying');
        this.timer = setTimeout(loop, this.intervalMs * 5);
      }
    };
    loop();
  }

  async stop() {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    while (this.running) await new Promise((r) => setTimeout(r, 10));
  }

  async flush() {
    this.running = true;
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query(
        `SELECT id, topic, msg_key, payload FROM outbox
         WHERE published_at IS NULL
         ORDER BY created_at, id
         LIMIT $1
         FOR UPDATE SKIP LOCKED`,
        [this.batchSize],
      );
      if (rows.length === 0) {
        await client.query('COMMIT');
        return 0;
      }
      const byTopic = new Map();
      for (const row of rows) {
        if (!byTopic.has(row.topic)) byTopic.set(row.topic, []);
        byTopic.get(row.topic).push({ key: row.msg_key, value: JSON.stringify(row.payload) });
      }
      for (const [topic, messages] of byTopic) {
        await this.bus.publish(topic, messages);
      }
      await client.query('UPDATE outbox SET published_at = now() WHERE id = ANY($1::uuid[])', [
        rows.map((r) => r.id),
      ]);
      await client.query('COMMIT');
      this.logger.debug({ count: rows.length }, 'published outbox batch');
      return rows.length;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
      this.running = false;
    }
  }
}
