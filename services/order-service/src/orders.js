import { randomUUID } from 'node:crypto';
import { Topics, createMessage, enqueue, markProcessed, withTransaction } from '@orchestrator/common';
import { OrderStatus, startSaga, transition } from './saga.js';

function toOrder(row) {
  if (!row) return null;
  return {
    id: row.id,
    customerId: row.customer_id,
    status: row.status,
    items: row.items,
    totalCents: row.total_cents,
    failureReason: row.failure_reason,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    saga: row.saga_state ? { id: row.saga_id, state: row.saga_state } : undefined,
  };
}

async function enqueueStep(client, { orderId, sagaId, commands, events }) {
  for (const cmd of commands) {
    await enqueue(client, cmd.topic, createMessage({ type: cmd.type, orderId, sagaId, payload: cmd.payload }));
  }
  for (const evt of events) {
    await enqueue(client, Topics.ORDER_EVENTS, createMessage({ type: evt.type, orderId, sagaId, payload: evt.payload }));
  }
}

/**
 * Creates the order and starts its saga in a single transaction. If the
 * client retries with the same Idempotency-Key, the original order is returned.
 */
export async function placeOrder(pool, { customerId, items, paymentToken, idempotencyKey }) {
  return withTransaction(pool, async (client) => {
    if (idempotencyKey) {
      const existing = await client.query(
        `SELECT o.*, s.saga_id, s.state AS saga_state FROM orders o
         JOIN saga_instances s ON s.order_id = o.id
         WHERE o.idempotency_key = $1`,
        [idempotencyKey],
      );
      if (existing.rows[0]) return { order: toOrder(existing.rows[0]), created: false };
    }

    const orderId = randomUUID();
    const sagaId = randomUUID();
    const step = startSaga({ order: { customerId, items, paymentToken } });

    const { rows } = await client.query(
      `INSERT INTO orders (id, customer_id, status, items, idempotency_key)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (idempotency_key) DO NOTHING
       RETURNING *`,
      [orderId, customerId, OrderStatus.PENDING, JSON.stringify(items), idempotencyKey ?? null],
    );
    if (!rows[0]) {
      // Lost a race with a concurrent request using the same key.
      const again = await client.query(
        `SELECT o.*, s.saga_id, s.state AS saga_state FROM orders o
         JOIN saga_instances s ON s.order_id = o.id WHERE o.idempotency_key = $1`,
        [idempotencyKey],
      );
      return { order: toOrder(again.rows[0]), created: false };
    }

    await client.query(
      `INSERT INTO saga_instances (saga_id, order_id, state, data) VALUES ($1, $2, $3, $4)`,
      [sagaId, orderId, step.state, step.data],
    );
    await client.query(
      `INSERT INTO saga_steps (saga_id, from_state, to_state, trigger) VALUES ($1, NULL, $2, 'OrderPlaced')`,
      [sagaId, step.state],
    );
    await enqueueStep(client, { orderId, sagaId, commands: step.commands, events: step.events });

    return {
      order: toOrder({ ...rows[0], saga_id: sagaId, saga_state: step.state }),
      created: true,
    };
  });
}

/**
 * Advances the saga for one reply. The inbox check, the state change and the
 * next command all commit together, so a crash at any point is safe to retry.
 */
export async function handleReply(pool, logger, reply) {
  await withTransaction(pool, async (client) => {
    if (!(await markProcessed(client, reply))) {
      logger.debug({ id: reply.id, type: reply.type }, 'duplicate reply ignored');
      return;
    }

    const { rows } = await client.query(
      'SELECT saga_id, order_id, state, data FROM saga_instances WHERE order_id = $1 FOR UPDATE',
      [reply.orderId],
    );
    const saga = rows[0];
    if (!saga) {
      logger.warn({ orderId: reply.orderId, type: reply.type }, 'reply for unknown saga');
      return;
    }

    const next = transition(saga, reply);
    if (!next) {
      logger.info({ orderId: reply.orderId, state: saga.state, type: reply.type }, 'reply does not apply to current state');
      return;
    }

    await client.query(
      'UPDATE saga_instances SET state = $2, data = $3, updated_at = now() WHERE saga_id = $1',
      [saga.saga_id, next.state, next.data],
    );
    await client.query(
      'INSERT INTO saga_steps (saga_id, from_state, to_state, trigger) VALUES ($1, $2, $3, $4)',
      [saga.saga_id, saga.state, next.state, reply.type],
    );

    const { status = null, totalCents = null, failureReason = null } = next.order;
    await client.query(
      `UPDATE orders SET
         status = COALESCE($2, status),
         total_cents = COALESCE($3, total_cents),
         failure_reason = COALESCE($4, failure_reason),
         updated_at = now()
       WHERE id = $1`,
      [saga.order_id, status, totalCents, failureReason],
    );

    await enqueueStep(client, {
      orderId: saga.order_id,
      sagaId: saga.saga_id,
      commands: next.commands,
      events: next.events,
    });

    logger.info({ orderId: saga.order_id, from: saga.state, to: next.state, trigger: reply.type }, 'saga advanced');
  });
}

export async function getOrder(pool, orderId) {
  const { rows } = await pool.query(
    `SELECT o.*, s.saga_id, s.state AS saga_state FROM orders o
     LEFT JOIN saga_instances s ON s.order_id = o.id
     WHERE o.id = $1`,
    [orderId],
  );
  if (!rows[0]) return null;
  const order = toOrder(rows[0]);
  const steps = await pool.query(
    'SELECT from_state, to_state, trigger, occurred_at FROM saga_steps WHERE saga_id = $1 ORDER BY id',
    [rows[0].saga_id],
  );
  order.saga.steps = steps.rows.map((s) => ({
    from: s.from_state,
    to: s.to_state,
    trigger: s.trigger,
    at: s.occurred_at,
  }));
  return order;
}

export async function listOrders(pool, { customerId, limit }) {
  const { rows } = await pool.query(
    `SELECT o.*, s.saga_id, s.state AS saga_state FROM orders o
     LEFT JOIN saga_instances s ON s.order_id = o.id
     WHERE ($1::text IS NULL OR o.customer_id = $1)
     ORDER BY o.created_at DESC
     LIMIT $2`,
    [customerId ?? null, limit],
  );
  return rows.map(toOrder);
}
