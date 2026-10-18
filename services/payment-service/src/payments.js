import { randomUUID } from 'node:crypto';
import {
  Commands,
  Replies,
  Topics,
  createMessage,
  enqueue,
  markProcessed,
  withTransaction,
} from '@orchestrator/common';
import { authorize } from './gateway.js';

function toPayment(row) {
  return {
    id: row.id,
    orderId: row.order_id,
    customerId: row.customer_id,
    amountCents: row.amount_cents,
    status: row.status,
    declineReason: row.decline_reason,
    createdAt: row.created_at,
  };
}

function replyFor(command, payment) {
  return payment.status === 'SUCCEEDED'
    ? createMessage({
        type: Replies.PAYMENT_SUCCEEDED,
        orderId: command.orderId,
        sagaId: command.sagaId,
        payload: { paymentId: payment.id, amountCents: payment.amount_cents },
      })
    : createMessage({
        type: Replies.PAYMENT_FAILED,
        orderId: command.orderId,
        sagaId: command.sagaId,
        payload: { paymentId: payment.id, reason: payment.decline_reason },
      });
}

export async function handleCommand(pool, logger, command, { limitCents }) {
  if (command.type !== Commands.PROCESS_PAYMENT) {
    logger.warn({ type: command.type }, 'unknown command');
    return;
  }

  await withTransaction(pool, async (client) => {
    if (!(await markProcessed(client, command))) {
      logger.debug({ id: command.id }, 'duplicate command ignored');
      return;
    }

    // One payment per order: the unique constraint is the last line of defence
    // against charging twice, even if two different command ids arrive.
    const existing = await client.query('SELECT * FROM payments WHERE order_id = $1', [command.orderId]);
    if (existing.rows[0]) {
      await enqueue(client, Topics.SAGA_REPLIES, replyFor(command, existing.rows[0]));
      return;
    }

    const { amountCents, customerId, paymentToken } = command.payload;
    const decision = authorize({ amountCents, paymentToken }, { limitCents });
    const { rows } = await client.query(
      `INSERT INTO payments (id, order_id, customer_id, amount_cents, status, decline_reason)
       VALUES ($1, $2, $3, $4, $5, $6) RETURNING *`,
      [
        randomUUID(),
        command.orderId,
        customerId,
        amountCents,
        decision.approved ? 'SUCCEEDED' : 'FAILED',
        decision.approved ? null : decision.reason,
      ],
    );
    await enqueue(client, Topics.SAGA_REPLIES, replyFor(command, rows[0]));
    logger.info({ orderId: command.orderId, status: rows[0].status }, 'payment processed');
  });
}

export async function getPaymentForOrder(pool, orderId) {
  const { rows } = await pool.query('SELECT * FROM payments WHERE order_id = $1', [orderId]);
  return rows[0] ? toPayment(rows[0]) : null;
}
