import { randomUUID } from 'node:crypto';

/**
 * Every message on the bus uses the same envelope so consumers can
 * deduplicate on `id` and correlate on `sagaId` / `orderId`.
 */
export function createMessage({ type, orderId, sagaId, payload = {} }) {
  if (!type) throw new Error('message type is required');
  if (!orderId) throw new Error('orderId is required');
  return {
    id: randomUUID(),
    type,
    orderId,
    sagaId: sagaId ?? null,
    occurredAt: new Date().toISOString(),
    payload,
  };
}

export function parseMessage(raw) {
  const text = Buffer.isBuffer(raw) ? raw.toString('utf8') : String(raw);
  const msg = JSON.parse(text);
  if (!msg || typeof msg !== 'object' || !msg.id || !msg.type) {
    throw new Error('invalid message envelope');
  }
  return msg;
}
