// Runs against the full stack started with `docker compose up` (real Kafka,
// three PostgreSQL databases, Redis). Stock is compared before/after rather
// than against fixed numbers so the suite can be rerun on the same volumes.
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

const BASE = process.env.GATEWAY_URL ?? 'http://localhost:3000/api';

async function call(path, init = {}) {
  const res = await fetch(`${BASE}${path}`, {
    ...init,
    headers: { 'content-type': 'application/json', ...(init.headers ?? {}) },
  });
  return { status: res.status, body: await res.json() };
}

async function settled(orderId, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const { body } = await call(`/orders/${orderId}`);
    if (body.status && body.status !== 'PENDING') return body;
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error(`order ${orderId} did not settle within ${timeoutMs}ms`);
}

async function stock(sku) {
  const { body } = await call('/products');
  return body.products.find((p) => p.sku === sku).available;
}

describe('end to end over Kafka', () => {
  it('confirms a paid order', async () => {
    const before = await stock('CB-005');
    const placed = await call('/orders', {
      method: 'POST',
      body: JSON.stringify({ customerId: 'e2e-happy', items: [{ sku: 'CB-005', quantity: 2 }], paymentToken: 'tok_visa' }),
    });
    assert.equal(placed.status, 202);
    const order = await settled(placed.body.id);
    assert.equal(order.status, 'CONFIRMED');
    assert.equal(order.totalCents, 2 * 1299);
    assert.equal(await stock('CB-005'), before - 2);
    const payment = await call(`/payments/${order.id}`);
    assert.equal(payment.body.status, 'SUCCEEDED');
  });

  it('rolls back the reservation when the card is declined', async () => {
    const before = await stock('MS-002');
    const placed = await call('/orders', {
      method: 'POST',
      body: JSON.stringify({ customerId: 'e2e-declined', items: [{ sku: 'MS-002', quantity: 1 }], paymentToken: 'tok_decline' }),
    });
    const order = await settled(placed.body.id);
    assert.equal(order.status, 'CANCELLED');
    assert.equal(order.failureReason, 'payment: card declined');
    assert.deepEqual(
      order.saga.steps.map((s) => s.to),
      ['RESERVING_INVENTORY', 'PROCESSING_PAYMENT', 'COMPENSATING', 'FAILED'],
    );
    assert.equal(await stock('MS-002'), before);
  });

  it('cancels when stock is short', async () => {
    const placed = await call('/orders', {
      method: 'POST',
      body: JSON.stringify({ customerId: 'e2e-short', items: [{ sku: 'HD-004', quantity: 999 }], paymentToken: 'tok_visa' }),
    });
    const order = await settled(placed.body.id);
    assert.equal(order.status, 'CANCELLED');
    assert.match(order.failureReason, /insufficient stock/);
  });

  it('reports every dependency as healthy', async () => {
    const res = await fetch(BASE.replace(/\/api$/, '/health'));
    const body = await res.json();
    assert.equal(res.status, 200);
    assert.deepEqual(body.services, {
      orders: 'ok',
      inventory: 'ok',
      payments: 'ok',
      redis: 'ok',
    });
  });
});
