import { after, before, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { startStack, waitFor } from './harness.js';

async function placeOrder(baseUrl, body, headers = {}) {
  const res = await fetch(`${baseUrl}/orders`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify(body),
  });
  return { status: res.status, body: await res.json() };
}

const getJson = async (url) => {
  const res = await fetch(url);
  return { status: res.status, body: await res.json() };
};

function settled(baseUrl, orderId) {
  return waitFor(async () => {
    const { body } = await getJson(`${baseUrl}/orders/${orderId}`);
    return body.status !== 'PENDING' ? body : null;
  });
}

async function stockOf(baseUrl, sku) {
  const { body } = await getJson(`${baseUrl}/products`);
  return body.products.find((p) => p.sku === sku).available;
}

describe('order saga (services wired through an in-memory bus)', () => {
  let stack;
  before(async () => {
    stack = await startStack();
  });
  after(async () => stack.stop());

  it('confirms an order, charges the reserved total and keeps the stock taken', async () => {
    const before = await stockOf(stack.baseUrl, 'KB-001');
    const placed = await placeOrder(stack.baseUrl, {
      customerId: 'cust-happy',
      items: [{ sku: 'KB-001', quantity: 2 }, { sku: 'MS-002', quantity: 1 }],
      paymentToken: 'tok_visa',
    });
    assert.equal(placed.status, 202);
    assert.equal(placed.body.status, 'PENDING');

    const order = await settled(stack.baseUrl, placed.body.id);
    assert.equal(order.status, 'CONFIRMED');
    assert.equal(order.totalCents, 2 * 8999 + 2999);
    assert.deepEqual(
      order.saga.steps.map((s) => s.to),
      ['RESERVING_INVENTORY', 'PROCESSING_PAYMENT', 'COMPLETED'],
    );

    const payment = await getJson(`${stack.baseUrl}/payments/${order.id}`);
    assert.equal(payment.body.status, 'SUCCEEDED');
    assert.equal(payment.body.amountCents, order.totalCents);
    assert.equal(await stockOf(stack.baseUrl, 'KB-001'), before - 2);
  });

  it('compensates a declined payment by releasing the reserved stock', async () => {
    const before = await stockOf(stack.baseUrl, 'MN-003');
    const placed = await placeOrder(stack.baseUrl, {
      customerId: 'cust-declined',
      items: [{ sku: 'MN-003', quantity: 1 }],
      paymentToken: 'tok_decline',
    });
    const order = await settled(stack.baseUrl, placed.body.id);

    assert.equal(order.status, 'CANCELLED');
    assert.equal(order.failureReason, 'payment: card declined');
    assert.deepEqual(
      order.saga.steps.map((s) => s.to),
      ['RESERVING_INVENTORY', 'PROCESSING_PAYMENT', 'COMPENSATING', 'FAILED'],
    );
    assert.equal(await stockOf(stack.baseUrl, 'MN-003'), before, 'stock is back where it started');

    const res = await getJson(`${stack.baseUrl}/reservations/${order.id}`);
    assert.deepEqual(res.body.reservations, [{ sku: 'MN-003', quantity: 1, status: 'RELEASED' }]);
  });

  it('cancels without charging when there is not enough stock', async () => {
    const placed = await placeOrder(stack.baseUrl, {
      customerId: 'cust-greedy',
      items: [{ sku: 'HD-004', quantity: 999 }],
      paymentToken: 'tok_visa',
    });
    const order = await settled(stack.baseUrl, placed.body.id);
    assert.equal(order.status, 'CANCELLED');
    assert.match(order.failureReason, /insufficient stock for HD-004/);
    assert.equal((await getJson(`${stack.baseUrl}/payments/${order.id}`)).status, 404, 'no payment attempted');
  });

  it('never oversells when orders race for the last units', async () => {
    // LT-006 starts with 3 units; 6 customers try to buy one each at the same time.
    const placed = await Promise.all(
      Array.from({ length: 6 }, (_, i) =>
        placeOrder(stack.baseUrl, {
          customerId: `racer-${i}`,
          items: [{ sku: 'LT-006', quantity: 1 }],
          paymentToken: 'tok_visa',
        }),
      ),
    );
    const orders = await Promise.all(placed.map((p) => settled(stack.baseUrl, p.body.id)));
    const confirmed = orders.filter((o) => o.status === 'CONFIRMED');
    assert.equal(confirmed.length, 3);
    assert.equal(orders.length - confirmed.length, 3);
    assert.equal(await stockOf(stack.baseUrl, 'LT-006'), 0);
  });

  it('returns the original order when a client retries with the same Idempotency-Key', async () => {
    const body = { customerId: 'cust-retry', items: [{ sku: 'CB-005', quantity: 1 }], paymentToken: 'tok_visa' };
    const first = await placeOrder(stack.baseUrl, body, { 'idempotency-key': 'checkout-123' });
    const second = await placeOrder(stack.baseUrl, body, { 'idempotency-key': 'checkout-123' });
    assert.equal(first.status, 202);
    assert.equal(second.status, 200);
    assert.equal(second.body.id, first.body.id);
    const { body: list } = await getJson(`${stack.baseUrl}/orders?customerId=cust-retry`);
    assert.equal(list.orders.length, 1);
  });

  it('rejects invalid orders with a 400 and the failing fields', async () => {
    const res = await placeOrder(stack.baseUrl, { customerId: '', items: [], paymentToken: 'tok_visa' });
    assert.equal(res.status, 400);
    assert.equal(res.body.error, 'validation_failed');
    assert.ok(res.body.details.some((d) => d.path === 'items'));
  });
});

describe('order saga with every message delivered twice', () => {
  let stack;
  before(async () => {
    stack = await startStack({ duplicateDelivery: true });
  });
  after(async () => stack.stop());

  it('charges once and reserves once despite duplicate deliveries', async () => {
    const before = await stockOf(stack.baseUrl, 'KB-001');
    const placed = await placeOrder(stack.baseUrl, {
      customerId: 'cust-dupes',
      items: [{ sku: 'KB-001', quantity: 3 }],
      paymentToken: 'tok_visa',
    });
    const order = await settled(stack.baseUrl, placed.body.id);
    await stack.bus.idle();

    assert.equal(order.status, 'CONFIRMED');
    assert.equal(await stockOf(stack.baseUrl, 'KB-001'), before - 3);
    const { rows } = await stack.pools.payments.query('SELECT count(*)::int AS n FROM payments WHERE order_id = $1', [
      order.id,
    ]);
    assert.equal(rows[0].n, 1);
    const steps = order.saga.steps.map((s) => s.to);
    assert.deepEqual(steps, ['RESERVING_INVENTORY', 'PROCESSING_PAYMENT', 'COMPLETED']);
  });

  it('compensates exactly once when payment fails', async () => {
    const before = await stockOf(stack.baseUrl, 'MS-002');
    const placed = await placeOrder(stack.baseUrl, {
      customerId: 'cust-dupes-2',
      items: [{ sku: 'MS-002', quantity: 4 }],
      paymentToken: 'tok_insufficient_funds',
    });
    const order = await settled(stack.baseUrl, placed.body.id);
    await stack.bus.idle();
    assert.equal(order.status, 'CANCELLED');
    assert.equal(await stockOf(stack.baseUrl, 'MS-002'), before);
  });
});

describe('api gateway rate limiting', () => {
  let stack;
  before(async () => {
    stack = await startStack({ rateLimit: { limit: 3, windowMs: 2000 } });
  });
  after(async () => stack.stop());

  it('allows the limit, then answers 429 with Retry-After', async () => {
    const statuses = [];
    let last;
    for (let i = 0; i < 4; i++) {
      last = await fetch(`${stack.baseUrl}/products`, { headers: { 'x-api-key': 'client-a' } });
      statuses.push(last.status);
    }
    assert.deepEqual(statuses, [200, 200, 200, 429]);
    assert.ok(Number(last.headers.get('retry-after')) >= 1);
    assert.equal(last.headers.get('x-ratelimit-remaining'), '0');

    const other = await fetch(`${stack.baseUrl}/products`, { headers: { 'x-api-key': 'client-b' } });
    assert.equal(other.status, 200, 'limits are per client');
  });
});
