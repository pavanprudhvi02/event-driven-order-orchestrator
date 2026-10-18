import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { MemoryBus, createMessage, parseMessage } from '../src/index.js';

describe('message envelope', () => {
  it('creates messages with a unique id and timestamp', () => {
    const a = createMessage({ type: 'X', orderId: 'o1', payload: { n: 1 } });
    const b = createMessage({ type: 'X', orderId: 'o1' });
    assert.notEqual(a.id, b.id);
    assert.equal(a.sagaId, null);
    assert.ok(Date.parse(a.occurredAt));
  });

  it('requires a type and an order id', () => {
    assert.throws(() => createMessage({ orderId: 'o1' }), /type/);
    assert.throws(() => createMessage({ type: 'X' }), /orderId/);
  });

  it('round-trips through JSON and rejects bad envelopes', () => {
    const msg = createMessage({ type: 'X', orderId: 'o1' });
    assert.deepEqual(parseMessage(Buffer.from(JSON.stringify(msg))), msg);
    assert.throws(() => parseMessage('{"hello":1}'), /invalid message envelope/);
    assert.throws(() => parseMessage('not json'));
  });
});

describe('memory bus', () => {
  it('delivers in order to every subscribed group', async () => {
    const bus = new MemoryBus();
    const seenA = [];
    const seenB = [];
    await bus.subscribe({ groupId: 'a', topics: ['t'], handler: (m) => seenA.push(m.payload.n) });
    await bus.subscribe({ groupId: 'b', topics: ['t', 'other'], handler: (m) => seenB.push(m.payload.n) });
    const msgs = [1, 2, 3].map((n) => ({ key: 'k', value: JSON.stringify(createMessage({ type: 'X', orderId: 'o', payload: { n } })) }));
    await bus.publish('t', msgs);
    await bus.idle();
    assert.deepEqual(seenA, [1, 2, 3]);
    assert.deepEqual(seenB, [1, 2, 3]);
  });

  it('can deliver every message twice to exercise idempotency', async () => {
    const bus = new MemoryBus({ duplicateDelivery: true });
    let count = 0;
    await bus.subscribe({ groupId: 'a', topics: ['t'], handler: () => count++ });
    await bus.publish('t', [{ key: 'k', value: JSON.stringify(createMessage({ type: 'X', orderId: 'o' })) }]);
    await bus.idle();
    assert.equal(count, 2);
  });
});
