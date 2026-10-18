import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Commands, OrderEvents, Replies, Topics } from '@orchestrator/common';
import { OrderStatus, SagaState, startSaga, transition } from '../src/saga.js';

const order = {
  customerId: 'cust-1',
  items: [{ sku: 'KB-001', quantity: 2 }],
  paymentToken: 'tok_visa',
};

const reply = (type, payload = {}) => ({ id: 'r1', type, orderId: 'o1', payload });

function begin() {
  const s = startSaga({ order });
  return { state: s.state, data: s.data };
}

describe('place-order saga', () => {
  it('starts by reserving inventory and announcing the order', () => {
    const s = startSaga({ order });
    assert.equal(s.state, SagaState.RESERVING_INVENTORY);
    assert.deepEqual(s.commands, [
      { topic: Topics.INVENTORY_COMMANDS, type: Commands.RESERVE_INVENTORY, payload: { items: order.items } },
    ]);
    assert.equal(s.events[0].type, OrderEvents.ORDER_CREATED);
  });

  it('charges the reserved total once stock is held', () => {
    const next = transition(begin(), reply(Replies.INVENTORY_RESERVED, { totalCents: 17998, lines: [] }));
    assert.equal(next.state, SagaState.PROCESSING_PAYMENT);
    assert.equal(next.order.totalCents, 17998);
    assert.equal(next.commands[0].type, Commands.PROCESS_PAYMENT);
    assert.equal(next.commands[0].payload.amountCents, 17998);
    assert.equal(next.commands[0].payload.paymentToken, 'tok_visa');
  });

  it('confirms the order when payment succeeds', () => {
    let saga = begin();
    saga = transition(saga, reply(Replies.INVENTORY_RESERVED, { totalCents: 500, lines: [] }));
    const done = transition(saga, reply(Replies.PAYMENT_SUCCEEDED, { paymentId: 'p1' }));
    assert.equal(done.state, SagaState.COMPLETED);
    assert.equal(done.order.status, OrderStatus.CONFIRMED);
    assert.deepEqual(done.commands, []);
    assert.equal(done.events[0].type, OrderEvents.ORDER_CONFIRMED);
  });

  it('cancels without compensation when stock is rejected', () => {
    const next = transition(begin(), reply(Replies.INVENTORY_REJECTED, { reason: 'insufficient stock' }));
    assert.equal(next.state, SagaState.FAILED);
    assert.equal(next.order.status, OrderStatus.CANCELLED);
    assert.match(next.order.failureReason, /insufficient stock/);
    assert.deepEqual(next.commands, []);
  });

  it('releases stock before cancelling when payment fails', () => {
    let saga = begin();
    saga = transition(saga, reply(Replies.INVENTORY_RESERVED, { totalCents: 500, lines: [] }));
    const compensating = transition(saga, reply(Replies.PAYMENT_FAILED, { reason: 'card declined' }));
    assert.equal(compensating.state, SagaState.COMPENSATING);
    assert.equal(compensating.order.status, undefined, 'order stays pending until stock is back');
    assert.equal(compensating.commands[0].type, Commands.RELEASE_INVENTORY);

    const failed = transition(compensating, reply(Replies.INVENTORY_RELEASED));
    assert.equal(failed.state, SagaState.FAILED);
    assert.equal(failed.order.status, OrderStatus.CANCELLED);
    assert.equal(failed.order.failureReason, 'payment: card declined');
    assert.equal(failed.events[0].type, OrderEvents.ORDER_CANCELLED);
  });

  it('ignores replies that do not belong to the current step', () => {
    assert.equal(transition(begin(), reply(Replies.PAYMENT_SUCCEEDED)), null);
    const completed = { state: SagaState.COMPLETED, data: {} };
    assert.equal(transition(completed, reply(Replies.INVENTORY_RESERVED, { totalCents: 1 })), null);
    assert.equal(transition({ state: SagaState.FAILED, data: {} }, reply(Replies.INVENTORY_RELEASED)), null);
  });
});
