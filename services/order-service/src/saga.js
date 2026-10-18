import { Commands, OrderEvents, Replies, Topics } from '@orchestrator/common';

/**
 * Place-order saga, written as a pure state machine so it can be tested
 * without a database or a broker. The service persists the returned state
 * and enqueues the returned commands/events in one transaction.
 *
 *   RESERVING_INVENTORY --InventoryReserved--> PROCESSING_PAYMENT --PaymentSucceeded--> COMPLETED
 *          |                                         |
 *   InventoryRejected                           PaymentFailed
 *          v                                         v
 *        FAILED  <--------InventoryReleased------ COMPENSATING
 */
export const SagaState = Object.freeze({
  RESERVING_INVENTORY: 'RESERVING_INVENTORY',
  PROCESSING_PAYMENT: 'PROCESSING_PAYMENT',
  COMPENSATING: 'COMPENSATING',
  COMPLETED: 'COMPLETED',
  FAILED: 'FAILED',
});

export const OrderStatus = Object.freeze({
  PENDING: 'PENDING',
  CONFIRMED: 'CONFIRMED',
  CANCELLED: 'CANCELLED',
});

export const TERMINAL_STATES = new Set([SagaState.COMPLETED, SagaState.FAILED]);

export function startSaga({ order }) {
  return {
    state: SagaState.RESERVING_INVENTORY,
    data: {
      customerId: order.customerId,
      items: order.items,
      paymentToken: order.paymentToken,
    },
    commands: [
      {
        topic: Topics.INVENTORY_COMMANDS,
        type: Commands.RESERVE_INVENTORY,
        payload: { items: order.items },
      },
    ],
    events: [
      {
        type: OrderEvents.ORDER_CREATED,
        payload: { customerId: order.customerId, items: order.items },
      },
    ],
  };
}

/**
 * Returns the next step for a reply, or null when the reply does not apply to
 * the current state (a late duplicate, or a reply for a step already done).
 */
export function transition(saga, reply) {
  const { state, data } = saga;

  switch (state) {
    case SagaState.RESERVING_INVENTORY:
      if (reply.type === Replies.INVENTORY_RESERVED) {
        const totalCents = reply.payload.totalCents;
        return {
          state: SagaState.PROCESSING_PAYMENT,
          data: { ...data, totalCents, lines: reply.payload.lines },
          order: { totalCents },
          commands: [
            {
              topic: Topics.PAYMENT_COMMANDS,
              type: Commands.PROCESS_PAYMENT,
              payload: { amountCents: totalCents, customerId: data.customerId, paymentToken: data.paymentToken },
            },
          ],
          events: [],
        };
      }
      if (reply.type === Replies.INVENTORY_REJECTED) {
        return fail(data, `inventory: ${reply.payload.reason}`);
      }
      return null;

    case SagaState.PROCESSING_PAYMENT:
      if (reply.type === Replies.PAYMENT_SUCCEEDED) {
        return {
          state: SagaState.COMPLETED,
          data: { ...data, paymentId: reply.payload.paymentId },
          order: { status: OrderStatus.CONFIRMED },
          commands: [],
          events: [
            {
              type: OrderEvents.ORDER_CONFIRMED,
              payload: { totalCents: data.totalCents, paymentId: reply.payload.paymentId },
            },
          ],
        };
      }
      if (reply.type === Replies.PAYMENT_FAILED) {
        const failureReason = `payment: ${reply.payload.reason}`;
        // Stock was already reserved, so it has to be given back before the order is cancelled.
        return {
          state: SagaState.COMPENSATING,
          data: { ...data, failureReason },
          order: {},
          commands: [
            {
              topic: Topics.INVENTORY_COMMANDS,
              type: Commands.RELEASE_INVENTORY,
              payload: { reason: failureReason },
            },
          ],
          events: [],
        };
      }
      return null;

    case SagaState.COMPENSATING:
      if (reply.type === Replies.INVENTORY_RELEASED) {
        return fail(data, data.failureReason);
      }
      return null;

    default:
      return null;
  }
}

function fail(data, failureReason) {
  return {
    state: SagaState.FAILED,
    data: { ...data, failureReason },
    order: { status: OrderStatus.CANCELLED, failureReason },
    commands: [],
    events: [{ type: OrderEvents.ORDER_CANCELLED, payload: { reason: failureReason } }],
  };
}
