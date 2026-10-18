// Kafka topics. Commands go to the service that owns the step,
// replies come back to the orchestrator on a single topic.
export const Topics = Object.freeze({
  INVENTORY_COMMANDS: 'inventory.commands',
  PAYMENT_COMMANDS: 'payment.commands',
  SAGA_REPLIES: 'order.saga.replies',
  ORDER_EVENTS: 'order.events',
});

export const Commands = Object.freeze({
  RESERVE_INVENTORY: 'ReserveInventory',
  RELEASE_INVENTORY: 'ReleaseInventory',
  PROCESS_PAYMENT: 'ProcessPayment',
});

export const Replies = Object.freeze({
  INVENTORY_RESERVED: 'InventoryReserved',
  INVENTORY_REJECTED: 'InventoryRejected',
  INVENTORY_RELEASED: 'InventoryReleased',
  PAYMENT_SUCCEEDED: 'PaymentSucceeded',
  PAYMENT_FAILED: 'PaymentFailed',
});

export const OrderEvents = Object.freeze({
  ORDER_CREATED: 'OrderCreated',
  ORDER_CONFIRMED: 'OrderConfirmed',
  ORDER_CANCELLED: 'OrderCancelled',
});

export const ALL_TOPICS = Object.values(Topics);
