import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { OutboxRelay, Topics, messagingMigrationsDir, migrate } from '@orchestrator/common';
import { createApp } from './app.js';
import { handleCommand } from './payments.js';

const migrationsDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../migrations');

export function createPaymentService({
  pool,
  bus,
  logger,
  relayIntervalMs,
  limitCents = Number(process.env.PAYMENT_LIMIT_CENTS ?? 500000),
}) {
  const app = createApp({ pool, logger });
  const relay = new OutboxRelay({ pool, bus, logger, intervalMs: relayIntervalMs });

  return {
    app,
    async start() {
      await migrate(pool, [messagingMigrationsDir, migrationsDir]);
      await bus.subscribe({
        groupId: 'payment-service',
        topics: [Topics.PAYMENT_COMMANDS],
        handler: (msg) => handleCommand(pool, logger, msg, { limitCents }),
      });
      relay.start();
    },
    async stop() {
      await relay.stop();
    },
  };
}
