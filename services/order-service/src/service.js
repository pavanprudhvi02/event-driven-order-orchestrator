import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { OutboxRelay, Topics, messagingMigrationsDir, migrate } from '@orchestrator/common';
import { createApp } from './app.js';
import { handleReply } from './orders.js';

const migrationsDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../migrations');

export function createOrderService({ pool, bus, logger, relayIntervalMs }) {
  const app = createApp({ pool, logger });
  const relay = new OutboxRelay({ pool, bus, logger, intervalMs: relayIntervalMs });

  return {
    app,
    async start() {
      await migrate(pool, [messagingMigrationsDir, migrationsDir]);
      await bus.subscribe({
        groupId: 'order-service',
        topics: [Topics.SAGA_REPLIES],
        handler: (msg) => handleReply(pool, logger, msg),
      });
      relay.start();
    },
    async stop() {
      await relay.stop();
    },
  };
}
