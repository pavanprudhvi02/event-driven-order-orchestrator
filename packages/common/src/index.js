import path from 'node:path';
import { fileURLToPath } from 'node:url';

export * from './topics.js';
export * from './message.js';
export * from './logger.js';
export * from './db.js';
export * from './outbox.js';
export * from './http.js';
export * from './runtime.js';
export { KafkaBus } from './bus/kafka.js';
export { MemoryBus } from './bus/memory.js';

export const messagingMigrationsDir = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  '../migrations',
);
