import Redis from 'ioredis';
import { closeServer, createLogger, handleShutdown } from '@orchestrator/common';
import { createApp } from './app.js';

const logger = createLogger('api-gateway');
const redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379', { maxRetriesPerRequest: 1 });
redis.on('error', (err) => logger.warn({ err: err.message }, 'redis error'));

const app = createApp({
  redis,
  logger,
  upstreams: {
    orders: process.env.ORDER_SERVICE_URL ?? 'http://localhost:3001',
    inventory: process.env.INVENTORY_SERVICE_URL ?? 'http://localhost:3002',
    payments: process.env.PAYMENT_SERVICE_URL ?? 'http://localhost:3003',
  },
  rateLimit: {
    limit: Number(process.env.RATE_LIMIT_MAX ?? 100),
    windowMs: Number(process.env.RATE_LIMIT_WINDOW_MS ?? 60000),
  },
});

const port = Number(process.env.PORT ?? 3000);
const server = app.listen(port, () => logger.info({ port }, 'api-gateway listening'));

handleShutdown({ logger, steps: [closeServer(server), () => redis.quit()] });
