import Redis from 'ioredis';
import { MemoryBus, createLogger, createPool } from '@orchestrator/common';
import { createOrderService } from '../../services/order-service/src/service.js';
import { createInventoryService } from '../../services/inventory-service/src/service.js';
import { createPaymentService } from '../../services/payment-service/src/service.js';
import { createApp as createGateway } from '../../services/api-gateway/src/app.js';

process.env.LOG_LEVEL ??= 'silent';

const PG = process.env.TEST_PG_URL ?? 'postgres://postgres:postgres@localhost:5432';

function listen(app) {
  return new Promise((resolve) => {
    const server = app.listen(0, '127.0.0.1', () => resolve(server));
  });
}

async function resetDatabase(pool) {
  await pool.query('DROP SCHEMA public CASCADE; CREATE SCHEMA public;');
}

/**
 * Boots the three services and the gateway in one process, wired through an
 * in-memory bus but backed by real PostgreSQL databases and a real Redis.
 */
export async function startStack({ duplicateDelivery = false, rateLimit = { limit: 1000, windowMs: 60000 } } = {}) {
  const logger = createLogger('test');
  const bus = new MemoryBus({ duplicateDelivery });
  const pools = {
    orders: createPool(`${PG}/orders`),
    inventory: createPool(`${PG}/inventory`),
    payments: createPool(`${PG}/payments`),
  };
  for (const pool of Object.values(pools)) await resetDatabase(pool);

  const services = {
    orders: createOrderService({ pool: pools.orders, bus, logger, relayIntervalMs: 20 }),
    inventory: createInventoryService({ pool: pools.inventory, bus, logger, relayIntervalMs: 20 }),
    payments: createPaymentService({ pool: pools.payments, bus, logger, relayIntervalMs: 20, limitCents: 500000 }),
  };
  for (const s of Object.values(services)) await s.start();

  const servers = {};
  for (const [name, s] of Object.entries(services)) servers[name] = await listen(s.app);
  const url = (server) => `http://127.0.0.1:${server.address().port}`;

  const redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379');
  await redis.flushdb();
  const gateway = createGateway({
    redis,
    logger,
    rateLimit,
    upstreams: { orders: url(servers.orders), inventory: url(servers.inventory), payments: url(servers.payments) },
  });
  servers.gateway = await listen(gateway);

  return {
    bus,
    pools,
    baseUrl: `${url(servers.gateway)}/api`,
    async stop() {
      for (const server of Object.values(servers)) await new Promise((r) => server.close(r));
      for (const s of Object.values(services)) await s.stop();
      await redis.quit();
      for (const pool of Object.values(pools)) await pool.end();
    },
  };
}

export async function waitFor(check, { timeoutMs = 5000, intervalMs = 25 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let last;
  while (Date.now() < deadline) {
    last = await check();
    if (last) return last;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(`condition not met within ${timeoutMs}ms (last value: ${JSON.stringify(last)})`);
}
