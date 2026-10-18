import express from 'express';
import { accessLog, asyncRoute, errorHandler, notFound, requestId } from '@orchestrator/common';
import { createRateLimiter } from './rateLimiter.js';

const HOP_BY_HOP = new Set(['connection', 'keep-alive', 'transfer-encoding', 'content-length', 'host']);

/**
 * Forwards a request to an upstream service and streams the answer back.
 * Uses the built-in fetch with a timeout so a slow service cannot hold
 * gateway connections open forever.
 */
function proxyTo(baseUrl, { timeoutMs }) {
  return asyncRoute(async (req, res) => {
    const url = new URL(req.originalUrl.replace(/^\/api/, ''), baseUrl);
    const headers = { 'x-request-id': req.id };
    for (const name of ['content-type', 'idempotency-key', 'accept']) {
      if (req.get(name)) headers[name] = req.get(name);
    }
    let upstream;
    try {
      upstream = await fetch(url, {
        method: req.method,
        headers,
        body: ['GET', 'HEAD'].includes(req.method) ? undefined : JSON.stringify(req.body ?? {}),
        signal: AbortSignal.timeout(timeoutMs),
      });
    } catch (err) {
      const status = err.name === 'TimeoutError' ? 504 : 502;
      return res.status(status).json({ error: status === 504 ? 'upstream_timeout' : 'upstream_unavailable' });
    }
    res.status(upstream.status);
    upstream.headers.forEach((value, name) => {
      if (!HOP_BY_HOP.has(name)) res.set(name, value);
    });
    res.send(Buffer.from(await upstream.arrayBuffer()));
  });
}

export function createApp({ redis, logger, upstreams, rateLimit, timeoutMs = 5000 }) {
  const app = express();
  app.disable('x-powered-by');
  app.set('trust proxy', true);
  app.use(requestId());
  app.use(accessLog(logger));

  app.get('/health', asyncRoute(async (req, res) => {
    const checks = await Promise.all(
      Object.entries(upstreams).map(async ([name, base]) => {
        try {
          const r = await fetch(new URL('/health', base), { signal: AbortSignal.timeout(2000) });
          return [name, r.ok ? 'ok' : 'down'];
        } catch {
          return [name, 'down'];
        }
      }),
    );
    let redisStatus = 'ok';
    try {
      await redis.ping();
    } catch {
      redisStatus = 'down';
    }
    const services = Object.fromEntries([...checks, ['redis', redisStatus]]);
    const healthy = Object.values(services).every((s) => s === 'ok');
    res.status(healthy ? 200 : 503).json({ status: healthy ? 'ok' : 'degraded', services });
  }));

  const api = express.Router();
  api.use(createRateLimiter({ redis, logger, ...rateLimit }));
  api.use(express.json({ limit: '64kb' }));
  const opts = { timeoutMs };
  api.use('/orders', proxyTo(upstreams.orders, opts));
  api.use('/products', proxyTo(upstreams.inventory, opts));
  api.use('/reservations', proxyTo(upstreams.inventory, opts));
  api.use('/payments', proxyTo(upstreams.payments, opts));
  app.use('/api', api);

  app.use(notFound());
  app.use(errorHandler(logger));
  return app;
}
