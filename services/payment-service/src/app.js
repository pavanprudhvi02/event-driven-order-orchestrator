import express from 'express';
import { HttpError, accessLog, asyncRoute, errorHandler, notFound, requestId } from '@orchestrator/common';
import { getPaymentForOrder } from './payments.js';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function createApp({ pool, logger }) {
  const app = express();
  app.disable('x-powered-by');
  app.use(requestId());
  app.use(accessLog(logger));

  app.get('/health', asyncRoute(async (req, res) => {
    await pool.query('SELECT 1');
    res.json({ status: 'ok', service: 'payment-service' });
  }));

  app.get('/payments/:orderId', asyncRoute(async (req, res) => {
    if (!UUID_RE.test(req.params.orderId)) throw new HttpError(404, 'not_found');
    const payment = await getPaymentForOrder(pool, req.params.orderId);
    if (!payment) throw new HttpError(404, 'not_found');
    res.json(payment);
  }));

  app.use(notFound());
  app.use(errorHandler(logger));
  return app;
}
