import express from 'express';
import { z } from 'zod';
import { HttpError, accessLog, asyncRoute, errorHandler, notFound, requestId } from '@orchestrator/common';
import { getOrder, listOrders, placeOrder } from './orders.js';

const placeOrderSchema = z.object({
  customerId: z.string().trim().min(1).max(64),
  items: z
    .array(
      z.object({
        sku: z.string().trim().min(1).max(64),
        quantity: z.number().int().positive().max(1000),
      }),
    )
    .min(1)
    .max(50)
    .refine((items) => new Set(items.map((i) => i.sku)).size === items.length, {
      message: 'each sku may appear only once',
    }),
  paymentToken: z.string().trim().min(1).max(128),
});

const uuid = z.uuid();

const describeIssues = (error) =>
  error.issues.map((issue) => ({ path: issue.path.join('.'), message: issue.message }));

export function createApp({ pool, logger }) {
  const app = express();
  app.disable('x-powered-by');
  app.use(requestId());
  app.use(accessLog(logger));
  app.use(express.json({ limit: '64kb' }));

  app.get('/health', asyncRoute(async (req, res) => {
    await pool.query('SELECT 1');
    res.json({ status: 'ok', service: 'order-service' });
  }));

  app.post('/orders', asyncRoute(async (req, res) => {
    const parsed = placeOrderSchema.safeParse(req.body);
    if (!parsed.success) throw new HttpError(400, 'validation_failed', describeIssues(parsed.error));
    const idempotencyKey = req.get('idempotency-key')?.slice(0, 128);
    const { order, created } = await placeOrder(pool, { ...parsed.data, idempotencyKey });
    res.status(created ? 202 : 200).location(`/orders/${order.id}`).json(order);
  }));

  app.get('/orders', asyncRoute(async (req, res) => {
    const limit = Math.min(Number(req.query.limit) || 20, 100);
    const orders = await listOrders(pool, { customerId: req.query.customerId, limit });
    res.json({ orders });
  }));

  app.get('/orders/:id', asyncRoute(async (req, res) => {
    if (!uuid.safeParse(req.params.id).success) throw new HttpError(404, 'not_found');
    const order = await getOrder(pool, req.params.id);
    if (!order) throw new HttpError(404, 'not_found');
    res.json(order);
  }));

  app.use(notFound());
  app.use(errorHandler(logger));
  return app;
}
