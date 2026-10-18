import {
  closeServer,
  createBusFromEnv,
  createLogger,
  createPool,
  handleShutdown,
  prepareBus,
} from '@orchestrator/common';
import { createOrderService } from './service.js';

const logger = createLogger('order-service');
const pool = createPool(process.env.DATABASE_URL);
const bus = createBusFromEnv({ clientId: 'order-service', logger });

await prepareBus(bus);
const service = createOrderService({ pool, bus, logger });
await service.start();

const port = Number(process.env.PORT ?? 3001);
const server = service.app.listen(port, () => logger.info({ port }, 'order-service listening'));

handleShutdown({
  logger,
  steps: [closeServer(server), () => service.stop(), () => bus.disconnect(), () => pool.end()],
});
