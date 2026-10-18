import {
  closeServer,
  createBusFromEnv,
  createLogger,
  createPool,
  handleShutdown,
  prepareBus,
} from '@orchestrator/common';
import { createPaymentService } from './service.js';

const logger = createLogger('payment-service');
const pool = createPool(process.env.DATABASE_URL);
const bus = createBusFromEnv({ clientId: 'payment-service', logger });

await prepareBus(bus);
const service = createPaymentService({ pool, bus, logger });
await service.start();

const port = Number(process.env.PORT ?? 3003);
const server = service.app.listen(port, () => logger.info({ port }, 'payment-service listening'));

handleShutdown({
  logger,
  steps: [closeServer(server), () => service.stop(), () => bus.disconnect(), () => pool.end()],
});
