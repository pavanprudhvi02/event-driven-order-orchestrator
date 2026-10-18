import {
  closeServer,
  createBusFromEnv,
  createLogger,
  createPool,
  handleShutdown,
  prepareBus,
} from '@orchestrator/common';
import { createInventoryService } from './service.js';

const logger = createLogger('inventory-service');
const pool = createPool(process.env.DATABASE_URL);
const bus = createBusFromEnv({ clientId: 'inventory-service', logger });

await prepareBus(bus);
const service = createInventoryService({ pool, bus, logger });
await service.start();

const port = Number(process.env.PORT ?? 3002);
const server = service.app.listen(port, () => logger.info({ port }, 'inventory-service listening'));

handleShutdown({
  logger,
  steps: [closeServer(server), () => service.stop(), () => bus.disconnect(), () => pool.end()],
});
