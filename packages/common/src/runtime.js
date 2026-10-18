import { KafkaBus } from './bus/kafka.js';
import { ALL_TOPICS } from './topics.js';

export function createBusFromEnv({ clientId, logger }) {
  const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',').map((b) => b.trim());
  return new KafkaBus({ brokers, clientId, logger });
}

export async function prepareBus(bus) {
  await bus.connect();
  await bus.ensureTopics([...ALL_TOPICS, ...ALL_TOPICS.map((t) => `${t}.dlq`)]);
}

/**
 * Stops the HTTP server, the relay and the bus in order when the process is
 * asked to terminate (docker stop, Kubernetes rolling update, Ctrl+C).
 */
export function handleShutdown({ logger, steps }) {
  let shuttingDown = false;
  const shutdown = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'shutting down');
    for (const step of steps) {
      try {
        await step();
      } catch (err) {
        logger.error({ err }, 'shutdown step failed');
      }
    }
    process.exit(0);
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

export function closeServer(server) {
  return () => new Promise((resolve) => server.close(() => resolve()));
}
