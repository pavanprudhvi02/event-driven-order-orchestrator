import pino from 'pino';

export function createLogger(service) {
  return pino({
    name: service,
    level: process.env.LOG_LEVEL ?? (process.env.NODE_ENV === 'test' ? 'silent' : 'info'),
    base: { service },
    timestamp: pino.stdTimeFunctions.isoTime,
  });
}
