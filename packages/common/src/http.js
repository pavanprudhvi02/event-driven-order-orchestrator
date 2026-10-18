import { randomUUID } from 'node:crypto';

export class HttpError extends Error {
  constructor(status, message, details) {
    super(message);
    this.status = status;
    this.details = details;
  }
}

export function requestId() {
  return (req, res, next) => {
    req.id = req.get('x-request-id') ?? randomUUID();
    res.set('x-request-id', req.id);
    next();
  };
}

export function accessLog(logger) {
  return (req, res, next) => {
    const start = process.hrtime.bigint();
    res.on('finish', () => {
      const ms = Number(process.hrtime.bigint() - start) / 1e6;
      logger.info(
        { reqId: req.id, method: req.method, path: req.originalUrl, status: res.statusCode, ms: Math.round(ms) },
        'request',
      );
    });
    next();
  };
}

export function notFound() {
  return (req, res) => res.status(404).json({ error: 'not_found' });
}

export function errorHandler(logger) {
  // Express recognises error middleware by its four arguments.
  return (err, req, res, _next) => {
    if (err instanceof HttpError) {
      return res.status(err.status).json({ error: err.message, details: err.details });
    }
    if (err?.type === 'entity.parse.failed') {
      return res.status(400).json({ error: 'invalid_json' });
    }
    logger.error({ err, reqId: req.id }, 'unhandled error');
    return res.status(500).json({ error: 'internal_error' });
  };
}

/** Wraps an async route so rejected promises reach the error handler. */
export const asyncRoute = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
