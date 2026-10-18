import { randomUUID } from 'node:crypto';

// Sliding-window log kept in a Redis sorted set, one per client.
// Runs as a Lua script so trimming, counting and adding are atomic
// even with several gateway replicas sharing the same Redis.
const SLIDING_WINDOW = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count = redis.call('ZCARD', key)
if count >= limit then
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  local retry = window
  if oldest[2] then retry = tonumber(oldest[2]) + window - now end
  return {0, count, retry}
end
redis.call('ZADD', key, now, member)
redis.call('PEXPIRE', key, window)
return {1, count + 1, 0}
`;

export function createRateLimiter({ redis, limit, windowMs, logger, keyPrefix = 'ratelimit' }) {
  redis.defineCommand('slidingWindow', { numberOfKeys: 1, lua: SLIDING_WINDOW });

  return async function rateLimit(req, res, next) {
    const client = req.get('x-api-key') ?? req.ip;
    const key = `${keyPrefix}:${client}`;
    try {
      const [allowed, count, retryMs] = await redis.slidingWindow(key, Date.now(), windowMs, limit, randomUUID());
      res.set('X-RateLimit-Limit', String(limit));
      res.set('X-RateLimit-Remaining', String(Math.max(limit - count, 0)));
      if (!allowed) {
        res.set('Retry-After', String(Math.max(Math.ceil(retryMs / 1000), 1)));
        return res.status(429).json({ error: 'rate_limited' });
      }
      return next();
    } catch (err) {
      // Fail open: losing Redis should degrade rate limiting, not take the API down.
      logger.error({ err }, 'rate limiter unavailable, allowing request');
      return next();
    }
  };
}
