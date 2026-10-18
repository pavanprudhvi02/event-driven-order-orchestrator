import fs from 'node:fs/promises';
import path from 'node:path';
import pg from 'pg';

export function createPool(connectionString) {
  if (!connectionString) throw new Error('DATABASE_URL is not set');
  return new pg.Pool({ connectionString, max: Number(process.env.PG_POOL_MAX ?? 10) });
}

export async function withTransaction(pool, fn) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Applies the .sql files from each directory (in order, then by file name), once each.
 * An advisory lock keeps two replicas from migrating at the same time.
 */
export async function migrate(pool, dirs) {
  const files = [];
  for (const dir of [].concat(dirs)) {
    const names = (await fs.readdir(dir)).filter((f) => f.endsWith('.sql')).sort();
    files.push(...names.map((name) => ({ name, full: path.join(dir, name) })));
  }
  await withTransaction(pool, async (client) => {
    await client.query('SELECT pg_advisory_xact_lock(727274)');
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        name TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )`);
    const { rows } = await client.query('SELECT name FROM schema_migrations');
    const applied = new Set(rows.map((r) => r.name));
    for (const { name, full } of files) {
      if (applied.has(name)) continue;
      const sql = await fs.readFile(full, 'utf8');
      await client.query(sql);
      await client.query('INSERT INTO schema_migrations (name) VALUES ($1)', [name]);
    }
  });
}
