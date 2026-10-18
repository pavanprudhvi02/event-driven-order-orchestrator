import {
  Commands,
  Replies,
  Topics,
  createMessage,
  enqueue,
  markProcessed,
  withTransaction,
} from '@orchestrator/common';

function reply(client, command, type, payload) {
  return enqueue(
    client,
    Topics.SAGA_REPLIES,
    createMessage({ type, orderId: command.orderId, sagaId: command.sagaId, payload }),
  );
}

async function reserve(client, command) {
  const { orderId } = command;
  const items = command.payload.items ?? [];

  // A redelivered command must get the same answer as the first time.
  const prior = await client.query(
    `SELECT r.sku, r.quantity, p.unit_price_cents FROM reservations r
     JOIN products p ON p.sku = r.sku WHERE r.order_id = $1`,
    [orderId],
  );
  if (prior.rows.length) return reply(client, command, Replies.INVENTORY_RESERVED, priced(prior.rows));
  const rejected = await client.query('SELECT reason FROM rejected_reservations WHERE order_id = $1', [orderId]);
  if (rejected.rows[0]) {
    return reply(client, command, Replies.INVENTORY_REJECTED, { reason: rejected.rows[0].reason });
  }

  // Lock rows in a fixed order so two orders for the same SKUs cannot deadlock.
  const skus = items.map((i) => i.sku).sort();
  const { rows } = await client.query(
    'SELECT sku, available, unit_price_cents FROM products WHERE sku = ANY($1) ORDER BY sku FOR UPDATE',
    [skus],
  );
  const stock = new Map(rows.map((r) => [r.sku, r]));

  let reason = null;
  for (const item of items) {
    const product = stock.get(item.sku);
    if (!product) {
      reason = `unknown sku ${item.sku}`;
      break;
    }
    if (product.available < item.quantity) {
      reason = `insufficient stock for ${item.sku} (requested ${item.quantity}, available ${product.available})`;
      break;
    }
  }

  if (reason) {
    await client.query('INSERT INTO rejected_reservations (order_id, reason) VALUES ($1, $2)', [orderId, reason]);
    return reply(client, command, Replies.INVENTORY_REJECTED, { reason });
  }

  for (const item of items) {
    await client.query('UPDATE products SET available = available - $2, updated_at = now() WHERE sku = $1', [
      item.sku,
      item.quantity,
    ]);
    await client.query(
      `INSERT INTO reservations (order_id, sku, quantity, status) VALUES ($1, $2, $3, 'RESERVED')`,
      [orderId, item.sku, item.quantity],
    );
  }
  const lines = items.map((i) => ({ sku: i.sku, quantity: i.quantity, unit_price_cents: stock.get(i.sku).unit_price_cents }));
  return reply(client, command, Replies.INVENTORY_RESERVED, priced(lines));
}

function priced(rows) {
  const lines = rows.map((r) => ({
    sku: r.sku,
    quantity: r.quantity,
    unitPriceCents: r.unit_price_cents,
    lineTotalCents: r.quantity * r.unit_price_cents,
  }));
  return { lines, totalCents: lines.reduce((sum, l) => sum + l.lineTotalCents, 0) };
}

async function release(client, command) {
  const { rows } = await client.query(
    `UPDATE reservations SET status = 'RELEASED', updated_at = now()
     WHERE order_id = $1 AND status = 'RESERVED'
     RETURNING sku, quantity`,
    [command.orderId],
  );
  for (const r of rows) {
    await client.query('UPDATE products SET available = available + $2, updated_at = now() WHERE sku = $1', [
      r.sku,
      r.quantity,
    ]);
  }
  // Replying even when nothing was reserved keeps the compensation idempotent.
  return reply(client, command, Replies.INVENTORY_RELEASED, {
    released: rows.map((r) => ({ sku: r.sku, quantity: r.quantity })),
  });
}

export async function handleCommand(pool, logger, command) {
  await withTransaction(pool, async (client) => {
    if (!(await markProcessed(client, command))) {
      logger.debug({ id: command.id }, 'duplicate command ignored');
      return;
    }
    switch (command.type) {
      case Commands.RESERVE_INVENTORY:
        await reserve(client, command);
        break;
      case Commands.RELEASE_INVENTORY:
        await release(client, command);
        break;
      default:
        logger.warn({ type: command.type }, 'unknown command');
        return;
    }
    logger.info({ orderId: command.orderId, type: command.type }, 'command handled');
  });
}

export async function listProducts(pool) {
  const { rows } = await pool.query('SELECT sku, name, unit_price_cents, available FROM products ORDER BY sku');
  return rows.map((r) => ({ sku: r.sku, name: r.name, unitPriceCents: r.unit_price_cents, available: r.available }));
}

export async function getReservations(pool, orderId) {
  const { rows } = await pool.query(
    'SELECT sku, quantity, status FROM reservations WHERE order_id = $1 ORDER BY sku',
    [orderId],
  );
  return rows;
}
