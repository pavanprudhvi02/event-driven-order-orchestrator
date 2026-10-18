CREATE TABLE products (
  sku              TEXT PRIMARY KEY,
  name             TEXT NOT NULL,
  unit_price_cents INTEGER NOT NULL CHECK (unit_price_cents >= 0),
  available        INTEGER NOT NULL CHECK (available >= 0),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE reservations (
  order_id   UUID NOT NULL,
  sku        TEXT NOT NULL REFERENCES products (sku),
  quantity   INTEGER NOT NULL CHECK (quantity > 0),
  status     TEXT NOT NULL CHECK (status IN ('RESERVED', 'RELEASED')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (order_id, sku)
);

-- Orders whose reservation was rejected, so a redelivered command gets the same answer.
CREATE TABLE rejected_reservations (
  order_id   UUID PRIMARY KEY,
  reason     TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
