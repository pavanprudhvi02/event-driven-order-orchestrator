CREATE TABLE orders (
  id              UUID PRIMARY KEY,
  customer_id     TEXT NOT NULL,
  status          TEXT NOT NULL CHECK (status IN ('PENDING', 'CONFIRMED', 'CANCELLED')),
  items           JSONB NOT NULL,
  total_cents     INTEGER,
  failure_reason  TEXT,
  idempotency_key TEXT UNIQUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX orders_customer_idx ON orders (customer_id, created_at DESC);

CREATE TABLE saga_instances (
  saga_id    UUID PRIMARY KEY,
  order_id   UUID NOT NULL UNIQUE REFERENCES orders (id),
  state      TEXT NOT NULL,
  data       JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Audit trail of every step, useful when debugging a stuck or failed order.
CREATE TABLE saga_steps (
  id          BIGSERIAL PRIMARY KEY,
  saga_id     UUID NOT NULL REFERENCES saga_instances (saga_id),
  from_state  TEXT,
  to_state    TEXT NOT NULL,
  trigger     TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX saga_steps_saga_idx ON saga_steps (saga_id, id);
