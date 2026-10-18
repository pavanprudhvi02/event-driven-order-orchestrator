CREATE TABLE payments (
  id             UUID PRIMARY KEY,
  order_id       UUID NOT NULL UNIQUE,
  customer_id    TEXT NOT NULL,
  amount_cents   INTEGER NOT NULL CHECK (amount_cents >= 0),
  status         TEXT NOT NULL CHECK (status IN ('SUCCEEDED', 'FAILED')),
  decline_reason TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
