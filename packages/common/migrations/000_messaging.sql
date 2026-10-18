-- Transactional outbox: rows are written in the same transaction as the
-- business change and published to Kafka afterwards by the relay.
CREATE TABLE IF NOT EXISTS outbox (
  id           UUID PRIMARY KEY,
  topic        TEXT NOT NULL,
  msg_key      TEXT NOT NULL,
  type         TEXT NOT NULL,
  payload      JSONB NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
  ON outbox (created_at)
  WHERE published_at IS NULL;

-- Inbox for idempotent consumers: a message id is processed at most once.
CREATE TABLE IF NOT EXISTS processed_messages (
  message_id   UUID PRIMARY KEY,
  message_type TEXT NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
