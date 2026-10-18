-- Demo catalogue so the stack is usable right after `docker compose up`.
INSERT INTO products (sku, name, unit_price_cents, available) VALUES
  ('KB-001',  'Mechanical keyboard',       8999, 50),
  ('MS-002',  'Wireless mouse',            2999, 100),
  ('MN-003',  '27 inch monitor',          27999, 20),
  ('HD-004',  'Noise cancelling headset', 15999, 15),
  ('CB-005',  'USB-C cable',               1299, 500),
  ('LT-006',  'Laptop stand',              4599, 3)
ON CONFLICT (sku) DO NOTHING;
