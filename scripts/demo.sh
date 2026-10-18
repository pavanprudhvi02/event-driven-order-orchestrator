#!/bin/sh
# Walks through both saga paths against a running stack (docker compose up).
set -e
API=${API:-http://localhost:3000/api}

wait_for() {
  i=0
  while [ $i -lt 40 ]; do
    status=$(curl -s "$API/orders/$1" | sed -n 's/.*"status":"\([A-Z]*\)".*/\1/p' | head -1)
    if [ "$status" != "PENDING" ] && [ -n "$status" ]; then break; fi
    i=$((i + 1)); sleep 0.25
  done
  curl -s "$API/orders/$1"; echo
}

place() {
  curl -s -X POST "$API/orders" -H 'content-type: application/json' -d "$1" \
    | sed -n 's/.*"id":"\([0-9a-f-]*\)".*/\1/p'
}

echo "== Catalogue"
curl -s "$API/products"; echo

echo
echo "== Happy path: 2 keyboards, card approved"
id=$(place '{"customerId":"demo","items":[{"sku":"KB-001","quantity":2}],"paymentToken":"tok_visa"}')
wait_for "$id"

echo
echo "== Compensation: monitor reserved, card declined, stock released"
id=$(place '{"customerId":"demo","items":[{"sku":"MN-003","quantity":1}],"paymentToken":"tok_decline"}')
wait_for "$id"
curl -s "$API/reservations/$id"; echo
