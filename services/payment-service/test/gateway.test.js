import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { authorize } from '../src/gateway.js';

const limits = { limitCents: 10000 };

describe('payment authorization', () => {
  it('approves a normal charge', () => {
    assert.deepEqual(authorize({ amountCents: 2500, paymentToken: 'tok_visa' }, limits), { approved: true });
  });

  it('declines the test decline tokens', () => {
    assert.equal(authorize({ amountCents: 100, paymentToken: 'tok_decline' }, limits).reason, 'card declined');
    assert.equal(
      authorize({ amountCents: 100, paymentToken: 'tok_insufficient_funds' }, limits).reason,
      'insufficient funds',
    );
  });

  it('declines charges above the per-order limit', () => {
    const result = authorize({ amountCents: 10001, paymentToken: 'tok_visa' }, limits);
    assert.equal(result.approved, false);
    assert.match(result.reason, /exceeds limit/);
    assert.equal(authorize({ amountCents: 10000, paymentToken: 'tok_visa' }, limits).approved, true);
  });
});
