/**
 * Stand-in for a card processor. Decisions are deterministic so the failure
 * path of the saga can be demonstrated and tested on demand:
 *
 *   tok_decline              -> card declined
 *   tok_insufficient_funds   -> insufficient funds
 *   amount over the limit    -> exceeds per-order limit
 *   anything else            -> approved
 */
export function authorize({ amountCents, paymentToken }, { limitCents }) {
  if (paymentToken === 'tok_decline') return { approved: false, reason: 'card declined' };
  if (paymentToken === 'tok_insufficient_funds') return { approved: false, reason: 'insufficient funds' };
  if (amountCents > limitCents) {
    return { approved: false, reason: `amount ${amountCents} exceeds limit ${limitCents}` };
  }
  return { approved: true };
}
