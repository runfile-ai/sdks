/**
 * Best-effort PII classifier + redactor (the client-side PII boundary).
 *
 * Runs in the background flusher, before encryption. Identifies sensitive
 * classes and applies the customer's redaction policy: drop / tokenize (via
 * Vault) / hash / pass_through / tokenize_with_fallback.
 *
 * Honest caveat: regex/heuristic classification has false negatives. The real
 * controls are the customer's policy, deliberate schema `data_classification`
 * tagging, and the fact that whatever is missed is still encrypted at rest.
 *
 * Skeleton: signatures in place; bodies TODO.
 */

export const PII_CLASSES = [
  'person_name',
  'email_address',
  'phone_number',
  'address',
  'ssn',
  'tax_id',
  'bank_account',
  'credit_card',
  'dob',
  'health_record',
  'government_id',
  'internal_id',
] as const;

export type PiiClass = (typeof PII_CLASSES)[number];

export class Classifier {
  /** Annotate the payload with detected PII spans/classes. */
  classify(_payload: unknown): unknown {
    throw new Error('not implemented');
  }
}

export class Redactor {
  /** Apply the policy treatment per class. */
  apply(_classified: unknown, _policy: unknown): unknown {
    throw new Error('not implemented');
  }
}
