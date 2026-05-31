/**
 * Redaction-policy fetch + cache.
 *
 * Fetched from `GET /v1/policies/current` on startup and refreshed every TTL
 * (default 300s) in the background. Configures the classifier/redactor. On fetch
 * failure the SDK keeps using the cached policy and emits an `sdk_diagnostic`.
 *
 * Skeleton: signatures in place; bodies TODO.
 */

export const DEFAULT_TTL_SECONDS = 300;

export interface RedactionPolicy {
  policyVersion: string;
  classificationRules: Array<Record<string, unknown>>;
  ttlSeconds: number;
}

export class PolicyCache {
  constructor(private readonly ttlSeconds: number = DEFAULT_TTL_SECONDS) {}

  async get(): Promise<RedactionPolicy> {
    throw new Error('not implemented');
  }

  async refresh(): Promise<RedactionPolicy> {
    throw new Error('not implemented');
  }
}
