/**
 * Per-`(tenant, agent)` data-key cache.
 *
 * The SDK holds no AWS credentials. It mints a data key from `POST /v1/data-keys`
 * (authenticated with the `rf_*` API key), caches `{ keyId, plaintext, wrapped }`
 * in process memory keyed by `(tenant, agent)` with a 24h TTL, encrypts payloads
 * locally under `plaintext`, and attaches `keyId` + `wrapped` to each event.
 * `plaintext` is never written to disk and is zeroized on expiry / shutdown.
 *
 * No external cache: each instance mints its own key; the per-event `keyId`
 * disambiguates at read time. Serverless mints roughly per invocation — accepted.
 *
 * Skeleton: signatures in place; bodies TODO.
 */

export const TTL_MS = 24 * 60 * 60 * 1000;

export interface DataKey {
  keyId: string;
  plaintext: Uint8Array; // 32-byte AES-256 key; memory only, never persisted
  wrapped: string; // base64 KMS-wrapped form; travels with each event
  algorithm: 'aes-256-gcm';
}

/** Lazily fetches and caches one data key per `(tenant, agent)` with a TTL. */
export class DataKeyCache {
  constructor(private readonly ttlMs: number = TTL_MS) {}

  async getOrFetch(_tenantId: string, _agentIdentity: string): Promise<DataKey> {
    throw new Error('not implemented');
  }

  /** Overwrite cached plaintext key bytes and drop entries (shutdown / expiry). */
  zeroize(): void {
    throw new Error('not implemented');
  }
}
