/**
 * Client-side AES-256-GCM envelope encryption of redacted payloads.
 *
 * Runs in the background flusher, after redaction, before shipping. Encrypts
 * under the cached per-`(tenant, agent)` plaintext data key with a fresh
 * per-event nonce, producing the `payloadRef` the Ingest API stores as
 * ciphertext. The Ingest API cannot decrypt — only Vault / Selective Decrypt can.
 *
 * Skeleton: signature + result shape in place; body TODO.
 */

export interface Encrypted {
  ciphertext: Uint8Array;
  nonce: Uint8Array;
}

/** AES-256-GCM encrypt with a fresh 12-byte nonce (auth tag appended to ciphertext). */
export function aesGcmEncrypt(_plaintext: Uint8Array, _dataKey: Uint8Array): Encrypted {
  throw new Error('not implemented');
}
