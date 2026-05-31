/**
 * On-disk spool for durability across process restarts.
 *
 * Append-only files under `~/.runfile/spool/` holding ONLY already-redacted,
 * already-encrypted batches — ciphertext, wrapped data keys, and event
 * metadata. Plaintext payloads and plaintext data keys are NEVER written here.
 * A drain reader ships spooled batches when the network is healthy. Bounded
 * (default 1 GB); on overflow, drop whole runs atomically (never mid-run events).
 *
 * Skeleton: signatures in place; bodies TODO.
 */

import { homedir } from 'node:os';
import { join } from 'node:path';

export const DEFAULT_SPOOL_DIR = join(homedir(), '.runfile', 'spool');
export const DEFAULT_MAX_BYTES = 1024 * 1024 * 1024; // 1 GB

export class Spool {
  constructor(
    private readonly directory: string = DEFAULT_SPOOL_DIR,
    private readonly maxBytes: number = DEFAULT_MAX_BYTES,
  ) {}

  /** Append an encrypted batch (ciphertext only) for later retry. */
  write(_encryptedBatch: Uint8Array): void {
    throw new Error('not implemented');
  }

  /** Read spooled batches and ship them when the network is available. */
  async drain(): Promise<void> {
    throw new Error('not implemented');
  }
}
