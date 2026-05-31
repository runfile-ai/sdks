/**
 * In-process buffer + background flusher.
 *
 * The hot path appends buffered events (metadata + still-cleartext payload, in
 * memory only). The flusher drains on size (default 100 items), interval
 * (default 2s), explicit `flush()`, or `beforeExit`; it classifies, redacts,
 * encrypts, batches, and ships. On sustained failure it spools ciphertext to
 * disk.
 *
 * Overflow: never drop individual events (that tears a run's hash chain). Apply
 * backpressure; if disabled, drop WHOLE runs atomically and emit a loud
 * `sdk_diagnostic` (`code=run_dropped_overflow`).
 *
 * Skeleton: signatures in place; bodies TODO.
 */

import type { Run } from './run.js';

export interface BufferedEvent {
  event: unknown;
  rawPayload?: unknown; // cleartext; memory-only until the flusher processes it
  run: Run;
}

export class EventBuffer {
  constructor(
    private readonly softCap: number = 10_000,
    private readonly flushThreshold: number = 100,
  ) {}

  append(_item: BufferedEvent): void {
    throw new Error('not implemented');
  }

  /** Process and ship buffered items as one or more mixed batches. */
  async drain(): Promise<void> {
    throw new Error('not implemented');
  }
}
