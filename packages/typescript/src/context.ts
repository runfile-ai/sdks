/**
 * Ambient context propagation via `AsyncLocalStorage`.
 *
 * Customers never thread `runId` / `parentEventId` manually. The store carries
 * the current run, parent event, and parallel group across `await` boundaries
 * within a single Node event loop — but NOT across worker threads or child
 * processes unless the customer propagates context explicitly.
 */

import { AsyncLocalStorage } from 'node:async_hooks';
import type { Run } from './run.js';

export interface AmbientContext {
  run?: Run;
  parentEventId?: string;
  parallelGroupId?: string;
}

export const contextStore = new AsyncLocalStorage<AmbientContext>();

/** The current ambient run, or `undefined` outside a run. */
export function currentRun(): Run | undefined {
  return contextStore.getStore()?.run;
}

/**
 * Open a `parallelGroupId` around concurrent operations and close it when `fn`
 * resolves. Events captured inside are tagged concurrent.
 */
export async function parallelGroup<T>(_fn: () => Promise<T>): Promise<T> {
  throw new Error('not implemented');
}
