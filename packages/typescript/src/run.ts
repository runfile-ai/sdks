/**
 * The in-memory Run model, lifecycle calls, and event capture.
 *
 * A `Run` tracks per-run bookkeeping the SDK owns: `runId`, `agentIdentity`,
 * `lifecycleState`, the `parentEventId` for the next event, the open
 * parallel-group stack, `localSeq` (monotonic per segment, reset on resume),
 * `segmentIndex`, and `lastEventHashLocal` (used as `prevEventHashIntent`).
 *
 * The hot path (`captureEvent`) only builds metadata and appends to the buffer
 * with the still-cleartext payload. Classification, redaction, and encryption
 * happen later in the background flusher.
 *
 * Skeleton: signatures and the `withRun` shape in place; bodies TODO.
 */

export interface Run {
  runId: string;
  agentIdentity: string;
  lifecycleState: 'active' | 'awaiting_human' | 'awaiting_webhook' | 'awaiting_schedule' | 'ended';
  conversationId?: string;
  segmentIndex: number;
  localSeq: number;
  parentEventId?: string;
  lastEventHashLocal?: string;
}

export interface StartRunOptions {
  agentIdentity: string;
  conversationId?: string;
  continuedFrom?: { runId: string; eventId: string };
}

export interface SuspendOptions {
  reason: string;
  expectedResumer?: string;
  expectedResumeBy?: string;
}

export interface ResumeOptions {
  runId: string;
  triggeredBy: string;
  resumerPrincipal?: string;
}

export interface CaptureEventOptions {
  kind: string;
  name: string;
  payload?: unknown;
  modelRef?: unknown;
  [key: string]: unknown;
}

/**
 * Create a run, set ambient context (AsyncLocalStorage), run `fn`, and end the
 * run on completion. The run boundary is dynamic per invocation — hence a
 * callback wrapper rather than a decorator.
 */
export async function withRun<T>(opts: StartRunOptions, fn: () => Promise<T>): Promise<T> {
  startRun(opts);
  try {
    const result = await fn();
    endRun({ outcome: 'success' });
    return result;
  } catch (err) {
    endRun({ outcome: 'failure' });
    throw err;
  }
}

export function startRun(_opts: StartRunOptions): Run {
  throw new Error('not implemented');
}

export function endRun(_opts: { outcome: 'success' | 'failure' | 'incomplete' | 'abandoned' }): void {
  throw new Error('not implemented');
}

export function suspendRun(_opts: SuspendOptions): void {
  throw new Error('not implemented');
}

export function resumeRun(_opts: ResumeOptions): void {
  throw new Error('not implemented');
}

export function abandonRun(_opts: { runId: string; reason?: string }): void {
  throw new Error('not implemented');
}

export async function captureEvent(_opts: CaptureEventOptions): Promise<void> {
  throw new Error('not implemented');
}
