/**
 * Vercel AI SDK adapter.
 *
 * Consumes OTel spans emitted with `experimental_telemetry: { isEnabled: true }`,
 * and/or hooks `onStart` / `onStepFinish` / `onToolCallFinish` for richer
 * capture.
 *
 * Deferred to v1.5. Skeleton placeholder.
 */

export interface VercelInstrumentOptions {
  agentIdentity: string;
}

export function instrument<T>(_target: T, _opts: VercelInstrumentOptions): T {
  throw new Error('not implemented: vercel-ai-sdk is deferred to v1.5');
}
