/**
 * Runfile SDK for TypeScript / Node.
 *
 * Observe an agent and translate framework-native signals into Runfile runs and
 * events. The customer's framework remains the source of truth; this SDK is a
 * passive observer.
 *
 * Primary path — framework adapters:
 * ```ts
 * import { init } from '@runfile-ai/sdk';
 * import { instrument } from '@runfile-ai/sdk/integrations/langgraph';
 *
 * await init({ apiKey: process.env.RUNFILE_API_KEY!, environment: 'production' });
 * const graph = instrument(originalGraph, { agentIdentity: 'did:web:bank.com:agents:loan-triage:v2' });
 * ```
 *
 * Fallback — manual API:
 * ```ts
 * await withRun({ agentIdentity: 'did:web:acme.com:agents:custom' }, async () => {
 *   await captureEvent({ kind: 'llm_call', name: 'messages.create', modelRef: {} });
 * });
 * ```
 *
 * See `sdk-design.md` for the full design. This module is the public surface.
 */

export { SDK_NAME, SCHEMA_VERSION } from './constants.js';
export { init, flush, shutdown, getInstance, RunfileClient } from './client.js';
export { currentRun, parallelGroup } from './context.js';
export {
  withRun,
  startRun,
  endRun,
  suspendRun,
  resumeRun,
  abandonRun,
  captureEvent,
} from './run.js';
export type { Run } from './run.js';
