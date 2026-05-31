/**
 * OpenAI Agents SDK (TypeScript) adapter.
 *
 * Same `RunHooks` / `result.interruptions` shape as the Python adapter. Wraps
 * `Runner.run` to emit run/tool/handoff/approval events.
 *
 * Deferred to v1.5 (Python ships first). Skeleton placeholder.
 */

export interface InstrumentRunnerOptions {
  agentIdentity: string;
  conversationId?: string;
}

export function instrumentRunner<R>(_runner: R, _opts: InstrumentRunnerOptions): R {
  throw new Error('not implemented: openai-agents (TS) is deferred to v1.5');
}
