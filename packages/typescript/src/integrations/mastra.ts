/**
 * Mastra adapter (OTel GenAI spans).
 *
 * Mastra emits OTel GenAI spans via its built-in OTLP exporter. The adapter
 * consumes those spans and maps each to a Runfile event with `otelAttributes`
 * populated (chat → llm_call, execute_tool → tool_call, invoke_agent → run or
 * delegate). Suspension is not auto-detected (no OTel "suspended" span) —
 * customers call `suspendRun(...)` explicitly or accept activity gaps.
 *
 * Deferred to v1.5. Skeleton placeholder.
 */

export interface MastraInstrumentOptions {
  agentIdentity: string;
}

export function instrument<T>(_target: T, _opts: MastraInstrumentOptions): T {
  throw new Error('not implemented: mastra is deferred to v1.5');
}
