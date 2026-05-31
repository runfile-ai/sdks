/**
 * LangGraph.js adapter.
 *
 * Registers a callback handler implementing the LangChain + LangGraph callback
 * interfaces; translates node/LLM/tool callbacks into Runfile events, detects
 * `__interrupt__` → `run_suspend` and resume → `run_resume`, and subgraph entry
 * → `delegate` + child run. Same-run resume vs fork is detected via the
 * `thread_id`→`runId` map. Maintains context via `AsyncLocalStorage`.
 *
 * v1 TypeScript coverage. Skeleton: `instrument()` signature in place; body TODO.
 */

export interface InstrumentOptions {
  agentIdentity: string;
  conversationId?: string;
}

/** Register the Runfile callback handler on `graph` and return it. */
export function instrument<G>(_graph: G, _opts: InstrumentOptions): G {
  throw new Error('not implemented');
}
