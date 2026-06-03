---
"@runfile-ai/sdk": minor
---

Python SDK — LangGraph adapter (`runfile_ai.integrations.langgraph`):

- **One-line `instrument(graph, agent_identity=...)`** wires a sync handler onto the graph via `with_config({"callbacks": [...]})`; the returned graph is used exactly as before (`invoke` / `ainvoke` / `stream`). Built on the verified `langgraph.callbacks.GraphCallbackHandler` (1.x) so it receives both the LangChain callbacks and graph interrupt/resume; falls back to the plain `BaseCallbackHandler` on older LangGraph (no interrupt/resume capture).
- **Witness-authored lifecycle.** The top-level chain start/end author the `run_create` / `run_end` chain events (plus companion items), per the witness-authored-lifecycle model — no server-synthesised genesis.
- **One `llm_call` per model turn** (LangChain already coalesces a turn), with `model_ref` (provider/model id from `response_metadata`, tokens from `usage_metadata`).
- **Causal `tool_call` / `tool_result` parenting from the framework's own ids** — each `tool_call` parents on the issuing `llm_call` (matched by `tool_call_id` against `AIMessage.tool_calls`), each `tool_result` on its `tool_call` (matched by the stable tool `run_id`). When one model turn issues ≥2 tool calls, the flusher groups them into a shared `parallel_group_id`.
- **HITL via `interrupt()` / `Command(resume=...)`** → `run_suspend` (`framework_inferred`, `awaiting_human_input`) + `run_update` → `awaiting_human`, then `run_resume` (new segment) + `run_update` → `active`. The resuming invocation (new root run-id, same `thread_id`) is routed to the *same* run, not a duplicate; the interrupted root `chain_end` is correctly treated as a pause, not a `run_end`.
- **Control-flow vs real errors.** `GraphInterrupt` / `GraphBubbleUp` surfacing through `on_tool_error` / `on_chain_error` are suspensions, not failures, and are not recorded as failed results or run ends; genuine tool/chain errors produce a `tool_result` (`outcome=failure`) / `run_end` (`outcome=failure`).
- Turn-atomic flushing is driven so a model turn's concurrent tool fan-out stays whole for the flusher's grouping. Every emitted item validates against the ingest schema (`framework="langgraph"`); the adapter is a transparent no-op when the SDK isn't initialised and never raises into the customer's graph.

Verified against real `langgraph` 1.2 / `langchain-core` 1.4 (tests drive an actual `create_react_agent` graph). Node-boundary (`graph_node_enter`/`exit`), subgraph `delegate`, and explicit fork/`continued_from` capture are deferred follow-ups.
