# @runfile-ai/sdk

## 0.2.0

### Minor Changes

- [#8](https://github.com/runfile-ai/sdks/pull/8) [`0f8d702`](https://github.com/runfile-ai/sdks/commit/0f8d702ba234ae8fa5215ce119c798e778b5d4db) Thanks [@ada-raj](https://github.com/ada-raj)! - Python SDK — LangGraph adapter (`runfile_ai.integrations.langgraph`):

  - **One-line `instrument(graph, agent_identity=...)`** wires a sync handler onto the graph via `with_config({"callbacks": [...]})`; the returned graph is used exactly as before (`invoke` / `ainvoke` / `stream`). Built on the verified `langgraph.callbacks.GraphCallbackHandler` (1.x) so it receives both the LangChain callbacks and graph interrupt/resume; falls back to the plain `BaseCallbackHandler` on older LangGraph (no interrupt/resume capture).
  - **Witness-authored lifecycle.** The top-level chain start/end author the `run_create` / `run_end` chain events (plus companion items), per the witness-authored-lifecycle model — no server-synthesised genesis.
  - **One `llm_call` per model turn** (LangChain already coalesces a turn), with `model_ref` (provider/model id from `response_metadata`, tokens from `usage_metadata`).
  - **Causal `tool_call` / `tool_result` parenting from the framework's own ids** — each `tool_call` parents on the issuing `llm_call` (matched by `tool_call_id` against `AIMessage.tool_calls`), each `tool_result` on its `tool_call` (matched by the stable tool `run_id`). When one model turn issues ≥2 tool calls, the flusher groups them into a shared `parallel_group_id`.
  - **HITL via `interrupt()` / `Command(resume=...)`** → `run_suspend` (`framework_inferred`, `awaiting_human_input`) + `run_update` → `awaiting_human`, then `run_resume` (new segment) + `run_update` → `active`. The resuming invocation (new root run-id, same `thread_id`) is routed to the _same_ run, not a duplicate; the interrupted root `chain_end` is correctly treated as a pause, not a `run_end`.
  - **Control-flow vs real errors.** `GraphInterrupt` / `GraphBubbleUp` surfacing through `on_tool_error` / `on_chain_error` are suspensions, not failures, and are not recorded as failed results or run ends; genuine tool/chain errors produce a `tool_result` (`outcome=failure`) / `run_end` (`outcome=failure`).
  - Turn-atomic flushing is driven so a model turn's concurrent tool fan-out stays whole for the flusher's grouping. Every emitted item validates against the ingest schema (`framework="langgraph"`); the adapter is a transparent no-op when the SDK isn't initialised and never raises into the customer's graph.

  Verified against real `langgraph` 1.2 / `langchain-core` 1.4 (tests drive an actual `create_react_agent` graph). Node-boundary (`graph_node_enter`/`exit`), subgraph `delegate`, and explicit fork/`continued_from` capture are deferred follow-ups.

## 0.1.2

### Patch Changes

- [#5](https://github.com/runfile-ai/sdks/pull/5) [`c4d6d73`](https://github.com/runfile-ai/sdks/commit/c4d6d73280befd24fc62907213ed96ebc543ddd5) Thanks [@ada-raj](https://github.com/ada-raj)! - Python SDK — Claude adapter model-turn fidelity (capture how the agent actually thought):

  - **Reasoning captured, not discarded.** `ThinkingBlock` carries its content on `.thinking`, not `.text`; the adapter previously recorded every extended-thinking block as the bare token `"ThinkingBlock"`, losing the "why" behind a decision. It now reads each block's real content (`.thinking` / `.text`), so the model's reasoning lands in the trail. (Opus omits thinking text unless `thinking={"display":"summarized"}` is set; omitted blocks are marked, not blanked.)
  - **One `llm_call` per real model turn, fully assembled.** A turn streamed as several `AssistantMessage`s (Thinking / text / ToolUse, shared `message_id`) is accumulated and emitted as ONE `llm_call` — carrying the full content and the turn's cumulative usage — deferred until the turn's first tool / next turn / stream end, so the single call still precedes and parents its tool calls. No more counting stream fragments as calls, repeating per-turn usage, or dropping later blocks.
  - **True per-turn token usage under prompt caching.** `model_ref.input_tokens` is uncached + cache-read + cache-creation (not the bare non-cached delta); breakdown in `otel_attributes.extra`.
  - **Concurrency grouping is structural and single-authority.** Parallel tool calls are grouped by the flusher from the call/result ordering (≥2 consecutive calls, no result between); sequential calls are never grouped. (Removed the adapter's same-turn grouping, which couldn't tell concurrent from sequential.)

## 0.1.1

### Patch Changes

- [#4](https://github.com/runfile-ai/sdks/pull/4) [`8ccbcb7`](https://github.com/runfile-ai/sdks/commit/8ccbcb725f0bf83c45fb1be42c9177fa73194b34) Thanks [@ada-raj](https://github.com/ada-raj)! - Python SDK fixes (drives the shared `runfile-ai` version):

  - **Claude adapter — causal event DAG.** `parent_event_id` is now derived from the SDK's own `tool_use_id` (each `tool_result` parents on its `tool_call`, each `tool_call` on the issuing `llm_call`), instead of the previous-event-by-`local_seq`. This removes false causal edges and fixes `tool_call → tool_result` pairing under concurrent/interleaved tool calls. Concurrent tool calls in one assistant message also share a `parallel_group_id`.
  - **Redaction — structural handling of JSON-string payloads.** A string leaf that is itself a JSON object/array (e.g. an MCP tool result delivered as text) is now parsed and redacted field-by-field, then re-embedded as a string. Previously a value-anchored detector (e.g. a `^\d{4}-\d{2}-\d{2}$` date-of-birth rule) could not match a value buried in the blob and leaked it in cleartext; structured PII is now redacted regardless of payload shape.
