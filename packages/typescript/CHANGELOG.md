# @runfile-ai/sdk

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
