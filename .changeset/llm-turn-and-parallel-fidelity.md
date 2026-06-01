---
"@runfile-ai/sdk": patch
---

Python SDK — Claude adapter model-turn fidelity (capture how the agent actually thought):

- **Reasoning captured, not discarded.** `ThinkingBlock` carries its content on `.thinking`, not `.text`; the adapter previously recorded every extended-thinking block as the bare token `"ThinkingBlock"`, losing the "why" behind a decision. It now reads each block's real content (`.thinking` / `.text`), so the model's reasoning lands in the trail. (Opus omits thinking text unless `thinking={"display":"summarized"}` is set; omitted blocks are marked, not blanked.)
- **One `llm_call` per real model turn, fully assembled.** A turn streamed as several `AssistantMessage`s (Thinking / text / ToolUse, shared `message_id`) is accumulated and emitted as ONE `llm_call` — carrying the full content and the turn's cumulative usage — deferred until the turn's first tool / next turn / stream end, so the single call still precedes and parents its tool calls. No more counting stream fragments as calls, repeating per-turn usage, or dropping later blocks.
- **True per-turn token usage under prompt caching.** `model_ref.input_tokens` is uncached + cache-read + cache-creation (not the bare non-cached delta); breakdown in `otel_attributes.extra`.
- **Concurrency grouping is structural and single-authority.** Parallel tool calls are grouped by the flusher from the call/result ordering (≥2 consecutive calls, no result between); sequential calls are never grouped. (Removed the adapter's same-turn grouping, which couldn't tell concurrent from sequential.)
