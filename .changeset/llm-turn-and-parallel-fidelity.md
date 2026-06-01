---
"@runfile-ai/sdk": patch
---

Python SDK — Claude adapter model-turn & concurrency fidelity:

- **One `llm_call` per model turn.** The CLI streams a turn's Thinking/text/ToolUse blocks as separate `AssistantMessage`s sharing a `message_id`; these are now coalesced into a single `llm_call` (tagged `labels.claude_message_id`) instead of one per block, so the trail records real model calls (not stream fragments) and stops triple-counting per-turn token usage.
- **Concurrent tool calls grouped.** Tool calls dispatched in parallel (≥2 consecutive `tool_call`s sharing an issuing `llm_call` with no `tool_result` between) now share a `parallel_group_id`, assigned flusher-side from the structural signal. Sequential calls are never grouped; no group is ever fabricated.
