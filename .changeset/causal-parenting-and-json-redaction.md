---
"@runfile-ai/sdk": patch
---

Python SDK fixes (drives the shared `runfile-ai` version):

- **Claude adapter — causal event DAG.** `parent_event_id` is now derived from the SDK's own `tool_use_id` (each `tool_result` parents on its `tool_call`, each `tool_call` on the issuing `llm_call`), instead of the previous-event-by-`local_seq`. This removes false causal edges and fixes `tool_call → tool_result` pairing under concurrent/interleaved tool calls. Concurrent tool calls in one assistant message also share a `parallel_group_id`.
- **Redaction — structural handling of JSON-string payloads.** A string leaf that is itself a JSON object/array (e.g. an MCP tool result delivered as text) is now parsed and redacted field-by-field, then re-embedded as a string. Previously a value-anchored detector (e.g. a `^\d{4}-\d{2}-\d{2}$` date-of-birth rule) could not match a value buried in the blob and leaked it in cleartext; structured PII is now redacted regardless of payload shape.
