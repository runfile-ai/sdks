# SDK build status

`runfile/sdks` — the open-source SDK repo. The **Python SDK (`runfile-ai`) is
live on PyPI** (core capture API plus the Claude Agent SDK and LangGraph adapters at
`0.2.0`); **`0.3.0` adds the OpenAI Agents SDK adapter** (tracing-processor based, with
HITL tool-approval suspend/resume). The TypeScript SDK and the Verifier CLI are still
being filled in package by package.

## Naming (decided)

| Surface | Value |
|---------|-------|
| PyPI distribution | `runfile-ai` |
| Python import | `runfile_ai` |
| npm package | `@runfile-ai/sdk` |
| Go module | `github.com/runfile-ai/sdks/packages/verifier-cli` |
| Wire `sdk.name` (Python) | `runfile-ai` |
| Wire `sdk.name` (TS) | `@runfile-ai/sdk` |

## Wire `sdk.name` — DONE

The SDKs report `sdk.name = "runfile-ai"` / `"@runfile-ai/sdk"`. These are in the
deployed schema's `SdkNameEnum` (`@runfile-ai/schemas` >= 0.6.0, generated to
Python/Go/JSON) and accepted by the live Ingest validator — real batches are
accepted. (If these wire identifiers ever change, update `SdkNameEnum`,
regenerate, and redeploy the Ingest / Event-Processor validators first.)

## Per-package state

| Package | Scaffold | Core logic | Adapters |
|---------|:--------:|:----------:|----------|
| `runfile-ai` (Python) | ✅ | ✅ (manual API end-to-end) | ✅ Claude SDK, ✅ LangGraph, ✅ OpenAI Agents · ⏳ MCP |
| `@runfile-ai/sdk` (TS) | ✅ | ⏳ | ⏳ LangGraph.js, Claude SDK (v1); OpenAI/Mastra/Vercel (v1.5) |
| `runfile-verifier` (Go) | ✅ | ⏳ | n/a |

## Python core — DONE (branch `python-sdk-core`, 57 tests, mypy --strict + ruff)

- Data-key fetch + per-(tenant,agent) cache (TTL, zeroize) · AES-256-GCM encrypt.
- Run lifecycle + event construction (local_seq, segments, parent chaining,
  parallel groups), manual + context-manager API.
- Background-thread flusher: hash chain (shared canonical projection, server
  parity), redact → encrypt → validate (Pydantic) → mixed batch → POST
  /v1/batches with Idempotency-Key, exponential backoff, 207 handling.
- Redaction policy fetch + cache; on-disk spool (ciphertext-only) + atexit drain;
  buffer overflow (backpressure / whole-run drop, never mid-run); PII classifier
  + redaction (drop/hash/pass_through; Luhn for cards).
- Never drops an individual event (chain integrity); never writes plaintext to disk.

## Framework adapters — Claude Agent SDK + LangGraph (`0.2.0`) + OpenAI Agents (`0.3.0`) DONE

- **Claude Agent SDK** (`integrations/anthropic.py`) — `observe_query()` wraps the
  SDK `query()`: whole-turn assembly (one `llm_call`/turn), reasoning capture, true
  input tokens under prompt caching, parallel tool-call grouping, witness-authored
  lifecycle events.
- **LangGraph** (`integrations/langgraph.py`) — `instrument()` registers a handler
  subclassing `GraphCallbackHandler`: node enter/exit, `llm_call`, tool call/result,
  `__interrupt__` → `run_suspend`, resume stitching, subgraph `delegate`.
- **OpenAI Agents** (`integrations/openai_agents.py`) — `instrument()` registers a
  **tracing `TracingProcessor`** (the canonical observability surface — hooks lack tool
  call-ids/args, so they can't chain parallel tools); `instrument_runner()` adds a thin
  `Runner.run` wrapper for HITL. Maps the verified span tree (`AgentSpanData`→run,
  `TurnSpanData`→`llm_call`, `FunctionSpanData`→`tool_call`/`tool_result`,
  `HandoffSpanData`→`handoff`+new run); parallel tools (siblings of a turn) →
  `parallel_group`; tool-approval `result.interruptions` → `run_suspend` /
  `tool_approval_requested`, `RunState`-resume → `run_resume` + `tool_approval_granted` /
  `tool_approval_denied` (decision read from the resumed `RunState`). Design +
  empirically-verified span tree in `private/v3/openai-agents-adapter-design.md`.

All three are exercised end to end by the credit-line decision agent example (same agent,
multiple runtimes), installed from PyPI via the `runfile-ai[anthropic]` /
`runfile-ai[langgraph]` / `runfile-ai[openai-agents]` extras.

## Remaining Python work

1. **MCP adapter** — `integrations/mcp.py` is still a skeleton (`instrument()`
   signature only). OpenAI Agents follow-ups: `as_tool` delegation, streaming-path
   lifecycle bracketing, and `Generation`/`Response`-span model-id enrichment.
2. **Vault tokenization** — `tokenize`/`tokenize_with_fallback` currently drop;
   wire the Vault `/v1/tokenize` client (needs the Vault request/response contract).
3. Decorators (`capture_decision`), telemetry/logging.

Then: TypeScript core (mirror), Verifier CLI logic.
