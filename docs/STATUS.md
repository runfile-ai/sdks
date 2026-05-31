# SDK build status

Scaffold of `runfile/sdks` — the open-source SDK repo. The shell (package
manifests, module skeletons, CI, release wiring) is in place; SDK logic is
filled in package by package.

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
| `runfile-ai` (Python) | ✅ | ✅ (manual API end-to-end) | ⏳ LangGraph, OpenAI Agents, Claude SDK, MCP (v1) |
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

## Remaining Python work

1. **Vault tokenization** — `tokenize`/`tokenize_with_fallback` currently drop;
   wire the Vault `/v1/tokenize` client (needs the Vault request/response contract).
2. **Framework adapters** — LangGraph first (v1 priority), then OpenAI Agents,
   Claude Agent SDK, MCP. The customer-facing surface.
3. Decorators (`capture_decision`), size-based flush trigger, telemetry/logging.

Then: TypeScript core (mirror), Verifier CLI logic, end-to-end examples.
