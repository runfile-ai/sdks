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

## Open cross-repo follow-up — wire `sdk.name` (BLOCKS first release)

The SDKs report `sdk.name = "runfile-ai"` / `"@runfile-ai/sdk"`. The deployed
schema's `SdkNameEnum` (`schemas/src/event.ts`) and the Ingest API
`Runfile-SDK-Name` header enum still use the old identifiers. Before any SDK
release that talks to prod:

1. Update `SdkNameEnum` in `schemas/` to include `runfile-ai` and `@runfile-ai/sdk`.
2. Regenerate Python / Go / JSON-Schema artifacts; add a changeset (minor — additive).
3. Bump and redeploy the Ingest API + Event Processor validators in `platform/`.

Until then, real batches from these SDKs are rejected at ingest validation.

## Per-package state

| Package | Scaffold | Core logic | Adapters |
|---------|:--------:|:----------:|----------|
| `runfile-ai` (Python) | ✅ | ⏳ | ⏳ LangGraph, OpenAI Agents, Claude SDK, MCP (v1) |
| `@runfile-ai/sdk` (TS) | ✅ | ⏳ | ⏳ LangGraph.js, Claude SDK (v1); OpenAI/Mastra/Vercel (v1.5) |
| `runfile-verifier` (Go) | ✅ | ⏳ | n/a |

## Suggested build order (per ingestion docs)

1. Core: data-key fetch + cache, AES-256-GCM encrypt, event construction,
   buffer + flusher, spool, HTTP client, idempotency, retry/backoff.
2. Manual API end-to-end (metadata-only, then payload-bearing) against a mocked
   Ingest API.
3. First adapter: Python LangGraph (most documented signal surface).
4. Remaining v1 adapters; Verifier CLI logic; examples.
