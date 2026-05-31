# Runfile SDKs

Open-source SDKs and customer-facing tooling for [Runfile](https://runfile.ai) —
the tamper-evident audit trail for AI agents. This repo is where engineers
integrate Runfile into their agent code, and where audit firms verify that the
Verifier CLI does what we claim.

> **License:** Apache-2.0. The git history of this repo is public forever — keep
> backend IP, sales context, and security discussion out of it (those live in
> the private `runfile/platform` monorepo).

## Packages

| Package | Registry | Language | Wire `sdk.name` |
|---------|----------|----------|-----------------|
| [`runfile-ai`](packages/python) | PyPI (`pip install runfile-ai`, `import runfile_ai`) | Python 3.11+ | `runfile-ai` |
| [`@runfile-ai/sdk`](packages/typescript) | npm | TypeScript / Node 20+ | `@runfile-ai/sdk` |
| [`runfile-verifier`](packages/verifier-cli) | GitHub Releases | Go 1.22+ | — |

All three depend on the published schema contract — `@runfile-ai/schemas` (npm),
`runfile-ai-schemas` (PyPI), and `github.com/runfile-ai/schemas` (Go) — as a
**versioned package, never a path link**. A schema change is an explicit
dependency bump here.

## What the SDK does

Observes the customer's agent (via framework adapters or a manual API) and
translates framework-native signals into Runfile runs and events:

1. Capture events (LangGraph, OpenAI Agents, Claude Agent SDK, MCP, manual).
2. Manage run lifecycle from observable signals (start / suspend / resume / end).
3. Classify + redact PII client-side (the PII boundary).
4. Envelope-encrypt the redacted payload locally under a per-`(tenant, agent)`
   data key fetched from `POST /v1/data-keys` (no AWS credentials client-side).
5. Batch and ship to the Ingest API (`POST /v1/batches`) off the hot path.

The full design lives in the private design docs (`sdk-design.md`,
`sdk-ingest-api.md`, `event-schema.md`).

## Layout

```
sdks/
├── packages/
│   ├── python/         # runfile-ai (PyPI)
│   ├── typescript/     # @runfile-ai/sdk (npm)
│   └── verifier-cli/   # runfile-verifier (Go, GitHub Releases)
├── examples/           # end-to-end demo apps
└── docs/               # SDK docs (docs.runfile.ai)
```

## Status

🚧 **Scaffold.** This is the repository shell — package manifests, module
skeletons, CI, and release wiring are in place; the SDK logic is being filled in
package by package. See [`docs/STATUS.md`](docs/STATUS.md).

## Development

```bash
pnpm install            # TypeScript workspace + tooling
pnpm -r build
pnpm -r test
```

Python and Go each have their own local setup — see their package READMEs.
