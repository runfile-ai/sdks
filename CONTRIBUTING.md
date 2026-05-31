# Contributing to the Runfile SDKs

This is an Apache-2.0, public repository. Its git history is permanent and
public — do not commit backend internals, infrastructure details, customer
names, or security-incident context. Those belong in the private
`runfile/platform` monorepo.

## Repo shape

A pnpm workspace for the TypeScript package and examples, a standalone Python
project, and a standalone Go module for the Verifier CLI:

- `packages/python` — `runfile-ai` (import `runfile_ai`). Hatchling project.
- `packages/typescript` — `@runfile-ai/sdk`. pnpm workspace member.
- `packages/verifier-cli` — `github.com/runfile-ai/sdks/packages/verifier-cli`. Own `go.mod`.

## The schema contract

The SDKs depend on the published schema packages, **not** path links:

- TS: `@runfile-ai/schemas` (npm)
- Python: `runfile-ai-schemas` (PyPI), imported as `runfile_schemas`
- Go: `github.com/runfile-ai/schemas`

When a schema field is needed, bump the dependency to the released schema
version — don't reach into the schemas repo.

### Open follow-up: wire `sdk.name`

The SDKs report `sdk.name = "runfile-ai"` (Python) and `"@runfile-ai/sdk"` (TS)
on every event. The schema's `SdkNameEnum` (and the Ingest API
`Runfile-SDK-Name` header enum) must include these values. Until the schema is
updated + regenerated + the Ingest/Event-Processor validators redeployed, real
batches from these SDKs will be rejected at validation. Track this with the
schemas repo before the first SDK release. See `packages/*/…/constants`.

## Versioning

- TS: [changesets](https://github.com/changesets/changesets) — `pnpm changeset`.
- Python: version in `packages/python/pyproject.toml`; released via PyPI
  trusted publishing (OIDC).
- Verifier CLI: tagged GitHub Release, pinned to the schema version it was built
  against.

SDK versions are independent of the schema version (an SDK can be `0.4.2` while
schemas is `1.0.0`); they pin a range like `^0.5.0` and accept additive minors.

## Tests

Every package ships unit tests and ships against both the latest published
schema and the lowest supported schema version in its range. Network is mocked —
SDK tests never hit a live Ingest API.
