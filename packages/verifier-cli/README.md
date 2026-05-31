# runfile-verifier

Offline verifier for Runfile evidence bundles. Distributed as a single static Go
binary via GitHub Releases.

```bash
runfile-verifier verify ./evidence-bundle/
```

Proves — with no network and no trust in Runfile's servers — that:

1. the event hash chain is intact (within and across runs in a conversation),
2. every event is included in its day's signed Merkle manifest,
3. each manifest root carries a valid tenant KMS signature, and
4. (optionally) the weekly meta-root is anchored in Sigstore Rekor.

Tamper with one byte of any event and checks 1–2 fail. This is the
differentiator audit firms run themselves.

- **Module:** `github.com/runfile-ai/sdks/packages/verifier-cli`
- **Go:** 1.22+
- Pinned to the schema version it was built against (verifies that version and
  any newer additive minor).

> 🚧 Scaffold — CLI wiring and package layout are in place; verification logic is
> TODO (`pkg/verify`, `pkg/bundle`, `pkg/rekor`).

## Local development

```bash
go build ./...
go test ./...
go build -o bin/runfile-verifier ./cmd/runfile-verifier
```
