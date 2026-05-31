module github.com/runfile-ai/sdks/packages/verifier-cli

go 1.22

// TODO: depend on github.com/runfile-ai/schemas for the generated Run/Event/
// manifest/evidence Go structs once the verification logic is wired. Pinned to
// the schema version this binary is built against (a bundle under schema 1.0 is
// verifiable by a verifier built against 1.0.x or any newer 1.x).
