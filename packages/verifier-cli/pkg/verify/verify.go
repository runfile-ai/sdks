// Package verify holds the offline verification logic for evidence bundles.
//
// Verification proves, with no network and no trust in Runfile's servers:
//  1. Event chain integrity — events within each run link by prev_event_hash in
//     local_seq order; segments stitch via the run_resume → run_suspend hash;
//     conversation continuity threads across runs via the run_create →
//     prev run's run_end reference.
//  2. Merkle inclusion — each event is a leaf in its day's signed manifest.
//  3. KMS signature — each manifest root is signed by the tenant's KMS key.
//  4. Rekor anchoring (optional) — the weekly meta-root is in the public log.
//
// "watch it break when I tamper one byte": any altered byte breaks (1) and (2).
//
// Skeleton: result shape + signature in place; body TODO.
package verify

import (
	"errors"

	"github.com/runfile-ai/sdks/packages/verifier-cli/pkg/bundle"
)

// Result is the outcome of verifying a bundle.
type Result struct {
	ChainValid     bool
	MerkleValid    bool
	SignatureValid bool
	RekorValid     bool // false when no Rekor proof is present (not a failure)
	Findings       []string
}

// OK reports whether every required check passed.
func (r Result) OK() bool {
	return r.ChainValid && r.MerkleValid && r.SignatureValid
}

// Verify runs all offline checks over a parsed bundle.
func Verify(b *bundle.Bundle) (Result, error) {
	_ = b
	return Result{}, errors.New("verify.Verify: not implemented")
}
