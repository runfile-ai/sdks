// Package bundle parses Runfile evidence bundles.
//
// An evidence bundle contains the events of one or more runs, the signed daily
// Merkle manifest(s) those events were committed to, the KMS signature over
// each manifest root, and (optionally) a Rekor inclusion proof for the weekly
// meta-root. This package turns the on-disk bundle into typed structures the
// verifier walks.
//
// Skeleton: types and signatures in place; bodies TODO.
package bundle

import "errors"

// Bundle is a parsed evidence bundle.
type Bundle struct {
	// TODO: Events, Manifests, Signatures, RekorProof — populated from the
	// generated github.com/runfile-ai/schemas Go structs.
	Path string
}

// Load reads and parses an evidence bundle from a path (file or directory).
func Load(path string) (*Bundle, error) {
	_ = path
	return nil, errors.New("bundle.Load: not implemented")
}
