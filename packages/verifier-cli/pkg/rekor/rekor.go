// Package rekor verifies Sigstore Rekor inclusion proofs for weekly meta-roots.
//
// The Merkle Builder anchors the meta-root of each week's manifests to the
// public Sigstore Rekor instance. This package checks an inclusion proof
// offline against a bundled Rekor entry, so verification needs no live Rekor
// call (a proof can also be fetched live when online).
//
// Skeleton: signature in place; body TODO.
package rekor

import "errors"

// VerifyInclusion checks a Rekor inclusion proof for the given meta-root hash.
func VerifyInclusion(metaRoot string, proof []byte) (bool, error) {
	_, _ = metaRoot, proof
	return false, errors.New("rekor.VerifyInclusion: not implemented")
}
