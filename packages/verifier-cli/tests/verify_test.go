package tests

import (
	"testing"

	"github.com/runfile-ai/sdks/packages/verifier-cli/pkg/verify"
)

// Scaffold test: Result.OK() is false on a zero-value (no checks passed) result.
// Real verification tests against fixture bundles are added with the logic.
func TestZeroResultNotOK(t *testing.T) {
	if (verify.Result{}).OK() {
		t.Fatal("zero-value Result should not be OK")
	}
}
