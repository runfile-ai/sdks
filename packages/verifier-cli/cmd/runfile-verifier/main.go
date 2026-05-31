// Command runfile-verifier verifies a Runfile evidence bundle offline.
//
// Usage:
//
//	runfile-verifier verify <bundle-path>
//
// Exit code 0 means the event chain, Merkle inclusions, and KMS signatures all
// verified; non-zero means a check failed or the bundle could not be read.
//
// Skeleton: CLI wiring in place; verification returns "not implemented".
package main

import (
	"fmt"
	"os"

	"github.com/runfile-ai/sdks/packages/verifier-cli/pkg/bundle"
	"github.com/runfile-ai/sdks/packages/verifier-cli/pkg/verify"
)

func main() {
	if len(os.Args) < 3 || os.Args[1] != "verify" {
		fmt.Fprintln(os.Stderr, "usage: runfile-verifier verify <bundle-path>")
		os.Exit(2)
	}

	b, err := bundle.Load(os.Args[2])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	result, err := verify.Verify(b)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	if !result.OK() {
		fmt.Fprintln(os.Stderr, "verification FAILED")
		for _, f := range result.Findings {
			fmt.Fprintf(os.Stderr, "  - %s\n", f)
		}
		os.Exit(1)
	}

	fmt.Println("verification OK")
}
