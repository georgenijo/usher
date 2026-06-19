package main

import (
	"testing"
)

// TestSelftest drives runSelftest end to end in a clean temp env: a broker fronts
// the REAL bundled mcpserver (re-exec'd via the test binary's USHER_FAKE_MODE=
// mcpserver branch) and the full MCP handshake plus a tools/call must round-trip.
// This is the "selftest passes in a clean temp env" assertion from the spec.
func TestSelftest(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)

	// The backend is the test binary re-exec'd as the bundled mcpserver, matching
	// the production shape ([<usher> mcpserver]) without needing a separate build.
	backendCmd := fakeBackendCommand(t, "mcpserver")

	if err := runSelftest(backendCmd, dir); err != nil {
		t.Fatalf("selftest failed in clean temp env: %v", err)
	}
}
