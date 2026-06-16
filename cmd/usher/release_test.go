package main

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestVersionLdflagStamp guards the #21 distribution contract: `version` is a
// package var (not a const) precisely so the release build can stamp the real
// tag via `-ldflags "-X main.version=<tag>"`. The .goreleaser.yaml relies on
// this; if someone reverts version to a const, the linker silently no-ops the
// -X and the released binary reports the dev default. This test compiles the
// command with an injected version and asserts `usher version` echoes it.
func TestVersionLdflagStamp(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping ldflag build test in -short mode")
	}

	const want = "1.2.3-ldflag-test"
	bin := filepath.Join(t.TempDir(), "usher-vtest")

	build := exec.Command("go", "build",
		"-ldflags", "-X main.version="+want,
		"-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build with ldflags failed: %v\n%s", err, out)
	}

	out, err := exec.Command(bin, "version").CombinedOutput()
	if err != nil {
		t.Fatalf("running %s version: %v\n%s", bin, err, out)
	}
	got := strings.TrimSpace(string(out))
	if want_ := "usher " + want; got != want_ {
		t.Fatalf("version output = %q, want %q (ldflags -X main.version did not stamp; is version a const again?)", got, want_)
	}
}

// TestVersionDefault asserts the in-process default is non-empty and carries no
// stray whitespace, so a plain `go build` / `go install` (no ldflags) still
// reports a sane version rather than an empty string.
func TestVersionDefault(t *testing.T) {
	if version == "" {
		t.Fatal("default version is empty; go install would report a blank version")
	}
	if strings.TrimSpace(version) != version {
		t.Fatalf("default version %q has surrounding whitespace", version)
	}
	// version must be an addressable package var (not a const) so the linker's
	// -X main.version can override it; taking its address would not compile if it
	// were a const, so this line also documents that requirement.
	_ = &version
}
