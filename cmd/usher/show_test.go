package main

import (
	"io"
	"strings"
	"testing"

	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/keychain"
)

// writeShowConfig persists cfg into the (already-isolated) state dir so backendShow
// has something to load.
func writeShowConfig(t *testing.T, cfg *config.Config) {
	t.Helper()
	if err := cfg.Save(config.DefaultPath()); err != nil {
		t.Fatalf("save config: %v", err)
	}
}

// TestBackendShowExisting renders a registered stdio/inherit backend and checks
// that name, transport, auth, and the command + args all appear. captureStdout
// (shared with the lifecycle tests) redirects os.Stdout, which is where
// backendShow prints.
func TestBackendShowExisting(t *testing.T) {
	t.Setenv("USHER_STATE_DIR", t.TempDir())
	writeShowConfig(t, &config.Config{Backends: []config.Backend{{
		Name:      "fs",
		Transport: "stdio",
		Command:   []string{"/usr/local/bin/fs-server", "mcp", "--root", "/tmp"},
		Auth:      "inherit",
		Default:   true,
	}}})

	out := captureStdout(t, func() {
		if err := backendShow([]string{"fs"}); err != nil {
			t.Errorf("backendShow(fs) = %v, want nil", err)
		}
	})
	for _, want := range []string{"fs", "stdio", "inherit", "/usr/local/bin/fs-server", "mcp --root /tmp"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n--- output ---\n%s", want, out)
		}
	}
}

// TestBackendShowAbsent: showing a name that is not registered is an error and
// never falls back to the default backend.
func TestBackendShowAbsent(t *testing.T) {
	t.Setenv("USHER_STATE_DIR", t.TempDir())
	writeShowConfig(t, &config.Config{Backends: []config.Backend{{
		Name: "fs", Transport: "stdio", Command: []string{"x"}, Auth: "inherit", Default: true,
	}}})

	if err := backendShow([]string{"nope"}); err == nil {
		t.Fatal("backendShow(nope) = nil, want error for an absent backend")
	}
}

// TestBackendShowUsage: a missing/empty name is a usage error.
func TestBackendShowUsage(t *testing.T) {
	for _, args := range [][]string{nil, {}, {""}, {"a", "b"}} {
		if err := backendShow(args); err == nil {
			t.Errorf("backendShow(%v) = nil, want usage error", args)
		}
	}
}

// TestBackendShowKeychainPresence covers the auth=env reporting path: a key the
// (stubbed) Keychain holds reads "set", an absent key reads "MISSING", and the
// secret VALUE never appears in the output. keychainGet is stubbed so the test
// never touches the real Keychain.
func TestBackendShowKeychainPresence(t *testing.T) {
	t.Setenv("USHER_STATE_DIR", t.TempDir())
	writeShowConfig(t, &config.Config{Backends: []config.Backend{{
		Name:      "api",
		Transport: "stdio",
		Command:   []string{"/bin/api"},
		Auth:      "env",
		EnvKeys:   []string{"PRESENT_KEY", "ABSENT_KEY"},
	}}})

	const secret = "super-secret-value"
	orig := keychainGet
	keychainGet = func(backend, account string) (string, error) {
		if backend != "api" {
			t.Errorf("keychainGet backend = %q, want api", backend)
		}
		if account == "PRESENT_KEY" {
			return secret, nil
		}
		return "", keychain.ErrNotFound
	}
	t.Cleanup(func() { keychainGet = orig })

	out := captureStdout(t, func() {
		if err := backendShow([]string{"api"}); err != nil {
			t.Errorf("backendShow(api) = %v, want nil", err)
		}
	})
	if !strings.Contains(out, "PRESENT_KEY (set)") {
		t.Errorf("output missing PRESENT_KEY (set)\n%s", out)
	}
	if !strings.Contains(out, "ABSENT_KEY (MISSING)") {
		t.Errorf("output missing ABSENT_KEY (MISSING)\n%s", out)
	}
	if strings.Contains(out, secret) {
		t.Errorf("secret VALUE leaked into output:\n%s", out)
	}
}

// TestBackendShowKeychainUnavailable: when the Keychain itself errors (not a
// clean not-found), presence degrades to "unknown" rather than failing the
// command — so `usher backend show` still works in an environment without a
// usable Keychain.
func TestBackendShowKeychainUnavailable(t *testing.T) {
	t.Setenv("USHER_STATE_DIR", t.TempDir())
	writeShowConfig(t, &config.Config{Backends: []config.Backend{{
		Name:      "api",
		Transport: "stdio",
		Command:   []string{"/bin/api"},
		Auth:      "env",
		EnvKeys:   []string{"SOME_KEY"},
	}}})

	orig := keychainGet
	keychainGet = func(backend, account string) (string, error) {
		return "", io.ErrUnexpectedEOF // any non-ErrNotFound failure
	}
	t.Cleanup(func() { keychainGet = orig })

	out := captureStdout(t, func() {
		if err := backendShow([]string{"api"}); err != nil {
			t.Errorf("backendShow(api) = %v, want nil (keychain failure must degrade gracefully)", err)
		}
	})
	if !strings.Contains(out, "SOME_KEY (unknown)") {
		t.Errorf("output missing SOME_KEY (unknown)\n%s", out)
	}
}
