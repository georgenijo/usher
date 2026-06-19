package main

import (
	"path/filepath"
	"testing"

	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/keychain"
)

// withFakeKeychain swaps the package-level keychain indirections for an
// in-memory store so rename's secret-migration path can be exercised without
// touching the real login Keychain, and restores them when the test ends. The
// store is keyed "service/account" where service is "usher.<backend>".
func withFakeKeychain(t *testing.T) map[string]string {
	t.Helper()
	store := map[string]string{}
	key := func(backend, account string) string { return "usher." + backend + "/" + account }

	origGet, origSet, origDel := keychainGet, keychainSet, keychainDelete
	keychainGet = func(backend, account string) (string, error) {
		if v, ok := store[key(backend, account)]; ok {
			return v, nil
		}
		return "", keychain.ErrNotFound
	}
	keychainSet = func(backend, account, secret string) error {
		store[key(backend, account)] = secret
		return nil
	}
	keychainDelete = func(backend, account string) error {
		delete(store, key(backend, account))
		return nil
	}
	t.Cleanup(func() {
		keychainGet, keychainSet, keychainDelete = origGet, origSet, origDel
	})
	return store
}

// seedBackend writes a single backend straight into an isolated config so the
// rename tests don't depend on backendAdd's handshake probe.
func seedBackend(t *testing.T, dir string, be config.Backend, makeDefault bool) {
	t.Helper()
	cfg, err := config.Load(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	cfg.Add(be, makeDefault)
	if err := cfg.Save(filepath.Join(dir, "config.json")); err != nil {
		t.Fatalf("Save: %v", err)
	}
}

// TestBackendRenameRoundTrip: seeding a backend then renaming it leaves NEW in
// the config, OLD gone, and every other field (transport/auth/command/default)
// carried over unchanged.
func TestBackendRenameRoundTrip(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)
	withFakeKeychain(t)

	seedBackend(t, dir, config.Backend{
		Name:      "old",
		Transport: "stdio",
		Command:   []string{"/usr/bin/true"},
		Auth:      "inherit",
	}, true)

	if err := backendRename([]string{"old", "new"}); err != nil {
		t.Fatalf("backendRename(old new) = %v, want nil", err)
	}

	cfg, err := config.Load(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if be := cfg.ResolveBackend("old"); be != nil {
		t.Errorf("backend %q still present after rename: %+v", "old", be)
	}
	be := cfg.ResolveBackend("new")
	if be == nil {
		t.Fatal("backend \"new\" missing after rename")
	}
	if be.Transport != "stdio" || be.Auth != "inherit" || !be.Default {
		t.Errorf("renamed backend lost fields: %+v", be)
	}
	if len(be.Command) != 1 || be.Command[0] != "/usr/bin/true" {
		t.Errorf("renamed backend command = %v, want [/usr/bin/true]", be.Command)
	}
}

// TestBackendRenameMigratesKeychain: an auth=env backend's secrets move from
// usher.<old> to usher.<new>, and the old items are deleted.
func TestBackendRenameMigratesKeychain(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)
	store := withFakeKeychain(t)

	seedBackend(t, dir, config.Backend{
		Name:      "svc",
		Transport: "stdio",
		Command:   []string{"/usr/bin/true"},
		Auth:      "env",
		EnvKeys:   []string{"API_KEY", "TOKEN"},
	}, false)
	store["usher.svc/API_KEY"] = "sk-1"
	store["usher.svc/TOKEN"] = "tok-2"

	if err := backendRename([]string{"svc", "renamed"}); err != nil {
		t.Fatalf("backendRename(svc renamed) = %v, want nil", err)
	}

	if _, ok := store["usher.svc/API_KEY"]; ok {
		t.Error("old keychain item usher.svc/API_KEY not deleted")
	}
	if _, ok := store["usher.svc/TOKEN"]; ok {
		t.Error("old keychain item usher.svc/TOKEN not deleted")
	}
	if store["usher.renamed/API_KEY"] != "sk-1" {
		t.Errorf("usher.renamed/API_KEY = %q, want sk-1", store["usher.renamed/API_KEY"])
	}
	if store["usher.renamed/TOKEN"] != "tok-2" {
		t.Errorf("usher.renamed/TOKEN = %q, want tok-2", store["usher.renamed/TOKEN"])
	}
}

// TestBackendRenameKeychainReadFailureContinues: a key whose secret can't be
// read is skipped with a warning, but the rename (config + the readable keys)
// still completes.
func TestBackendRenameKeychainReadFailureContinues(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)
	store := withFakeKeychain(t)

	seedBackend(t, dir, config.Backend{
		Name:      "svc",
		Transport: "stdio",
		Command:   []string{"/usr/bin/true"},
		Auth:      "env",
		EnvKeys:   []string{"GOOD", "MISSING"},
	}, false)
	store["usher.svc/GOOD"] = "ok"
	// MISSING intentionally absent: its read fails and must be skipped.

	if err := backendRename([]string{"svc", "renamed"}); err != nil {
		t.Fatalf("backendRename with a missing key = %v, want nil (read failure must not abort)", err)
	}

	cfg, err := config.Load(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ResolveBackend("renamed") == nil {
		t.Error("rename did not complete despite a single key read failure")
	}
	if store["usher.renamed/GOOD"] != "ok" {
		t.Errorf("readable key not migrated: usher.renamed/GOOD = %q", store["usher.renamed/GOOD"])
	}
}

// TestBackendRenameErrors covers the guard rails: a non-existent OLD, a NEW that
// already exists, identical names, and bad arity all error and leave config alone.
func TestBackendRenameErrors(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)
	withFakeKeychain(t)

	seedBackend(t, dir, config.Backend{Name: "a", Transport: "stdio", Command: []string{"x"}, Auth: "inherit"}, true)
	seedBackend(t, dir, config.Backend{Name: "b", Transport: "stdio", Command: []string{"y"}, Auth: "inherit"}, false)

	cases := []struct {
		name string
		args []string
	}{
		{"absent old", []string{"ghost", "z"}},
		{"new already exists", []string{"a", "b"}},
		{"identical names", []string{"a", "a"}},
		{"missing new arg", []string{"a"}},
		{"empty new", []string{"a", ""}},
		{"no args", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := backendRename(tc.args); err == nil {
				t.Fatalf("backendRename(%v) = nil, want error", tc.args)
			}
		})
	}

	// The original backends must be untouched by any rejected rename.
	cfg, err := config.Load(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ResolveBackend("a") == nil || cfg.ResolveBackend("b") == nil {
		t.Error("a rejected rename mutated existing backends")
	}
}
