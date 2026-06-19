package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/mcp"
	"github.com/georgenijo/usher/internal/mcpserver"
)

// The cmd/usher probe tests spawn a backend child process. Rather than depend on
// the real cua-driver, TestMain re-execs this test binary as a minimal MCP
// server when USHER_FAKE_BACKEND is set (the same pattern the broker package
// uses). USHER_FAKE_MODE selects the child's behaviour so one helper covers both
// a well-behaved backend and a broken one.
func TestMain(m *testing.M) {
	if os.Getenv("USHER_FAKE_BACKEND") != "" {
		fakeBackendMain()
		return
	}
	os.Exit(m.Run())
}

// fakeBackendMain is a minimal MCP server keyed by USHER_FAKE_MODE:
//
//	ok            answer initialize with a valid result (probe succeeds)
//	noresult      answer initialize with an empty result (probe fails: not MCP)
//	error         answer initialize with a JSON-RPC error (probe fails)
//	silent        read but never reply (probe fails on the context deadline)
//	chatty        like "ok" but emits notifications/tools/list_changed before each
//	              reply (server-everything's behaviour) — the prober must skip the
//	              notification and still find the matching response
//
// A child with no recognized mode falls through to "ok".
func fakeBackendMain() {
	mode := os.Getenv("USHER_FAKE_MODE")
	// "mcpserver" runs the REAL bundled server (echo/add/now) so the selftest
	// exercises the genuine backend end to end rather than the fake handshake
	// below. It drives its own read loop over stdin/stdout, so branch before ours.
	if mode == "mcpserver" {
		_ = mcpserver.Run(os.Stdin, os.Stdout)
		return
	}
	conn := mcp.NewConn(os.Stdin, os.Stdout)
	for {
		msg, err := conn.Read()
		if err != nil {
			return // EOF / half-close
		}
		// A chatty backend slips a server-initiated notification in ahead of each
		// of its responses. A prober that grabs the first message would mistake it
		// for the response (regression guard for commit 7524818).
		if mode == "chatty" && (msg.Method == "initialize" || msg.Method == "tools/list") {
			_ = conn.Write(&mcp.Message{JSONRPC: "2.0", Method: "notifications/tools/list_changed"})
		}
		if msg.Method == "tools/list" {
			// doctor counts these; a well-behaved "ok" backend advertises two
			// tools. Other modes never get this far (they fail at initialize).
			result, _ := json.Marshal(map[string]any{
				"tools": []map[string]any{
					{"name": "echo"},
					{"name": "add"},
				},
			})
			_ = conn.Write(&mcp.Message{JSONRPC: "2.0", ID: msg.ID, Result: result})
			continue
		}
		if msg.Method != "initialize" {
			continue // ignore notifications/initialized etc.
		}
		switch mode {
		case "silent":
			// Read the initialize but never answer; the prober's context deadline
			// must fire and kill us.
			continue
		case "error":
			errObj, _ := json.Marshal(map[string]any{"code": -32000, "message": "nope"})
			_ = conn.Write(&mcp.Message{JSONRPC: "2.0", ID: msg.ID, Error: errObj})
		case "noresult":
			_ = conn.Write(&mcp.Message{JSONRPC: "2.0", ID: msg.ID, Result: json.RawMessage(``)})
		default: // "ok"
			result, _ := json.Marshal(map[string]any{
				"protocolVersion": "2024-11-05",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "fake", "version": "9.9.9"},
			})
			_ = conn.Write(&mcp.Message{JSONRPC: "2.0", ID: msg.ID, Result: result})
		}
	}
}

// fakeBackendCommand returns an argv that re-execs this test binary as the fake
// MCP backend in the given mode. The mode + the re-exec flag are carried as
// inline env on a /bin/sh wrapper so they don't pollute the test process. The
// path to re-exec is carried in USHER_FAKE_BIN rather than relying on POSIX
// argument-zero ($0) semantics, making the intent explicit and shell-portable.
func fakeBackendCommand(t *testing.T, mode string) []string {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	script := "USHER_FAKE_BIN='" + self + "'; " +
		"USHER_FAKE_BACKEND=1 USHER_FAKE_MODE='" + mode + "' " +
		`exec "$USHER_FAKE_BIN"`
	return []string{"/bin/sh", "-c", script}
}

// TestProbeBackend table-drives probeBackend against the fake backend's modes.
func TestProbeBackend(t *testing.T) {
	cases := []struct {
		name    string
		mode    string
		command []string
		wantErr bool
	}{
		{"valid mcp server", "ok", nil, false},
		{"initialize error", "error", nil, true},
		{"empty result", "noresult", nil, true},
		{"silent backend hits deadline", "silent", nil, true},
		{"command exits immediately", "", []string{"/usr/bin/false"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := tc.command
			if cmd == nil {
				cmd = fakeBackendCommand(t, tc.mode)
			}
			be := &config.Backend{Name: "fake", Transport: "stdio", Command: cmd, Auth: "inherit"}

			// "silent" needs a short, real deadline; the others resolve fast.
			timeout := 8 * time.Second
			if tc.mode == "silent" {
				timeout = 1500 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			err := probeBackend(ctx, be, nil)
			if tc.wantErr && err == nil {
				t.Fatalf("probeBackend(%s) = nil, want error", tc.name)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("probeBackend(%s) = %v, want nil", tc.name, err)
			}
		})
	}
}

// TestProbeBackendDetail confirms the shared probe engine measures a non-zero
// initialize latency and counts the tools an "ok" backend advertises.
func TestProbeBackendDetail(t *testing.T) {
	be := &config.Backend{Name: "fake", Transport: "stdio", Command: fakeBackendCommand(t, "ok"), Auth: "inherit"}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	res, err := probeBackendDetail(ctx, be, nil)
	if err != nil {
		t.Fatalf("probeBackendDetail(ok) = %v, want nil", err)
	}
	if res.Latency <= 0 {
		t.Errorf("latency = %v, want > 0", res.Latency)
	}
	if res.Tools != 2 {
		t.Errorf("tools = %d, want 2", res.Tools)
	}
}

// TestProbeBackendDetailChatty guards the commit-7524818 regression: a backend
// that emits a server-initiated notification (notifications/tools/list_changed)
// before each reply must not have that notification mistaken for the initialize
// or tools/list response. The probe must skip past it, succeed, and still count
// the two advertised tools.
func TestProbeBackendDetailChatty(t *testing.T) {
	be := &config.Backend{Name: "fake", Transport: "stdio", Command: fakeBackendCommand(t, "chatty"), Auth: "inherit"}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	res, err := probeBackendDetail(ctx, be, nil)
	if err != nil {
		t.Fatalf("probeBackendDetail(chatty) = %v, want nil", err)
	}
	if res.Tools != 2 {
		t.Errorf("tools = %d, want 2 (notification must be skipped, not counted as response)", res.Tools)
	}
}

// TestCmdDoctor table-drives cmdDoctor over an isolated state dir. With only a
// healthy backend it returns nil; once a backend that can't speak MCP is also
// registered, doctor must return an error so the process exits non-zero.
func TestCmdDoctor(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)

	cfg := &config.Config{Backends: []config.Backend{
		{Name: "good", Transport: "stdio", Command: fakeBackendCommand(t, "ok"), Auth: "inherit"},
	}}
	if err := cfg.Save(config.DefaultPath()); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// All healthy -> nil (exit 0).
	if err := cmdDoctor([]string{"--timeout", "8s"}); err != nil {
		t.Fatalf("cmdDoctor(all healthy) = %v, want nil", err)
	}

	// Add a backend that exits immediately (speaks no MCP) -> doctor must fail.
	cfg.Backends = append(cfg.Backends, config.Backend{
		Name: "bad", Transport: "stdio", Command: []string{"/usr/bin/false"}, Auth: "inherit",
	})
	if err := cfg.Save(config.DefaultPath()); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := cmdDoctor([]string{"--timeout", "8s"}); err == nil {
		t.Fatal("cmdDoctor(one failing) = nil, want error (must exit non-zero)")
	}
}

// TestBackendAddHandshakeProbe drives backendAdd end-to-end against an isolated
// state dir, enforcing the done criterion "refuse to register if the handshake
// fails": a valid backend registers with a successful probe (no error, persisted);
// a backend that cannot speak MCP is REJECTED — backendAdd returns an error and
// nothing is written to config.json.
func TestBackendAddHandshakeProbe(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)

	// 1. A valid MCP backend: registers, probe ok, no error.
	okCmd := fakeBackendCommand(t, "ok")
	args := append([]string{"good", "--auth", "inherit", "--"}, okCmd...)
	if err := backendAdd(args); err != nil {
		t.Fatalf("backendAdd(good) = %v, want nil", err)
	}

	// 2. A backend that exits immediately (`false` speaks no MCP): the handshake
	//    fails, so backendAdd MUST return an error and MUST NOT persist it.
	args = []string{"bad", "--auth", "inherit", "--", "/usr/bin/false"}
	if err := backendAdd(args); err == nil {
		t.Fatal("backendAdd(bad) = nil, want error (handshake failure must refuse registration)")
	}

	cfg, err := config.Load(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if be := cfg.ResolveBackend("good"); be == nil {
		t.Error("backend \"good\" not persisted")
	}
	if bad := cfg.ResolveBackend("bad"); bad != nil {
		t.Errorf("backend \"bad\" was persisted despite a failed handshake: %+v", bad)
	}
}

// TestConfigInit drives the `usher config init` command through its CLI entry
// against an isolated state dir: a first init writes a loadable config, a second
// without --force is refused, and --force overwrites.
func TestConfigInit(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)
	path := filepath.Join(dir, "config.json")

	// First init writes a config that round-trips through the loader.
	if err := configInit(nil); err != nil {
		t.Fatalf("configInit(first) = %v, want nil", err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load after init: %v", err)
	}
	if len(cfg.Backends) != 0 {
		t.Errorf("scaffolded Backends = %v, want empty", cfg.Backends)
	}

	// Second init without --force must refuse.
	if err := configInit(nil); err == nil {
		t.Fatal("configInit(second, no --force) = nil, want refusal")
	}

	// --force overwrites without error.
	if err := configInit([]string{"--force"}); err != nil {
		t.Fatalf("configInit(--force) = %v, want nil", err)
	}
}

// TestBackendListMarksDisabled: `usher backend list` annotates a disabled backend
// with a trailing "(disabled)" marker while an enabled one is shown bare.
func TestBackendListMarksDisabled(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)

	cfg := &config.Config{Backends: []config.Backend{
		{Name: "cua", Transport: "stdio", Command: []string{"cua-driver"}, Auth: "inherit", Default: true},
		{Name: "fs", Transport: "stdio", Command: []string{"fs-mcp"}, Auth: "none", Disabled: true},
	}}
	if err := cfg.Save(config.DefaultPath()); err != nil {
		t.Fatalf("Save: %v", err)
	}

	out := captureStdout(t, func() {
		if err := backendList(nil); err != nil {
			t.Fatalf("backendList: %v", err)
		}
	})

	if !strings.Contains(out, "fs (disabled)") {
		t.Errorf("backend list missing disabled marker for fs:\n%s", out)
	}
	if strings.Contains(out, "cua (disabled)") {
		t.Errorf("backend list wrongly marked enabled cua as disabled:\n%s", out)
	}
}

// TestBackendRemoveRoundTrip drives add-then-remove against an isolated state
// dir: a registered backend is removed and no longer resolves, while a sibling
// backend is left untouched and the config file persists the change.
func TestBackendRemoveRoundTrip(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)
	path := filepath.Join(dir, "config.json")

	okCmd := fakeBackendCommand(t, "ok")
	if err := backendAdd(append([]string{"alpha", "--auth", "inherit", "--"}, okCmd...)); err != nil {
		t.Fatalf("backendAdd(alpha) = %v", err)
	}
	if err := backendAdd(append([]string{"beta", "--auth", "inherit", "--"}, okCmd...)); err != nil {
		t.Fatalf("backendAdd(beta) = %v", err)
	}

	if err := backendRemove([]string{"alpha"}); err != nil {
		t.Fatalf("backendRemove(alpha) = %v, want nil", err)
	}

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if be := cfg.ResolveBackend("alpha"); be != nil {
		t.Errorf("backend alpha still present after remove: %+v", be)
	}
	if be := cfg.ResolveBackend("beta"); be == nil {
		t.Error("remove dropped the unrelated backend beta")
	}
}

// TestBackendRemoveAbsent: removing a backend that was never registered is a
// clear error and writes nothing.
func TestBackendRemoveAbsent(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)

	if err := backendRemove([]string{"ghost"}); err == nil {
		t.Fatal("backendRemove(ghost) = nil, want error (absent backend)")
	}
	// No usage variants should write a config file.
	if err := backendRemove([]string{}); err == nil {
		t.Fatal("backendRemove() = nil, want usage error")
	}
}

// TestBackendRemovePurgesEnvKeys: removing an auth=env backend purges each of
// its namespaced Keychain secrets exactly once, via the keychainDelete
// indirection (no real Keychain touched). The --yes flag is accepted.
func TestBackendRemovePurgesEnvKeys(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)
	path := filepath.Join(dir, "config.json")

	// Seed a config with an auth=env backend directly (skip the secret prompt
	// that backendAdd would require).
	cfg := &config.Config{Backends: []config.Backend{
		{Name: "db", Transport: "stdio", Command: []string{"db-mcp"}, Auth: "env", EnvKeys: []string{"DB_USER", "DB_PASS"}},
		{Name: "fs", Transport: "stdio", Command: []string{"fs-mcp"}, Auth: "none"},
	}}
	if err := cfg.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	var deleted []string
	orig := keychainDelete
	keychainDelete = func(backend, account string) error {
		deleted = append(deleted, backend+"/"+account)
		return nil
	}
	defer func() { keychainDelete = orig }()

	if err := backendRemove([]string{"--yes", "db"}); err != nil {
		t.Fatalf("backendRemove(--yes db) = %v, want nil", err)
	}

	want := []string{"db/DB_USER", "db/DB_PASS"}
	if !reflect.DeepEqual(deleted, want) {
		t.Errorf("keychain deletes = %v, want %v", deleted, want)
	}

	// Removing the auth=none sibling must NOT touch the Keychain.
	deleted = nil
	if err := backendRemove([]string{"fs"}); err != nil {
		t.Fatalf("backendRemove(fs) = %v", err)
	}
	if len(deleted) != 0 {
		t.Errorf("auth=none remove purged Keychain keys: %v", deleted)
	}
}

// TestBackendExportImportRoundTrip exports a fleet from one state dir and imports
// it into a fresh, empty state dir, proving the portable JSON round-trips a
// backend's name/transport/auth/command/env-key-NAMES and that import
// handshake-validates each entry before persisting it.
func TestBackendExportImportRoundTrip(t *testing.T) {
	// Source machine: register two valid MCP backends.
	srcDir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", srcDir)
	okCmd := fakeBackendCommand(t, "ok")
	if err := backendAdd(append([]string{"alpha", "--auth", "inherit", "--"}, okCmd...)); err != nil {
		t.Fatalf("backendAdd(alpha) = %v", err)
	}
	if err := backendAdd(append([]string{"beta", "--auth", "inherit", "--"}, okCmd...)); err != nil {
		t.Fatalf("backendAdd(beta) = %v", err)
	}

	// Export to a file. (--out keeps stdout clean and gives us bytes to import.)
	exportFile := filepath.Join(t.TempDir(), "backends.json")
	if err := backendExport([]string{"--out", exportFile}); err != nil {
		t.Fatalf("backendExport = %v", err)
	}

	// Sanity: the export is a JSON array carrying both backends and NO secret
	// fields (config.Backend has none, so this is structural — assert anyway).
	exported, err := os.ReadFile(exportFile)
	if err != nil {
		t.Fatal(err)
	}
	var dump []config.Backend
	if err := json.Unmarshal(exported, &dump); err != nil {
		t.Fatalf("export is not a JSON array of backends: %v", err)
	}
	seen := map[string]bool{}
	for _, be := range dump {
		seen[be.Name] = true
	}
	if !seen["alpha"] || !seen["beta"] {
		t.Fatalf("export missing added backends; got %v", seen)
	}

	// Destination machine: a fresh, empty state dir.
	dstDir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dstDir)
	if err := backendImport([]string{exportFile}); err != nil {
		t.Fatalf("backendImport = %v", err)
	}

	cfg, err := config.Load(filepath.Join(dstDir, "config.json"))
	if err != nil {
		t.Fatalf("Load(dst): %v", err)
	}
	for _, name := range []string{"alpha", "beta"} {
		be := cfg.ResolveBackend(name)
		if be == nil {
			t.Fatalf("backend %q not imported", name)
		}
		if be.Transport != "stdio" || be.Auth != "inherit" {
			t.Errorf("backend %q round-trip mismatch: %+v", name, be)
		}
		if len(be.Command) == 0 {
			t.Errorf("backend %q lost its command on round-trip", name)
		}
	}
}

// TestBackendImportRejectsInvalid proves import refuses a backend that cannot
// speak MCP: the bad entry is handshake-validated, fails, and is NOT persisted,
// while a valid sibling entry in the same file still imports.
func TestBackendImportRejectsInvalid(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)

	okCmd := fakeBackendCommand(t, "ok")
	manifest := []config.Backend{
		{Name: "good", Transport: "stdio", Auth: "inherit", Command: okCmd},
		// `/usr/bin/false` exits immediately and speaks no MCP: handshake must fail.
		{Name: "bad", Transport: "stdio", Auth: "inherit", Command: []string{"/usr/bin/false"}},
	}
	file := filepath.Join(t.TempDir(), "manifest.json")
	b, _ := json.MarshalIndent(manifest, "", "  ")
	if err := os.WriteFile(file, b, 0o644); err != nil {
		t.Fatal(err)
	}

	// Import returns a non-nil error because one entry failed its handshake.
	if err := backendImport([]string{file}); err == nil {
		t.Fatal("backendImport = nil, want error (an invalid backend must fail import)")
	}

	cfg, err := config.Load(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ResolveBackend("good") == nil {
		t.Error("valid backend \"good\" was not imported despite passing its handshake")
	}
	if bad := cfg.ResolveBackend("bad"); bad != nil {
		t.Errorf("invalid backend \"bad\" was persisted despite a failed handshake: %+v", bad)
	}
}

// TestBackendImportCollision proves a name that already exists is skipped by
// default and only overwritten with --force.
func TestBackendImportCollision(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)

	okCmd := fakeBackendCommand(t, "ok")
	// Pre-register a backend that the manifest will also name.
	if err := backendAdd(append([]string{"dup", "--auth", "inherit", "--"}, okCmd...)); err != nil {
		t.Fatalf("backendAdd(dup) = %v", err)
	}

	// Manifest names "dup" with a DIFFERENT command so we can detect an overwrite.
	manifest := []config.Backend{
		{Name: "dup", Transport: "stdio", Auth: "inherit", Command: []string{"/bin/sh", "-c", "overwritten"}},
	}
	file := filepath.Join(t.TempDir(), "manifest.json")
	b, _ := json.MarshalIndent(manifest, "", "  ")
	if err := os.WriteFile(file, b, 0o644); err != nil {
		t.Fatal(err)
	}

	overwritten := []string{"/bin/sh", "-c", "overwritten"}

	// Without --force: collision is skipped, original command preserved, no error.
	if err := backendImport([]string{file}); err != nil {
		t.Fatalf("backendImport (no force) = %v, want nil (collision is skipped, not fatal)", err)
	}
	cfg, _ := config.Load(filepath.Join(dir, "config.json"))
	if be := cfg.ResolveBackend("dup"); be == nil || reflect.DeepEqual(be.Command, overwritten) {
		t.Errorf("dup was overwritten without --force: %+v", be)
	}

	// With --force: the manifest entry replaces the existing backend. The
	// overwriting command speaks no MCP, so the handshake fails and import returns
	// an error — but that proves --force REACHED the validate step (vs skipping).
	if err := backendImport([]string{"--force", file}); err == nil {
		t.Fatal("backendImport (--force) = nil, want error (overwrite command speaks no MCP)")
	}
	// The failed handshake means the bad overwrite was NOT persisted; the original
	// good backend remains untouched.
	cfg, _ = config.Load(filepath.Join(dir, "config.json"))
	if be := cfg.ResolveBackend("dup"); be == nil || reflect.DeepEqual(be.Command, overwritten) {
		t.Errorf("--force persisted a backend whose handshake failed: %+v", be)
	}
}

// TestBackendExportNoSecrets proves the export carries only env key NAMES, never
// secret values — config.Backend has no secret field, so a marshaled export
// cannot contain a value even for an auth=env backend.
func TestBackendExportNoSecrets(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)

	// Write a config with a single auth=env backend directly (avoids the Keychain
	// prompt that `backend add --auth env` would trigger, and the seeded default).
	cfg := &config.Config{Backends: []config.Backend{{
		Name:      "envbacked",
		Transport: "stdio",
		Auth:      "env",
		Command:   []string{"cmd"},
		EnvKeys:   []string{"ANTHROPIC_API_KEY"},
	}}}
	if err := cfg.Save(filepath.Join(dir, "config.json")); err != nil {
		t.Fatal(err)
	}

	exportFile := filepath.Join(t.TempDir(), "out.json")
	if err := backendExport([]string{"--out", exportFile}); err != nil {
		t.Fatalf("backendExport = %v", err)
	}
	out, err := os.ReadFile(exportFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "ANTHROPIC_API_KEY") {
		t.Error("export dropped the env key NAME, which must be portable")
	}
	// The env key VALUE lives only in the Keychain; config.Backend has no field for
	// it, so a KEY=VALUE pair can never appear in the export.
	if strings.Contains(string(out), "=") {
		t.Errorf("export appears to contain a secret value: %s", out)
	}
}

// TestBackendAddValidation covers the flag/strategy reconciliation that rejects
// bad invocations before any process is spawned or config written.
func TestBackendAddValidation(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)

	cases := []struct {
		name string
		args []string
	}{
		{"http transport stubbed", []string{"x", "--transport", "http", "--", "cmd"}},
		{"unknown transport", []string{"x", "--transport", "carrier-pigeon", "--", "cmd"}},
		{"unknown auth", []string{"x", "--auth", "magic", "--", "cmd"}},
		{"oauth not supported", []string{"x", "--auth", "oauth", "--", "cmd"}},
		{"env without keys", []string{"x", "--auth", "env", "--", "cmd"}},
		{"env-flag without auth-env", []string{"x", "--auth", "inherit", "--env", "K", "--", "cmd"}},
		{"missing separator", []string{"x", "--auth", "inherit"}},
		{"no command after --", []string{"x", "--auth", "inherit", "--"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := backendAdd(tc.args); err == nil {
				t.Fatalf("backendAdd(%v) = nil, want error", tc.args)
			}
		})
	}

	// None of the rejected invocations should have written a config file.
	if _, err := os.Stat(filepath.Join(dir, "config.json")); !os.IsNotExist(err) {
		t.Errorf("config.json exists after only-rejected adds (err=%v); validation should precede Save", err)
	}
}

// TestBackendList_JSON: `usher backend list --json` emits a JSON array whose
// entries carry the expected fields and never leak secret VALUES (only envKeys
// names). The default (no --json) still prints the human table.
func TestBackendList_JSON(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)

	cfg := &config.Config{}
	cfg.Add(config.Backend{
		Name:      "fs",
		Transport: "stdio",
		Auth:      "inherit",
		Command:   []string{"/usr/bin/fs-mcp", "--root", "/tmp"},
	}, true)
	cfg.Add(config.Backend{
		Name:      "api",
		Transport: "stdio",
		Auth:      "env",
		Command:   []string{"/usr/bin/api-mcp"},
		EnvKeys:   []string{"API_TOKEN"},
	}, false)
	if err := cfg.Save(config.DefaultPath()); err != nil {
		t.Fatal(err)
	}

	out := captureStdout(t, func() {
		if err := backendList([]string{"--json"}); err != nil {
			t.Errorf("backendList(--json) = %v, want nil", err)
		}
	})

	var got []backendListJSON
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out)
	}
	if len(got) != 2 {
		t.Fatalf("got %d backends, want 2: %+v", len(got), got)
	}

	fs := got[0]
	if fs.Name != "fs" || fs.Transport != "stdio" || fs.Auth != "inherit" || !fs.Default {
		t.Errorf("fs entry = %+v, want name=fs transport=stdio auth=inherit default=true", fs)
	}
	if len(fs.Command) != 3 || fs.Command[0] != "/usr/bin/fs-mcp" {
		t.Errorf("fs command = %v, want the full argv", fs.Command)
	}

	api := got[1]
	if api.Name != "api" || api.Auth != "env" || api.Default {
		t.Errorf("api entry = %+v, want name=api auth=env default=false", api)
	}
	if len(api.EnvKeys) != 1 || api.EnvKeys[0] != "API_TOKEN" {
		t.Errorf("api envKeys = %v, want [API_TOKEN]", api.EnvKeys)
	}
}

// TestBackendList_JSON_Empty: with a config that has no backends, --json emits an
// empty array (not `null`) so consumers can iterate unconditionally. (Load seeds
// a default backend when NO file exists, so we persist an explicit empty config.)
func TestBackendList_JSON_Empty(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)
	if err := (&config.Config{}).Save(config.DefaultPath()); err != nil {
		t.Fatal(err)
	}

	out := captureStdout(t, func() {
		if err := backendList([]string{"--json"}); err != nil {
			t.Errorf("backendList(--json) = %v, want nil", err)
		}
	})
	var got []backendListJSON
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out)
	}
	if got == nil || len(got) != 0 {
		t.Errorf("got %v, want an empty (non-nil) array", got)
	}
}
