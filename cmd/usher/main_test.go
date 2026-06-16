package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/mcp"
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
//
// A child with no recognized mode falls through to "ok".
func fakeBackendMain() {
	mode := os.Getenv("USHER_FAKE_MODE")
	conn := mcp.NewConn(os.Stdin, os.Stdout)
	for {
		msg, err := conn.Read()
		if err != nil {
			return // EOF / half-close
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
// inline env on a /bin/sh wrapper so they don't pollute the test process.
func fakeBackendCommand(t *testing.T, mode string) []string {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	script := "USHER_FAKE_BACKEND=1 USHER_FAKE_MODE=" + mode + ` exec "$0"`
	return []string{"/bin/sh", "-c", script, self}
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

// TestBackendAddHandshakeProbe drives backendAdd end-to-end against an isolated
// state dir: a valid backend registers with a successful probe; a backend that
// cannot speak MCP still registers (advisory, non-fatal probe) and is persisted
// to config.json. Neither path returns an error from backendAdd.
func TestBackendAddHandshakeProbe(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)

	// 1. A valid MCP backend: registers, probe ok, no error.
	okCmd := fakeBackendCommand(t, "ok")
	args := append([]string{"good", "--auth", "inherit", "--"}, okCmd...)
	if err := backendAdd(args); err != nil {
		t.Fatalf("backendAdd(good) = %v, want nil", err)
	}

	// 2. A backend that exits immediately (`false` speaks no MCP): probe fails
	//    but registration is non-fatal, so backendAdd still returns nil and the
	//    backend is written to config.
	args = []string{"bad", "--auth", "inherit", "--", "/usr/bin/false"}
	if err := backendAdd(args); err != nil {
		t.Fatalf("backendAdd(bad) = %v, want nil (probe is advisory)", err)
	}

	cfg, err := config.Load(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if be := cfg.ResolveBackend("good"); be == nil {
		t.Error("backend \"good\" not persisted")
	}
	bad := cfg.ResolveBackend("bad")
	if bad == nil {
		t.Fatal("backend \"bad\" not persisted despite advisory probe failure")
	}
	if len(bad.EnvKeys) != 0 {
		t.Errorf("bad.EnvKeys = %v, want none (auth=inherit stores no secrets)", bad.EnvKeys)
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
