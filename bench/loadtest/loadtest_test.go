package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/georgenijo/usher/internal/backend"
	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/mcp"
	"github.com/georgenijo/usher/internal/procstat"
)

// TestMain re-execs the test binary as a fake cua-driver child when
// USHER_FAKE_BACKEND is set (the same idiom internal/broker uses), so armDirect
// can spawn real child processes without the real cua-driver binary or
// screen-recording permission. The fake answers a full MCP handshake and
// get_screen_size, then exits on EOF.
func TestMain(m *testing.M) {
	if os.Getenv("USHER_FAKE_BACKEND") != "" {
		fakeBackendMain()
		return
	}
	os.Exit(m.Run())
}

// fakeBackendMain is a minimal MCP server: initialize -> result, an empty
// tools/list, and get_screen_size echoes a fixed size. It stays alive (does not
// exit after one call) so the harness can sample its pid while it is connected,
// and exits on EOF (the harness Close()ing stdin / killing it).
func fakeBackendMain() {
	conn := mcp.NewConn(os.Stdin, os.Stdout)
	for {
		m, err := conn.Read()
		if err != nil {
			return // EOF / killed: exit
		}
		switch m.Method {
		case "initialize":
			result, _ := json.Marshal(map[string]any{
				"protocolVersion": "2024-11-05",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "fake-cua", "version": "0"},
			})
			_ = conn.Write(&mcp.Message{JSONRPC: "2.0", ID: m.ID, Result: result})
		case "notifications/initialized":
			// no reply to a notification
		case "tools/call":
			result, _ := json.Marshal(map[string]any{
				"content": []any{map[string]any{"type": "text", "text": "width=1920 height=1080"}},
			})
			_ = conn.Write(&mcp.Message{JSONRPC: "2.0", ID: m.ID, Result: result})
		default:
			if len(m.ID) > 0 {
				_ = conn.Write(&mcp.Message{JSONRPC: "2.0", ID: m.ID, Result: json.RawMessage(`{}`)})
			}
		}
	}
}

// fakeConfigPath writes a config.json into an isolated dir whose single default
// backend re-execs THIS test binary as the fake cua-driver child. armDirect loads
// it via --config to spawn real children with no real cua dependency.
func fakeConfigPath(t *testing.T) string {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// Wrap in /bin/sh -c so the child gets USHER_FAKE_BACKEND in its env without
	// polluting the test process's environment.
	script := `USHER_FAKE_BACKEND=1 exec "$0"`
	cfg := &config.Config{Backends: []config.Backend{{
		Name:      "fake-cua",
		Transport: "stdio",
		Command:   []string{"/bin/sh", "-c", script, self},
		Auth:      "inherit",
		Default:   true,
	}}}
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := cfg.Save(path); err != nil {
		t.Fatal(err)
	}
	return path
}

// startFakeChild spawns one fake-cua child from the backend spec and waits for it
// to be ready (Start returns once stdio is wired). The caller Close()s it.
func startFakeChild(t *testing.T, be *config.Backend) *backend.Stdio {
	t.Helper()
	sb := backend.NewStdio(be.Name, be.Command, nil)
	if err := sb.Start(context.Background()); err != nil {
		t.Fatalf("start fake child: %v", err)
	}
	return sb
}

// TestRunSyntheticClient drives a full MCP session against the fake backend over
// a stdio child and asserts a clean initialize -> initialized -> call -> cancel
// with no stream corruption (the fake never errors on a malformed frame, and the
// client returns nil on ctx-cancel).
func TestRunSyntheticClient(t *testing.T) {
	opts := options{
		configPath:  fakeConfigPath(t),
		callEvery:   20 * time.Millisecond,
		sampleEvery: 50 * time.Millisecond,
		duration:    300 * time.Millisecond,
	}
	cfg, err := config.Load(opts.configPath)
	if err != nil {
		t.Fatal(err)
	}
	be := cfg.ResolveBackend("")
	sb := startFakeChild(t, be)
	defer sb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if err := runSyntheticClient(ctx, sb.Conn(), 20*time.Millisecond); err != nil {
		t.Fatalf("runSyntheticClient: %v", err)
	}
}

// TestArmDirectSpawnsAndReaps runs the direct arm with n=2 against the fake
// backend and asserts: exactly 2 distinct backend child pids were sampled, both
// alive during the run, the harness (broker role) was sampled, and — the leak
// guard — every child pid is dead after teardown (armDirect returns no error).
func TestArmDirectSpawnsAndReaps(t *testing.T) {
	opts := options{
		configPath:  fakeConfigPath(t),
		callEvery:   20 * time.Millisecond,
		sampleEvery: 40 * time.Millisecond,
		duration:    250 * time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r, err := armDirect(ctx, opts, 2)
	if err != nil {
		t.Fatalf("armDirect (n=2): %v", err)
	}
	if r.arm != "direct" || r.clients != 2 {
		t.Fatalf("unexpected result arm=%q clients=%d", r.arm, r.clients)
	}

	// Exactly 2 distinct backend pids, all alive at the sampled tick.
	pids := map[int]bool{}
	var brokerSeen bool
	for _, p := range r.procs {
		switch p.Role {
		case string(procstat.RoleBackend):
			if !p.Alive {
				t.Errorf("backend pid %d (%s) not alive during run", p.PID, p.Label)
			}
			pids[p.PID] = true
		case string(procstat.RoleBroker):
			brokerSeen = true
		}
	}
	if len(pids) != 2 {
		t.Errorf("want 2 distinct backend pids, got %d: %v", len(pids), pids)
	}
	if !brokerSeen {
		t.Error("harness (broker role) was not sampled")
	}
	if r.backendCount != 2 {
		t.Errorf("rollup backendCount = %d, want 2", r.backendCount)
	}

	// Leak guard: every spawned child must be dead now (armDirect already swept;
	// re-sweep here to prove it from the test's vantage point).
	var pidList []int
	for pid := range pids {
		pidList = append(pidList, pid)
	}
	if alive := liveChildPIDs(pidList); len(alive) > 0 {
		t.Errorf("leak: child pids still alive after armDirect returned: %v", alive)
	}
}

// TestParseFlags covers defaults, the arm validation, and a few floors.
func TestParseFlags(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		o, err := parseFlags(nil)
		if err != nil {
			t.Fatal(err)
		}
		if o.arm != "both" || o.clients != 15 {
			t.Fatalf("defaults: arm=%q clients=%d, want both/15", o.arm, o.clients)
		}
		if o.callEvery != 500*time.Millisecond {
			t.Fatalf("default call = %v, want 500ms", o.callEvery)
		}
	})
	t.Run("bad arm", func(t *testing.T) {
		if _, err := parseFlags([]string{"--arm", "nope"}); err == nil {
			t.Fatal("expected error for bad --arm")
		}
	})
	t.Run("clients floor", func(t *testing.T) {
		if _, err := parseFlags([]string{"--clients", "0"}); err == nil {
			t.Fatal("expected error for --clients 0")
		}
	})
	t.Run("cua override", func(t *testing.T) {
		o, err := parseFlags([]string{"--cua", "/path/to/cua-driver mcp"})
		if err != nil {
			t.Fatal(err)
		}
		if len(o.cuaCommand) != 2 || o.cuaCommand[0] != "/path/to/cua-driver" {
			t.Fatalf("cua override = %v", o.cuaCommand)
		}
	})
}

// TestReportRollupAndTable checks the per-role rollup sums per-PID rows (never a
// system total), skips dead rows, and that the table + headline render the
// expected substrings.
func TestReportRollupAndTable(t *testing.T) {
	r := &armResult{arm: "direct", clients: 2, procs: []procstat.ProcSample{
		{PID: 1, Role: string(procstat.RoleBroker), Label: "harness", RSSKB: 10240, Alive: true},
		{PID: 2, Role: string(procstat.RoleBackend), Label: "cua#1", RSSKB: 51200, Alive: true},
		{PID: 3, Role: string(procstat.RoleBackend), Label: "cua#2", RSSKB: 51200, Alive: true},
		{PID: 4, Role: string(procstat.RoleBackend), Label: "cua#3", RSSKB: 99999, Alive: false}, // dead: ignored
	}}
	r.rollup()
	if r.backendCount != 2 {
		t.Errorf("backendCount = %d, want 2 (dead row excluded)", r.backendCount)
	}
	if r.backendRSSMB != 100.0 { // 51200+51200 KB = 100 MB
		t.Errorf("backendRSSMB = %.1f, want 100.0", r.backendRSSMB)
	}
	if r.brokerRSSMB != 10.0 {
		t.Errorf("brokerRSSMB = %.1f, want 10.0", r.brokerRSSMB)
	}

	var buf bytes.Buffer
	r.printTable(&buf)
	out := buf.String()
	for _, want := range []string{"per-PID", "cua#1", "DEAD", "backend=100.0 MB (2 children)"} {
		if !strings.Contains(out, want) {
			t.Errorf("table missing %q\n%s", want, out)
		}
	}

	var hb bytes.Buffer
	printHeadline(&hb, &armResult{arm: "broker", clients: 2, backendCount: 1, backendRSSMB: 50.0, clientCount: 2}, r)
	if h := hb.String(); !strings.Contains(h, "broker=<1 child, 50.0 MB>") || !strings.Contains(h, "direct=<2 children, 100.0 MB>") {
		t.Errorf("headline unexpected:\n%s", h)
	}
}
