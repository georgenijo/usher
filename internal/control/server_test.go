package control

// server_test.go drives the control plane with net/http/httptest (no real socket
// for the unit tests, plus one real Listen to prove the loopback bind). It uses a
// re-exec of the test binary as a minimal MCP server (fakeControlBackendMain, via
// TestMain) so a POST start genuinely transitions a backend through the
// supervisor's state machine, not just a stub.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/georgenijo/usher/internal/broker"
	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/mcp"
)

// TestMain routes a re-exec with USHER_CONTROL_FAKE set into a tiny MCP server so
// the supervisor can actually bring a backend live in these tests; otherwise it
// runs the suite normally.
func TestMain(m *testing.M) {
	if os.Getenv("USHER_CONTROL_FAKE") != "" {
		fakeControlBackendMain()
		return
	}
	os.Exit(m.Run())
}

// fakeControlBackendMain is a minimal MCP server (initialize + tools/list),
// enough for the supervisor's one-time handshake to succeed so the backend goes
// live. It exits on EOF (the supervisor's Stop half-closes stdin).
func fakeControlBackendMain() {
	tools := strings.Split(os.Getenv("USHER_CONTROL_TOOLS"), ",")
	conn := mcp.NewConn(os.Stdin, os.Stdout)
	for {
		msg, err := conn.Read()
		if err != nil {
			return
		}
		switch msg.Method {
		case "initialize":
			result, _ := json.Marshal(map[string]any{
				"protocolVersion": "2024-11-05",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "fake", "version": "9.9.9"},
			})
			_ = conn.Write(&mcp.Message{JSONRPC: "2.0", ID: msg.ID, Result: result})
		case "notifications/initialized":
			// no reply
		case "tools/list":
			var objs []map[string]any
			for _, tname := range tools {
				if tname == "" {
					continue
				}
				objs = append(objs, map[string]any{"name": tname, "inputSchema": map[string]any{"type": "object"}})
			}
			result, _ := json.Marshal(map[string]any{"tools": objs})
			_ = conn.Write(&mcp.Message{JSONRPC: "2.0", ID: msg.ID, Result: result})
		}
	}
}

// fakeBackendCmd builds an argv that re-execs THIS test binary as the fake MCP
// server, advertising the given tools.
func fakeBackendCmd(t *testing.T, tools []string) []string {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	script := "USHER_CONTROL_FAKE=1 USHER_CONTROL_TOOLS=" + strings.Join(tools, ",") + ` exec "$0"`
	return []string{"/bin/sh", "-c", script, self}
}

// testServer builds a control Server over a supervisor wired to one fake backend
// named "fake", returning the server, the bus (so a test can publish events), and
// a cancel that tears the pool down.
func testServer(t *testing.T, tools []string) (*Server, *broker.Hub, context.CancelFunc) {
	t.Helper()
	cfg := &config.Config{
		Backends: []config.Backend{{
			Name:      "fake",
			Transport: "stdio",
			Command:   fakeBackendCmd(t, tools),
			Default:   true,
		}},
	}
	bus := broker.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	sv := broker.NewSupervisor(ctx, cfg, bus)
	srv := New(bus, sv, cfg)
	// Start the registry watcher so /api/connections reflects published events.
	go srv.reg.Watch(ctx, bus)
	return srv, bus, func() {
		sv.StopAll()
		cancel()
	}
}

// doReq runs one request against the server's handler and returns the recorder.
func doReq(t *testing.T, srv *Server, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

// TestListen_BindsLoopback proves the real listener binds 127.0.0.1 and never a
// routable address.
func TestListen_BindsLoopback(t *testing.T) {
	srv := New(broker.NewHub(), nil, nil)
	srv.SetAddr("127.0.0.1:0") // :0 → an ephemeral loopback port
	ln, err := srv.Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()
	if got := ln.Addr().String(); !strings.HasPrefix(got, "127.0.0.1:") {
		t.Fatalf("listener bound %q, want a 127.0.0.1 address", got)
	}
}

// TestListen_RejectsNonLoopback proves a non-loopback override fails closed
// rather than exposing the API on a routable interface.
func TestListen_RejectsNonLoopback(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:7187", ":7187", "192.168.1.10:7187", "8.8.8.8:80"} {
		srv := New(broker.NewHub(), nil, nil)
		srv.SetAddr(addr)
		if _, err := srv.Listen(); err == nil {
			t.Fatalf("Listen(%q) succeeded; want a non-loopback rejection", addr)
		}
	}
}

// TestBackends_ReflectsSupervisorState verifies GET /api/backends mirrors the
// pool: stopped before any start, live after.
func TestBackends_ReflectsSupervisorState(t *testing.T) {
	srv, _, cleanup := testServer(t, []string{"click", "type_text"})
	defer cleanup()

	rec := doReq(t, srv, "GET", "/api/backends")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/backends status = %d, want 200", rec.Code)
	}
	var list []broker.BackendStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode backends: %v (body=%s)", err, rec.Body.String())
	}
	if len(list) != 1 || list[0].Name != "fake" {
		t.Fatalf("backends = %+v, want one named fake", list)
	}
	if list[0].State != "stopped" {
		t.Fatalf("pre-start state = %q, want stopped", list[0].State)
	}

	// Bring it live via the supervisor directly, then re-list.
	if err := srv.sv.Start("fake"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	rec = doReq(t, srv, "GET", "/api/backends")
	_ = json.Unmarshal(rec.Body.Bytes(), &list)
	if list[0].State != "live" {
		t.Fatalf("post-start state = %q, want live", list[0].State)
	}
	if list[0].ToolCount != 2 {
		t.Fatalf("toolCount = %d, want 2", list[0].ToolCount)
	}
}

// TestPostStart_TransitionsState is the management path: a POST start drives the
// supervisor and the response carries the NEW live state.
func TestPostStart_TransitionsState(t *testing.T) {
	srv, _, cleanup := testServer(t, []string{"click"})
	defer cleanup()

	rec := doReq(t, srv, "POST", "/api/backends/fake/start")
	if rec.Code != http.StatusOK {
		t.Fatalf("POST start status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	var st broker.BackendStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatalf("decode start response: %v (body=%s)", err, rec.Body.String())
	}
	if st.State != "live" {
		t.Fatalf("POST start returned state %q, want live", st.State)
	}

	// And stop transitions it back.
	rec = doReq(t, srv, "POST", "/api/backends/fake/stop")
	if rec.Code != http.StatusOK {
		t.Fatalf("POST stop status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &st)
	if st.State != "stopped" {
		t.Fatalf("POST stop returned state %q, want stopped", st.State)
	}
}

// TestPostRestart_StaysLive verifies a restart returns to live.
func TestPostRestart_StaysLive(t *testing.T) {
	srv, _, cleanup := testServer(t, []string{"click"})
	defer cleanup()

	if rec := doReq(t, srv, "POST", "/api/backends/fake/start"); rec.Code != http.StatusOK {
		t.Fatalf("start: %d %s", rec.Code, rec.Body.String())
	}
	rec := doReq(t, srv, "POST", "/api/backends/fake/restart")
	if rec.Code != http.StatusOK {
		t.Fatalf("POST restart status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	var st broker.BackendStatus
	_ = json.Unmarshal(rec.Body.Bytes(), &st)
	if st.State != "live" {
		t.Fatalf("post-restart state = %q, want live", st.State)
	}
}

// TestPostUnknownBackend_404 verifies a management POST for an unconfigured
// backend is a 404 JSON error, not a 500 or a silent success.
func TestPostUnknownBackend_404(t *testing.T) {
	srv, _, cleanup := testServer(t, []string{"click"})
	defer cleanup()

	rec := doReq(t, srv, "POST", "/api/backends/nope/start")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("POST start on unknown backend status = %d, want 404", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode 404 body: %v (body=%s)", err, rec.Body.String())
	}
	if body["error"] == "" {
		t.Fatalf("404 body carries no error message: %s", rec.Body.String())
	}
}

// TestConnections_ReflectsEvents verifies the connection registry, fed by the
// event bus, surfaces an opened connection and drops it on close.
func TestConnections_ReflectsEvents(t *testing.T) {
	srv, bus, cleanup := testServer(t, []string{"click"})
	defer cleanup()

	// The registry's Watch subscribes asynchronously; the Hub is drop-oldest, so an
	// event published before the subscription exists is lost. Wait for the
	// subscriber to be live before emitting so the test is deterministic.
	waitFor(t, func() bool { return bus.SubscriberCount() >= 1 })

	bus.Emit(broker.ConnOpenEvent{TS: time.Now(), ConnID: "abc123", PID: 4242, Backend: "fake"})
	waitFor(t, func() bool {
		rec := doReq(t, srv, "GET", "/api/connections")
		var list []ConnInfo
		_ = json.Unmarshal(rec.Body.Bytes(), &list)
		return len(list) == 1 && list[0].ConnID == "abc123" && list[0].AgentPID == 4242
	})

	bus.Emit(broker.ConnCloseEvent{TS: time.Now(), ConnID: "abc123", Reason: "client-eof"})
	waitFor(t, func() bool {
		rec := doReq(t, srv, "GET", "/api/connections")
		var list []ConnInfo
		_ = json.Unmarshal(rec.Body.Bytes(), &list)
		return len(list) == 0
	})
}

// TestConfig_ServesSnapshot verifies GET /api/config returns the config snapshot.
func TestConfig_ServesSnapshot(t *testing.T) {
	srv, _, cleanup := testServer(t, []string{"click"})
	defer cleanup()

	rec := doReq(t, srv, "GET", "/api/config")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/config status = %d, want 200", rec.Code)
	}
	var cfg config.Config
	if err := json.Unmarshal(rec.Body.Bytes(), &cfg); err != nil {
		t.Fatalf("decode config: %v (body=%s)", err, rec.Body.String())
	}
	if len(cfg.Backends) != 1 || cfg.Backends[0].Name != "fake" {
		t.Fatalf("config backends = %+v, want one named fake", cfg.Backends)
	}
}

// TestIndex_ServesUI verifies GET / returns the embedded HTML shell.
func TestIndex_ServesUI(t *testing.T) {
	srv, _, cleanup := testServer(t, []string{"click"})
	defer cleanup()

	rec := doReq(t, srv, "GET", "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("GET / content-type = %q, want text/html", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "EventSource") {
		t.Fatal("served UI does not reference EventSource; embed may be wrong")
	}
	// The DONE criteria require a visible loopback-only note in the page.
	if !strings.Contains(strings.ToLower(body), "loopback") {
		t.Fatal("served UI is missing the loopback-only note")
	}
	// And it must be self-contained: no external CDN/script/style references, so
	// the dashboard works with zero network access beyond the daemon itself.
	for _, ref := range []string{"http://", "https://", "//cdn", "src=\"http", "href=\"http"} {
		if strings.Contains(body, ref) {
			t.Fatalf("served UI references an external asset (%q); must be self-contained", ref)
		}
	}
}

// waitFor polls cond up to ~2s, failing the test if it never holds. Used for the
// event-driven registry, which updates asynchronously off the bus.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within 2s")
}
