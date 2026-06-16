package broker

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/georgenijo/usher/internal/audit"
	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/mcp"
)

// socketTestBroker builds a broker whose single backend is a re-exec of the test
// binary as the fake MCP server (the same fakeBackendMain used by the fanout
// tests). On the daemon (socket) path every accepted connection is MULTIPLEXED
// onto ONE shared, supervised backend child — not a fresh child per connection.
func socketTestBroker(t *testing.T, name string, tools []string) *Broker {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	script := "USHER_FAKE_BACKEND=1 USHER_FAKE_NAME=" + name +
		" USHER_FAKE_TOOLS=" + joinComma(tools) + ` exec "$0"`
	cfg := &config.Config{
		Backends: []config.Backend{{
			Name:      name,
			Transport: "stdio",
			Command:   []string{"/bin/sh", "-c", script, self},
			Default:   true,
		}},
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Discard audit so the socket test does not spew the message log to stderr.
	// Rebuild both pipelines against the discard logger (New wired the audit
	// stages to the original stderr logger).
	b.audit = audit.New(io.Discard)
	b.inbound = NewPipeline(NewGateStage(), NewArbitrateStage(), NewAuditStage(b.audit, Inbound))
	b.outbound = NewPipeline(NewArbitrateStage(), NewTrimStage(), NewAuditStage(b.audit, Outbound))
	return b
}

func joinComma(s []string) string {
	out := ""
	for i, v := range s {
		if i > 0 {
			out += ","
		}
		out += v
	}
	return out
}

// shortSockPath returns a Unix-socket path short enough for macOS's 104-byte
// sun_path limit (t.TempDir's deep path overflows it). It uses os.MkdirTemp
// under the system temp root and registers cleanup.
func shortSockPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "usher")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "u.sock")
}

// TestServeSocket_TwoConcurrentClients is the daemon done criterion under the mux
// model: two clients dialing the daemon's Unix socket are MULTIPLEXED onto ONE
// shared backend child (not a child each), yet each still completes its own MCP
// handshake (initialize + tools/list) with valid, unmixed responses correlated to
// its own request ids. After both finish, the supervisor reports exactly one live
// backend (the shared child). Cancelling the context closes the listener and the
// pool; ServeSocket returns nil.
func TestServeSocket_TwoConcurrentClients(t *testing.T) {
	b := socketTestBroker(t, "fake", []string{"click", "type_text"})

	sockPath := shortSockPath(t)
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srvDone := make(chan error, 1)
	go func() { srvDone <- b.ServeSocket(ctx, "fake", ln) }()

	// Drive one client end-to-end: initialize then tools/list, asserting valid
	// responses correlated to the request ids.
	runClient := func(t *testing.T) {
		conn, err := net.Dial("unix", sockPath)
		if err != nil {
			t.Errorf("dial: %v", err)
			return
		}
		defer conn.Close()
		r := bufio.NewReaderSize(conn, 1<<16)

		// initialize
		if _, err := conn.Write([]byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n")); err != nil {
			t.Errorf("write initialize: %v", err)
			return
		}
		m := readMsg(t, r)
		if m.IDString() != "1" || len(m.Result) == 0 {
			t.Errorf("initialize response unexpected: id=%s result=%s err=%s", m.IDString(), m.Result, m.Error)
			return
		}

		// notifications/initialized (no reply expected)
		if _, err := conn.Write([]byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}` + "\n")); err != nil {
			t.Errorf("write initialized: %v", err)
			return
		}

		// tools/list — must come back with the backend's advertised tools.
		if _, err := conn.Write([]byte(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}` + "\n")); err != nil {
			t.Errorf("write tools/list: %v", err)
			return
		}
		m = readMsg(t, r)
		if m.IDString() != "2" {
			t.Errorf("tools/list response id = %s, want 2", m.IDString())
			return
		}
		var res struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		}
		if err := json.Unmarshal(m.Result, &res); err != nil {
			t.Errorf("tools/list result invalid JSON: %v (%s)", err, m.Result)
			return
		}
		if len(res.Tools) != 2 {
			t.Errorf("tools/list returned %d tools, want 2: %s", len(res.Tools), m.Result)
		}
	}

	// Two clients concurrently. They SHARE one supervised backend child; each keeps
	// its own identity, inflight map, and window-lock ownership, and the mux
	// rewrites ids so neither client sees the other's traffic.
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runClient(t)
		}()
	}
	wg.Wait()

	// The pool holds exactly one backend, and it is the single shared child both
	// clients were multiplexed onto — live with its tools cached.
	if sv := b.Supervisor(); sv == nil {
		t.Fatal("daemon path did not build a supervisor")
	} else {
		snap := sv.Snapshot()
		if len(snap) != 1 {
			t.Fatalf("supervisor snapshot = %d backends, want 1 shared child", len(snap))
		}
		if snap[0].State != "live" {
			t.Fatalf("shared backend state = %q, want live", snap[0].State)
		}
	}

	// Shutdown: cancelling ctx closes the listener; ServeSocket returns nil.
	cancel()
	select {
	case err := <-srvDone:
		if err != nil {
			t.Errorf("ServeSocket returned error on shutdown: %v", err)
		}
	case <-time.After(testDeadline):
		t.Fatal("ServeSocket did not return after ctx cancel")
	}
}

// TestServeSocket_AcceptErrorIsCleanOnClose verifies that closing the listener
// out from under ServeSocket (the shutdown path) returns nil, not a surfaced
// accept error.
func TestServeSocket_AcceptErrorIsCleanOnClose(t *testing.T) {
	b := socketTestBroker(t, "fake", []string{"click"})
	sockPath := shortSockPath(t)
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	srvDone := make(chan error, 1)
	go func() { srvDone <- b.ServeSocket(ctx, "fake", ln) }()

	// Cancel before any connection: the accept loop must unwind cleanly.
	cancel()
	select {
	case err := <-srvDone:
		if err != nil {
			t.Errorf("ServeSocket = %v on clean shutdown, want nil", err)
		}
	case <-time.After(testDeadline):
		t.Fatal("ServeSocket did not return after ctx cancel")
	}
}

// readMsg reads one framed JSON-RPC message from r, failing the test on error.
func readMsg(t *testing.T, r *bufio.Reader) *mcp.Message {
	t.Helper()
	line, err := r.ReadBytes('\n')
	if err != nil && len(line) == 0 {
		t.Fatalf("read: %v", err)
	}
	var m mcp.Message
	if err := json.Unmarshal(line, &m); err != nil {
		t.Fatalf("decode %q: %v", line, err)
	}
	return &m
}
