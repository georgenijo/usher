package broker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// muxConn is one in-test agent connection to the daemon socket. It drives the MCP
// handshake then issues tools/call requests, asserting each response comes back
// under the id it sent (the mux's id-rewrite must be invisible to the client).
type muxConn struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

// dialMux dials the daemon socket and completes the per-client handshake
// (initialize + tools/list), proving each client gets its OWN valid handshake off
// the one shared, already-initialized child.
func dialMux(t *testing.T, sockPath string) *muxConn {
	t.Helper()
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	mc := &muxConn{t: t, conn: conn, r: bufio.NewReaderSize(conn, 1<<16)}
	mc.send(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`)
	if m := mc.recv(); m.IDString() != "1" || len(m.Result) == 0 {
		t.Fatalf("initialize response unexpected: id=%s result=%s err=%s", m.IDString(), m.Result, m.Error)
	}
	mc.send(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	return mc
}

func (mc *muxConn) send(line string) {
	mc.t.Helper()
	if _, err := mc.conn.Write([]byte(line + "\n")); err != nil {
		mc.t.Fatalf("send: %v", err)
	}
}

// recv reads one framed message with a deadline so a misrouted (never-arriving)
// response fails fast instead of hanging the suite.
func (mc *muxConn) recv() *mcpMessage {
	mc.t.Helper()
	type res struct {
		m   *mcpMessage
		err error
	}
	ch := make(chan res, 1)
	go func() {
		line, err := mc.r.ReadBytes('\n')
		if err != nil && len(line) == 0 {
			ch <- res{err: err}
			return
		}
		var m mcpMessage
		if jerr := json.Unmarshal([]byte(strings.TrimRight(string(line), "\r\n")), &m); jerr != nil {
			ch <- res{err: jerr}
			return
		}
		ch <- res{m: &m}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			mc.t.Fatalf("recv: %v", r.err)
		}
		return r.m
	case <-time.After(testDeadline):
		mc.t.Fatal("recv: timed out (response likely misrouted to another client)")
		return nil
	}
}

func (mc *muxConn) close() { _ = mc.conn.Close() }

// mcpMessage is a minimal local decode of a JSON-RPC message for the mux tests,
// avoiding a dependency on internal helpers. callText extracts the echoed
// "backend=<name> tool=<bare>" text the fake backend returns for a tools/call.
type mcpMessage struct {
	ID     json.RawMessage `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
}

func (m *mcpMessage) IDString() string {
	if len(m.ID) == 0 {
		return ""
	}
	return string(m.ID)
}

// callText pulls the first text content item out of a tools/call result.
func (m *mcpMessage) callText(t *testing.T) string {
	t.Helper()
	var res struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(m.Result, &res); err != nil {
		t.Fatalf("result not a tool result: %v (%s)", err, m.Result)
	}
	if len(res.Content) == 0 {
		t.Fatalf("tool result has no content: %s", m.Result)
	}
	return res.Content[0].Text
}

// startMuxDaemon stands up a daemon broker over a Unix socket whose single shared
// backend is the fake MCP server, and returns the socket path plus a cancel that
// tears the daemon and pool down.
func startMuxDaemon(t *testing.T, tools []string) (sockPath string, cancel context.CancelFunc) {
	t.Helper()
	b := socketTestBroker(t, "fake", tools)
	sockPath = shortSockPath(t)
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancelFn := context.WithCancel(context.Background())
	srvDone := make(chan error, 1)
	go func() { srvDone <- b.ServeSocket(ctx, "fake", ln) }()
	t.Cleanup(func() {
		cancelFn()
		select {
		case <-srvDone:
		case <-time.After(testDeadline):
			t.Error("ServeSocket did not return after ctx cancel")
		}
	})
	return sockPath, cancelFn
}

// TestMux_TwoClientsNoCrossTalk is the protocol-critical case: two clients share
// ONE backend child, interleave tools/call requests with overlapping client ids
// (both use id 7 / "x"), and each must get back exactly its OWN response under
// its OWN id — the mux's broker-unique child ids must keep the two id spaces from
// colliding. The fake backend echoes the bare tool name, so a misroute would also
// surface as the wrong tool.
func TestMux_TwoClientsNoCrossTalk(t *testing.T) {
	sock, _ := startMuxDaemon(t, []string{"click", "type_text", "scroll"})
	a := dialMux(t, sock)
	bcl := dialMux(t, sock)
	defer a.close()
	defer bcl.close()

	// Both clients use the SAME client id (7) for DIFFERENT tools. Without the
	// mux's per-child id rewrite this would collide on the shared child.
	a.send(`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"click","arguments":{}}}`)
	bcl.send(`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"scroll","arguments":{}}}`)

	ra := a.recv()
	rb := bcl.recv()
	if ra.IDString() != "7" {
		t.Errorf("client A response id = %s, want 7", ra.IDString())
	}
	if rb.IDString() != "7" {
		t.Errorf("client B response id = %s, want 7", rb.IDString())
	}
	if got := ra.callText(t); got != "backend=fake tool=click" {
		t.Errorf("client A got %q, want its own click result (cross-talk?)", got)
	}
	if got := rb.callText(t); got != "backend=fake tool=scroll" {
		t.Errorf("client B got %q, want its own scroll result (cross-talk?)", got)
	}
}

// TestMux_StringAndNumericIDRoundTrip pins the id byte-fidelity guard: a client
// with a STRING id ("abc") and a client with a NUMERIC id (42) both get their id
// restored EXACTLY (quotes and all) on the wire, despite the mux rewriting both
// to numeric broker-unique child ids internally.
func TestMux_StringAndNumericIDRoundTrip(t *testing.T) {
	sock, _ := startMuxDaemon(t, []string{"click"})
	strClient := dialMux(t, sock)
	numClient := dialMux(t, sock)
	defer strClient.close()
	defer numClient.close()

	strClient.send(`{"jsonrpc":"2.0","id":"abc","method":"tools/call","params":{"name":"click","arguments":{}}}`)
	numClient.send(`{"jsonrpc":"2.0","id":42,"method":"tools/call","params":{"name":"click","arguments":{}}}`)

	rs := strClient.recv()
	rn := numClient.recv()
	// The string id must come back quoted ("\"abc\""), the numeric id bare ("42").
	if rs.IDString() != `"abc"` {
		t.Errorf("string-id client response id = %s, want \"abc\" (byte-faithful)", rs.IDString())
	}
	if rn.IDString() != "42" {
		t.Errorf("numeric-id client response id = %s, want 42", rn.IDString())
	}
}

// TestMux_InterleavedManyRequests stresses the routing table under concurrency:
// two clients each fire many tools/call requests back-to-back; every response
// must come back under the matching id and carry that client's tool. Run under
// -race to catch a torn routes map or a shared-conn write tear.
func TestMux_InterleavedManyRequests(t *testing.T) {
	sock, _ := startMuxDaemon(t, []string{"click", "scroll"})
	a := dialMux(t, sock)
	bcl := dialMux(t, sock)
	defer a.close()
	defer bcl.close()

	const n = 25
	var wg sync.WaitGroup
	wg.Add(2)

	check := func(mc *muxConn, tool string) {
		defer wg.Done()
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("%s-%d", tool, i)
			mc.send(fmt.Sprintf(`{"jsonrpc":"2.0","id":%q,"method":"tools/call","params":{"name":%q,"arguments":{}}}`, id, tool))
			r := mc.recv()
			wantID := fmt.Sprintf("%q", id)
			if r.IDString() != wantID {
				t.Errorf("response id = %s, want %s", r.IDString(), wantID)
				return
			}
			if got := r.callText(t); got != "backend=fake tool="+tool {
				t.Errorf("response for id %s = %q, want tool %q (misroute)", id, got, tool)
				return
			}
		}
	}
	go check(a, "click")
	go check(bcl, "scroll")
	wg.Wait()
}

// TestMux_LockAcquireReleaseRoundTrip exercises the per-client inflight
// correlation under the mux: a window-mutating tool-call (click with pid/window)
// acquires the lock on the inbound path and the response releases it on the
// outbound path, both correlated by the broker-unique CHILD id (the inflight key
// the mux Records under). Because the lock is released, the SAME client can issue
// a second click to the same window without being refused window-busy — proving
// the release found the inflight entry under the rewritten id. The arbitrate
// stages must be wired (socketTestBroker's pipelines), so build a daemon whose
// inbound/outbound carry real ArbitrateStages.
func TestMux_LockAcquireReleaseRoundTrip(t *testing.T) {
	b := socketTestBroker(t, "fake", []string{"click"})
	// socketTestBroker rebuilds the pipelines with pass-through arbitrate; rewire
	// them to enforce the lock so this test actually exercises acquire/release.
	b.inbound = NewPipeline(NewGateStage(), NewArbitrateStage(), NewAuditStage(b.audit, Inbound))
	b.outbound = NewPipeline(NewArbitrateStage(), NewTrimStage(), NewAuditStage(b.audit, Outbound))

	sock := shortSockPath(t)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	srvDone := make(chan error, 1)
	go func() { srvDone <- b.ServeSocket(ctx, "fake", ln) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-srvDone:
		case <-time.After(testDeadline):
			t.Error("ServeSocket did not return after ctx cancel")
		}
	})

	a := dialMux(t, sock)
	defer a.close()

	// First click on (pid 1, window 2): acquires then releases the lock.
	a.send(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"click","arguments":{"pid":1,"window_id":2}}}`)
	if r := a.recv(); r.IDString() != "1" || len(r.Error) > 0 {
		t.Fatalf("first click failed: id=%s err=%s", r.IDString(), r.Error)
	}
	// Second click on the SAME window: must NOT be refused window-busy — the first
	// call's lock was released via the inflight entry keyed by the rewritten id.
	a.send(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"click","arguments":{"pid":1,"window_id":2}}}`)
	r := a.recv()
	if r.IDString() != "2" {
		t.Fatalf("second click response id = %s, want 2", r.IDString())
	}
	if len(r.Error) > 0 {
		t.Fatalf("second click was refused %s — lock not released through the mux", r.Error)
	}
}

// TestMux_DisconnectDoesNotDisturbOther verifies one client hanging up mid-flight
// leaves the OTHER client and the shared child fully functional: after A
// disconnects, B can still issue calls and get correct responses, and the
// supervisor still reports the one shared child live.
func TestMux_DisconnectDoesNotDisturbOther(t *testing.T) {
	sock, _ := startMuxDaemon(t, []string{"click", "type_text"})
	a := dialMux(t, sock)
	bcl := dialMux(t, sock)
	defer bcl.close()

	// A issues a call and reads it, then drops its connection.
	a.send(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"click","arguments":{}}}`)
	if r := a.recv(); r.callText(t) != "backend=fake tool=click" {
		t.Fatalf("client A first call wrong: %q", r.callText(t))
	}
	a.close()

	// Give the daemon a moment to process A's disconnect (detach, lock reclaim).
	time.Sleep(20 * time.Millisecond)

	// B is unaffected: still gets correct, isolated responses.
	for i := 0; i < 5; i++ {
		bcl.send(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"tools/call","params":{"name":"type_text","arguments":{}}}`, i))
		r := bcl.recv()
		if r.IDString() != fmt.Sprintf("%d", i) {
			t.Fatalf("client B response id = %s, want %d after A disconnect", r.IDString(), i)
		}
		if got := r.callText(t); got != "backend=fake tool=type_text" {
			t.Fatalf("client B response wrong after A disconnect: %q", got)
		}
	}
}
