package broker

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/georgenijo/usher/internal/audit"
	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/mcp"
)

// testDeadline bounds how long a test waits for a broker response or for
// ServeMulti to return after the client closes. It is generous so that the
// real-subprocess fanout (each backend is a re-exec of the test binary) is not
// falsely failed under heavy load (e.g. the whole suite under -race), yet far
// under go test's 10-minute default so a genuinely wedged path still fails.
const testDeadline = 45 * time.Second

// --- fake backend, run as a real stdio child process ------------------------
//
// The fanout spawns its backends with backend.NewStdio (a child process speaking
// newline-delimited JSON-RPC). Rather than require the real cua-driver, the test
// re-execs the test binary itself with USHER_FAKE_BACKEND set; TestMain routes
// that into fakeBackendMain, a minimal MCP server. The tool set it advertises is
// taken from USHER_FAKE_TOOLS (comma-separated bare tool names), so two backends
// in one ServeMulti expose different tools.

func TestMain(m *testing.M) {
	if os.Getenv("USHER_FAKE_BACKEND") != "" {
		fakeBackendMain()
		return
	}
	os.Exit(m.Run())
}

// fakeBackendMain is a minimal MCP server: it answers initialize, returns its
// configured tools on tools/list, and for tools/call echoes the bare tool name
// it received in the result text so the test can assert correct routing and
// namespace stripping. It exits on EOF (half-close).
func fakeBackendMain() {
	name := os.Getenv("USHER_FAKE_NAME")
	tools := strings.Split(os.Getenv("USHER_FAKE_TOOLS"), ",")
	conn := mcp.NewConn(os.Stdin, os.Stdout)
	for {
		m, err := conn.Read()
		if err != nil {
			return // EOF: half-closed, exit
		}
		switch {
		case m.Method == "initialize":
			result, _ := json.Marshal(map[string]any{
				"protocolVersion": "2024-11-05",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": name, "version": "9.9.9"},
			})
			_ = conn.Write(&mcp.Message{JSONRPC: "2.0", ID: m.ID, Result: result})
		case m.Method == "notifications/initialized":
			// no reply to a notification
		case m.Method == "tools/list":
			var toolObjs []map[string]any
			for _, t := range tools {
				if t == "" {
					continue
				}
				toolObjs = append(toolObjs, map[string]any{
					"name":        t,
					"description": "desc of " + t,
					"inputSchema": map[string]any{"type": "object"},
				})
			}
			result, _ := json.Marshal(map[string]any{"tools": toolObjs})
			_ = conn.Write(&mcp.Message{JSONRPC: "2.0", ID: m.ID, Result: result})
		case m.Method == "tools/call":
			// Echo "backend=<name> tool=<bare name>" so the test verifies the
			// namespace was stripped before forwarding.
			var p struct {
				Name string `json:"name"`
			}
			_ = json.Unmarshal(m.Params, &p)
			text := "backend=" + name + " tool=" + p.Name
			result, _ := json.Marshal(map[string]any{
				"content": []any{map[string]any{"type": "text", "text": text}},
			})
			_ = conn.Write(&mcp.Message{JSONRPC: "2.0", ID: m.ID, Result: result})
		case len(m.ID) > 0:
			// Any other REQUEST (ping, resources/list, …): answer with a result that
			// names this backend, so a test fanning a non-tools/call request can
			// assert exactly ONE response arrives (not one per backend) and tell
			// which backend produced it.
			result, _ := json.Marshal(map[string]any{"answeredBy": name})
			_ = conn.Write(&mcp.Message{JSONRPC: "2.0", ID: m.ID, Result: result})
		}
	}
}

// multiTestBroker builds a broker whose config holds N fake stdio backends, each
// a re-exec of the test binary with its own name + tool set carried in env. The
// env is injected per-backend by wrapping the command in `/bin/sh -c` that sets
// the vars then execs the test binary, so each child sees its own identity.
func multiTestBroker(t *testing.T, backends map[string][]string) *Broker {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	var cfg config.Config
	// Deterministic order: sort by name so the merged tools/list is stable.
	for _, name := range sortedKeys(backends) {
		tools := backends[name]
		// /bin/sh -c 'exec "$0"' self  — with env vars set inline. We set the
		// fake-backend env on the sh invocation via a leading `env` segment so
		// each child has its own USHER_FAKE_* without polluting the test process.
		script := "USHER_FAKE_BACKEND=1 USHER_FAKE_NAME=" + name +
			" USHER_FAKE_TOOLS=" + strings.Join(tools, ",") + ` exec "$0"`
		cfg.Backends = append(cfg.Backends, config.Backend{
			Name:      name,
			Transport: "stdio",
			Command:   []string{"/bin/sh", "-c", script, self},
		})
	}

	b, err := New(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	b.audit = audit.New(io.Discard)
	return b
}

func sortedKeys(m map[string][]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// tiny insertion sort to avoid pulling in sort just for tests
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j-1] > keys[j]; j-- {
			keys[j-1], keys[j] = keys[j], keys[j-1]
		}
	}
	return keys
}

// clientPipes wires a client <-> broker over two os.Pipe pairs and returns the
// reader/writer the test uses plus the in/out the broker reads/writes. A
// helper goroutine running ServeMulti is started; done receives its error.
type clientPipes struct {
	clientToBroker *io.PipeWriter // test writes requests here
	brokerToClient *io.PipeReader // test reads responses here
	resp           *bufio.Reader  // framed reader over brokerToClient
}

// runServeMulti starts b.ServeMulti against the named backends over in-memory
// pipes and returns a clientPipes plus a done channel.
func runServeMulti(t *testing.T, b *Broker, names []string) (*clientPipes, chan error) {
	t.Helper()
	reqR, reqW := io.Pipe()   // broker reads client requests from reqR
	respR, respW := io.Pipe() // broker writes responses to respW
	cp := &clientPipes{
		clientToBroker: reqW,
		brokerToClient: respR,
		resp:           bufio.NewReaderSize(respR, 1<<20),
	}
	done := make(chan error, 1)
	go func() {
		done <- b.ServeMulti(t.Context(), names, reqR, respW)
	}()
	return cp, done
}

// send writes one JSON-RPC line to the broker.
func (cp *clientPipes) send(t *testing.T, line string) {
	t.Helper()
	if _, err := cp.clientToBroker.Write([]byte(line + "\n")); err != nil {
		t.Fatalf("send: %v", err)
	}
}

// recv reads one JSON-RPC message from the broker (with a deadline so a hung
// test fails fast rather than blocking the suite).
func (cp *clientPipes) recv(t *testing.T) *mcp.Message {
	t.Helper()
	type res struct {
		m   *mcp.Message
		err error
	}
	ch := make(chan res, 1)
	go func() {
		line, err := cp.resp.ReadBytes('\n')
		if err != nil && len(line) == 0 {
			ch <- res{err: err}
			return
		}
		var m mcp.Message
		if jerr := json.Unmarshal([]byte(strings.TrimRight(string(line), "\r\n")), &m); jerr != nil {
			ch <- res{err: jerr}
			return
		}
		ch <- res{m: &m}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("recv: %v", r.err)
		}
		return r.m
	case <-time.After(testDeadline):
		t.Fatal("recv: timed out waiting for broker response")
		return nil
	}
}

// handshake drives initialize + initialized and returns the merged tools array
// from a tools/list call, so each test starts from a ready broker.
func (cp *clientPipes) handshake(t *testing.T) {
	t.Helper()
	cp.send(t, `{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}`)
	init := cp.recv(t)
	if !init.IsResponse() {
		t.Fatalf("initialize: expected response, got %+v", init)
	}
	// Confirm the broker advertises itself, not a backend.
	var res map[string]json.RawMessage
	if err := json.Unmarshal(init.Result, &res); err != nil {
		t.Fatalf("initialize result: %v", err)
	}
	var si struct{ Name string }
	_ = json.Unmarshal(res["serverInfo"], &si)
	if si.Name != serverInfoName {
		t.Errorf("initialize serverInfo.name = %q, want %q", si.Name, serverInfoName)
	}
	cp.send(t, `{"jsonrpc":"2.0","method":"notifications/initialized"}`)
}

// listTools sends tools/list and returns the namespaced names in the result.
func (cp *clientPipes) listTools(t *testing.T) []string {
	t.Helper()
	cp.send(t, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	resp := cp.recv(t)
	if resp.IDString() != "1" {
		t.Fatalf("tools/list response id = %q, want 1", resp.IDString())
	}
	var res struct {
		Tools []map[string]json.RawMessage `json:"tools"`
	}
	if err := json.Unmarshal(resp.Result, &res); err != nil {
		t.Fatalf("tools/list result: %v", err)
	}
	var names []string
	for _, tool := range res.Tools {
		var n string
		_ = json.Unmarshal(tool["name"], &n)
		names = append(names, n)
	}
	return names
}

// --- the tests ---------------------------------------------------------------

// TestServeMulti_MergedNamespacedToolsList aggregates two backends and asserts
// tools/list returns every tool, each prefixed with its backend, schemas intact.
func TestServeMulti_MergedNamespacedToolsList(t *testing.T) {
	b := multiTestBroker(t, map[string][]string{
		"cua": {"click", "get_window_state"},
		"fs":  {"read_file"},
	})
	cp, done := runServeMulti(t, b, nil) // nil = all configured
	defer drainDone(t, cp, done)

	cp.handshake(t)
	got := cp.listTools(t)

	want := map[string]bool{
		"cua__click":            false,
		"cua__get_window_state": false,
		"fs__read_file":         false,
	}
	for _, n := range got {
		if _, ok := want[n]; !ok {
			t.Errorf("unexpected tool in merged list: %q", n)
			continue
		}
		want[n] = true
	}
	for n, seen := range want {
		if !seen {
			t.Errorf("merged tools/list missing %q (got %v)", n, got)
		}
	}
}

// TestServeMulti_RoutesCallToOwningBackend sends a namespaced tools/call to each
// backend and asserts (a) the right backend handled it, (b) the namespace was
// stripped before the backend saw the tool name, and (c) the response carries
// the client's ORIGINAL id (proving the id-remap round-trips).
func TestServeMulti_RoutesCallToOwningBackend(t *testing.T) {
	b := multiTestBroker(t, map[string][]string{
		"cua": {"click"},
		"fs":  {"read_file"},
	})
	cp, done := runServeMulti(t, b, nil)
	defer drainDone(t, cp, done)

	cp.handshake(t)

	// Call cua__click.
	cp.send(t, `{"jsonrpc":"2.0","id":42,"method":"tools/call","params":{"name":"cua__click","arguments":{"pid":1,"window_id":2}}}`)
	r1 := cp.recv(t)
	if r1.IDString() != "42" {
		t.Errorf("cua call response id = %q, want 42 (client id must be restored)", r1.IDString())
	}
	if text := resultText(t, r1); text != "backend=cua tool=click" {
		t.Errorf("cua call routed wrong: result text = %q", text)
	}

	// Call fs__read_file.
	cp.send(t, `{"jsonrpc":"2.0","id":"abc","method":"tools/call","params":{"name":"fs__read_file","arguments":{"path":"/tmp/x"}}}`)
	r2 := cp.recv(t)
	if r2.IDString() != `"abc"` {
		t.Errorf("fs call response id = %q, want \"abc\"", r2.IDString())
	}
	if text := resultText(t, r2); text != "backend=fs tool=read_file" {
		t.Errorf("fs call routed wrong: result text = %q", text)
	}
}

// TestServeMulti_UnroutableToolNames covers the two refusal paths: a bare tool
// name with no separator, and a separator naming an unknown backend.
func TestServeMulti_UnroutableToolNames(t *testing.T) {
	b := multiTestBroker(t, map[string][]string{
		"cua": {"click"},
	})
	cp, done := runServeMulti(t, b, nil)
	defer drainDone(t, cp, done)

	cp.handshake(t)

	// No separator -> ErrToolNotNamespaced.
	cp.send(t, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"click","arguments":{}}}`)
	r1 := cp.recv(t)
	if code := errorCode(t, r1); code != ErrToolNotNamespaced {
		t.Errorf("bare tool name error code = %d, want %d", code, ErrToolNotNamespaced)
	}
	if r1.IDString() != "1" {
		t.Errorf("error response id = %q, want 1", r1.IDString())
	}

	// Unknown backend prefix -> ErrUnknownBackend.
	cp.send(t, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"ghost__click","arguments":{}}}`)
	r2 := cp.recv(t)
	if code := errorCode(t, r2); code != ErrUnknownBackend {
		t.Errorf("unknown backend error code = %d, want %d", code, ErrUnknownBackend)
	}
}

// TestServeMulti_ConcurrentCallsToDifferentBackends fires a call to each of two
// backends back-to-back and matches the two responses by their client ids,
// proving the N outbound pumps demultiplex correctly (responses may arrive in
// either order). Run under -race to stress the shared inflight map and the
// single client conn written from N pumps.
func TestServeMulti_ConcurrentCallsToDifferentBackends(t *testing.T) {
	b := multiTestBroker(t, map[string][]string{
		"cua": {"click"},
		"fs":  {"read_file"},
	})
	cp, done := runServeMulti(t, b, nil)
	defer drainDone(t, cp, done)

	cp.handshake(t)

	cp.send(t, `{"jsonrpc":"2.0","id":100,"method":"tools/call","params":{"name":"cua__click","arguments":{}}}`)
	cp.send(t, `{"jsonrpc":"2.0","id":200,"method":"tools/call","params":{"name":"fs__read_file","arguments":{}}}`)

	want := map[string]string{
		"100": "backend=cua tool=click",
		"200": "backend=fs tool=read_file",
	}
	for range want {
		r := cp.recv(t)
		exp, ok := want[r.IDString()]
		if !ok {
			t.Fatalf("unexpected response id %q", r.IDString())
		}
		if got := resultText(t, r); got != exp {
			t.Errorf("id %s: result = %q, want %q", r.IDString(), got, exp)
		}
		delete(want, r.IDString())
	}
}

// TestServeMulti_NonToolCallRequestNotDuplicated guards the fan-out de-dup fix:
// a non-tools/call REQUEST (here a ping) carries an id and expects EXACTLY ONE
// response. With two backends aggregated, fanning the request to both would yield
// two identical responses for one client id — a stream corruption. The broker
// must route such a request to a single backend, so the client sees one response.
// We prove "exactly one" by sending a follow-up tools/call with a distinct id and
// asserting the very next response is that call's (id 2), not a duplicate ping
// (id 1): a second ping response would have been queued ahead of it.
func TestServeMulti_NonToolCallRequestNotDuplicated(t *testing.T) {
	b := multiTestBroker(t, map[string][]string{
		"cua": {"click"},
		"fs":  {"read_file"},
	})
	cp, done := runServeMulti(t, b, nil)
	defer drainDone(t, cp, done)

	cp.handshake(t)

	// A single ping must produce a single response with the client's id.
	cp.send(t, `{"jsonrpc":"2.0","id":1,"method":"ping"}`)
	r1 := cp.recv(t)
	if r1.IDString() != "1" {
		t.Fatalf("ping response id = %q, want 1", r1.IDString())
	}
	if !r1.IsResponse() {
		t.Fatalf("ping: expected a response, got %+v", r1)
	}

	// Follow with a tools/call (id 2). If the ping had been duplicated, a second
	// ping response (id 1) would be sitting in the pipe ahead of this one.
	cp.send(t, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"cua__click","arguments":{}}}`)
	r2 := cp.recv(t)
	if r2.IDString() != "2" {
		t.Fatalf("next response id = %q, want 2 (a duplicate ping response would precede it)", r2.IDString())
	}
	if text := resultText(t, r2); text != "backend=cua tool=click" {
		t.Errorf("tools/call routed wrong: %q", text)
	}
}

// TestServeMulti_SubsetByName aggregates only the named subset, not all
// configured backends, and confirms the other backend's tools are absent.
func TestServeMulti_SubsetByName(t *testing.T) {
	b := multiTestBroker(t, map[string][]string{
		"cua": {"click"},
		"fs":  {"read_file"},
	})
	cp, done := runServeMulti(t, b, []string{"cua"}) // only cua
	defer drainDone(t, cp, done)

	cp.handshake(t)
	got := cp.listTools(t)
	if len(got) != 1 || got[0] != "cua__click" {
		t.Errorf("subset tools/list = %v, want [cua__click]", got)
	}

	// fs__read_file must now be unroutable (fs is not aggregated).
	cp.send(t, `{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"fs__read_file","arguments":{}}}`)
	r := cp.recv(t)
	if code := errorCode(t, r); code != ErrUnknownBackend {
		t.Errorf("excluded backend call code = %d, want %d", code, ErrUnknownBackend)
	}
}

// TestServeStdio_BackCompatUnchanged is the regression guard for the legacy 1:1
// path: ServeStdio against a single fake backend must forward tools/call with
// the BARE tool name (no namespace prefix) and leave the request id untouched,
// exactly as before #17. It exercises the real ServeStdio (not ServeMulti).
func TestServeStdio_BackCompatUnchanged(t *testing.T) {
	b := multiTestBroker(t, map[string][]string{
		"cua": {"click"},
	})
	reqR, reqW := io.Pipe()
	respR, respW := io.Pipe()
	cp := &clientPipes{
		clientToBroker: reqW,
		brokerToClient: respR,
		resp:           bufio.NewReaderSize(respR, 1<<20),
	}
	done := make(chan error, 1)
	go func() { done <- b.ServeStdio(t.Context(), "cua", reqR, respW) }()
	defer drainDone(t, cp, done)

	// Handshake passes straight through to the single backend.
	cp.send(t, `{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}`)
	init := cp.recv(t)
	// The single-backend path forwards the backend's OWN serverInfo verbatim.
	var res map[string]json.RawMessage
	if err := json.Unmarshal(init.Result, &res); err != nil {
		t.Fatalf("initialize result: %v", err)
	}
	var si struct{ Name string }
	_ = json.Unmarshal(res["serverInfo"], &si)
	if si.Name != "cua" {
		t.Errorf("ServeStdio must forward backend serverInfo verbatim; name = %q, want cua", si.Name)
	}
	cp.send(t, `{"jsonrpc":"2.0","method":"notifications/initialized"}`)

	// A bare (un-namespaced) tools/call goes straight through: the backend echoes
	// the bare tool name it received, and the client id is untouched.
	cp.send(t, `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"click","arguments":{}}}`)
	r := cp.recv(t)
	if r.IDString() != "5" {
		t.Errorf("ServeStdio response id = %q, want 5 (unchanged)", r.IDString())
	}
	if text := resultText(t, r); text != "backend=cua tool=click" {
		t.Errorf("ServeStdio forwarded wrong tool: %q", text)
	}
}

// TestServeMulti_BusyErrorCarriesClientID exercises the id-restore on an in-band
// refusal: a window held by another owner makes a routed mutating call time out,
// and ArbitrateStage answers a window-busy error. Because the inbound side
// remapped the request id to a backend-side id, the error must be re-stamped
// with the client's ORIGINAL id (clientReply wrapper) so the client correlates
// it to its request.
func TestServeMulti_BusyErrorCarriesClientID(t *testing.T) {
	b := multiTestBroker(t, map[string][]string{
		"cua": {"click"},
	})
	// This test exercises the window-busy refusal path, which only fires if
	// classifyToolCall treats the bare "click" (the namespace "cua__" is stripped
	// before ArbitrateStage sees it) as a window-MUTATING tool that locks
	// windowKey{pid, window_id}. If a future toolclass.go change reclassified
	// "click" as read-only or windowless, the routed call would never contend for
	// the pre-held lock, no ErrWindowBusy would be returned, and this test would
	// stop asserting the id-restore behaviour it claims to cover. Keep "click"
	// mutating in classifyToolCall for this test to remain meaningful.
	//
	// Tight bounded wait so the contended call is refused fast; hold the lock so
	// the routed click can never acquire it.
	b.locks = newLockRegistry(time.Minute, 20*time.Millisecond)
	heldKey := windowKey{pid: 1, windowID: 2}
	if _, res := b.locks.Acquire(heldKey, "other-owner"); res != acquired {
		t.Fatalf("precondition: could not pre-hold the window lock (res=%v)", res)
	}

	cp, done := runServeMulti(t, b, nil)
	defer drainDone(t, cp, done)

	cp.handshake(t)

	cp.send(t, `{"jsonrpc":"2.0","id":"call-77","method":"tools/call","params":{"name":"cua__click","arguments":{"pid":1,"window_id":2}}}`)
	r := cp.recv(t)
	if r.IDString() != `"call-77"` {
		t.Errorf("busy error id = %q, want \"call-77\" (client id must be restored, not the backend-side id)", r.IDString())
	}
	if code := errorCode(t, r); code != ErrWindowBusy {
		t.Errorf("busy error code = %d, want %d", code, ErrWindowBusy)
	}
}

// --- small result/error helpers ---------------------------------------------

func resultText(t *testing.T, m *mcp.Message) string {
	t.Helper()
	var res struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(m.Result, &res); err != nil {
		t.Fatalf("result not a tool result: %v (raw %s)", err, m.Result)
	}
	if len(res.Content) == 0 {
		t.Fatalf("result has no content: %s", m.Result)
	}
	return res.Content[0].Text
}

func errorCode(t *testing.T, m *mcp.Message) int {
	t.Helper()
	if len(m.Error) == 0 {
		t.Fatalf("expected an error response, got result: %s", m.Result)
	}
	var e struct {
		Code int `json:"code"`
	}
	if err := json.Unmarshal(m.Error, &e); err != nil {
		t.Fatalf("error object: %v", err)
	}
	return e.Code
}

// drainDone closes the client write side so the broker half-closes the backends
// and ServeMulti returns, then waits for it (with a deadline) so no goroutine or
// child process leaks between tests.
func drainDone(t *testing.T, cp *clientPipes, done chan error) {
	t.Helper()
	_ = cp.clientToBroker.Close()
	// Keep draining the client side while the broker shuts down: an outbound pump
	// may still be mid-write to the client pipe, and io.Pipe.Write blocks until a
	// reader consumes it. Without this drain the pump could wedge and ServeMulti
	// would never reach its outbound-pump join. (A real client always keeps
	// reading, so this mirrors production rather than masking a broker bug.)
	go func() {
		for {
			if _, err := cp.resp.ReadBytes('\n'); err != nil {
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(testDeadline):
		t.Error("ServeMulti did not return after client close")
	}
	_ = cp.brokerToClient.Close()
}
