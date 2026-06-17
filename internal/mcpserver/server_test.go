package mcpserver

import (
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/georgenijo/usher/internal/mcp"
)

// harness wires a client <-> the mcpserver over two io.Pipe pairs and runs
// Run on a goroutine, exactly as the broker would over a child process's
// stdio. The client writes requests with one mcp.Conn and reads replies with
// another. Closing clientToServer (the server's stdin) gives Run a clean EOF.
type harness struct {
	client         *mcp.Conn      // client writes requests / reads replies
	clientToServer *io.PipeWriter // closing this EOFs the server, ending Run
	done           chan error
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	// in  = client -> server (server reads its stdin here)
	inR, inW := io.Pipe()
	// out = server -> client (server writes its stdout here)
	outR, outW := io.Pipe()

	h := &harness{
		client:         mcp.NewConn(outR, inW),
		clientToServer: inW,
		done:           make(chan error, 1),
	}
	go func() { h.done <- Run(inR, outW) }()
	return h
}

// call writes a request and returns the server's reply. A short timeout keeps a
// wedged server from hanging the whole suite.
func (h *harness) call(t *testing.T, m *mcp.Message) *mcp.Message {
	t.Helper()
	if err := h.client.Write(m); err != nil {
		t.Fatalf("write %s: %v", m.Method, err)
	}
	type res struct {
		m   *mcp.Message
		err error
	}
	ch := make(chan res, 1)
	go func() {
		got, err := h.client.Read()
		ch <- res{got, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("read reply to %s: %v", m.Method, r.err)
		}
		return r.m
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for reply to %s", m.Method)
		return nil
	}
}

// notify writes a notification (no id, no reply expected).
func (h *harness) notify(t *testing.T, m *mcp.Message) {
	t.Helper()
	if err := h.client.Write(m); err != nil {
		t.Fatalf("write notification %s: %v", m.Method, err)
	}
}

// close half-closes the server's stdin and waits for Run to return cleanly.
func (h *harness) close(t *testing.T) {
	t.Helper()
	_ = h.clientToServer.Close()
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatalf("Run returned error on EOF: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after stdin EOF")
	}
}

// req is a small request builder for the table-driven tests.
func req(id int, method string, params any) *mcp.Message {
	var raw json.RawMessage
	if params != nil {
		b, _ := json.Marshal(params)
		raw = b
	}
	idRaw, _ := json.Marshal(id)
	return &mcp.Message{JSONRPC: "2.0", ID: idRaw, Method: method, Params: raw}
}

// toolResult is the decoded shape of an MCP tools/call result.
type toolResult struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	IsError bool `json:"isError"`
}

func decodeToolResult(t *testing.T, m *mcp.Message) toolResult {
	t.Helper()
	if len(m.Error) > 0 {
		t.Fatalf("tools/call returned a JSON-RPC error: %s", m.Error)
	}
	var tr toolResult
	if err := json.Unmarshal(m.Result, &tr); err != nil {
		t.Fatalf("decode tool result %s: %v", m.Result, err)
	}
	if len(tr.Content) == 0 {
		t.Fatalf("tool result has no content: %s", m.Result)
	}
	return tr
}

func TestInitialize(t *testing.T) {
	h := newHarness(t)
	defer h.close(t)

	resp := h.call(t, req(1, "initialize", map[string]any{
		"protocolVersion": protocolVersion,
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "test", "version": "0.0.0"},
	}))
	if len(resp.Result) == 0 {
		t.Fatalf("initialize returned no result (broker rejects empty): %+v", resp)
	}
	var res struct {
		ProtocolVersion string `json:"protocolVersion"`
		Capabilities    struct {
			Tools map[string]any `json:"tools"`
		} `json:"capabilities"`
		ServerInfo struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"serverInfo"`
	}
	if err := json.Unmarshal(resp.Result, &res); err != nil {
		t.Fatalf("decode initialize result: %v", err)
	}
	if res.ProtocolVersion != protocolVersion {
		t.Errorf("protocolVersion = %q, want %q", res.ProtocolVersion, protocolVersion)
	}
	if res.ServerInfo.Name != serverName {
		t.Errorf("serverInfo.name = %q, want %q", res.ServerInfo.Name, serverName)
	}
	if res.ServerInfo.Version != serverVersion {
		t.Errorf("serverInfo.version = %q, want %q", res.ServerInfo.Version, serverVersion)
	}
	if res.Capabilities.Tools == nil {
		t.Errorf("capabilities.tools missing; broker expects a tools capability")
	}
}

func TestNotificationsInitializedNoReply(t *testing.T) {
	h := newHarness(t)
	defer h.close(t)

	// A notification must NOT produce a reply. Send it, then send a request and
	// confirm the very next frame is the request's reply (not a stray response to
	// the notification, which would have id absent / mismatch).
	h.notify(t, &mcp.Message{JSONRPC: "2.0", Method: "notifications/initialized"})

	resp := h.call(t, req(7, "tools/list", nil))
	if resp.IDString() != "7" {
		t.Fatalf("expected reply to id 7 (notification must be silent), got id %q", resp.IDString())
	}
}

func TestToolsList(t *testing.T) {
	h := newHarness(t)
	defer h.close(t)

	resp := h.call(t, req(2, "tools/list", nil))
	var res struct {
		Tools []struct {
			Name        string          `json:"name"`
			Description string          `json:"description"`
			InputSchema json.RawMessage `json:"inputSchema"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(resp.Result, &res); err != nil {
		t.Fatalf("decode tools/list: %v", err)
	}
	got := map[string]bool{}
	for _, tl := range res.Tools {
		got[tl.Name] = true
		if tl.Description == "" {
			t.Errorf("tool %q has empty description", tl.Name)
		}
		if len(tl.InputSchema) == 0 {
			t.Errorf("tool %q has empty inputSchema", tl.Name)
		}
	}
	for _, want := range []string{"echo", "add", "now"} {
		if !got[want] {
			t.Errorf("tools/list missing tool %q (got %v)", want, got)
		}
	}
	if len(res.Tools) != 3 {
		t.Errorf("tools/list returned %d tools, want 3", len(res.Tools))
	}
}

func TestToolsCall(t *testing.T) {
	h := newHarness(t)
	defer h.close(t)

	tests := []struct {
		name    string
		tool    string
		args    any
		want    string // exact expected text (empty = use check)
		check   func(t *testing.T, text string)
		isError bool
	}{
		{
			name: "echo round-trips",
			tool: "echo",
			args: map[string]any{"text": "hello world"},
			want: "hello world",
		},
		{
			name: "echo empty string",
			tool: "echo",
			args: map[string]any{"text": ""},
			want: "",
		},
		{
			name: "add integers",
			tool: "add",
			args: map[string]any{"a": 2, "b": 3},
			want: "5",
		},
		{
			name: "add negatives",
			tool: "add",
			args: map[string]any{"a": -4, "b": 1.5},
			want: "-2.5",
		},
		{
			name: "now parses as RFC3339",
			tool: "now",
			args: map[string]any{},
			check: func(t *testing.T, text string) {
				if _, err := time.Parse(time.RFC3339, text); err != nil {
					t.Errorf("now returned %q which is not RFC3339: %v", text, err)
				}
			},
		},
		{
			name:    "unknown tool is an in-band error",
			tool:    "frobnicate",
			args:    map[string]any{},
			isError: true,
		},
	}

	id := 100
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id++
			resp := h.call(t, req(id, "tools/call", map[string]any{
				"name":      tc.tool,
				"arguments": tc.args,
			}))
			tr := decodeToolResult(t, resp)
			if tr.IsError != tc.isError {
				t.Fatalf("isError = %v, want %v (result: %s)", tr.IsError, tc.isError, resp.Result)
			}
			if tc.isError {
				return // text is an implementation-defined message
			}
			if tr.Content[0].Type != "text" {
				t.Errorf("content[0].type = %q, want text", tr.Content[0].Type)
			}
			text := tr.Content[0].Text
			if tc.check != nil {
				tc.check(t, text)
				return
			}
			if text != tc.want {
				t.Errorf("%s(%v) = %q, want %q", tc.tool, tc.args, text, tc.want)
			}
		})
	}
}

// TestUnknownRequestStillReplies verifies a non-MCP-lifecycle request (e.g.
// ping) gets a result so a probing client never hangs.
func TestUnknownRequestStillReplies(t *testing.T) {
	h := newHarness(t)
	defer h.close(t)

	resp := h.call(t, req(9, "ping", nil))
	if len(resp.Result) == 0 {
		t.Fatalf("ping got no result: %+v", resp)
	}
	if resp.IDString() != "9" {
		t.Errorf("ping reply id = %q, want 9", resp.IDString())
	}
}

// TestFullHandshake exercises the exact MCP lifecycle in order:
// initialize -> notifications/initialized -> tools/list -> tools/call, the same
// sequence usher's supervisor drives against a real backend child.
func TestFullHandshake(t *testing.T) {
	h := newHarness(t)
	defer h.close(t)

	if r := h.call(t, req(1, "initialize", map[string]any{"protocolVersion": protocolVersion})); len(r.Result) == 0 {
		t.Fatal("initialize empty result")
	}
	h.notify(t, &mcp.Message{JSONRPC: "2.0", Method: "notifications/initialized"})
	if r := h.call(t, req(2, "tools/list", nil)); len(r.Result) == 0 {
		t.Fatal("tools/list empty result")
	}
	r := h.call(t, req(3, "tools/call", map[string]any{"name": "add", "arguments": map[string]any{"a": 40, "b": 2}}))
	tr := decodeToolResult(t, r)
	if tr.Content[0].Text != "42" {
		t.Errorf("add(40,2) = %q, want 42", tr.Content[0].Text)
	}
}
