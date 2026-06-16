package broker

import (
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/georgenijo/usher/internal/audit"
	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/identity"
	"github.com/georgenijo/usher/internal/mcp"
)

// newTestBroker builds a broker with a discarded audit log for integration
// tests that don't care about the audit stream.
func newTestBroker(t *testing.T) *Broker {
	t.Helper()
	b, err := New(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	b.audit = audit.New(io.Discard)
	b.inbound = NewPipeline(NewGateStage(), NewArbitrateStage(), NewAuditStage(b.audit, Inbound))
	b.outbound = NewPipeline(NewTrimStage(), NewAuditStage(b.audit, Outbound))
	return b
}

// TestBroker_InboundRecordOutboundTrim drives both pumps over in-memory pipes,
// sharing one inflight map exactly as ServeStdio does: the inbound pump records
// the tools/call request, the outbound pump's TrimStage consumes it and trims
// the matching fat AX response, while tools/list passes through byte-identical.
func TestBroker_InboundRecordOutboundTrim(t *testing.T) {
	b := newTestBroker(t)
	inflight := NewInflightMap()
	id := identity.New()

	// Inbound side: client -> backend. We feed two requests, capture what the
	// backend receives (should be byte-identical — inbound stages are stubs).
	clientReqs := []string{
		`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"get_window_state","arguments":{"pid":1,"window_id":2}}}`,
	}
	inClientR, inClientW := io.Pipe()   // client writes requests here
	inBackendR, inBackendW := io.Pipe() // backend reads requests here
	clientSrc := mcp.NewConn(inClientR, io.Discard)
	backendDst := mcp.NewConn(inBackendR, inBackendW) // dst only needs Write

	go func() {
		for _, r := range clientReqs {
			_, _ = inClientW.Write([]byte(r + "\n"))
		}
		_ = inClientW.Close()
	}()

	// Run the inbound pump; it records into inflight and forwards to backendDst.
	go func() { _ = b.pump(id, "test", Inbound, inflight, clientSrc, backendDst, b.inbound) }()

	// Drain what the backend received.
	gotReqs := readAllMessages(t, backendDst, len(clientReqs))
	for i, m := range gotReqs {
		if m.Raw == nil {
			t.Errorf("inbound req %d lost its Raw bytes", i)
		}
	}
	// The inflight map now holds id 7 -> tools/call.
	// (We do not consume it here — TrimStage does, on the outbound side.)

	// Outbound side: backend -> client. tools/list result (large, with the word
	// AXWindow in a schema description) and a fat get_window_state result.
	listResult := `{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"get_window_state","description":"walk the AXWindow tree"}]}}`
	fat := fatAXResult(t, "7")
	fatBytes, _ := json.Marshal(fat)
	backendResps := []string{listResult, string(fatBytes)}

	outBackendR, outBackendW := io.Pipe() // backend writes responses here
	outClientR, outClientW := io.Pipe()   // client reads responses here
	backendSrc := mcp.NewConn(outBackendR, io.Discard)
	clientDst := mcp.NewConn(outClientR, outClientW)

	go func() {
		for _, r := range backendResps {
			_, _ = outBackendW.Write([]byte(r + "\n"))
		}
		_ = outBackendW.Close()
	}()
	go func() { _ = b.pump(id, "test", Outbound, inflight, backendSrc, clientDst, b.outbound) }()

	gotResps := readAllMessages(t, clientDst, len(backendResps))

	// Response 0: tools/list — byte-identical pass-through.
	if string(gotResps[0].Raw) != listResult {
		t.Errorf("tools/list mutated:\n got: %s\nwant: %s", gotResps[0].Raw, listResult)
	}

	// Response 1: get_window_state — trimmed to the digest and shrunk.
	if len(gotResps[1].Result) >= len(fat.Result) {
		t.Errorf("fat result not trimmed: %d -> %d", len(fat.Result), len(gotResps[1].Result))
	}
	var res toolResult
	if err := json.Unmarshal(gotResps[1].Result, &res); err != nil {
		t.Fatalf("trimmed result invalid JSON: %v", err)
	}
	var ct contentText
	_ = json.Unmarshal(res.Content[1], &ct)
	if !strings.HasPrefix(ct.Text, "BUTTONS (act by element_index):") {
		t.Errorf("expected digest in trimmed text item, got:\n%s", ct.Text)
	}
}

// readAllMessages reads exactly n messages off conn, failing on early EOF.
func readAllMessages(t *testing.T, conn *mcp.Conn, n int) []*mcp.Message {
	t.Helper()
	var out []*mcp.Message
	for i := 0; i < n; i++ {
		m, err := conn.Read()
		if err != nil {
			t.Fatalf("read message %d: %v", i, err)
		}
		out = append(out, m)
	}
	return out
}
