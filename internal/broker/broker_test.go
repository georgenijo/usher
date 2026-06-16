package broker

import (
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

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
	go func() { _ = b.pump(id, "test", Inbound, inflight, nil, clientSrc, backendDst, b.inbound) }()

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
	go func() { _ = b.pump(id, "test", Outbound, inflight, nil, backendSrc, clientDst, b.outbound) }()

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

// TestNew_TrimThresholdFromConfig verifies the on-disk Config.TrimThreshold is
// wired into the outbound trim stage, and that a zero (unset) value falls back
// to the built-in default.
func TestNew_TrimThresholdFromConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  int
		want int
	}{
		{"unset falls back to default", 0, DefaultTrimThreshold},
		{"custom value is honored", 512, 512},
		{"negative is treated as unset", -1, DefaultTrimThreshold},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := New(&config.Config{TrimThreshold: tc.cfg})
			if err != nil {
				t.Fatal(err)
			}
			ts := outboundTrimStage(t, b)
			if ts.threshold != tc.want {
				t.Errorf("trim threshold = %d, want %d", ts.threshold, tc.want)
			}
		})
	}
}

// outboundTrimStage extracts the broker's outbound TrimStage so a test can
// inspect the threshold it was constructed with.
func outboundTrimStage(t *testing.T, b *Broker) *TrimStage {
	t.Helper()
	for _, s := range b.outbound.stages {
		if ts, ok := s.(*TrimStage); ok {
			return ts
		}
	}
	t.Fatal("outbound pipeline has no TrimStage")
	return nil
}

// TestBroker_ReclaimOnDisconnect drives a mutating tool-call through the real
// inbound pump (acquiring a per-window write-lock), then ends the connection via
// reclaim and confirms the lock is freed — the reclaim-on-death path wired
// through the broker, not just the registry in isolation.
func TestBroker_ReclaimOnDisconnect(t *testing.T) {
	b, err := New(&config.Config{LockWaitSeconds: 0})
	if err != nil {
		t.Fatal(err)
	}
	b.audit = audit.New(io.Discard)
	// Tight bounded wait so the contention probe below is fast.
	b.locks = newLockRegistry(time.Minute, 20*time.Millisecond)

	id := identity.Identity{ID: "dying-agent", PID: 4242}
	inflight := NewInflightMap()

	// Feed one mutating click into the inbound pump.
	clientReqR, clientReqW := io.Pipe()
	backendR, backendW := io.Pipe()
	clientSrc := mcp.NewConn(clientReqR, io.Discard)
	backendDst := mcp.NewConn(backendR, backendW)

	req := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"click","arguments":{"pid":4242,"window_id":7}}}`
	go func() {
		_, _ = clientReqW.Write([]byte(req + "\n"))
		// Hold the connection open: the pump blocks on the next Read.
	}()
	go func() {
		_ = b.pump(id, "test", Inbound, inflight, func(*mcp.Message) error { return nil }, clientSrc, backendDst, b.inbound)
	}()

	// The backend must receive the forwarded click (proving the lock was taken,
	// not refused), and the lock is now held.
	_ = readAllMessages(t, backendDst, 1)
	key := windowKey{pid: 4242, windowID: 7}
	if _, res := b.locks.Acquire(key, "probe"); res != timedOut {
		t.Fatal("window should be held after the click acquired its lock")
	}

	// The caller dies: reclaim frees every lock it held.
	b.reclaim(id)
	if _, res := b.locks.Acquire(key, "next"); res != acquired {
		t.Error("reclaim-on-death must free the dead caller's window-lock")
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
