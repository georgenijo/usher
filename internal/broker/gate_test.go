package broker

import (
	"encoding/json"
	"io"
	"testing"

	"github.com/georgenijo/usher/internal/audit"
	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/identity"
	"github.com/georgenijo/usher/internal/mcp"
)

// gateCtx builds an inbound Context wired with an inflight map and a capturing
// Reply, as the inbound pump would. Mirrors arbCtx but without the lock registry
// (the gate never touches it).
func gateCtx(inflight *InflightMap, replies *[]*mcp.Message) *Context {
	return &Context{
		Identity: identity.Identity{ID: "agent"},
		Backend:  "test",
		Dir:      Inbound,
		Inflight: inflight,
		Reply: func(m *mcp.Message) error {
			*replies = append(*replies, m)
			return nil
		},
	}
}

// blockPolicy is a small block-list policy for the membership tests.
func blockPolicy(blocked ...string) Policy {
	return Policy{BlockedTools: toSet(blocked)}
}

// TestGate_PassThroughWhenNilPolicy: a gate with an empty policy forwards every
// message unchanged — the skeleton back-compat contract.
func TestGate_PassThroughWhenNilPolicy(t *testing.T) {
	st := NewGateStage()
	var replies []*mcp.Message
	msgs := []*mcp.Message{
		callMsg("1", "kill_app", `{"pid":1}`),
		{JSONRPC: "2.0", ID: json.RawMessage("2"), Method: "tools/list"},
		{JSONRPC: "2.0", ID: json.RawMessage("3"), Method: "initialize"},
		{JSONRPC: "2.0", Method: "notifications/initialized"},
	}
	for _, m := range msgs {
		out, err := st.Process(gateCtx(NewInflightMap(), &replies), m)
		if err != nil {
			t.Fatalf("%s errored: %v", m.Method, err)
		}
		if out != m {
			t.Errorf("%s must pass through untouched with an empty policy", m.Method)
		}
	}
	if len(replies) != 0 {
		t.Errorf("empty policy must produce no error replies, got %d", len(replies))
	}
}

// TestGate_HandshakeNeverGated: initialize, notifications/initialized, and
// tools/list cross the gate untouched even with a non-empty block-list — the MCP
// handshake must keep working (the broker's hardest constraint).
func TestGate_HandshakeNeverGated(t *testing.T) {
	st := NewGateStagePolicy(blockPolicy("kill_app"))
	var replies []*mcp.Message
	msgs := []*mcp.Message{
		{JSONRPC: "2.0", ID: json.RawMessage("0"), Method: "initialize", Params: json.RawMessage(`{"protocolVersion":"2024-11-05"}`)},
		{JSONRPC: "2.0", Method: "notifications/initialized"}, // a notification: no id
		{JSONRPC: "2.0", ID: json.RawMessage("1"), Method: "tools/list"},
	}
	for _, m := range msgs {
		out, err := st.Process(gateCtx(NewInflightMap(), &replies), m)
		if err != nil {
			t.Fatalf("%s errored: %v", m.Method, err)
		}
		if out != m {
			t.Errorf("%s must pass through untouched", m.Method)
		}
	}
	if len(replies) != 0 {
		t.Errorf("handshake messages must not produce error replies, got %d", len(replies))
	}
}

// TestGate_BenignToolNotBlocked: a tools/call NOT on the block-list is forwarded;
// the membership check is exact.
func TestGate_BenignToolNotBlocked(t *testing.T) {
	st := NewGateStagePolicy(blockPolicy("kill_app"))
	var replies []*mcp.Message
	m := callMsg("5", "click", `{"pid":1,"window_id":2,"element_index":3}`)
	out, err := st.Process(gateCtx(NewInflightMap(), &replies), m)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("a benign tool-call must be forwarded")
	}
	if len(replies) != 0 {
		t.Fatalf("benign tool must produce no error reply, got %d", len(replies))
	}
}

// TestGate_BlockedToolReturnsErrorAndDrops: kill_app is refused with a
// well-formed JSON-RPC error carrying the original id and ErrToolBlocked, and the
// call is NOT forwarded. Mirror of TestArbitrate_BusyReturnsErrorAndDrops.
func TestGate_BlockedToolReturnsErrorAndDrops(t *testing.T) {
	st := NewGateStagePolicy(blockPolicy("kill_app"))
	inflight := NewInflightMap()
	inflight.Record("9", InflightEntry{Method: "tools/call", ToolName: "kill_app"})
	var replies []*mcp.Message

	m := callMsg("9", "kill_app", `{"pid":1}`)
	out, err := st.Process(gateCtx(inflight, &replies), m)
	if err != nil {
		t.Fatal(err)
	}
	if out != nil {
		t.Fatal("a blocked call must be dropped (never forwarded to the backend)")
	}
	if len(replies) != 1 {
		t.Fatalf("expected one blocked error reply, got %d", len(replies))
	}
	r := replies[0]
	if r.IDString() != "9" {
		t.Errorf("error reply id = %q, want \"9\"", r.IDString())
	}
	var eo struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(r.Error, &eo); err != nil {
		t.Fatalf("error object invalid: %v", err)
	}
	if eo.Code != ErrToolBlocked {
		t.Errorf("error code = %d, want %d", eo.Code, ErrToolBlocked)
	}
	if eo.Message == "" {
		t.Error("blocked error must carry a non-empty message")
	}
}

// TestGate_OverrideAllowsBlockedTool: a tool on BOTH the block-list and the
// allow-list is forwarded — the override wins (the config/env escape hatch).
func TestGate_OverrideAllowsBlockedTool(t *testing.T) {
	st := NewGateStagePolicy(Policy{
		BlockedTools: toSet([]string{"kill_app"}),
		AllowedTools: toSet([]string{"kill_app"}),
	})
	var replies []*mcp.Message
	m := callMsg("3", "kill_app", `{"pid":1}`)
	out, err := st.Process(gateCtx(NewInflightMap(), &replies), m)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("an allow-listed tool must be forwarded even when it is also blocked")
	}
	if len(replies) != 0 {
		t.Fatalf("an overridden tool must produce no error reply, got %d", len(replies))
	}
}

// TestGate_BlockedToolConsumesInflight: after a block, the inflight entry the
// pump recorded is cleaned up, so a never-arriving response cannot leak the map.
func TestGate_BlockedToolConsumesInflight(t *testing.T) {
	st := NewGateStagePolicy(blockPolicy("kill_app"))
	inflight := NewInflightMap()
	inflight.Record("9", InflightEntry{Method: "tools/call", ToolName: "kill_app"})
	var replies []*mcp.Message

	if _, err := st.Process(gateCtx(inflight, &replies), callMsg("9", "kill_app", `{"pid":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, ok := inflight.Peek("9"); ok {
		t.Error("a blocked call must consume its inflight entry (no leak for a never-arriving response)")
	}
}

// TestGate_NilReplyIsNoOp: blocking with a nil Reply returns (nil, nil) without
// panicking — defensive guard matching the ArbitrateStage idiom.
func TestGate_NilReplyIsNoOp(t *testing.T) {
	st := NewGateStagePolicy(blockPolicy("kill_app"))
	ctx := &Context{Identity: identity.Identity{ID: "agent"}, Backend: "test", Dir: Inbound}
	out, err := st.Process(ctx, callMsg("9", "kill_app", `{"pid":1}`))
	if err != nil {
		t.Fatalf("nil Reply must not error: %v", err)
	}
	if out != nil {
		t.Fatal("a blocked call must still be dropped even with a nil Reply")
	}
}

// TestGate_OutboundMessageAlwaysPasses: a message on the OUTBOUND path is never
// gated, even one shaped like a blocked tools/call (a mis-routed request). The
// direction guard protects the response stream.
func TestGate_OutboundMessageAlwaysPasses(t *testing.T) {
	st := NewGateStagePolicy(blockPolicy("kill_app"))
	var replies []*mcp.Message
	m := callMsg("9", "kill_app", `{"pid":1}`)
	ctx := &Context{Identity: identity.Identity{ID: "agent"}, Backend: "test", Dir: Outbound}
	out, err := st.Process(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if out != m {
		t.Fatal("an outbound message must pass through untouched")
	}
	if len(replies) != 0 {
		t.Errorf("outbound path must not reply, got %d", len(replies))
	}
}

// TestGate_UnparseableParamsPassThrough: a tools/call whose params is not an
// object (a JSON array) is forwarded rather than blocked or errored — graceful
// degradation lets the backend reject the malformed call.
func TestGate_UnparseableParamsPassThrough(t *testing.T) {
	st := NewGateStagePolicy(blockPolicy("kill_app"))
	var replies []*mcp.Message
	m := &mcp.Message{JSONRPC: "2.0", ID: json.RawMessage("9"), Method: "tools/call", Params: json.RawMessage(`["not","an","object"]`)}
	out, err := st.Process(gateCtx(NewInflightMap(), &replies), m)
	if err != nil {
		t.Fatal(err)
	}
	if out != m {
		t.Fatal("a tools/call with unparseable params must pass through")
	}
	if len(replies) != 0 {
		t.Errorf("unparseable params must not produce an error reply, got %d", len(replies))
	}
}

// TestBroker_GatePreventsForward drives the real inbound pump with a blocked
// tools/call and a benign one: the backend must receive ONLY the benign call,
// and the client (via the Reply closure, the same path ServeStdio uses) must
// receive a well-formed ErrToolBlocked error for the blocked id. Mirrors
// TestBroker_InboundRecordOutboundTrim's in-memory-pipe wiring.
func TestBroker_GatePreventsForward(t *testing.T) {
	b, err := New(&config.Config{}) // policy = built-in defaults (kill_app blocked)
	if err != nil {
		t.Fatal(err)
	}
	b.audit = audit.New(io.Discard)
	inflight := NewInflightMap()
	id := identity.New()

	// The client sends a blocked call (kill_app, id 1) then a benign one
	// (click, id 2). Only the click should reach the backend.
	clientReqs := []string{
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"kill_app","arguments":{"pid":1}}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"click","arguments":{"pid":1,"window_id":2,"element_index":3}}}`,
	}

	inClientR, inClientW := io.Pipe()   // client writes requests here
	inBackendR, inBackendW := io.Pipe() // backend reads forwarded requests here
	clientSrc := mcp.NewConn(inClientR, io.Discard)
	backendDst := mcp.NewConn(inBackendR, inBackendW)

	// Reply captures the out-of-band error the gate sends back to the client.
	var replies []*mcp.Message
	reply := func(m *mcp.Message) error {
		replies = append(replies, m)
		return nil
	}

	go func() {
		for _, r := range clientReqs {
			_, _ = inClientW.Write([]byte(r + "\n"))
		}
		_ = inClientW.Close()
	}()
	go func() { _ = b.pump(id, "test", Inbound, inflight, reply, clientSrc, backendDst, b.inbound) }()

	// The backend should receive exactly ONE forwarded request: the benign click.
	got := readAllMessages(t, backendDst, 1)
	if got[0].IDString() != "2" {
		t.Fatalf("backend received id %q, want \"2\" (the benign click; kill_app must be blocked)", got[0].IDString())
	}
	if tn := toolNameIf(got[0]); tn != "click" {
		t.Errorf("forwarded tool = %q, want \"click\"", tn)
	}

	// The client should have received exactly one error reply, for the blocked id.
	if len(replies) != 1 {
		t.Fatalf("expected one blocked error reply to the client, got %d", len(replies))
	}
	if replies[0].IDString() != "1" {
		t.Errorf("error reply id = %q, want \"1\"", replies[0].IDString())
	}
	var eo struct {
		Code int `json:"code"`
	}
	if err := json.Unmarshal(replies[0].Error, &eo); err != nil {
		t.Fatalf("error object invalid: %v", err)
	}
	if eo.Code != ErrToolBlocked {
		t.Errorf("error code = %d, want %d", eo.Code, ErrToolBlocked)
	}
	// The blocked id must not leak an inflight entry; the benign one is still
	// recorded (its response has not arrived).
	if _, ok := inflight.Peek("1"); ok {
		t.Error("blocked call must not leave a dangling inflight entry")
	}
	if _, ok := inflight.Peek("2"); !ok {
		t.Error("the forwarded benign call must still be recorded inflight")
	}
}

// TestPolicyFromConfig verifies the broker assembles the gate policy: built-in
// defaults are always blocked, config adds to the block-list, and the config +
// USHER_ALLOW_TOOLS env list override (unblock) a tool.
func TestPolicyFromConfig(t *testing.T) {
	t.Run("built-in defaults are blocked", func(t *testing.T) {
		p := policyFromConfig(&config.Config{})
		if !p.blocks("kill_app") {
			t.Error("kill_app must be blocked by the built-in default set")
		}
	})

	t.Run("config adds to the block-list", func(t *testing.T) {
		p := policyFromConfig(&config.Config{BlockedTools: []string{"drag"}})
		if !p.blocks("drag") {
			t.Error("a config-blocked tool must be refused")
		}
		if !p.blocks("kill_app") {
			t.Error("config additions must not drop the built-in defaults")
		}
	})

	t.Run("config allow-list overrides", func(t *testing.T) {
		p := policyFromConfig(&config.Config{AllowedTools: []string{"kill_app"}})
		if p.blocks("kill_app") {
			t.Error("a config-allowed tool must override the built-in block")
		}
	})

	t.Run("env allow-list overrides", func(t *testing.T) {
		t.Setenv(config.EnvAllowTools, "kill_app, send")
		p := policyFromConfig(&config.Config{})
		if p.blocks("kill_app") {
			t.Error("USHER_ALLOW_TOOLS must unblock kill_app")
		}
		if p.blocks("send") {
			t.Error("USHER_ALLOW_TOOLS entries are comma-split and trimmed")
		}
		if !p.blocks("submit") {
			t.Error("a default tool not in the env list must stay blocked")
		}
	})
}

// TestNew_InboundPipelineHasGatePolicy verifies New wires a policy-bearing gate
// onto the inbound pipeline (not the empty pass-through stub).
func TestNew_InboundPipelineHasGatePolicy(t *testing.T) {
	b, err := New(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	var gate *GateStage
	for _, s := range b.inbound.stages {
		if g, ok := s.(*GateStage); ok {
			gate = g
			break
		}
	}
	if gate == nil {
		t.Fatal("inbound pipeline has no GateStage")
	}
	if len(gate.policy.BlockedTools) == 0 {
		t.Error("the inbound gate must carry the built-in block-list, not an empty policy")
	}
}

// TestPolicy_BlocksRespectsAllowList unit-tests the membership logic directly:
// the allow-list always wins over the block-list.
func TestPolicy_BlocksRespectsAllowList(t *testing.T) {
	cases := []struct {
		name    string
		policy  Policy
		tool    string
		blocked bool
	}{
		{"blocked, not allowed", Policy{BlockedTools: toSet([]string{"send"})}, "send", true},
		{"not in block-list", Policy{BlockedTools: toSet([]string{"send"})}, "click", false},
		{"blocked but allowed", Policy{BlockedTools: toSet([]string{"send"}), AllowedTools: toSet([]string{"send"})}, "send", false},
		{"empty policy", Policy{}, "kill_app", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.policy.blocks(tc.tool); got != tc.blocked {
				t.Errorf("blocks(%q) = %v, want %v", tc.tool, got, tc.blocked)
			}
		})
	}
}
