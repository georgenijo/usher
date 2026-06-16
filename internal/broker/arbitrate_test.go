package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/georgenijo/usher/internal/identity"
	"github.com/georgenijo/usher/internal/mcp"
)

// arbCtx builds an inbound Context wired with a lock registry, inflight map, and
// a capturing Reply, as the inbound pump would.
func arbCtx(locks *lockRegistry, inflight *InflightMap, owner string, replies *[]*mcp.Message) *Context {
	return &Context{
		Identity: identity.Identity{ID: owner},
		Backend:  "test",
		Dir:      Inbound,
		Inflight: inflight,
		Locks:    locks,
		Reply: func(m *mcp.Message) error {
			*replies = append(*replies, m)
			return nil
		},
	}
}

// callMsg builds a tools/call request message with the given id, tool, and args.
func callMsg(id, tool, args string) *mcp.Message {
	params, _ := json.Marshal(map[string]json.RawMessage{
		"name":      json.RawMessage(`"` + tool + `"`),
		"arguments": json.RawMessage(args),
	})
	return &mcp.Message{JSONRPC: "2.0", ID: json.RawMessage(id), Method: "tools/call", Params: params}
}

// respMsg builds a result response for the given id.
func respMsg(id string) *mcp.Message {
	return &mcp.Message{JSONRPC: "2.0", ID: json.RawMessage(id), Result: json.RawMessage(`{"content":[{"type":"text","text":"ok"}]}`)}
}

// TestArbitrate_AcquireForwardsAndStamps: a mutating tool-call is forwarded and
// its lock is stamped into the inflight entry so the response can release it.
func TestArbitrate_AcquireForwardsAndStamps(t *testing.T) {
	locks := newLockRegistry(time.Minute, time.Minute)
	inflight := NewInflightMap()
	inflight.Record("7", InflightEntry{Method: "tools/call", ToolName: "click"})
	var replies []*mcp.Message

	st := NewArbitrateStage()
	m := callMsg("7", "click", `{"pid":1,"window_id":2,"element_index":3}`)
	out, err := st.Process(arbCtx(locks, inflight, "agent-A", &replies), m)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("a mutating call that acquired its lock must be forwarded")
	}
	if len(replies) != 0 {
		t.Fatal("no error reply expected when the lock is free")
	}
	entry, ok := inflight.Peek("7")
	if !ok || !entry.Locked {
		t.Fatal("lock must be stamped into the inflight entry")
	}
	if entry.LockKey.pid != 1 || entry.LockKey.windowID != 2 {
		t.Errorf("stamped key = %+v, want pid=1 window=2", entry.LockKey)
	}
}

// TestArbitrate_ReadOnlyNeverLocks: a read-only tool is forwarded without ever
// touching the registry, even while a write-lock is held on the same window.
func TestArbitrate_ReadOnlyNeverLocks(t *testing.T) {
	locks := newLockRegistry(time.Minute, 30*time.Millisecond)
	key := windowKey{pid: 1, windowID: 2}
	locks.Acquire(key, "writer") // hold the window
	inflight := NewInflightMap()
	inflight.Record("8", InflightEntry{Method: "tools/call", ToolName: "get_window_state"})
	var replies []*mcp.Message

	st := NewArbitrateStage()
	m := callMsg("8", "get_window_state", `{"pid":1,"window_id":2}`)
	// This must return promptly (no blocking on the held write-lock).
	done := make(chan *mcp.Message, 1)
	go func() {
		out, _ := st.Process(arbCtx(locks, inflight, "reader", &replies), m)
		done <- out
	}()
	select {
	case out := <-done:
		if out == nil {
			t.Fatal("a read-only call must be forwarded, not dropped")
		}
	case <-time.After(time.Second):
		t.Fatal("read-only call blocked on a held write-lock — reads must never block")
	}
	if entry, _ := inflight.Peek("8"); entry.Locked {
		t.Error("read-only call must not stamp a lock")
	}
}

// TestArbitrate_BusyReturnsErrorAndDrops: a second writer on a held window is
// refused with a JSON-RPC busy error and its call is NOT forwarded.
func TestArbitrate_BusyReturnsErrorAndDrops(t *testing.T) {
	locks := newLockRegistry(time.Minute, 30*time.Millisecond)
	locks.Acquire(windowKey{pid: 1, windowID: 2}, "holder")
	inflight := NewInflightMap()
	inflight.Record("9", InflightEntry{Method: "tools/call", ToolName: "click"})
	var replies []*mcp.Message

	st := NewArbitrateStage()
	m := callMsg("9", "click", `{"pid":1,"window_id":2}`)
	out, err := st.Process(arbCtx(locks, inflight, "contender", &replies), m)
	if err != nil {
		t.Fatal(err)
	}
	if out != nil {
		t.Fatal("a refused call must be dropped (not forwarded to the backend)")
	}
	if len(replies) != 1 {
		t.Fatalf("expected one busy error reply, got %d", len(replies))
	}
	// The reply is a JSON-RPC error carrying the request id and the busy code.
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
	if eo.Code != ErrWindowBusy {
		t.Errorf("error code = %d, want %d", eo.Code, ErrWindowBusy)
	}
}

// TestArbitrate_ReleaseOnResponse: the outbound pass releases the lock the
// request took, so the next writer on that window can then acquire it.
func TestArbitrate_ReleaseOnResponse(t *testing.T) {
	locks := newLockRegistry(time.Minute, 30*time.Millisecond)
	inflight := NewInflightMap()
	inflight.Record("7", InflightEntry{Method: "tools/call", ToolName: "click"})
	var replies []*mcp.Message
	st := NewArbitrateStage()

	// Inbound: acquire.
	if _, err := st.Process(arbCtx(locks, inflight, "A", &replies), callMsg("7", "click", `{"pid":1,"window_id":2}`)); err != nil {
		t.Fatal(err)
	}
	// While held, another writer is refused.
	if _, res := locks.Acquire(windowKey{pid: 1, windowID: 2}, "B"); res != timedOut {
		t.Fatal("window should be held after acquire")
	}

	// Outbound: the matching response releases the lock.
	outCtx := &Context{Identity: identity.Identity{ID: "A"}, Backend: "test", Dir: Outbound, Inflight: inflight, Locks: locks}
	if _, err := st.Process(outCtx, respMsg("7")); err != nil {
		t.Fatal(err)
	}
	// The entry must still be present for the downstream TrimStage to Consume.
	if _, ok := inflight.Peek("7"); !ok {
		t.Error("release must not consume the inflight entry (TrimStage still needs it)")
	}
	// Now the window is free.
	if _, res := locks.Acquire(windowKey{pid: 1, windowID: 2}, "C"); res != acquired {
		t.Error("window should be free after the matching response released it")
	}
}

// TestArbitrate_ReleaseIgnoresUnlockedResponse: a response to an ungated request
// (read-only, or a request that took no lock) is passed through untouched.
func TestArbitrate_ReleaseIgnoresUnlockedResponse(t *testing.T) {
	locks := newLockRegistry(time.Minute, time.Minute)
	inflight := NewInflightMap()
	inflight.Record("5", InflightEntry{Method: "tools/call", ToolName: "get_window_state"})
	st := NewArbitrateStage()
	outCtx := &Context{Identity: identity.Identity{ID: "A"}, Backend: "test", Dir: Outbound, Inflight: inflight, Locks: locks}
	out, err := st.Process(outCtx, respMsg("5"))
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("an unlocked response must pass through")
	}
}

// TestArbitrate_NilLocksPassThrough: with no registry wired, the stage is the
// skeleton pass-through (back-compat for the trim-only test broker).
func TestArbitrate_NilLocksPassThrough(t *testing.T) {
	st := NewArbitrateStage()
	ctx := &Context{Identity: identity.New(), Backend: "test", Dir: Inbound}
	m := callMsg("7", "click", `{"pid":1,"window_id":2}`)
	out, err := st.Process(ctx, m)
	if err != nil || out == nil {
		t.Fatalf("nil registry must be a pass-through: out=%v err=%v", out, err)
	}
}

// TestArbitrate_HandshakePassesThrough: initialize / notifications/initialized /
// tools/list must cross the stage untouched (no lock, no error), preserving the
// MCP handshake — the broker's hardest constraint.
func TestArbitrate_HandshakePassesThrough(t *testing.T) {
	locks := newLockRegistry(time.Minute, time.Minute)
	inflight := NewInflightMap()
	var replies []*mcp.Message
	st := NewArbitrateStage()

	msgs := []*mcp.Message{
		{JSONRPC: "2.0", ID: json.RawMessage("0"), Method: "initialize", Params: json.RawMessage(`{"protocolVersion":"2024-11-05"}`)},
		{JSONRPC: "2.0", Method: "notifications/initialized"}, // a notification: no id
		{JSONRPC: "2.0", ID: json.RawMessage("1"), Method: "tools/list"},
	}
	for _, m := range msgs {
		out, err := st.Process(arbCtx(locks, inflight, "agent", &replies), m)
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

// TestArbitrate_TwoWritersSerializeThroughStage: two stage invocations on the
// same window serialize — the first holds, the second blocks until the first's
// response releases, then acquires. This is the cross-session contention path
// the broker arbitrates (each "session" is a distinct owner).
func TestArbitrate_TwoWritersSerializeThroughStage(t *testing.T) {
	locks := newLockRegistry(time.Minute, time.Second)
	inflight := NewInflightMap()
	st := NewArbitrateStage()
	var replies []*mcp.Message
	winArgs := `{"pid":1,"window_id":2}`

	// Writer A acquires.
	inflight.Record("1", InflightEntry{Method: "tools/call", ToolName: "click"})
	if out, _ := st.Process(arbCtx(locks, inflight, "A", &replies), callMsg("1", "click", winArgs)); out == nil {
		t.Fatal("writer A should acquire and be forwarded")
	}

	// Writer B (a different owner) blocks on the same window.
	inflight.Record("2", InflightEntry{Method: "tools/call", ToolName: "click"})
	bDone := make(chan *mcp.Message, 1)
	go func() {
		out, _ := st.Process(arbCtx(locks, inflight, "B", &replies), callMsg("2", "click", winArgs))
		bDone <- out
	}()
	select {
	case <-bDone:
		t.Fatal("writer B proceeded while writer A held the window")
	case <-time.After(50 * time.Millisecond):
	}

	// A's response releases; B then acquires and is forwarded.
	outCtx := &Context{Identity: identity.Identity{ID: "A"}, Backend: "test", Dir: Outbound, Inflight: inflight, Locks: locks}
	if _, err := st.Process(outCtx, respMsg("1")); err != nil {
		t.Fatal(err)
	}
	select {
	case out := <-bDone:
		if out == nil {
			t.Fatal("writer B should have been forwarded after A released")
		}
		if entry, _ := inflight.Peek("2"); !entry.Locked {
			t.Error("writer B should now hold the lock")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("writer B did not proceed after A released the window")
	}
}
