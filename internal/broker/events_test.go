package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/georgenijo/usher/internal/audit"
	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/identity"
	"github.com/georgenijo/usher/internal/mcp"
)

// syncBuffer is a concurrency-safe io.Writer the audit subscriber goroutine
// writes to while the test reads — the audit.Logger's underlying log.Logger
// serialises its own writes, but the test reads from another goroutine, so the
// buffer access itself must be guarded.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) string() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *syncBuffer) contains(s string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Contains(b.buf.String(), s)
}

// drainEvents reads up to n events off ch, returning early if it stalls past the
// deadline — so a test that expects N events fails fast instead of hanging.
func drainEvents(t *testing.T, ch <-chan Event, n int) []Event {
	t.Helper()
	var got []Event
	deadline := time.After(2 * time.Second)
	for len(got) < n {
		select {
		case e := <-ch:
			got = append(got, e)
		case <-deadline:
			t.Fatalf("only received %d/%d events before deadline", len(got), n)
		}
	}
	return got
}

// TestHub_FanOutToEverySubscriber: one Emit reaches every live subscriber.
func TestHub_FanOutToEverySubscriber(t *testing.T) {
	h := NewHub()
	chA, cancelA := h.Subscribe(4)
	chB, cancelB := h.Subscribe(4)
	defer cancelA()
	defer cancelB()

	if h.SubscriberCount() != 2 {
		t.Fatalf("subscriber count = %d, want 2", h.SubscriberCount())
	}

	want := RequestEvent{ConnID: "c1", Tool: "click", RPCID: "7"}
	h.Emit(want)

	for name, ch := range map[string]<-chan Event{"A": chA, "B": chB} {
		got := drainEvents(t, ch, 1)[0]
		re, ok := got.(RequestEvent)
		if !ok {
			t.Fatalf("subscriber %s got %T, want RequestEvent", name, got)
		}
		if re.ConnID != "c1" || re.Tool != "click" || re.RPCID != "7" {
			t.Errorf("subscriber %s got %+v, want %+v", name, re, want)
		}
	}
}

// TestHub_UnsubscribeStopsDelivery: a cancelled subscriber receives no further
// events, its channel is closed, and the count drops.
func TestHub_UnsubscribeStopsDelivery(t *testing.T) {
	h := NewHub()
	ch, cancel := h.Subscribe(4)
	cancel()

	if h.SubscriberCount() != 0 {
		t.Fatalf("subscriber count after cancel = %d, want 0", h.SubscriberCount())
	}
	// The channel is closed: a receive yields the zero value with ok=false.
	if _, ok := <-ch; ok {
		t.Error("cancel must close the subscriber channel")
	}
	// A second cancel is a harmless no-op (idempotent).
	cancel()
	// Emitting with no subscribers must not panic.
	h.Emit(ConnOpenEvent{ConnID: "x"})
}

// TestHub_DropOldestNeverBlocks: a subscriber that never drains must not stall
// the publisher; once its buffer is full the OLDEST queued event is dropped and
// Emit returns promptly. We fill a depth-2 buffer then emit more, and assert both
// that Emit never blocks and that the surviving events are the NEWEST.
func TestHub_DropOldestNeverBlocks(t *testing.T) {
	h := NewHub()
	ch, cancel := h.Subscribe(2) // buffer depth 2
	defer cancel()

	// Emit four events into a depth-2 buffer WITHOUT draining. Each Emit must
	// return; a blocking publisher would deadlock this goroutine and the test
	// would time out.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 4; i++ {
			h.Emit(ConnCloseEvent{Reason: rpcIDForIndex(i)})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Emit blocked on a full subscriber buffer — the hot-path guarantee is broken")
	}

	// Drain the surviving events. With drop-oldest on a depth-2 buffer fed 4
	// events, the two NEWEST (indices 2 and 3) survive; the oldest two are dropped.
	got := drainEvents(t, ch, 2)
	survivors := []string{got[0].(ConnCloseEvent).Reason, got[1].(ConnCloseEvent).Reason}
	want := []string{rpcIDForIndex(2), rpcIDForIndex(3)}
	for i := range want {
		if survivors[i] != want[i] {
			t.Errorf("survivor[%d] = %q, want %q (drop-oldest keeps the newest)", i, survivors[i], want[i])
		}
	}
}

// rpcIDForIndex turns a small int into a stable label for the drop-oldest test.
func rpcIDForIndex(i int) string { return string(rune('0' + i)) }

// TestHub_OneSlowSubscriberDoesNotStarveOthers: a frozen subscriber (never
// drains) must not prevent a healthy subscriber from receiving every event. This
// is the "a slow browser tab can't stall the broker" guarantee end to end.
func TestHub_OneSlowSubscriberDoesNotStarveOthers(t *testing.T) {
	h := NewHub()
	slow, cancelSlow := h.Subscribe(1) // never drained
	fast, cancelFast := h.Subscribe(64)
	defer cancelSlow()
	defer cancelFast()
	_ = slow

	const n = 32
	for i := 0; i < n; i++ {
		h.Emit(GateBlockEvent{Tool: "send", ConnID: rpcIDForIndex(i % 10)})
	}
	// The fast subscriber, with a buffer >= n, must have every event.
	got := drainEvents(t, fast, n)
	if len(got) != n {
		t.Fatalf("fast subscriber received %d events, want %d (a slow peer must not drop its events)", len(got), n)
	}
}

// TestHub_NilHubEmitIsNoOp: a nil Hub (a path that never built one) tolerates
// Emit/Publish/SubscriberCount without panicking, so the pump can call Emit
// unconditionally.
func TestHub_NilHubEmitIsNoOp(t *testing.T) {
	var h *Hub
	h.Emit(ConnOpenEvent{})
	h.Publish(ConnCloseEvent{})
	if h.SubscriberCount() != 0 {
		t.Error("nil hub must report zero subscribers")
	}
}

// TestHub_ConcurrentEmitAndSubscribe stresses the Hub under -race: many emitters
// fan to a churning set of subscribers. It asserts no panic/data race rather than
// exact delivery (drop-oldest makes counts nondeterministic).
func TestHub_ConcurrentEmitAndSubscribe(t *testing.T) {
	h := NewHub()
	var wg sync.WaitGroup

	// Emitters.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				h.Emit(RequestEvent{ConnID: "c", RPCID: "x"})
			}
		}()
	}
	// Subscribers that come and go, draining as they can.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				ch, cancel := h.Subscribe(8)
				select {
				case <-ch:
				default:
				}
				cancel()
			}
		}()
	}
	wg.Wait()
}

// TestMarshalEvent_InjectsType: the JSON the SSE layer ships carries a "type"
// tag equal to the event's kind, alongside the event's own fields.
func TestMarshalEvent_InjectsType(t *testing.T) {
	cases := []struct {
		event    Event
		wantType string
		wantKey  string // a field that must survive the splice
		wantVal  string
	}{
		{RequestEvent{ConnID: "c1", Tool: "click", RPCID: "7"}, "request", "tool", "click"},
		{ConnOpenEvent{ConnID: "c1", Backend: "cua"}, "conn.open", "backend", "cua"},
		{BackendStateEvent{Backend: "cua", From: "stopped", To: "live"}, "backend.state", "to", "live"},
		{GateBlockEvent{Tool: "send", ConnID: "c2"}, "gate.block", "tool", "send"},
		{LockEvent{Key: "pid=1 window=2", ConnID: "c3", Acquired: true}, "lock", "key", "pid=1 window=2"},
	}
	for _, tc := range cases {
		t.Run(tc.wantType, func(t *testing.T) {
			raw, err := MarshalEvent(tc.event)
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]any
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("MarshalEvent produced invalid JSON %q: %v", raw, err)
			}
			if got["type"] != tc.wantType {
				t.Errorf("type = %v, want %q", got["type"], tc.wantType)
			}
			if got[tc.wantKey] != tc.wantVal {
				t.Errorf("%s = %v, want %q (event fields must survive the type splice)", tc.wantKey, got[tc.wantKey], tc.wantVal)
			}
			if Kind(tc.event) != tc.wantType {
				t.Errorf("Kind() = %q, want %q", Kind(tc.event), tc.wantType)
			}
		})
	}
}

// TestMarshalEvent_ResourceSample asserts the per-process resource event splices
// "type":"resource.sample" and carries its per-pid rows through intact (the
// nested array survives the type splice, role-tagged, RSS in KB). It is the wire
// contract the RESOURCES dashboard panel reads.
func TestMarshalEvent_ResourceSample(t *testing.T) {
	ev := ResourceSampleEvent{Procs: []ProcStat{
		{PID: 42, Role: "backend", Label: "cua", RSSKB: 2048, CPUPct: 1.5, Alive: true},
		{PID: 7, Role: "client", Label: "client-c1", RSSKB: 512, Alive: false},
	}}
	raw, err := MarshalEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Type  string     `json:"type"`
		Procs []ProcStat `json:"procs"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("invalid JSON %q: %v", raw, err)
	}
	if got.Type != "resource.sample" {
		t.Errorf("type = %q, want resource.sample", got.Type)
	}
	if len(got.Procs) != 2 {
		t.Fatalf("procs = %d rows, want 2 (per-pid rows must survive the splice)", len(got.Procs))
	}
	if got.Procs[0].PID != 42 || got.Procs[0].Role != "backend" || got.Procs[0].RSSKB != 2048 || !got.Procs[0].Alive {
		t.Errorf("row 0 = %+v, want backend pid=42 rss=2048 alive", got.Procs[0])
	}
	if got.Procs[1].Role != "client" || got.Procs[1].Alive {
		t.Errorf("row 1 = %+v, want client, not alive", got.Procs[1])
	}
}

// TestBroker_PumpEmitsRequestAndResponse drives both pumps over in-memory pipes
// against a real New-built broker (with the bus wired) and asserts the inbound
// pump emits a Request event attributed to the right conn/pid/backend/tool, and
// the outbound pump emits a Response event with byte accounting. This is the
// attribution check from the DONE criteria.
func TestBroker_PumpEmitsRequestAndResponse(t *testing.T) {
	b, err := New(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	b.audit = audit.New(io.Discard)
	ch, cancel := b.bus.Subscribe(16)
	defer cancel()

	inflight := NewInflightMap()
	id := identity.Identity{ID: "conn-42", PID: 9001}

	// Inbound: a benign tools/call (get_window_state — read-only, never gated or
	// locked) so it forwards cleanly and emits exactly one Request event.
	clientReqR, clientReqW := io.Pipe()
	backendR, backendW := io.Pipe()
	clientSrc := mcp.NewConn(clientReqR, io.Discard)
	backendDst := mcp.NewConn(backendR, backendW)
	req := `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"get_window_state","arguments":{"pid":1,"window_id":2}}}`
	go func() {
		_, _ = clientReqW.Write([]byte(req + "\n"))
		_ = clientReqW.Close()
	}()
	go func() { _ = b.pump(id, "cua", Inbound, inflight, nil, clientSrc, backendDst, b.inbound) }()
	_ = readAllMessages(t, backendDst, 1) // backend received the forward

	// Outbound: the matching response.
	resp := `{"jsonrpc":"2.0","id":7,"result":{"content":[{"type":"text","text":"ok"}]}}`
	backendRespR, backendRespW := io.Pipe()
	clientDstR, clientDstW := io.Pipe()
	backendSrc := mcp.NewConn(backendRespR, io.Discard)
	clientDst := mcp.NewConn(clientDstR, clientDstW)
	go func() {
		_, _ = backendRespW.Write([]byte(resp + "\n"))
		_ = backendRespW.Close()
	}()
	go func() { _ = b.pump(id, "cua", Outbound, inflight, nil, backendSrc, clientDst, b.outbound) }()
	_ = readAllMessages(t, clientDst, 1) // client received the response

	// Collect the two lifecycle events (order across the two pumps is not
	// guaranteed, so bucket by type).
	var reqEv *RequestEvent
	var respEv *ResponseEvent
	for _, e := range drainEvents(t, ch, 2) {
		switch ev := e.(type) {
		case RequestEvent:
			reqEv = &ev
		case ResponseEvent:
			respEv = &ev
		}
	}
	if reqEv == nil {
		t.Fatal("inbound pump did not emit a Request event")
	}
	if reqEv.ConnID != "conn-42" || reqEv.AgentPID != 9001 || reqEv.Backend != "cua" {
		t.Errorf("request attribution = %+v, want conn-42/9001/cua", reqEv)
	}
	if reqEv.Tool != "get_window_state" || reqEv.RPCID != "7" {
		t.Errorf("request tool/id = %q/%q, want get_window_state/7", reqEv.Tool, reqEv.RPCID)
	}
	if respEv == nil {
		t.Fatal("outbound pump did not emit a Response event")
	}
	if respEv.ConnID != "conn-42" || respEv.Backend != "cua" || respEv.RPCID != "7" {
		t.Errorf("response attribution = %+v, want conn-42/cua/7", respEv)
	}
	if respEv.Bytes <= 0 || respEv.TrimmedFromBytes <= 0 {
		t.Errorf("response byte accounting = bytes:%d from:%d, both must be >0", respEv.Bytes, respEv.TrimmedFromBytes)
	}
}

// TestBroker_GateBlockEmitsEvent: a blocked destructive tool-call emits a
// GateBlock event (and does NOT emit a Request event, since the call never
// reaches the backend).
func TestBroker_GateBlockEmitsEvent(t *testing.T) {
	b, err := New(&config.Config{}) // built-in defaults block kill_app
	if err != nil {
		t.Fatal(err)
	}
	b.audit = audit.New(io.Discard)
	ch, cancel := b.bus.Subscribe(16)
	defer cancel()

	inflight := NewInflightMap()
	id := identity.Identity{ID: "conn-block", PID: 5}

	clientReqR, clientReqW := io.Pipe()
	backendR, backendW := io.Pipe()
	clientSrc := mcp.NewConn(clientReqR, io.Discard)
	backendDst := mcp.NewConn(backendR, backendW)
	reply := func(*mcp.Message) error { return nil } // swallow the in-band error reply

	go func() {
		_, _ = clientReqW.Write([]byte(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"kill_app","arguments":{"pid":1}}}` + "\n"))
		_ = clientReqW.Close()
	}()
	go func() { _ = b.pump(id, "cua", Inbound, inflight, reply, clientSrc, backendDst, b.inbound) }()

	e := drainEvents(t, ch, 1)[0]
	gb, ok := e.(GateBlockEvent)
	if !ok {
		t.Fatalf("got %T, want GateBlockEvent", e)
	}
	if gb.Tool != "kill_app" || gb.ConnID != "conn-block" {
		t.Errorf("gate block event = %+v, want tool=kill_app conn=conn-block", gb)
	}
	// No further event (no Request) should arrive: the backend never saw the call.
	select {
	case extra := <-ch:
		if _, isReq := extra.(RequestEvent); isReq {
			t.Error("a gated call must not also emit a Request event (it never reached the backend)")
		}
	case <-time.After(100 * time.Millisecond):
	}
}

// TestBroker_LockEmitsAcquireAndRelease: a mutating tool-call emits a Lock
// acquire on the inbound side and a Lock release on the matching response.
func TestBroker_LockEmitsAcquireAndRelease(t *testing.T) {
	b, err := New(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	b.audit = audit.New(io.Discard)
	ch, cancel := b.bus.Subscribe(16)
	defer cancel()

	inflight := NewInflightMap()
	id := identity.Identity{ID: "conn-lock", PID: 3}

	// Inbound: a click (mutating) acquires the window lock.
	cR, cW := io.Pipe()
	bR, bW := io.Pipe()
	clientSrc := mcp.NewConn(cR, io.Discard)
	backendDst := mcp.NewConn(bR, bW)
	go func() {
		_, _ = cW.Write([]byte(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"click","arguments":{"pid":1,"window_id":2,"element_index":3}}}` + "\n"))
		_ = cW.Close()
	}()
	go func() {
		_ = b.pump(id, "cua", Inbound, inflight, func(*mcp.Message) error { return nil }, clientSrc, backendDst, b.inbound)
	}()
	_ = readAllMessages(t, backendDst, 1)

	// Outbound: the matching response releases the lock.
	rR, rW := io.Pipe()
	odR, odW := io.Pipe()
	backendSrc := mcp.NewConn(rR, io.Discard)
	clientDst := mcp.NewConn(odR, odW)
	go func() {
		_, _ = rW.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"ok"}]}}` + "\n"))
		_ = rW.Close()
	}()
	go func() { _ = b.pump(id, "cua", Outbound, inflight, nil, backendSrc, clientDst, b.outbound) }()
	_ = readAllMessages(t, clientDst, 1)

	// Collect lock events (the Request/Response events also flow; filter).
	var acquire, release *LockEvent
	deadline := time.After(2 * time.Second)
	for acquire == nil || release == nil {
		select {
		case e := <-ch:
			if le, ok := e.(LockEvent); ok {
				if le.Acquired {
					acquire = &le
				} else {
					release = &le
				}
			}
		case <-deadline:
			t.Fatalf("missing lock events: acquire=%v release=%v", acquire != nil, release != nil)
		}
	}
	if acquire.ConnID != "conn-lock" || acquire.Key != "pid=1 window=2" {
		t.Errorf("acquire event = %+v, want conn-lock pid=1 window=2", acquire)
	}
	if release.ConnID != "conn-lock" || release.Key != "pid=1 window=2" {
		t.Errorf("release event = %+v, want conn-lock pid=1 window=2", release)
	}
}

// TestRunAuditSubscriber_LogsLifecycle: the audit subscriber consumes ConnOpen /
// ConnClose / BackendState events off the hub and writes the matching audit
// lines — Audit-as-a-subscriber, without changing the per-message wire log.
func TestRunAuditSubscriber_LogsLifecycle(t *testing.T) {
	var buf syncBuffer
	log := audit.New(&buf)
	h := NewHub()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		RunAuditSubscriber(ctx, h, log)
		close(done)
	}()

	// Give the subscriber a moment to register, then emit the lifecycle trio.
	waitForSubscriber(t, h)
	h.Emit(ConnOpenEvent{ConnID: "abc", PID: 123, Backend: "cua"})
	h.Emit(BackendStateEvent{Backend: "cua", From: "stopped", To: "live"})
	h.Emit(ConnCloseEvent{ConnID: "abc", Reason: "client-eof"})

	// Poll the log until all three lines have landed (the subscriber is async).
	waitForContains(t, &buf, "connect id=abc pid=123 backend=cua")
	waitForContains(t, &buf, "backend state stopped→live")
	waitForContains(t, &buf, "disconnect id=abc reason=client-eof")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunAuditSubscriber did not return on ctx cancel")
	}
}

// waitForSubscriber blocks until the hub has at least one subscriber, so an emit
// in a test cannot race ahead of the async subscriber's registration.
func waitForSubscriber(t *testing.T, h *Hub) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for h.SubscriberCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("subscriber never registered")
		case <-time.After(time.Millisecond):
		}
	}
}

// waitForContains polls buf until it contains want, failing past a deadline.
func waitForContains(t *testing.T, buf *syncBuffer, want string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if buf.contains(want) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("audit log never contained %q; got:\n%s", want, buf.string())
		case <-time.After(time.Millisecond):
		}
	}
}
