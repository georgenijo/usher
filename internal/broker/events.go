package broker

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/georgenijo/usher/internal/audit"
)

// events.go is usher's internal event bus. The broker emits structured events as
// messages cross the front desk — a connection opening or closing, a request
// being routed to a backend, a response coming back, a backend changing state, a
// destructive tool being blocked, a window-lock taken or freed — and a Hub fans
// each event to every subscriber. The live UI's SSE stream is one subscriber; the
// connection/lifecycle audit log is another.
//
// The single hard rule is that emitting an event must NEVER block the hot path:
// Emit is a non-blocking send, and a subscriber whose buffer is full drops its
// OLDEST queued event rather than back-pressuring the broker pump. A frozen
// browser tab (a slow SSE reader) therefore loses intermediate events but can
// never stall a tools/call. This is what lets the bus sit on the forwarding path
// without adding latency to it.

// Event is one thing that happened at the front desk. Go has no sum type, so the
// union is a small interface plus concrete structs (the same idiom the pipeline's
// Stage interface uses). kind() is the stable wire tag the SSE layer marshals as
// the event's "type"; it doubles as the SSE event name.
type Event interface {
	// kind returns the stable wire tag, e.g. "conn.open", "request".
	kind() string
}

// ConnOpenEvent is emitted when an agent connection completes its initialize
// handshake and is bound to a backend.
type ConnOpenEvent struct {
	TS      time.Time `json:"ts"`
	ConnID  string    `json:"connID"`
	PID     int       `json:"pid"`
	Backend string    `json:"backend"`
}

// ConnCloseEvent is emitted when an agent connection ends, with the reason the
// disconnect audit line carries (client-eof / signal / a transport error).
type ConnCloseEvent struct {
	TS     time.Time `json:"ts"`
	ConnID string    `json:"connID"`
	Reason string    `json:"reason"`
}

// RequestEvent is emitted when a client request is routed to a backend: which
// connection (and its agent PID) is calling which backend and tool, under which
// JSON-RPC id. Tool is "" for non-tools/call requests.
type RequestEvent struct {
	TS       time.Time `json:"ts"`
	ConnID   string    `json:"connID"`
	AgentPID int       `json:"agentPID"`
	Backend  string    `json:"backend"`
	Tool     string    `json:"tool"`
	RPCID    string    `json:"rpcID"`
}

// ResponseEvent is emitted when a backend response is forwarded back to a client.
// Bytes is the size of the message as forwarded; TrimmedFromBytes is its size
// before the outbound pipeline ran (equal to Bytes when nothing was trimmed), so
// the UI can show compaction savings.
type ResponseEvent struct {
	TS               time.Time `json:"ts"`
	ConnID           string    `json:"connID"`
	Backend          string    `json:"backend"`
	RPCID            string    `json:"rpcID"`
	Bytes            int       `json:"bytes"`
	TrimmedFromBytes int       `json:"trimmedFromBytes"`
}

// BackendStateEvent is emitted on every backend lifecycle transition (the
// supervisor feature fills this in): stopped→starting→live and so on, so the UI
// can watch a backend come live on demand.
type BackendStateEvent struct {
	TS      time.Time `json:"ts"`
	Backend string    `json:"backend"`
	From    string    `json:"from"`
	To      string    `json:"to"`
}

// GateBlockEvent is emitted when GateStage refuses a destructive tool-call by
// policy, so the UI can surface what was blocked and for whom.
type GateBlockEvent struct {
	TS     time.Time `json:"ts"`
	Tool   string    `json:"tool"`
	ConnID string    `json:"connID"`
}

// LockEvent is emitted when ArbitrateStage takes or frees a per-window
// write-lock. Acquired is true on take, false on release.
type LockEvent struct {
	TS       time.Time `json:"ts"`
	Key      string    `json:"key"`
	ConnID   string    `json:"connID"`
	Acquired bool      `json:"acquired"`
}

func (ConnOpenEvent) kind() string     { return "conn.open" }
func (ConnCloseEvent) kind() string    { return "conn.close" }
func (RequestEvent) kind() string      { return "request" }
func (ResponseEvent) kind() string     { return "response" }
func (BackendStateEvent) kind() string { return "backend.state" }
func (GateBlockEvent) kind() string    { return "gate.block" }
func (LockEvent) kind() string         { return "lock" }

// Kind exposes an event's wire tag to other packages (the SSE layer names the
// SSE event after it). It is the exported view of the unexported kind().
func Kind(e Event) string { return e.kind() }

// MarshalEvent renders an event as JSON with a "type" field injected, the shape
// the SSE/JSON layers ship to the browser: {"type":"request","ts":...,...}. The
// thin envelope wrapper avoids a custom MarshalJSON per event type.
func MarshalEvent(e Event) ([]byte, error) {
	inner, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	// Splice "type" in as the first field. inner is always a JSON object ("{...}"
	// for these structs), so we replace the leading "{" with `{"type":"x",`. An
	// empty object ("{}") is handled by dropping the trailing comma.
	tag, _ := json.Marshal(e.kind())
	prefix := append([]byte(`{"type":`), tag...)
	if len(inner) == 2 { // "{}" — no other fields
		return append(prefix, '}'), nil
	}
	prefix = append(prefix, ',')
	return append(prefix, inner[1:]...), nil
}

// Hub fans events to a dynamic set of subscribers. Emit is non-blocking
// (drop-oldest on a full subscriber); Subscribe hands out a buffered channel and
// an unsubscribe func. Safe for concurrent use by every pump goroutine plus the
// HTTP handlers that subscribe.
type Hub struct {
	mu   sync.RWMutex
	subs map[int]chan Event
	next int
}

// NewHub returns an empty hub with no subscribers.
func NewHub() *Hub {
	return &Hub{subs: make(map[int]chan Event)}
}

// Emit fans e to every subscriber non-blocking. A subscriber whose buffer is
// full has its OLDEST queued event dropped to make room for the newest, so a slow
// reader loses history but never back-pressures the caller — the hot-path latency
// guarantee. A nil hub is a no-op so the pump can call Emit unconditionally even
// on a path (e.g. a bare test broker) that never built one.
func (h *Hub) Emit(e Event) {
	if h == nil {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, ch := range h.subs {
		select {
		case ch <- e:
		default:
			// Slow subscriber: drop the oldest queued event, then retry once. Both
			// the drain and the resend are non-blocking, so a subscriber that fills
			// its buffer between the two selects simply drops this event too —
			// either way Emit returns without ever waiting on a reader.
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- e:
			default:
			}
		}
	}
}

// Publish is an alias for Emit, the name the broker's lifecycle call sites read
// more naturally with ("the pump publishes a Request event"). Identical
// non-blocking semantics.
func (h *Hub) Publish(e Event) { h.Emit(e) }

// Subscribe registers a new subscriber and returns its receive channel plus an
// unsubscribe func. buf is the per-subscriber buffer depth: a deeper buffer
// tolerates a slower reader before it starts dropping. The unsubscribe func is
// idempotent and closes the channel, so a ranging reader sees the channel close
// and exits. Callers must drain (or stop ranging) only after calling it.
func (h *Hub) Subscribe(buf int) (<-chan Event, func()) {
	if buf < 1 {
		buf = 1
	}
	ch := make(chan Event, buf)
	h.mu.Lock()
	id := h.next
	h.next++
	h.subs[id] = ch
	h.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			h.mu.Lock()
			if c, ok := h.subs[id]; ok {
				delete(h.subs, id)
				close(c)
			}
			h.mu.Unlock()
		})
	}
	return ch, cancel
}

// SubscriberCount reports the number of live subscribers — used by tests and by
// a future UI "N watchers" readout.
func (h *Hub) SubscriberCount() int {
	if h == nil {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs)
}

// RunAuditSubscriber consumes the Hub and writes connection/lifecycle audit
// lines, so the connection-level record (connect / disconnect / backend
// transitions) becomes event-driven — the audit log is "one subscriber, the SSE
// stream another". The per-MESSAGE wire log stays in AuditStage on the pipeline;
// this subscriber owns only the lifecycle-level lines. It returns when ctx is
// cancelled or the hub closes the subscription.
//
// It is started in its own goroutine on the daemon path; a nil hub or logger
// makes it a no-op so a bare test broker can skip it.
func RunAuditSubscriber(ctx context.Context, bus *Hub, log *audit.Logger) {
	if bus == nil || log == nil {
		return
	}
	ch, cancel := bus.Subscribe(256)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-ch:
			if !ok {
				return
			}
			switch ev := e.(type) {
			case ConnOpenEvent:
				log.ConnectID(ev.ConnID, ev.PID, ev.Backend)
			case ConnCloseEvent:
				log.DisconnectID(ev.ConnID, ev.Reason)
			case BackendStateEvent:
				log.Errorf(ev.Backend, "backend state %s→%s", ev.From, ev.To)
			}
		}
	}
}
