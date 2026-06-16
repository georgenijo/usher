package control

// registry.go tracks the daemon's LIVE agent connections so the control plane's
// GET /api/connections can answer "who is connected, talking to which backend".
// The broker's event bus is the source of truth: a ConnOpenEvent adds a row, a
// ConnCloseEvent removes it. The registry is one more Hub subscriber alongside the
// audit log and the SSE stream — it never touches the forwarding hot path, and a
// dropped event (the Hub is drop-oldest) at worst leaves a stale row that the next
// open/close reconciles. The UI reads this snapshot to paint the connections panel
// on load; the SSE stream carries the deltas.

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/georgenijo/usher/internal/broker"
)

// ConnInfo is the control-plane view of one live agent connection: which
// connection id, its peer agent PID, the backend it is bound to, and when it
// opened. It is what GET /api/connections marshals.
type ConnInfo struct {
	ConnID   string    `json:"connID"`
	AgentPID int       `json:"agentPID"`
	Backend  string    `json:"backend"`
	OpenedAt time.Time `json:"openedAt"`
}

// connRegistry holds the set of currently-open connections, keyed by connection
// id. It is fed entirely by the event bus (run via Watch) and read by the HTTP
// handler; mu guards both.
type connRegistry struct {
	mu    sync.Mutex
	conns map[string]ConnInfo
}

// newConnRegistry returns an empty registry.
func newConnRegistry() *connRegistry {
	return &connRegistry{conns: make(map[string]ConnInfo)}
}

// Watch consumes the Hub and maintains the live-connection set: a ConnOpenEvent
// adds the row, a ConnCloseEvent removes it. It returns when ctx is cancelled or
// the hub closes the subscription, mirroring RunAuditSubscriber. A nil bus makes
// it a no-op so a bare test server (one with no daemon behind it) can skip it.
func (r *connRegistry) Watch(ctx context.Context, bus *broker.Hub) {
	if bus == nil {
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
			r.apply(e)
		}
	}
}

// apply folds one event into the live-connection set. Only conn open/close move
// the set; every other event type is ignored here (the SSE stream and the audit
// subscriber consume those).
func (r *connRegistry) apply(e broker.Event) {
	switch ev := e.(type) {
	case broker.ConnOpenEvent:
		r.mu.Lock()
		r.conns[ev.ConnID] = ConnInfo{
			ConnID:   ev.ConnID,
			AgentPID: ev.PID,
			Backend:  ev.Backend,
			OpenedAt: ev.TS,
		}
		r.mu.Unlock()
	case broker.ConnCloseEvent:
		r.mu.Lock()
		delete(r.conns, ev.ConnID)
		r.mu.Unlock()
	}
}

// snapshot returns the live connections, sorted by open time (then id) so the
// listing is stable across polls.
func (r *connRegistry) snapshot() []ConnInfo {
	r.mu.Lock()
	out := make([]ConnInfo, 0, len(r.conns))
	for _, c := range r.conns {
		out = append(out, c)
	}
	r.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].OpenedAt.Equal(out[j].OpenedAt) {
			return out[i].ConnID < out[j].ConnID
		}
		return out[i].OpenedAt.Before(out[j].OpenedAt)
	})
	return out
}
