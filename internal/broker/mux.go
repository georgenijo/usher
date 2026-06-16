package broker

// mux.go multiplexes many agent connections onto ONE shared backend child. It is
// the inverse of the #17 fan-out: there, one client was fanned across N backends
// and the id space was owned per-call inside one connection; here, N distinct
// client connections share one backend's single JSON-RPC channel, so the id space
// is owned per-(shared child) and the response-routing table must name WHICH
// client to answer, not just which id.
//
// The discipline is identical to fanout's routeToolCall / pumpFanoutOutbound:
//   - inbound: rewrite the client's request id to a broker-unique child id,
//     record the original under the child id in the CLIENT's inflight map (so the
//     per-client gate/arbitrate/trim stages correlate on the same id the child
//     echoes back), and write to the one shared child conn (its Write is
//     mutex-serialized, so concurrent Forward from many goroutines is line-safe);
//   - outbound: the single readLoop reads the shared child, looks up the route by
//     child id, runs the OWNING client's outbound pipeline (keyed on the child id
//     so Trim.Consume / Arbitrate.release correlate), restores the client's
//     ORIGINAL id on the wire, and writes to that client's conn.
//
// The per-client MCP handshake is answered from the supervisor's cache: the
// shared child is initialized exactly once (in startAndHandshake); each client's
// initialize is answered from mb.initResult and each client's
// notifications/initialized is swallowed (never forwarded — the child already got
// its one). This keeps both contracts: every client session completes its own
// handshake, and the server child sees initialize+initialized exactly once.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/georgenijo/usher/internal/identity"
	"github.com/georgenijo/usher/internal/mcp"
)

// ErrBackendStopped is the JSON-RPC error code the mux answers a client with when
// the shared child it routed to has been stopped or has died. It is in the
// application-defined server-error range (-32000..-32099), so it never collides
// with a protocol code; it sits past the gate/lock family.
const ErrBackendStopped = -32000

// muxClient is one agent connection attached to a shared backend. Each client
// carries its OWN inflight map and identity: the inflight keying keeps the
// per-connection gate/arbitrate/trim correlation correct under sharing, and the
// identity is the lock owner so two agents on the same window genuinely contend
// against each other in the process-wide lock registry.
type muxClient struct {
	id       identity.Identity // per-connection identity (lock owner)
	conn     *mcp.Conn         // the agent's client conn (Write is serialized)
	inflight *InflightMap      // PER-CLIENT correlation (gate/arbitrate/trim)
}

// routeEntry names who to answer for one outstanding request to the shared child:
// the originating client and its ORIGINAL JSON-RPC id (IDString raw form, so a
// number id is "7" and a string id is "\"abc\"" — restoring with
// json.RawMessage(clientID) reproduces the exact wire form, the same trick the
// fanout relies on). It is the shared-child analogue of the fanout's per-call
// inflight ClientID, but it must also carry the client because many DISTINCT
// connections share this id space.
type routeEntry struct {
	client   *muxClient
	clientID string
}

// backendMux owns the response-routing table for one shared backend child. nextID
// is the broker-unique id allocator for the shared child (like fanout.nextID);
// routes maps a child-side id to the client+id that should receive the answer.
// clients is the set of attached connections, used to broadcast server→client
// notifications (which carry no id and thus have no single owner).
type backendMux struct {
	mb   *managedBackend
	b    *Broker
	bus  *Hub
	conn *mcp.Conn // the shared child's JSON-RPC channel, fixed for the mux's life

	nextID atomic.Uint64

	mu      sync.Mutex
	routes  map[string]routeEntry
	clients map[*muxClient]struct{}

	// closed is set once readLoop has exited (the shared child died or was
	// stopped); Forward checks it so a late call gets a clean in-band error
	// instead of writing to a dead conn.
	closed bool
}

// newBackendMux builds the mux for a freshly-live shared child. The supervisor
// calls it from startAndHandshake's tail and starts readLoop in its own
// goroutine — there is exactly one reader because there is one mb.conn.
func newBackendMux(mb *managedBackend, b *Broker, bus *Hub) *backendMux {
	return &backendMux{
		mb:      mb,
		b:       b,
		bus:     bus,
		conn:    mb.conn, // captured once; the mux is torn down when this conn dies
		routes:  make(map[string]routeEntry),
		clients: make(map[*muxClient]struct{}),
	}
}

// allocID returns the next broker-unique child-side request id, rendered as a
// JSON number string ("17") so it round-trips through IDString — the same form
// the fanout allocator uses.
func (mx *backendMux) allocID() string {
	return strconv.FormatUint(mx.nextID.Add(1), 10)
}

// attach registers a client connection so server→client notifications reach it
// and the UI's ref count reflects it.
func (mx *backendMux) attach(c *muxClient) {
	mx.mu.Lock()
	mx.clients[c] = struct{}{}
	mx.mu.Unlock()
	mx.mb.mu.Lock()
	mx.mb.refs++
	mx.mb.mu.Unlock()
}

// detach removes a client connection on its disconnect and drops every route
// still owned by it (so a vanished client cannot leave a dangling answer). It
// returns nothing; the caller has already reclaimed the client's window-locks.
func (mx *backendMux) detach(c *muxClient) {
	mx.mu.Lock()
	delete(mx.clients, c)
	for id, re := range mx.routes {
		if re.client == c {
			delete(mx.routes, id)
		}
	}
	mx.mu.Unlock()
	mx.mb.mu.Lock()
	if mx.mb.refs > 0 {
		mx.mb.refs--
	}
	mx.mb.mu.Unlock()
}

// attachedClients snapshots the current client set so a broadcast iterates
// without holding the lock across each Write.
func (mx *backendMux) attachedClients() []*muxClient {
	mx.mu.Lock()
	out := make([]*muxClient, 0, len(mx.clients))
	for c := range mx.clients {
		out = append(out, c)
	}
	mx.mu.Unlock()
	return out
}

// deleteRoute drops a route entry, used when an inbound stage refuses the call
// in-band so no orphaned route lingers for a response that will never arrive.
func (mx *backendMux) deleteRoute(childID string) {
	mx.mu.Lock()
	delete(mx.routes, childID)
	mx.mu.Unlock()
}

// Forward sends one client→backend REQUEST to the shared child: it allocates a
// child-unique id, records the route and the per-client inflight entry under that
// id, runs the inbound pipeline (gate/arbitrate/audit) keyed to this client, then
// writes to the single shared conn. An inbound stage that refuses the call
// in-band (gate-blocked, window-busy) answers the client via the wrapped reply
// and the forward is dropped, with the orphaned route + inflight cleared. A
// request that arrives after the child died gets a clean in-band error.
func (mx *backendMux) Forward(c *muxClient, m *mcp.Message) error {
	mx.mu.Lock()
	if mx.closed {
		mx.mu.Unlock()
		return c.conn.Write(mcp.ErrorResponse(m.ID, ErrBackendStopped,
			fmt.Sprintf("backend %q is not running", mx.mb.cfg.Name)))
	}
	childID := mx.allocID()
	origID := m.IDString()
	mx.routes[childID] = routeEntry{client: c, clientID: origID}
	mx.mu.Unlock()

	// Record under the CHILD id (carrying the client's original id) so the
	// per-client outbound pipeline (Arbitrate.Peek / Trim.Consume) correlates on
	// the same id the child will echo back, and so the wire id can be restored.
	c.inflight.Record(childID, InflightEntry{
		Method:      m.Method,
		ToolName:    toolNameIf(m),
		BackendName: mx.mb.cfg.Name,
		ClientID:    origID,
	})

	// Re-stamp the id to the child-unique one and force re-encode (id changed),
	// exactly as routeToolCall does.
	m.ID = json.RawMessage(childID)
	m.Raw = nil

	// A stage that refuses in-band stamps its error with m.ID — the child id we
	// just assigned. The client correlates on its ORIGINAL id, so wrap reply to
	// restore the client id on any response a stage injects, mirroring the
	// fanout's clientReply.
	clientReply := func(rm *mcp.Message) error {
		if rm != nil && rm.IsResponse() && rm.IDString() == childID {
			rm.ID = json.RawMessage(origID)
			rm.Raw = nil
		}
		return c.conn.Write(rm)
	}

	pctx := &Context{
		Identity: c.id,
		Backend:  mx.mb.cfg.Name,
		Dir:      Inbound,
		Inflight: c.inflight,
		Locks:    mx.b.locks,
		Reply:    clientReply,
		ClientID: origID,
	}
	out, err := mx.b.inbound.Run(pctx, m)
	if err != nil {
		mx.b.audit.Errorf(c.id.ID, "%s pipeline: %v", Inbound, err)
		c.inflight.Consume(childID)
		mx.deleteRoute(childID)
		return nil
	}
	if out == nil {
		// A stage dropped the call (gate-blocked, window-busy answered in-band).
		c.inflight.Consume(childID)
		mx.deleteRoute(childID)
		return nil
	}

	if err := mx.conn.Write(out); err != nil {
		// The shared child's conn is dead; answer the client so it does not hang.
		c.inflight.Consume(childID)
		mx.deleteRoute(childID)
		return c.conn.Write(mcp.ErrorResponse(json.RawMessage(origID), ErrBackendStopped,
			fmt.Sprintf("backend %q write failed: %v", mx.mb.cfg.Name, err)))
	}

	mx.bus.Emit(RequestEvent{
		TS: time.Now(), ConnID: c.id.ID, AgentPID: c.id.PID,
		Backend: mx.mb.cfg.Name, Tool: toolNameIf(out), RPCID: childID,
	})
	return nil
}

// forwardNotification sends a client→server notification (no id, no response) to
// the shared child verbatim. notifications/initialized is NOT routed here — it is
// swallowed in serveMuxConn, since the child already received its single one.
func (mx *backendMux) forwardNotification(m *mcp.Message) error {
	mx.mu.Lock()
	closed := mx.closed
	mx.mu.Unlock()
	if closed {
		return nil // nothing to forward to; a dead child drops notifications
	}
	return mx.conn.Write(m)
}

// readLoop is the ONE outbound reader for the shared child. It reads each child
// message, routes a response back to its owning client (running that client's
// outbound pipeline keyed on the child id, then restoring the client id on the
// wire), and broadcasts server→client notifications to every attached client. It
// is the inverse of pumpFanoutOutbound. On a read error the child has died:
// failAll answers every outstanding route so no agent hangs, and flips the
// supervisor state so the UI shows the death live.
func (mx *backendMux) readLoop() {
	for {
		m, err := mx.conn.Read()
		if err != nil {
			mx.failAll(err)
			return
		}

		if m.IsResponse() {
			mx.routeResponse(m)
			continue
		}

		// Server→client notification (e.g. notifications/tools/list_changed,
		// logging): no id, no single owner — broadcast to every attached client.
		for _, c := range mx.attachedClients() {
			_ = c.conn.Write(m)
		}
	}
}

// routeResponse dispatches one child response back to the client that issued the
// matching request. The child id stays on the message THROUGH the outbound
// pipeline (so Trim.Consume / Arbitrate.release correlate on it), then the
// client's original id is restored just before the wire — byte-faithfully via
// json.RawMessage(re.clientID).
func (mx *backendMux) routeResponse(m *mcp.Message) {
	childID := m.IDString()
	mx.mu.Lock()
	re, ok := mx.routes[childID]
	if ok {
		delete(mx.routes, childID)
	}
	mx.mu.Unlock()
	if !ok {
		return // unknown id: no client owns it (a late/duplicate response) — drop
	}
	c := re.client

	preBytes := len(m.Raw)
	pctx := &Context{
		Identity: c.id,
		Backend:  mx.mb.cfg.Name,
		Dir:      Outbound,
		Inflight: c.inflight,
		Locks:    mx.b.locks,
		ClientID: re.clientID,
	}
	out, err := mx.b.outbound.Run(pctx, m)
	if err != nil {
		mx.b.audit.Errorf(c.id.ID, "%s pipeline: %v", Outbound, err)
		return
	}
	if out == nil {
		return
	}

	// Restore the client's original id on the wire (raw form, byte-faithful).
	out.ID = json.RawMessage(re.clientID)
	out.Raw = nil
	if err := c.conn.Write(out); err != nil {
		return
	}
	mx.bus.Emit(ResponseEvent{
		TS: time.Now(), ConnID: c.id.ID, Backend: mx.mb.cfg.Name, RPCID: re.clientID,
		Bytes: outboundBytes(out), TrimmedFromBytes: preBytes,
	})
}

// failAll runs when the shared child's read loop ends (the child exited or was
// stopped). It answers every outstanding route with a backend-stopped error so no
// client hangs, marks the mux closed so later Forward calls get a clean in-band
// error, and flips the supervisor state to Failed (unless a Stop initiated the
// teardown, in which case Stop already drove the state) so the UI shows the death.
// Attached clients stay connected; their NEXT request re-triggers EnsureLive.
func (mx *backendMux) failAll(cause error) {
	mx.mu.Lock()
	mx.closed = true
	orphans := mx.routes
	mx.routes = make(map[string]routeEntry)
	mx.mu.Unlock()

	for childID, re := range orphans {
		re.client.inflight.Consume(childID)
		_ = re.client.conn.Write(mcp.ErrorResponse(
			json.RawMessage(re.clientID), ErrBackendStopped,
			fmt.Sprintf("backend %q exited", mx.mb.cfg.Name)))
	}

	// Flip the supervisor state to Failed if this death was unexpected. A Stop in
	// progress has already set StateStopping/StateStopped and will clear mb.mux, so
	// we only transition when we are still the live mux for a backend that thinks
	// it is live.
	mb := mx.mb
	mb.mu.Lock()
	if mb.mux == mx && mb.state == StateLive {
		mb.state = StateFailed
		mb.err = fmt.Errorf("shared child exited: %w", cause)
		mb.child = nil
		mb.conn = nil
		mb.mux = nil
		mb.emitStateLocked(StateLive, StateFailed)
	}
	mb.mu.Unlock()
}

// serveMuxConn is the per-connection handler on the daemon's socket path: it
// multiplexes one agent connection onto the shared backend child. It answers the
// client's initialize/tools/list from the supervisor's cache, triggers the lazy
// come-live on the first request that needs the child, swallows the client's
// notifications/initialized, and forwards every other request/notification onto
// the shared child via the mux. It returns when the client hangs up or ctx is
// cancelled (the conn is closed out from under the read by ServeSocket).
func (b *Broker) serveMuxConn(ctx context.Context, backendName string, c net.Conn) {
	defer c.Close()

	id := identity.NewForConn(c)
	client := mcp.NewConn(c, c)
	mc := &muxClient{id: id, conn: client, inflight: NewInflightMap()}

	be := b.cfg.ResolveBackend(backendName)
	if be != nil {
		backendName = be.Name
	}

	// Close the conn on ctx cancel so a daemon shutdown unblocks this connection's
	// blocking Read (the read then returns an error and the loop exits). done stops
	// the watcher when the connection ends on its own.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = c.Close()
		case <-done:
		}
	}()

	// On any exit, reclaim the connection's window-locks (reclaim-on-death, #16)
	// and detach from the shared child's mux so its routes/refs are cleaned up.
	var attached *backendMux
	defer func() {
		b.reclaim(id)
		if attached != nil {
			attached.detach(mc)
		}
	}()

	var mb *managedBackend
	reason := "client-eof"
	if ctx.Err() != nil {
		reason = "signal"
	}

	for {
		m, err := client.Read()
		if err != nil {
			if ctx.Err() != nil {
				reason = "signal"
			}
			break
		}

		switch {
		case m.IsRequest() && m.Method == "initialize":
			live, err := b.sv.EnsureLive(backendName) // COME LIVE on the first call
			if err != nil {
				_ = client.Write(mcp.ErrorResponse(m.ID, ErrBackendStopped, err.Error()))
				continue
			}
			mb = live
			attachOnce(mb, mc, &attached)
			// Emit ConnOpen BEFORE writing the result so a UI subscriber that snapshots
			// between attach and the write sees this connection in the registry, not
			// just in the backend's ref count.
			b.bus.Emit(ConnOpenEvent{TS: time.Now(), ConnID: id.ID, PID: id.PID, Backend: backendName})
			// Answer the client's initialize from cache, stamped with ITS id. The
			// shared child is never re-initialized. Snapshot initResult under mb.mu so
			// a concurrent Stop (which nils it under the same lock) cannot race the read
			// and hand this client a torn cache. If a Stop nilled the cache between
			// EnsureLive and this snapshot, initResult is empty — answer backend-stopped
			// rather than write a malformed result-less message (the client's next
			// request re-triggers come-live).
			mb.mu.Lock()
			initResult := append(json.RawMessage(nil), mb.initResult...)
			mb.mu.Unlock()
			if len(initResult) == 0 {
				_ = client.Write(mcp.ErrorResponse(m.ID, ErrBackendStopped, "backend is not running"))
				continue
			}
			_ = client.Write(&mcp.Message{
				JSONRPC: "2.0",
				ID:      append(json.RawMessage(nil), m.ID...),
				Result:  initResult,
			})

		case m.IsNotification() && m.Method == "notifications/initialized":
			// Swallow per-client: the shared child already received its single
			// notifications/initialized during startAndHandshake. Forwarding one per
			// client would be a protocol error against the child.

		case m.IsRequest() && m.Method == "tools/list":
			live, mux, err := b.ensureMuxLive(backendName, mc, &mb, &attached)
			if err != nil || mux == nil {
				_ = client.Write(mcp.ErrorResponse(m.ID, ErrBackendStopped, errOrStopped(err)))
				continue
			}
			// toolsResult is the child's FULL tools/list result object
			// ({"tools":[...]}), cached verbatim by the supervisor — answered as-is
			// (a fresh copy under this client's id). Do NOT re-wrap it. As with
			// initialize, a Stop racing between ensureMuxLive and this snapshot can nil
			// the cache; an empty copy means backend-stopped, not an empty result.
			live.mu.Lock()
			tools := append(json.RawMessage(nil), live.toolsResult...)
			live.mu.Unlock()
			if len(tools) == 0 {
				_ = client.Write(mcp.ErrorResponse(m.ID, ErrBackendStopped, "backend is not running"))
				continue
			}
			_ = client.Write(&mcp.Message{
				JSONRPC: "2.0",
				ID:      append(json.RawMessage(nil), m.ID...),
				Result:  tools,
			})

		case m.IsRequest():
			// tools/call, ping, resources/*, …: forward onto the shared child.
			_, mux, err := b.ensureMuxLive(backendName, mc, &mb, &attached)
			if err != nil || mux == nil {
				_ = client.Write(mcp.ErrorResponse(m.ID, ErrBackendStopped, errOrStopped(err)))
				continue
			}
			_ = mux.Forward(mc, m)

		case m.IsNotification():
			// A client→server notification (cancellation, logging): forward to the
			// shared child if it is live; otherwise drop (nothing to forward to).
			if mb != nil {
				mb.mu.Lock()
				mux := mb.mux
				mb.mu.Unlock()
				if mux != nil {
					_ = mux.forwardNotification(m)
				}
			}
		}
	}

	b.bus.Emit(ConnCloseEvent{TS: time.Now(), ConnID: id.ID, Reason: reason})
}

// ensureMuxLive resolves the shared child (and its mux) for a request that needs
// it, performing the lazy come-live for a non-standard client that skipped
// initialize. It attaches the client to the resolved mux exactly once and caches
// the resolved managedBackend in *mb so subsequent calls reuse it. It returns the
// backend AND a snapshot of its mux read under the lock, so the caller never sees
// a mux that a concurrent Stop nilled out between resolve and use. A nil mux with
// a nil error means the child died after EnsureLive succeeded; the caller answers
// backend-stopped.
func (b *Broker) ensureMuxLive(backendName string, mc *muxClient, mb **managedBackend, attached **backendMux) (*managedBackend, *backendMux, error) {
	// A live backend already resolved by the initialize branch: reuse it, unless
	// its child has since died (mux nil), in which case re-trigger come-live.
	if *mb != nil {
		(*mb).mu.Lock()
		liveMux := (*mb).mux
		state := (*mb).state
		(*mb).mu.Unlock()
		if state == StateLive && liveMux != nil {
			return *mb, liveMux, nil
		}
	}
	live, err := b.sv.EnsureLive(backendName)
	if err != nil {
		return nil, nil, err
	}
	*mb = live
	attachOnce(live, mc, attached)
	// Snapshot the mux under the lock: a Stop racing in here could already have
	// nilled it, in which case we report (live, nil, nil) and the caller refuses.
	live.mu.Lock()
	mux := live.mux
	live.mu.Unlock()
	return live, mux, nil
}

// errOrStopped renders an EnsureLive error, or a generic backend-stopped message
// when err is nil (the child died after a successful resolve).
func errOrStopped(err error) string {
	if err != nil {
		return err.Error()
	}
	return "backend is not running"
}

// attachOnce attaches mc to the backend's mux the first time, recording which mux
// it attached to in *attached so the deferred detach hits the right one and a
// client never double-attaches (which would double-count refs).
func attachOnce(mb *managedBackend, mc *muxClient, attached **backendMux) {
	mb.mu.Lock()
	mux := mb.mux
	mb.mu.Unlock()
	if mux == nil || *attached == mux {
		return
	}
	if *attached != nil {
		// The previous shared child died and a new one came live; move our
		// attachment to the new mux.
		(*attached).detach(mc)
	}
	mux.attach(mc)
	*attached = mux
}
