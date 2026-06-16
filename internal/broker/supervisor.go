package broker

// supervisor.go is the shared backend pool behind the always-on daemon. Today's
// socket path spawns a fresh backend child PER connection; the supervisor
// replaces that with a SHARED, supervised pool: each configured backend is one
// long-lived child with a lifecycle state machine
//
//	stopped → starting → live → failed   (and live/failed → stopping → stopped)
//
// A backend starts LAZILY on the first request routed to it (or on an explicit
// UI Start). The triggering caller blocks in EnsureLive until the one-time MCP
// handshake (initialize → notifications/initialized → tools/list) completes —
// that wait IS the "come live". Concurrent first-callers coalesce onto a single
// start attempt (the standard singleflight: a state flip under the lock plus a
// ready channel they all park on), so a burst of simultaneous first-calls can
// never double-spawn the child. Every transition publishes a BackendState event
// on the bus so the UI can watch a backend come live (and die) in real time.
//
// Client multiplexing onto the shared child (id-rewrite, response routing, the
// per-client cached initialize) is the NEXT layer; this file owns only the pool,
// the state machine, the one-time handshake, and the lifecycle controls. The
// legacy "usher serve --backend NAME" stdio path keeps its eager per-connection
// spawn (serveConn) untouched for back-compat.

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/georgenijo/usher/internal/backend"
	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/mcp"
)

// BackendState is the lifecycle state of one shared backend child. It is a small
// int enum mirroring the Direction/acquireResult idiom; String renders the wire
// tag the BackendState event and the UI use.
type BackendState int

const (
	// StateStopped: no child process exists.
	StateStopped BackendState = iota
	// StateStarting: the child has been spawned and the one-time handshake is in
	// flight. Concurrent callers coalesce here, parked on the ready channel.
	StateStarting
	// StateLive: the child is initialized, its tools/list is cached, and it is
	// accepting calls.
	StateLive
	// StateFailed: the last start/handshake errored. The error is surfaced to the
	// UI; a later call may retry (StateFailed is a valid start trigger).
	StateFailed
	// StateStopping: a graceful stop is draining the child.
	StateStopping
)

// String renders the state for events, snapshots, and the UI. The names match
// the design's wire tags ("stopped"|"starting"|"live"|"failed"|"stopping").
func (s BackendState) String() string {
	switch s {
	case StateStopped:
		return "stopped"
	case StateStarting:
		return "starting"
	case StateLive:
		return "live"
	case StateStopping:
		return "stopping"
	case StateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// managedBackend is one configured backend's shared child plus its state
// machine. All state mutation happens under mu; the slow handshake runs WITHOUT
// the lock held (callers park on ready instead) so a burst of first-calls does
// not serialise behind the lock.
type managedBackend struct {
	cfg *config.Backend // immutable config snapshot (Name, Command, Auth, EnvKeys)
	bus *Hub            // for BackendState events; nil-safe (Emit no-ops on nil)

	mu    sync.Mutex
	state BackendState
	err   error // last failure, surfaced to the UI when StateFailed

	child *backend.Stdio // nil unless starting/live/stopping
	conn  *mcp.Conn      // child.Conn(); the SINGLE shared JSON-RPC channel

	// ready is closed when the in-flight start attempt resolves (Live OR Failed).
	// Coalesced callers that arrived during StateStarting park on it; a fresh chan
	// is published per start attempt so a later retry has its own gate.
	ready chan struct{}

	// Cached handshake artifacts so the per-client mux layer can answer each new
	// client's initialize/tools/list WITHOUT re-initializing the shared child.
	// initResult is the child's initialize result, serverInfo re-stamped to usher;
	// toolsResult is the child's tools/list result, cached verbatim.
	initResult  json.RawMessage
	toolsResult json.RawMessage

	// mux multiplexes many client connections onto this shared child (id-rewrite,
	// response routing, the per-client cached initialize). It is built when the
	// child goes live and torn down (set nil) when it stops or dies. Nil unless
	// StateLive.
	mux *backendMux

	startedAt time.Time
	refs      int // attached client connections (UI readout; idle-stop is deferred)
}

// emitStateLocked records the transition and publishes a BackendState event.
// Called with mb.mu held (the state field is being flipped around it); Emit is
// non-blocking so holding the lock across it adds no latency.
func (mb *managedBackend) emitStateLocked(from, to BackendState) {
	mb.bus.Emit(BackendStateEvent{
		TS: time.Now(), Backend: mb.cfg.Name, From: from.String(), To: to.String(),
	})
}

// resultAfterReady reports the outcome of a resolved start attempt: (mb,nil)
// when Live, (nil,err) when Failed. Called after the caller has observed ready
// closed (or won the start itself), so the state is stable at Live or Failed.
func (mb *managedBackend) resultAfterReady() (*managedBackend, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if mb.state == StateLive {
		return mb, nil
	}
	if mb.err != nil {
		return nil, mb.err
	}
	return nil, fmt.Errorf("backend %q not live (state %s)", mb.cfg.Name, mb.state)
}

// snapshot reads the externally-visible status under the lock.
func (mb *managedBackend) snapshot() BackendStatus {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	st := BackendStatus{
		Name:  mb.cfg.Name,
		State: mb.state.String(),
		Refs:  mb.refs,
	}
	if mb.state == StateLive {
		st.StartedAt = mb.startedAt
		st.ToolCount = countTools(mb.toolsResult)
	}
	if mb.err != nil {
		st.Err = mb.err.Error()
	}
	return st
}

// BackendStatus is the UI/CLI view of one backend in the pool. It is built from
// managedBackend under the lock by Snapshot; the time/count fields are zero
// unless the backend is live.
type BackendStatus struct {
	Name      string    `json:"name"`
	State     string    `json:"state"`
	StartedAt time.Time `json:"startedAt,omitempty"`
	Refs      int       `json:"refs"`
	ToolCount int       `json:"toolCount"`
	Err       string    `json:"err,omitempty"`
}

// BackendSupervisor owns the shared pool: one managedBackend per configured
// backend, all StateStopped at construction (lazy start is the whole point — no
// child is spawned until the first call routes to it). The UI's backend list
// reads byName via Snapshot.
type BackendSupervisor struct {
	mu     sync.Mutex
	byName map[string]*managedBackend
	cfg    *config.Config
	bus    *Hub            // BackendState events; nil-safe
	ctx    context.Context // daemon lifetime; children are spawned under it

	// broker is the owning broker, used to build a backendMux for each live child
	// (the mux needs the broker's pipelines, lock registry, and audit). It may be
	// nil for a bare supervisor built directly in a test that never multiplexes
	// clients (the lifecycle tests); startAndHandshake then skips the mux.
	broker *Broker
}

// NewSupervisor pre-populates the pool from cfg.Backends, every backend
// StateStopped (no children spawned). ctx is the daemon lifetime — every shared
// child is spawned under it, so cancelling ctx tears the whole pool down. broker
// owns the pipelines/locks the per-child mux uses to multiplex clients; pass nil
// for a bare lifecycle-only supervisor (the mux is then never built).
func NewSupervisor(ctx context.Context, cfg *config.Config, bus *Hub) *BackendSupervisor {
	return newSupervisor(ctx, cfg, bus, nil)
}

// newSupervisor is the full constructor; NewSupervisor is the public form with a
// nil broker. The daemon path uses newSupervisorForBroker so the per-child mux
// can be built with the broker's pipelines and lock registry.
func newSupervisor(ctx context.Context, cfg *config.Config, bus *Hub, broker *Broker) *BackendSupervisor {
	if ctx == nil {
		ctx = context.Background()
	}
	sv := &BackendSupervisor{
		byName: make(map[string]*managedBackend, len(cfg.Backends)),
		cfg:    cfg,
		bus:    bus,
		ctx:    ctx,
		broker: broker,
	}
	for i := range cfg.Backends {
		be := &cfg.Backends[i]
		sv.byName[be.Name] = &managedBackend{cfg: be, bus: bus, state: StateStopped}
	}
	return sv
}

// newSupervisorForBroker builds the daemon-path supervisor wired to its owning
// broker, so each live child gets a backendMux (built from the broker's
// pipelines, lock registry, and audit) for client multiplexing.
func newSupervisorForBroker(ctx context.Context, b *Broker) *BackendSupervisor {
	return newSupervisor(ctx, b.cfg, b.bus, b)
}

// lookup returns the managedBackend for name, or an error when name is not a
// configured backend.
func (sv *BackendSupervisor) lookup(name string) (*managedBackend, error) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	mb, ok := sv.byName[name]
	if !ok {
		return nil, fmt.Errorf("no backend named %q (run: usher backend list)", name)
	}
	return mb, nil
}

// EnsureLive returns the live shared child for name, starting it (and running
// the one-time handshake) on first use. Concurrent callers coalesce onto a
// single start attempt: the triggering caller BLOCKS here until the handshake
// completes, then proceeds to forward — that wait is the "come live". A failed
// start surfaces the error and leaves the backend StateFailed; the NEXT call may
// retry it.
func (sv *BackendSupervisor) EnsureLive(name string) (*managedBackend, error) {
	mb, err := sv.lookup(name)
	if err != nil {
		return nil, err
	}
	// Bounded retry loop: the only path that re-enters is observing StateStopping
	// (a graceful stop draining), which we wait out then retry from Stopped.
	for attempt := 0; attempt < 100; attempt++ {
		mb.mu.Lock()
		switch mb.state {
		case StateLive:
			mb.mu.Unlock()
			return mb, nil

		case StateStarting:
			// Coalesce: another caller owns the in-flight start. Park on its ready
			// channel WITHOUT the lock so we never serialise behind the handshake.
			ready := mb.ready
			mb.mu.Unlock()
			<-ready
			return mb.resultAfterReady()

		case StateStopped, StateFailed:
			// We win the right to start. Flip to Starting and publish a fresh ready
			// channel before releasing the lock, so any caller that arrives next sees
			// StateStarting and parks on exactly this attempt's channel.
			from := mb.state
			mb.state = StateStarting
			mb.ready = make(chan struct{})
			mb.err = nil
			ready := mb.ready
			mb.emitStateLocked(from, StateStarting)
			mb.mu.Unlock()

			// The slow handshake runs WITHOUT the lock held.
			herr := sv.startAndHandshake(mb)

			mb.mu.Lock()
			if herr != nil {
				mb.state = StateFailed
				mb.err = herr
				mb.emitStateLocked(StateStarting, StateFailed)
			} else {
				mb.state = StateLive
				mb.startedAt = time.Now()
				mb.emitStateLocked(StateStarting, StateLive)
			}
			close(ready) // wake every coalesced waiter
			mb.mu.Unlock()
			return mb.resultAfterReady()

		case StateStopping:
			// A graceful stop is draining. Wait for it to reach Stopped, then retry.
			ready := mb.ready
			mb.mu.Unlock()
			if ready != nil {
				<-ready
			} else {
				time.Sleep(time.Millisecond)
			}
			continue
		}
		mb.mu.Unlock()
	}
	return nil, fmt.Errorf("backend %q: stuck stopping, gave up waiting to start", name)
}

// startAndHandshake spawns the shared child and runs the one-time MCP handshake
// against it: initialize (serverInfo re-stamped to usher and cached),
// notifications/initialized, then tools/list (cached). On any step error the
// child is killed and the error returned; the caller flips the backend to
// StateFailed. It runs WITHOUT mb.mu held.
func (sv *BackendSupervisor) startAndHandshake(mb *managedBackend) error {
	envExtra, err := config.EnvForBackend(mb.cfg)
	if err != nil {
		return err
	}
	sb := backend.NewStdio(mb.cfg.Name, mb.cfg.Command, envExtra)
	if err := sb.Start(sv.ctx); err != nil {
		return fmt.Errorf("start backend %q: %w", mb.cfg.Name, err)
	}
	conn := sb.Conn()

	// 1. initialize — one real handshake for the shared child's whole lifetime.
	initReq := &mcp.Message{
		JSONRPC: "2.0",
		ID:      json.RawMessage("1"),
		Method:  "initialize",
		Params:  json.RawMessage(`{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"usher","version":"` + serverInfoVersion + `"}}`),
	}
	if err := conn.Write(initReq); err != nil {
		_ = sb.Close()
		return fmt.Errorf("backend %q initialize write: %w", mb.cfg.Name, err)
	}
	initResp, err := conn.Read()
	if err != nil {
		_ = sb.Close()
		return fmt.Errorf("backend %q initialize read: %w", mb.cfg.Name, err)
	}
	if initResp.Error != nil && len(initResp.Error) > 0 {
		_ = sb.Close()
		return fmt.Errorf("backend %q initialize error: %s", mb.cfg.Name, initResp.Error)
	}

	// 2. notifications/initialized — the child requires it once before it will
	// answer tool/resource requests (the MCP lifecycle), so it MUST precede the
	// tools/list prefetch below.
	note := &mcp.Message{JSONRPC: "2.0", Method: "notifications/initialized"}
	if err := conn.Write(note); err != nil {
		_ = sb.Close()
		return fmt.Errorf("backend %q initialized write: %w", mb.cfg.Name, err)
	}

	// 3. tools/list — cached so a future per-client layer answers from cache.
	toolsReq := &mcp.Message{JSONRPC: "2.0", ID: json.RawMessage("2"), Method: "tools/list"}
	if err := conn.Write(toolsReq); err != nil {
		_ = sb.Close()
		return fmt.Errorf("backend %q tools/list write: %w", mb.cfg.Name, err)
	}
	toolsResp, err := conn.Read()
	if err != nil {
		_ = sb.Close()
		return fmt.Errorf("backend %q tools/list read: %w", mb.cfg.Name, err)
	}
	if toolsResp.Error != nil && len(toolsResp.Error) > 0 {
		_ = sb.Close()
		return fmt.Errorf("backend %q tools/list error: %s", mb.cfg.Name, toolsResp.Error)
	}

	// Publish the started child + cached handshake under the lock, and build the
	// per-child mux so the daemon path can multiplex many clients onto this one
	// shared conn. The mux owns the single outbound reader (readLoop), started in
	// its own goroutine: there is exactly one reader because there is one conn.
	mb.mu.Lock()
	mb.child = sb
	mb.conn = conn
	mb.initResult = stampServerInfo(initResp.Result)
	mb.toolsResult = append(json.RawMessage(nil), toolsResp.Result...)
	if sv.broker != nil {
		mb.mux = newBackendMux(mb, sv.broker, sv.bus)
		go mb.mux.readLoop()
	}
	mb.mu.Unlock()
	return nil
}

// Start brings a backend live explicitly (the UI "start" action). It is
// EnsureLive with the live handle discarded; a backend already live is a no-op.
func (sv *BackendSupervisor) Start(name string) error {
	_, err := sv.EnsureLive(name)
	return err
}

// Stop gracefully terminates a backend's shared child and returns it to
// StateStopped. It half-closes stdin so the child flushes and exits, then kills
// and reaps it. A backend that is already stopped/failed (no child) is a no-op.
// A start in flight (StateStarting) is waited out first so we never kill a child
// mid-handshake out from under the starting caller.
func (sv *BackendSupervisor) Stop(name string) error {
	mb, err := sv.lookup(name)
	if err != nil {
		return err
	}

	mb.mu.Lock()
	// If a start is in flight, wait for it to resolve before we stop, so we never
	// race the handshake. Release the lock while parked.
	for mb.state == StateStarting {
		ready := mb.ready
		mb.mu.Unlock()
		if ready != nil {
			<-ready
		}
		mb.mu.Lock()
	}
	if mb.state == StateStopped || mb.state == StateFailed || mb.child == nil {
		// Nothing live to stop; normalise a failed/stale state to stopped.
		if mb.state == StateFailed {
			mb.state = StateStopped
			mb.err = nil
			mb.emitStateLocked(StateFailed, StateStopped)
		}
		mb.mu.Unlock()
		return nil
	}
	from := mb.state
	mb.state = StateStopping
	// Publish a ready channel an EnsureLive caller can park on while we drain.
	mb.ready = make(chan struct{})
	drain := mb.ready
	child := mb.child
	mb.emitStateLocked(from, StateStopping)
	mb.mu.Unlock()

	// Graceful: half-close then hard-kill+reap (Close kills and waits).
	_ = child.CloseStdin()
	_ = child.Close()

	mb.mu.Lock()
	mb.child = nil
	mb.conn = nil
	mb.initResult = nil
	mb.toolsResult = nil
	// Drop the mux: its readLoop sees the child's EOF and runs failAll, which (with
	// the state already StateStopping) answers any outstanding routes with a
	// backend-stopped error rather than flipping state to Failed. Clearing mb.mux
	// here means a fresh come-live builds a new one.
	mb.mux = nil
	mb.refs = 0
	mb.state = StateStopped
	mb.emitStateLocked(StateStopping, StateStopped)
	mb.mu.Unlock()
	close(drain) // wake anyone who parked waiting for the stop to finish
	return nil
}

// Restart stops then starts a backend, so a wedged or updated child is replaced
// cleanly. A backend that was stopped is simply started.
func (sv *BackendSupervisor) Restart(name string) error {
	if err := sv.Stop(name); err != nil {
		return err
	}
	return sv.Start(name)
}

// Snapshot returns the UI/CLI view of every backend in the pool, in config order
// so the listing is stable.
func (sv *BackendSupervisor) Snapshot() []BackendStatus {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	out := make([]BackendStatus, 0, len(sv.cfg.Backends))
	for i := range sv.cfg.Backends {
		if mb, ok := sv.byName[sv.cfg.Backends[i].Name]; ok {
			out = append(out, mb.snapshot())
		}
	}
	return out
}

// StopAll gracefully stops every backend in the pool. The daemon calls it on
// shutdown so no shared child is orphaned. Errors are ignored (best-effort
// teardown); ctx cancel kills any child Stop misses anyway.
func (sv *BackendSupervisor) StopAll() {
	sv.mu.Lock()
	names := make([]string, 0, len(sv.byName))
	for name := range sv.byName {
		names = append(names, name)
	}
	sv.mu.Unlock()
	for _, name := range names {
		_ = sv.Stop(name)
	}
}

// stampServerInfo re-stamps an initialize result's serverInfo to advertise usher
// (the broker presents itself as one server, not as the backend), leaving
// protocolVersion and capabilities intact. A result that is empty or not an
// object is replaced with a minimal valid initialize so the handshake always
// completes. This is the single-child analogue of mergeInitialize (fanout.go),
// which both rely on to keep one serverInfo-stamping rule.
func stampServerInfo(result json.RawMessage) json.RawMessage {
	if len(result) == 0 {
		out, _ := json.Marshal(map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": serverInfoName, "version": serverInfoVersion},
		})
		return out
	}
	var res map[string]json.RawMessage
	if err := json.Unmarshal(result, &res); err != nil {
		// Not an object we can edit: forward verbatim (a copy so the caller's slice
		// is not aliased into the cache).
		return append(json.RawMessage(nil), result...)
	}
	si, _ := json.Marshal(map[string]any{"name": serverInfoName, "version": serverInfoVersion})
	res["serverInfo"] = si
	out, _ := json.Marshal(res)
	return out
}

// countTools reports how many tools a cached tools/list result advertises, for
// the UI's per-backend tool count. A malformed or empty result counts zero.
func countTools(result json.RawMessage) int {
	if len(result) == 0 {
		return 0
	}
	var res struct {
		Tools []json.RawMessage `json:"tools"`
	}
	if err := json.Unmarshal(result, &res); err != nil {
		return 0
	}
	return len(res.Tools)
}
