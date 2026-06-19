// Package broker is usher's front desk: it accepts an agent connection, routes
// it to a backend, runs every message through the middleware pipeline, and
// forwards verbatim by default. The stdio path here is the #14 skeleton; the
// always-on Unix-socket daemon with many concurrent connections is #20.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/georgenijo/usher/internal/audit"
	"github.com/georgenijo/usher/internal/backend"
	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/identity"
	"github.com/georgenijo/usher/internal/mcp"
	"github.com/georgenijo/usher/internal/procstat"
)

// sampleInterval is the cadence of the daemon's per-process resource sampler:
// how often the gated USHER_SAMPLE path runs one `ps` batch over the watched
// pids and emits a ResourceSampleEvent. One second is dense enough to watch a
// backend come live / a client connect in the dashboard without flooding the
// bus, and it matches the load harness's default poll cadence.
const sampleInterval = time.Second

// reconcileInterval is how often the backend-pid feeder polls sv.Snapshot() to
// reconcile which shared children are live. It is finer than sampleInterval so a
// backend that comes live (or dies) is reflected in the watch set within a
// fraction of a sampler tick, rather than up to a full second late — the next ps
// batch then attributes its RSS promptly. Polling a snapshot is cheap (no exec),
// so the finer cadence costs nothing meaningful.
const reconcileInterval = 200 * time.Millisecond

// Broker holds the shared config, the per-window write-lock registry, the two
// per-direction pipelines, the event bus, and (on the daemon path) the shared
// backend pool.
type Broker struct {
	cfg      *config.Config
	audit    *audit.Logger
	locks    *lockRegistry      // shared per-window write-locks (#16)
	inbound  *Pipeline          // client → backend
	outbound *Pipeline          // backend → client
	bus      *Hub               // structured event bus fanned to subscribers (SSE UI, audit)
	sv       *BackendSupervisor // shared backend pool, built lazily on the socket/daemon path
}

// Supervisor exposes the broker's shared backend pool, built on demand by the
// socket/daemon path. It is nil on the bare stdio path (which spawns its own
// per-connection child) and until ServeSocket has constructed it; callers must
// nil-check. The live UI's control plane reads it for the backend listing and
// drives Start/Stop/Restart through it.
func (b *Broker) Supervisor() *BackendSupervisor { return b.sv }

// Bus exposes the broker's event Hub so the daemon path can subscribe the
// connection-level audit log and the live UI's SSE stream to it. Never nil for a
// broker built by New.
func (b *Broker) Bus() *Hub { return b.bus }

// EnsureSupervisor builds the shared backend pool (if not already built) under
// ctx and returns it, so the daemon can construct the control plane against the
// SAME supervisor the socket accept loop drives. ServeSocket reuses whatever this
// installed (it only builds one when b.sv is still nil), so the control plane and
// the forwarding path share one pool — the UI's Start/Stop and a lazy come-live
// move the same state machine. Idempotent: a second call returns the first pool.
func (b *Broker) EnsureSupervisor(ctx context.Context) *BackendSupervisor {
	if b.sv == nil {
		b.sv = newSupervisorForBroker(ctx, b)
	}
	return b.sv
}

// StartAuditSubscriber wires the broker's audit logger to the event bus as ONE
// subscriber, so connection-level lifecycle lines (connect / disconnect / backend
// state transitions) become event-driven on the daemon path — the per-MESSAGE
// wire log stays on AuditStage in the pipeline. It runs in its own goroutine and
// returns when ctx is cancelled. The daemon calls it once on the socket path; the
// SSE stream is the bus's other reader.
func (b *Broker) StartAuditSubscriber(ctx context.Context) {
	go RunAuditSubscriber(ctx, b.bus, b.audit)
}

// New builds a broker from config, logging audit to stderr at the default
// (normal) verbosity — every line emits.
func New(cfg *config.Config) (*Broker, error) {
	return NewWithLevel(cfg, audit.LevelNormal)
}

// NewWithLevel builds a broker from config, logging audit to stderr at the given
// verbosity AND to a rotating file under the state dir. The file is an
// additional, durable copy: stderr behaviour is unchanged. If the file sink
// cannot be opened (unwritable state dir), the broker degrades to stderr-only
// rather than failing to start — audit is a record, not a gate on serving.
//
// `usher serve --quiet/--verbose` selects the level here; only the informational
// Infof lifecycle lines are gated — errors, gate-blocked/security lines, and the
// core per-message audit always emit (#log-verbosity, #audit-rotation).
func NewWithLevel(cfg *config.Config, level audit.Level) (*Broker, error) {
	var auditW io.Writer = os.Stderr
	if sink, err := audit.NewFileSink(cfg.AuditPath(), cfg.AuditMaxBytes, cfg.AuditKeep); err != nil {
		fmt.Fprintf(os.Stderr, "usher: audit file sink disabled: %v\n", err)
	} else {
		auditW = io.MultiWriter(os.Stderr, sink)
	}
	al := audit.NewLevel(auditW, level)
	// A zero (unset) config threshold means "use the built-in default".
	trimThreshold := DefaultTrimThreshold
	if cfg.TrimThreshold > 0 {
		trimThreshold = cfg.TrimThreshold
	}
	// The lock registry is process-wide so contention is arbitrated ACROSS
	// connections — two agents driving the same window must serialise even
	// though each has its own pair of pumps. Zero ttl/wait take the defaults.
	locks := newLockRegistry(cfg.LockTTL(), cfg.LockWait())
	// Build the gate policy from the built-in destructive-tool set plus any
	// config additions, with config + env allow-lists as the override (#18).
	policy := policyFromConfig(cfg)

	// The event bus fans structured events (conn/request/response/gate/lock,
	// plus backend-state from the supervisor) to subscribers. Emitting is
	// non-blocking, so wiring it into the pipeline stages and pumps adds no
	// latency to forwarding.
	bus := NewHub()

	// Wire the gate/arbitrate stages' callbacks to the bus. The stages take a
	// callback, not the Hub, so they stay independent of the event package; a nil
	// callback (the outbound-side stages, the bare test broker) emits nothing.
	gate := NewGateStagePolicy(policy)
	gate.OnBlock = func(tool, connID string) {
		bus.Emit(GateBlockEvent{TS: time.Now(), Tool: tool, ConnID: connID})
	}
	inArb := NewArbitrateStage()
	inArb.OnLock = func(key, connID string, acquired bool) {
		bus.Emit(LockEvent{TS: time.Now(), Key: key, ConnID: connID, Acquired: acquired})
	}
	outArb := NewArbitrateStage()
	outArb.OnLock = func(key, connID string, acquired bool) {
		bus.Emit(LockEvent{TS: time.Now(), Key: key, ConnID: connID, Acquired: acquired})
	}

	return &Broker{
		cfg:      cfg,
		audit:    al,
		locks:    locks,
		bus:      bus,
		inbound:  NewPipeline(gate, inArb, NewAuditStage(al, Inbound)),
		outbound: NewPipeline(outArb, NewTrimStageThreshold(trimThreshold), NewAuditStage(al, Outbound)),
	}, nil
}

// policyFromConfig assembles the gate's Policy: the built-in DefaultBlockedTools
// unioned with cfg.BlockedTools form the block-list; cfg.AllowedTools unioned
// with the USHER_ALLOW_TOOLS env list form the override. The env override is read
// here (serve time) so an operator can unblock a destructive tool for a single
// run without editing config.json.
func policyFromConfig(cfg *config.Config) Policy {
	blocked := append(append([]string(nil), DefaultBlockedTools...), cfg.BlockedTools...)
	allowed := append([]string(nil), cfg.AllowedTools...)
	if env := os.Getenv(config.EnvAllowTools); env != "" {
		for _, name := range strings.Split(env, ",") {
			allowed = append(allowed, strings.TrimSpace(name))
		}
	}
	return Policy{BlockedTools: toSet(blocked), AllowedTools: toSet(allowed)}
}

// ServeStdio bridges one agent (over in/out) to a backend until either side
// closes. backendName empty selects the configured default. This is the local
// path where the agent spawns `usher serve`, so the caller is this process: it
// delegates to serveConn with a nil net.Conn, making PeerPID fall back to
// os.Getpid().
func (b *Broker) ServeStdio(ctx context.Context, backendName string, in io.Reader, out io.Writer) error {
	return b.serveConn(ctx, backendName, nil, in, out)
}

// ServeSocket runs the always-on daemon: it accepts connections on ln and
// multiplexes each one onto the SHARED backend child via serveMuxConn. Unlike the
// legacy stdio path (ServeStdio→serveConn, which spawns a fresh child per
// connection), the daemon path routes every connection through one supervised,
// long-lived pool child — many agent connections share one backend, the inverse
// of #17's fan-out. The process-wide lock registry arbitrates contention ACROSS
// connections, which now matters more because they genuinely share the child.
// ServeSocket returns when ctx is cancelled (it closes ln, which unblocks Accept)
// or Accept fails for a non-shutdown reason.
//
// The daemon owns the shared BackendSupervisor (the pool): it is built here so
// the control plane can list backends and drive their lifecycle. By default the
// pool starts LAZILY — no child is spawned until the first client's initialize
// triggers come-live in serveMuxConn — so an idle daemon costs zero backend
// processes (this is what the broker-vs-direct load test demonstrates). Passing
// prewarm restores the eager start (the configured backend is brought live with
// the daemon) for operators who want the first-call latency hidden. Either way
// the pool's state machine and its BackendState events are the source of truth,
// and the pool is torn down on return so no shared child is orphaned.
func (b *Broker) ServeSocket(ctx context.Context, backendName string, ln net.Listener, prewarm bool) error {
	defer ln.Close()

	// Build the shared backend pool once per daemon, wired to this broker so each
	// live child gets a backendMux for client multiplexing.
	if b.sv == nil {
		b.sv = newSupervisorForBroker(ctx, b)
	}
	defer b.sv.StopAll()

	// Lazy by default: the first client's initialize triggers come-live in
	// serveMuxConn (its EnsureLive call). --prewarm opts back into the eager start
	// — the child is born with the daemon — for operators who want the latency
	// hidden before any client connects. A start failure is surfaced as a warning
	// rather than aborting the daemon, since a later request can retry.
	if prewarm {
		if be := b.cfg.ResolveBackend(backendName); be != nil {
			b.audit.Infof("supervisor", "prewarm: bringing %q live at daemon start", be.Name)
			if _, err := b.sv.EnsureLive(be.Name); err != nil {
				b.audit.Errorf("supervisor", "backend %q failed to come live: %v", be.Name, err)
			}
		}
	}

	// Per-process resource sampler (gated by USHER_SAMPLE). It attributes RSS/CPU
	// per pid — the broker self pid, each live shared backend child, and each
	// connected client — and emits one ResourceSampleEvent per tick on the bus so
	// the RESOURCES panel can watch the shared-pool flat line (backend RSS = 1×cua
	// no matter how many clients connect). Off by default: a normal daemon spawns
	// no `ps` calls and is byte-for-byte unchanged. Never a system total — only the
	// pids the feeders register are sampled.
	if sampleResourcesEnabled(b.cfg) {
		sampler := procstat.New(sampleInterval, func(ss []procstat.ProcSample) {
			procs := make([]ProcStat, len(ss))
			for i, s := range ss {
				procs[i] = ProcStat{
					PID: s.PID, Role: s.Role, Label: s.Label,
					RSSKB: s.RSSKB, CPUPct: s.CPUPct, Alive: s.Alive,
				}
			}
			b.bus.Emit(ResourceSampleEvent{TS: time.Now(), Procs: procs})
		})
		// The broker's own pid is the one fixed target, registered once.
		sampler.Set(procstat.Target{PID: os.Getpid(), Role: procstat.RoleBroker, Label: "broker"})
		b.audit.Infof("procstat", "per-process resource sampler on (interval %s)", sampleInterval)
		go sampler.Run(ctx)
		go b.feedSamplerFromBus(ctx, sampler)      // client pids: ConnOpen/Close
		go b.feedSamplerFromSnapshot(ctx, sampler) // backend pids: poll Snapshot()
	}

	// Close the listener on ctx cancel so the Accept loop unblocks and returns.
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			// A closed listener (our own shutdown) is the expected exit, not an
			// error to surface.
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept: %w", err)
		}
		// One goroutine per connection, all multiplexed onto the shared child. Each
		// connection keeps its own identity, inflight map, and window-lock ownership;
		// serveMuxConn closes the conn on return. ctx is threaded so a daemon
		// shutdown unblocks an idle client's blocking Read by closing its conn.
		go b.serveMuxConn(ctx, backendName, conn)
	}
}

// serveConn is the per-connection core shared by ServeStdio (conn == nil) and
// ServeSocket (conn == the accepted *net.UnixConn). It mints an identity from the
// connection's peer credentials, spawns the backend, and pumps both directions
// until either side closes or ctx is cancelled. in/out carry the JSON-RPC stream
// (conn itself on the socket path; os.Stdin/os.Stdout on the stdio path).
func (b *Broker) serveConn(ctx context.Context, backendName string, conn net.Conn, in io.Reader, out io.Writer) error {
	be := b.cfg.ResolveBackend(backendName)
	if be == nil {
		return fmt.Errorf("no backend configured (run: usher backend add <name> -- <command...>)")
	}
	if be.Transport != "stdio" {
		return fmt.Errorf("backend %q: transport %q not supported yet", be.Name, be.Transport)
	}

	id := identity.NewForConn(conn)
	b.audit.Connect(id, be.Name)
	b.bus.Emit(ConnOpenEvent{TS: time.Now(), ConnID: id.ID, PID: id.PID, Backend: be.Name})

	// Resolve the auth strategy's env additions (Keychain-backed secrets for
	// auth=env; nil for inherit/none) before spawning the child.
	envExtra, err := config.EnvForBackend(be)
	if err != nil {
		return err
	}
	sb := backend.NewStdio(be.Name, be.Command, envExtra)
	if err := sb.Start(ctx); err != nil {
		return fmt.Errorf("start backend %q: %w", be.Name, err)
	}
	defer sb.Close()

	client := mcp.NewConn(in, out)

	// One correlation map per connection: the inbound pump records id→method,
	// the outbound pump's stages consume it to recognize the response kind.
	inflight := NewInflightMap()

	// On any exit path, reclaim every write-lock this connection still holds so a
	// caller that dies mid-action cannot wedge its target window for the next
	// agent (reclaim-on-death, #16).
	defer b.reclaim(id)

	// Reply lets an inbound stage answer the client out of band — ArbitrateStage
	// uses it to return a JSON-RPC busy error for a contended window instead of
	// forwarding the call. It shares the client conn's serialized Write.
	reply := func(m *mcp.Message) error { return client.Write(m) }

	// Pump each direction in its own goroutine.
	inboundDone := make(chan error, 1)  // client → backend ended
	outboundDone := make(chan error, 1) // backend → client ended
	go func() {
		inboundDone <- b.pump(id, be.Name, Inbound, inflight, reply, client, sb.Conn(), b.inbound)
	}()
	go func() {
		outboundDone <- b.pump(id, be.Name, Outbound, inflight, nil, sb.Conn(), client, b.outbound)
	}()

	select {
	case <-ctx.Done():
		b.audit.Disconnect(id, "signal")
		b.bus.Emit(ConnCloseEvent{TS: time.Now(), ConnID: id.ID, Reason: "signal"})
		return nil
	case <-inboundDone:
		// Client hung up: half-close the backend so it flushes and exits, then
		// drain any in-flight responses before we let go.
		_ = sb.CloseStdin()
		<-outboundDone
		b.audit.Disconnect(id, "client-eof")
		b.bus.Emit(ConnCloseEvent{TS: time.Now(), ConnID: id.ID, Reason: "client-eof"})
		return nil
	case err := <-outboundDone:
		// Backend ended first: nothing left to forward.
		reason := "backend-eof"
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
			reason = err.Error()
		}
		b.audit.Disconnect(id, reason)
		b.bus.Emit(ConnCloseEvent{TS: time.Now(), ConnID: id.ID, Reason: reason})
		return nil
	}
}

// reclaim frees every write-lock still held by a connection that is ending, the
// reclaim-on-death path. It is safe to call when nothing is held (a no-op) and
// when the registry is nil (tests that bypass New).
func (b *Broker) reclaim(id identity.Identity) {
	if b.locks == nil {
		return
	}
	if n := b.locks.ReleaseOwner(id.ID); n > 0 {
		b.audit.Errorf(id.ID, "reclaimed %d held window-lock(s) on disconnect", n)
	}
}

// pump reads from src, runs the pipeline, and writes survivors to dst. A
// per-message pipeline error is logged and skipped; a read/write error ends the
// pump (and the connection). On the inbound side it records each request's
// method into inflight so outbound stages can correlate the response.
func (b *Broker) pump(id identity.Identity, beName string, dir Direction, inflight *InflightMap, reply func(*mcp.Message) error, src, dst *mcp.Conn, pipe *Pipeline) error {
	pctx := &Context{Identity: id, Backend: beName, Dir: dir, Inflight: inflight, Locks: b.locks, Reply: reply}
	for {
		m, err := src.Read()
		if err != nil {
			return err
		}
		if dir == Inbound && m.IsRequest() {
			inflight.Record(m.IDString(), InflightEntry{
				Method:   m.Method,
				ToolName: toolNameIf(m),
			})
		}
		// Capture the on-the-wire size before the pipeline so a trimmed outbound
		// response can report how much it shed (TrimmedFromBytes). A request keeps
		// its bytes through the pipeline, so this equals len(out.Raw) there.
		preBytes := len(m.Raw)
		wasRequest := m.IsRequest()
		wasResponse := m.IsResponse()
		out, err := pipe.Run(pctx, m)
		if err != nil {
			b.audit.Errorf(id.ID, "%s pipeline: %v", dir, err)
			continue
		}
		if out == nil {
			continue // dropped by a stage (e.g. gated tool-call); no forward, no event
		}
		if err := dst.Write(out); err != nil {
			return err
		}
		// Emit the lifecycle event AFTER a successful forward, so the bus reflects
		// only what actually crossed to the other side — a dropped request (gated)
		// emits its own GateBlock event from the stage, never a Request here.
		switch {
		case dir == Inbound && wasRequest:
			b.bus.Emit(RequestEvent{
				TS: time.Now(), ConnID: id.ID, AgentPID: id.PID, Backend: beName,
				Tool: toolNameIf(out), RPCID: out.IDString(),
			})
		case dir == Outbound && wasResponse:
			b.bus.Emit(ResponseEvent{
				TS: time.Now(), ConnID: id.ID, Backend: beName, RPCID: out.IDString(),
				Bytes: outboundBytes(out), TrimmedFromBytes: preBytes,
			})
		}
	}
}

// sampleResourcesEnabled reports whether the daemon should run the per-process
// resource sampler. It is on when the config asks for it OR the USHER_SAMPLE env
// var is truthy ("1"/"true"/"yes"/"on", case-insensitive) — the env form lets
// the load harness flip it without editing config.json, mirroring how the gate's
// allow-list is overridable at serve time.
func sampleResourcesEnabled(cfg *config.Config) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(config.EnvSampleResources))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// feedSamplerFromBus translates connection lifecycle events into client targets
// for the sampler: a ConnOpenEvent adds the connecting agent's pid (role=client),
// a ConnCloseEvent removes it. ConnCloseEvent carries no pid, so the feeder keeps
// a small ConnID→pid map of the connections it added and removes by ConnID on
// close — this keeps the watch set from accumulating dead pids across many
// connect/disconnect cycles. A pid shared by several connections (e.g. all the
// load test's synthetic clients live in one harness process) is only Removed once
// every ConnID using it has closed, so an active sibling connection is never
// dropped from the sample.
//
// It is one Hub subscriber (256-buffered, drop-oldest like every other), so it
// never back-pressures the forwarding path; a dropped ConnClose at worst leaves a
// now-dead pid in the watch set, which the next tick reports as Alive:false. It
// returns when ctx is cancelled or the hub closes the subscription.
func (b *Broker) feedSamplerFromBus(ctx context.Context, sampler *procstat.Sampler) {
	ch, cancel := b.bus.Subscribe(256)
	defer cancel()
	// byConn maps a live ConnID to its pid; refs counts how many open connections
	// share each pid, so a pid is only Removed when its last connection closes.
	byConn := make(map[string]int)
	refs := make(map[int]int)
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
				if ev.PID <= 0 {
					continue
				}
				// Ignore a duplicate open for a ConnID we already track (defensive; the
				// broker emits one ConnOpen per connection).
				if _, seen := byConn[ev.ConnID]; seen {
					continue
				}
				byConn[ev.ConnID] = ev.PID
				refs[ev.PID]++
				sampler.Set(procstat.Target{
					PID: ev.PID, Role: procstat.RoleClient, Label: "client-" + ev.ConnID,
				})
			case ConnCloseEvent:
				pid, ok := byConn[ev.ConnID]
				if !ok {
					continue
				}
				delete(byConn, ev.ConnID)
				if refs[pid]--; refs[pid] <= 0 {
					delete(refs, pid)
					sampler.Remove(pid) // last connection on this pid closed
				}
			}
		}
	}
}

// feedSamplerFromSnapshot keeps the sampler's BACKEND targets in sync with the
// supervisor's live children by polling sv.Snapshot() every reconcileInterval —
// the SAME []BackendStatus the UI reads, so the sampler stays decoupled from the
// supervisor's internals. A backend that is live with a non-zero pid is Set
// (role=backend); a backend that left live (its pid changed or dropped to 0) has
// its previously-tracked pid Removed, so a restarted child's old pid does not
// linger. It returns when ctx is cancelled.
func (b *Broker) feedSamplerFromSnapshot(ctx context.Context, sampler *procstat.Sampler) {
	t := time.NewTicker(reconcileInterval)
	defer t.Stop()
	// tracked maps a backend NAME to the pid we last registered for it, so a
	// child that died or was replaced has its stale pid removed before the new one
	// is added.
	tracked := make(map[string]int)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if b.sv == nil {
				continue
			}
			live := make(map[string]int)
			for _, st := range b.sv.Snapshot() {
				if st.State == "live" && st.PID > 0 {
					live[st.Name] = st.PID
				}
			}
			// Add/refresh live backends; remove ones whose pid changed or left live.
			for name, pid := range live {
				if old, ok := tracked[name]; ok && old != pid {
					sampler.Remove(old)
				}
				sampler.Set(procstat.Target{PID: pid, Role: procstat.RoleBackend, Label: name})
				tracked[name] = pid
			}
			for name, old := range tracked {
				if _, ok := live[name]; !ok {
					sampler.Remove(old)
					delete(tracked, name)
				}
			}
		}
	}
}

// outboundBytes returns the on-the-wire size of a forwarded message. A verbatim
// message still carries its original Raw; a stage that rewrote it (TrimStage)
// cleared Raw, so we re-marshal to measure the bytes the client will actually
// receive. Best-effort: a marshal error falls back to the (now stale) Raw length.
func outboundBytes(m *mcp.Message) int {
	if m.Raw != nil {
		return len(m.Raw)
	}
	if b, err := json.Marshal(m); err == nil {
		return len(b)
	}
	return len(m.Raw)
}
