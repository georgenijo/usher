// Package control is usher's localhost-only HTTP control plane: a small net/http
// surface the daemon serves so a human (and the embedded web UI) can see the
// shared backend pool, manage its lifecycle (start/stop/restart), watch who is
// calling which backend, and see a backend come live on demand. It is loopback
// ONLY — it binds 127.0.0.1 and never a routable address, because usher is a
// single-user local tool with no auth and the loopback interface is the security
// boundary. Live updates ride a Server-Sent-Events stream off the broker's event
// Hub; management actions are POSTs that drive the BackendSupervisor. Everything
// here is stdlib: net/http for the server, the Hub for events, go:embed for the UI
// — no router, no websocket library, no Node build step.
package control

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/georgenijo/usher/internal/broker"
)

// DefaultAddr is the loopback address the control plane binds when nothing
// overrides it. The host is the literal 127.0.0.1 (never ":7187", never
// 0.0.0.0) so the server is reachable only from this machine.
const DefaultAddr = "127.0.0.1:7187"

// EnvAddr overrides the control-plane listen address. Its value is still
// validated to be loopback (host 127.0.0.1 / localhost / ::1) — a non-loopback
// host is rejected, so an operator can move the port but never expose the API.
const EnvAddr = "USHER_UI_ADDR"

// Server is the control-plane HTTP server. It reads the shared backend pool
// (Supervisor) for the backend listing and drives its lifecycle from POST
// handlers, subscribes to the event Hub for the SSE stream, and maintains a
// live-connection registry (also Hub-fed) for GET /api/connections. cfgJSON is
// the redacted config the UI shows; it is captured once at construction.
type Server struct {
	bus     *broker.Hub
	sv      *broker.BackendSupervisor
	reg     *connRegistry
	res     *resourceState
	metrics *metricsState
	mux     *http.ServeMux
	addr    string

	// cfgSnapshot is the config view served by GET /api/config, captured at New so
	// the handler never re-reads disk on the hot path. It carries no secrets (the
	// config type already keeps secret VALUES out — only env var NAMES appear).
	cfgSnapshot any

	// startedAt is the server's construction time, used by GET /healthz to report
	// uptime. It is captured once at New so the liveness probe stays a cheap read.
	startedAt time.Time
}

// New builds a control server over the broker's event bus and shared backend
// supervisor. addr is the listen address ("" → DefaultAddr, or the EnvAddr
// override); it is validated to be loopback. cfgSnapshot is an arbitrary
// JSON-marshalable value served verbatim by GET /api/config (the daemon passes
// the loaded config; nil yields an empty object). A nil supervisor still serves —
// the backend list is empty and the POST routes report no-such-backend — so the
// HTTP surface is testable without a live daemon.
func New(bus *broker.Hub, sv *broker.BackendSupervisor, cfgSnapshot any) *Server {
	s := &Server{
		bus:         bus,
		sv:          sv,
		reg:         newConnRegistry(),
		res:         newResourceState(),
		metrics:     newMetricsState(),
		mux:         http.NewServeMux(),
		cfgSnapshot: cfgSnapshot,
		startedAt:   time.Now(),
	}
	s.routes()
	return s
}

// routes registers every handler on the server's mux. Go 1.26's ServeMux honors
// method + path-wildcard patterns ("POST /api/backends/{name}/start"), so the
// routing needs no third-party router.
func (s *Server) routes() {
	s.mux.HandleFunc("GET /", s.handleIndex)
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /api/backends", s.handleBackends)
	s.mux.HandleFunc("GET /api/connections", s.handleConnections)
	s.mux.HandleFunc("GET /api/resources", s.handleResources)
	s.mux.HandleFunc("GET /api/config", s.handleConfig)
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)
	s.mux.HandleFunc("GET /api/events", s.handleEvents)
	s.mux.HandleFunc("POST /api/backends/{name}/start", s.manage((*broker.BackendSupervisor).Start))
	s.mux.HandleFunc("POST /api/backends/{name}/stop", s.manage((*broker.BackendSupervisor).Stop))
	s.mux.HandleFunc("POST /api/backends/{name}/restart", s.manage((*broker.BackendSupervisor).Restart))
}

// Handler exposes the configured mux so tests can drive it with
// net/http/httptest without binding a socket.
func (s *Server) Handler() http.Handler { return s.mux }

// Listen binds the control plane's loopback listener. It validates the address is
// loopback FIRST (so a misconfigured EnvAddr can never expose the API on a
// routable interface), then listens on it. The returned listener's Addr starts
// with 127.0.0.1 (or ::1); the daemon prints it as the UI URL.
func (s *Server) Listen() (net.Listener, error) {
	addr := s.addr
	if addr == "" {
		addr = DefaultAddr
	}
	if err := validateLoopback(addr); err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("control plane listen on %s: %w", addr, err)
	}
	return ln, nil
}

// SetAddr overrides the listen address before Listen (the daemon resolves the
// EnvAddr override and passes it here). The value is validated at Listen.
func (s *Server) SetAddr(addr string) { s.addr = addr }

// Serve runs the HTTP server on ln until ctx is cancelled, then shuts it down
// gracefully. It also starts the connection registry's Hub watcher (so
// /api/connections reflects live state) under the same ctx. Serve blocks; the
// daemon runs it in its own goroutine.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	go s.reg.Watch(ctx, s.bus)
	go s.res.Watch(ctx, s.bus)
	go s.metrics.Watch(ctx, s.bus)

	srv := &http.Server{
		Handler:           s.mux,
		ReadHeaderTimeout: 5 * time.Second,
		// No WriteTimeout: the SSE stream is a long-lived response and a write
		// deadline would sever it. Idle connections are bounded by the client and
		// ctx-cancel shutdown below.
	}
	// Shut the server down when ctx is cancelled so a daemon SIGTERM closes the
	// control plane cleanly (in-flight SSE streams end via their request context).
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// handleBackends answers GET /api/backends with the supervisor's snapshot: every
// configured backend with its state, pid-equivalent refs, uptime, tool count, and
// last error. A nil supervisor yields an empty list (a bare test server).
func (s *Server) handleBackends(w http.ResponseWriter, _ *http.Request) {
	var snap []broker.BackendStatus
	if s.sv != nil {
		snap = s.sv.Snapshot()
	}
	if snap == nil {
		snap = []broker.BackendStatus{}
	}
	writeJSON(w, http.StatusOK, snap)
}

// handleConnections answers GET /api/connections with the live agent connections
// the registry tracks from the event bus (connID, agentPID, backend, openedAt).
func (s *Server) handleConnections(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.reg.snapshot())
}

// handleResources answers GET /api/resources with the latest per-process
// resource tick: the per-pid samples (pid, role, name, RSS in MB, CPU%) plus the
// per-role rollups (total backend RSS, broker RSS, client RSS, and the live
// backend-child count). Every number is a SUM OF PER-PID rows — never a machine
// total. Before the first sampler tick (or with sampling off) it is a
// well-formed empty payload, so the panel paints cleanly rather than erroring.
func (s *Server) handleResources(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.res.snapshot())
}

// handleConfig answers GET /api/config with the config snapshot captured at New.
// It carries no secrets (the config type keeps secret VALUES out of its on-disk
// form — only env-var NAMES appear), so it is safe to serve to the local UI.
func (s *Server) handleConfig(w http.ResponseWriter, _ *http.Request) {
	cfg := s.cfgSnapshot
	if cfg == nil {
		cfg = struct{}{}
	}
	writeJSON(w, http.StatusOK, cfg)
}

// healthz is the JSON body GET /healthz returns: a fixed "ok" status plus a few
// cheap, already-available liveness counters — the daemon pid, seconds since the
// control server was constructed, and the count of configured backends and live
// agent connections. It is a probe payload, never authoritative state; the rich
// listings live behind /api/backends and /api/connections.
type healthz struct {
	Status        string `json:"status"`
	PID           int    `json:"pid"`
	UptimeSeconds int64  `json:"uptimeSeconds"`
	Backends      int    `json:"backends"`
	Connections   int    `json:"connections"`
}

// handleHealthz answers GET /healthz with a 200 and the healthz probe payload. It
// touches only counters already on hand — the supervisor's backend count and the
// connection registry's live size — so it stays a cheap liveness check that never
// blocks on the broker or re-reads disk. A nil supervisor reports zero backends so
// a bare server (no daemon behind it) still answers a clean 200.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	backends := 0
	if s.sv != nil {
		backends = len(s.sv.Snapshot())
	}
	writeJSON(w, http.StatusOK, healthz{
		Status:        "ok",
		PID:           os.Getpid(),
		UptimeSeconds: int64(time.Since(s.startedAt).Seconds()),
		Backends:      backends,
		Connections:   len(s.reg.snapshot()),
	})
}

// handleMetrics answers GET /metrics with the broker counters in Prometheus text
// exposition format (plaintext "key value" lines, no client library). The
// counters are read-only observations folded off the SAME event bus the broker
// already emits to, so scraping never touches the forwarding hot path; the
// backends-configured gauge is read live from the supervisor here (it is config,
// not an event counter, and a nil supervisor reports zero — a bare test server).
func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	backends := 0
	if s.sv != nil {
		backends = len(s.sv.Snapshot())
	}
	// Prometheus text format is UTF-8 text/plain version 0.0.4; a plain scraper or
	// `grep` reads it either way.
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(s.metrics.render(backends)))
}

// manage builds a POST handler for a lifecycle action (Start/Stop/Restart),
// dispatched by the {name} path wildcard. It drives the supervisor method, then
// returns the backend's NEW state so the UI can reflect the transition without a
// follow-up poll. An unknown backend is a 404 JSON error; an action error
// (e.g. a failed start) is a 502 carrying the message — the state still reflects
// the failed transition the supervisor recorded.
func (s *Server) manage(action func(*broker.BackendSupervisor, string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if s.sv == nil {
			writeError(w, http.StatusNotFound, fmt.Sprintf("no backend named %q", name))
			return
		}
		if _, ok := s.sv.Find(name); !ok {
			writeError(w, http.StatusNotFound, fmt.Sprintf("no backend named %q", name))
			return
		}
		actErr := action(s.sv, name)
		// Report the post-action state regardless: a failed start still transitioned
		// the backend to "failed", which the UI must see.
		st, ok := s.sv.Find(name)
		if !ok {
			writeError(w, http.StatusNotFound, fmt.Sprintf("no backend named %q", name))
			return
		}
		if actErr != nil {
			writeJSON(w, http.StatusBadGateway, struct {
				broker.BackendStatus
				Error string `json:"error"`
			}{st, actErr.Error()})
			return
		}
		writeJSON(w, http.StatusOK, st)
	}
}

// writeJSON marshals v as the response body with the given status. A marshal
// error (should never happen for our types) falls back to a 500 plain message.
func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "marshal response: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// writeError writes a JSON {"error": msg} body with the given status, the shape
// every error response in this package uses so the UI can parse failures
// uniformly.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// validateLoopback returns an error unless addr's host is a loopback host
// (127.0.0.1, ::1, or localhost). This is the hard guarantee that the control
// plane never binds a routable interface, enforced before every Listen so a
// misconfigured EnvAddr fails closed rather than exposing the API.
func validateLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("control plane addr %q: %w (want host:port, e.g. 127.0.0.1:7187)", addr, err)
	}
	host = strings.TrimSpace(host)
	switch strings.ToLower(host) {
	case "127.0.0.1", "localhost", "::1":
		return nil
	}
	// Accept any IP the runtime classifies as loopback (e.g. 127.x.x.x), but
	// reject everything else — including the empty host, which would bind every
	// interface (0.0.0.0).
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("control plane addr %q is not loopback; refusing to bind a non-127.0.0.1 host (set %s to a loopback host:port)", addr, EnvAddr)
}
