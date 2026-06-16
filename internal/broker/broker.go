// Package broker is usher's front desk: it accepts an agent connection, routes
// it to a backend, runs every message through the middleware pipeline, and
// forwards verbatim by default. The stdio path here is the #14 skeleton; the
// always-on Unix-socket daemon with many concurrent connections is #20.
package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/georgenijo/usher/internal/audit"
	"github.com/georgenijo/usher/internal/backend"
	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/identity"
	"github.com/georgenijo/usher/internal/mcp"
)

// Broker holds the shared config, the per-window write-lock registry, and the
// two per-direction pipelines.
type Broker struct {
	cfg      *config.Config
	audit    *audit.Logger
	locks    *lockRegistry // shared per-window write-locks (#16)
	inbound  *Pipeline     // client → backend
	outbound *Pipeline     // backend → client
}

// New builds a broker from config, logging audit to stderr.
func New(cfg *config.Config) (*Broker, error) {
	al := audit.New(os.Stderr)
	// A zero (unset) config threshold means "use the built-in default".
	trimThreshold := DefaultTrimThreshold
	if cfg.TrimThreshold > 0 {
		trimThreshold = cfg.TrimThreshold
	}
	// The lock registry is process-wide so contention is arbitrated ACROSS
	// connections — two agents driving the same window must serialise even
	// though each has its own pair of pumps. Zero ttl/wait take the defaults.
	locks := newLockRegistry(cfg.LockTTL(), cfg.LockWait())
	return &Broker{
		cfg:      cfg,
		audit:    al,
		locks:    locks,
		inbound:  NewPipeline(NewGateStage(), NewArbitrateStage(), NewAuditStage(al, Inbound)),
		outbound: NewPipeline(NewArbitrateStage(), NewTrimStageThreshold(trimThreshold), NewAuditStage(al, Outbound)),
	}, nil
}

// ServeStdio bridges one agent (over in/out) to a backend until either side
// closes. backendName empty selects the configured default.
func (b *Broker) ServeStdio(ctx context.Context, backendName string, in io.Reader, out io.Writer) error {
	be := b.cfg.ResolveBackend(backendName)
	if be == nil {
		return fmt.Errorf("no backend configured (run: usher backend add <name> -- <command...>)")
	}
	if be.Transport != "stdio" {
		return fmt.Errorf("backend %q: transport %q not supported yet", be.Name, be.Transport)
	}

	id := identity.New()
	b.audit.Connect(id, be.Name)

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
		return nil
	case <-inboundDone:
		// Client hung up: half-close the backend so it flushes and exits, then
		// drain any in-flight responses before we let go.
		_ = sb.CloseStdin()
		<-outboundDone
		b.audit.Disconnect(id, "client-eof")
		return nil
	case err := <-outboundDone:
		// Backend ended first: nothing left to forward.
		reason := "backend-eof"
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
			reason = err.Error()
		}
		b.audit.Disconnect(id, reason)
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
		out, err := pipe.Run(pctx, m)
		if err != nil {
			b.audit.Errorf(id.ID, "%s pipeline: %v", dir, err)
			continue
		}
		if out == nil {
			continue // dropped by a stage
		}
		if err := dst.Write(out); err != nil {
			return err
		}
	}
}
