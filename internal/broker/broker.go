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

// Broker holds the shared config and the two per-direction pipelines.
type Broker struct {
	cfg      *config.Config
	audit    *audit.Logger
	inbound  *Pipeline // client → backend
	outbound *Pipeline // backend → client
}

// New builds a broker from config, logging audit to stderr.
func New(cfg *config.Config) (*Broker, error) {
	al := audit.New(os.Stderr)
	return &Broker{
		cfg:      cfg,
		audit:    al,
		inbound:  NewPipeline(NewGateStage(), NewArbitrateStage(), NewAuditStage(al, Inbound)),
		outbound: NewPipeline(NewTrimStage(), NewAuditStage(al, Outbound)),
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

	sb := backend.NewStdio(be.Name, be.Command)
	if err := sb.Start(ctx); err != nil {
		return fmt.Errorf("start backend %q: %w", be.Name, err)
	}
	defer sb.Close()

	client := mcp.NewConn(in, out)

	// One correlation map per connection: the inbound pump records id→method,
	// the outbound pump's stages consume it to recognize the response kind.
	inflight := NewInflightMap()

	// Pump each direction in its own goroutine.
	inboundDone := make(chan error, 1)  // client → backend ended
	outboundDone := make(chan error, 1) // backend → client ended
	go func() { inboundDone <- b.pump(id, be.Name, Inbound, inflight, client, sb.Conn(), b.inbound) }()
	go func() { outboundDone <- b.pump(id, be.Name, Outbound, inflight, sb.Conn(), client, b.outbound) }()

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

// pump reads from src, runs the pipeline, and writes survivors to dst. A
// per-message pipeline error is logged and skipped; a read/write error ends the
// pump (and the connection). On the inbound side it records each request's
// method into inflight so outbound stages can correlate the response.
func (b *Broker) pump(id identity.Identity, beName string, dir Direction, inflight *InflightMap, src, dst *mcp.Conn, pipe *Pipeline) error {
	pctx := &Context{Identity: id, Backend: beName, Dir: dir, Inflight: inflight}
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
