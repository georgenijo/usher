package broker

import (
	"github.com/georgenijo/usher/internal/identity"
	"github.com/georgenijo/usher/internal/mcp"
)

// Direction is which way a message is flowing through the broker.
type Direction int

const (
	// Inbound is client → backend (an agent's request).
	Inbound Direction = iota
	// Outbound is backend → client (a tool result).
	Outbound
)

// String renders the direction for audit lines.
func (d Direction) String() string {
	if d == Inbound {
		return "client→backend"
	}
	return "backend→client"
}

// Context carries per-message metadata to each stage.
type Context struct {
	Identity identity.Identity
	Backend  string
	Dir      Direction

	// Inflight correlates a request to its response (id → method/tool). The
	// inbound pump records into it; outbound stages that must act on a specific
	// response kind (TrimStage on tools/call results) consume from it. Nil-safe:
	// stages check for nil before use.
	Inflight *InflightMap

	// Locks is the shared per-window write-lock registry (#16). The inbound
	// ArbitrateStage acquires; the outbound ArbitrateStage releases on the
	// matching response. Nil-safe: ArbitrateStage forwards everything when nil.
	Locks *lockRegistry

	// Reply injects a message back toward the CLIENT, out of band of the normal
	// forward. A stage on the inbound path that must refuse a request (e.g.
	// ArbitrateStage answering a busy window with a JSON-RPC error) writes the
	// error response here and returns (nil, nil) to drop the forward, so the
	// agent gets an answer instead of hanging. Nil on the outbound path.
	Reply func(*mcp.Message) error

	// ClientID, when non-empty, is the response's ORIGINAL client-side JSON-RPC
	// id in multi-backend aggregation (#17). The fanout's outbound pump keeps the
	// backend-side id on the message through the pipeline so the inflight-keyed
	// stages (TrimStage.Consume, ArbitrateStage.release) still correlate, but the
	// id the client actually sees is the client id, restored just before the
	// wire. AuditStage prefers this so its log line shows the client-facing id
	// instead of the internal backend-side id. Empty on the single-backend path
	// and for responses the fanout did not remap.
	ClientID string
}

// Stage is one step in the middleware pipeline. Process may inspect or transform
// the message; returning (nil, nil) drops it; returning an error aborts this
// message (the link stays up). A stage that mutates a message must clear
// m.Raw so it is re-encoded on write.
type Stage interface {
	Name() string
	Process(ctx *Context, m *mcp.Message) (*mcp.Message, error)
}

// Pipeline runs an ordered list of stages over a message.
type Pipeline struct {
	stages []Stage
}

// NewPipeline builds a pipeline from stages in execution order.
func NewPipeline(stages ...Stage) *Pipeline { return &Pipeline{stages: stages} }

// Run threads the message through every stage. The first non-nil error stops
// the chain; a nil message from any stage means "dropped".
func (p *Pipeline) Run(ctx *Context, m *mcp.Message) (*mcp.Message, error) {
	var err error
	for _, s := range p.stages {
		if m, err = s.Process(ctx, m); err != nil {
			return nil, err
		}
		if m == nil {
			return nil, nil
		}
	}
	return m, nil
}
