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
