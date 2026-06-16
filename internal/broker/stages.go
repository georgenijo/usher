package broker

import (
	"github.com/georgenijo/usher/internal/audit"
	"github.com/georgenijo/usher/internal/mcp"
)

// The pipeline order encodes the broker's job. Requests (client→backend) pass
// through gate then arbitrate; responses (backend→client) pass through trim.
// Audit sits at the end of both so it records the message as actually
// forwarded. The three substantive stages below are deliberate pass-throughs in
// the skeleton — each is the home for a tracked issue:
//
//	GateStage      — policy / draft-before-destructive   (#18)
//	ArbitrateStage — per-window write-lock, TTL lease     (#16)
//	TrimStage      — compaction of oversized AX digests   (#15 ★, port of
//	                 GhostHands' Python `compaction`)
//
// Implementing one means filling in its Process; the wiring already exists.

// GateStage will block destructive/irreversible actions pending policy. #18.
type GateStage struct{}

// NewGateStage returns a pass-through gate stage.
func NewGateStage() *GateStage { return &GateStage{} }

// Name identifies the stage.
func (s *GateStage) Name() string { return "gate" }

// Process currently forwards unchanged.
func (s *GateStage) Process(_ *Context, m *mcp.Message) (*mcp.Message, error) { return m, nil }

// ArbitrateStage will acquire a per-window write-lock before a mutating action
// and release it on the matching response; reads stay ungated; leases are TTL'd
// and reclaimed on caller death. #16.
type ArbitrateStage struct{}

// NewArbitrateStage returns a pass-through arbitration stage.
func NewArbitrateStage() *ArbitrateStage { return &ArbitrateStage{} }

// Name identifies the stage.
func (s *ArbitrateStage) Name() string { return "arbitrate" }

// Process currently forwards unchanged.
func (s *ArbitrateStage) Process(_ *Context, m *mcp.Message) (*mcp.Message, error) { return m, nil }

// TrimStage will compact oversized tool responses (the AX-tree digest) before
// they reach the brain — the port of GhostHands' Python `compaction`. The
// highest-leverage stage and the first to implement. #15.
type TrimStage struct{}

// NewTrimStage returns a pass-through trim stage.
func NewTrimStage() *TrimStage { return &TrimStage{} }

// Name identifies the stage.
func (s *TrimStage) Name() string { return "trim" }

// Process currently forwards unchanged.
func (s *TrimStage) Process(_ *Context, m *mcp.Message) (*mcp.Message, error) { return m, nil }

// AuditStage logs every message crossing the broker in its direction.
type AuditStage struct {
	log *audit.Logger
	dir Direction
}

// NewAuditStage returns an audit stage bound to a direction.
func NewAuditStage(l *audit.Logger, dir Direction) *AuditStage {
	return &AuditStage{log: l, dir: dir}
}

// Name identifies the stage.
func (s *AuditStage) Name() string { return "audit" }

// Process records the message, then forwards it unchanged.
func (s *AuditStage) Process(ctx *Context, m *mcp.Message) (*mcp.Message, error) {
	s.log.Message(ctx.Identity.ID, s.dir.String(), m.Method, m.IDString(), len(m.Raw))
	return m, nil
}
