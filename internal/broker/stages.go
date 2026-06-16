package broker

import (
	"encoding/json"
	"fmt"
	"strings"

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

// DefaultTrimThreshold is the size (in bytes of a text content item) at or
// below which TrimStage leaves the result untouched. A small tool result (a
// click ack, a short list) carries no AX bloat, so compaction is skipped and
// the bytes pass through verbatim. Above it, an oversized AX snapshot is
// compacted to the actionable digest.
const DefaultTrimThreshold = 2000

// TrimStage compacts oversized tool-call RESULTS — the AX-tree digest a backend
// like cua-driver returns from get_window_state — down to the actionable
// BUTTONS list and DISPLAY values a brain needs, the port of GhostHands' Python
// `compaction` (see digest.go). It acts ONLY on responses to tools/call,
// correlated via the inflight map, and ONLY on text content that looks like an
// AX snapshot; tools/list schemas and protocol messages are never touched. #15.
type TrimStage struct {
	// threshold is the per-text-item size above which compaction runs.
	threshold int
}

// NewTrimStage returns a trim stage using the default threshold.
func NewTrimStage() *TrimStage { return &TrimStage{threshold: DefaultTrimThreshold} }

// NewTrimStageThreshold returns a trim stage with a custom size threshold. The
// broker wires Config.TrimThreshold through here (falling back to the default);
// tests use it to exercise the compaction boundary directly.
func NewTrimStageThreshold(threshold int) *TrimStage { return &TrimStage{threshold: threshold} }

// Name identifies the stage.
func (s *TrimStage) Name() string { return "trim" }

// toolResult mirrors the relevant shape of a tools/call result. Unknown fields
// are preserved across the round-trip by json.RawMessage on each content item,
// so the image item and any structuredContent survive untouched.
type toolResult struct {
	Content           []json.RawMessage `json:"content"`
	IsError           *bool             `json:"isError,omitempty"`
	StructuredContent json.RawMessage   `json:"structuredContent,omitempty"`
}

// contentText is the text-typed content item: {"type":"text","text":"..."}.
type contentText struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// Process compacts the AX digest in a tools/call result and forwards everything
// else verbatim. It clears m.Raw only when it actually rewrites a text item.
func (s *TrimStage) Process(ctx *Context, m *mcp.Message) (*mcp.Message, error) {
	// Only outbound responses are candidates; requests and notifications pass.
	if ctx.Dir != Outbound || !m.IsResponse() || ctx.Inflight == nil {
		return m, nil
	}
	// Correlate: the request that produced this response must be a tools/call.
	// This is the precise guard that excludes tools/list, initialize, and every
	// other response — only tools/call results reach the compaction below.
	entry, ok := ctx.Inflight.Consume(m.IDString())
	if !ok || entry.Method != "tools/call" {
		return m, nil
	}
	if len(m.Result) == 0 {
		return m, nil
	}

	var res toolResult
	if err := json.Unmarshal(m.Result, &res); err != nil {
		// Not the shape we expected (an error object, a non-tool result):
		// forward verbatim rather than risk corrupting the stream.
		return m, nil
	}

	changed := false
	for i, raw := range res.Content {
		var ct contentText
		if err := json.Unmarshal(raw, &ct); err != nil || ct.Type != "text" {
			continue // image items and other types are left byte-for-byte intact
		}
		// Only compact a text item that is both oversized AND an AX snapshot.
		// The "AXWindow" marker is the activating signal (a non-AX text result
		// — a long log line — is left alone even when large).
		if len(ct.Text) <= s.threshold || !strings.Contains(ct.Text, "AXWindow") {
			continue
		}
		ct.Text = digestText(ct.Text)
		nb, err := json.Marshal(ct)
		if err != nil {
			return nil, fmt.Errorf("trim: re-encode content: %w", err)
		}
		res.Content[i] = nb
		changed = true
	}

	if !changed {
		return m, nil // nothing matched; leave the original bytes in place
	}

	nr, err := json.Marshal(res)
	if err != nil {
		return nil, fmt.Errorf("trim: re-encode result: %w", err)
	}
	m.Result = nr
	m.Raw = nil // force Conn.Write to re-marshal from the struct fields
	return m, nil
}

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
