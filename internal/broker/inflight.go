package broker

import (
	"encoding/json"
	"sync"

	"github.com/georgenijo/usher/internal/mcp"
)

// InflightEntry records what a pending request asked for so the outbound side
// can act on the matching response — which carries no method of its own.
type InflightEntry struct {
	Method   string // JSON-RPC method, e.g. "tools/call", "tools/list"
	ToolName string // params.name when Method == "tools/call", else ""
}

// InflightMap correlates a request to its response. The broker sees responses
// without their request method (a JSON-RPC result has only an id), so a stage
// that must act on a specific kind of response — TrimStage on tools/call
// results, ArbitrateStage on the release-the-lock response (#16) — records the
// request on the inbound path and consumes it on the outbound path.
//
// The id key is the raw JSON id as IDString renders it ("7" for a number,
// "\"abc\"" for a string); Record and Consume must use the same rendering.
// Safe for concurrent use by the two pump goroutines.
type InflightMap struct {
	mu sync.Mutex
	m  map[string]InflightEntry
}

// NewInflightMap returns an empty correlation map.
func NewInflightMap() *InflightMap {
	return &InflightMap{m: make(map[string]InflightEntry)}
}

// Record stores entry under id. Empty ids (notifications carry none) are
// ignored so a stray "" key can never collide.
func (f *InflightMap) Record(id string, entry InflightEntry) {
	if id == "" {
		return
	}
	f.mu.Lock()
	f.m[id] = entry
	f.mu.Unlock()
}

// Consume returns the entry for id and removes it, so the map cannot grow
// without bound. ok is false for an unknown id (a notification, or a response
// to a request the broker never saw) — the caller then forwards unchanged.
func (f *InflightMap) Consume(id string) (entry InflightEntry, ok bool) {
	if id == "" {
		return InflightEntry{}, false
	}
	f.mu.Lock()
	entry, ok = f.m[id]
	if ok {
		delete(f.m, id)
	}
	f.mu.Unlock()
	return entry, ok
}

// toolNameIf returns params.name for a tools/call request and "" otherwise, so
// the cheap one-field unmarshal only runs on the message kind that needs it.
func toolNameIf(m *mcp.Message) string {
	if m.Method != "tools/call" {
		return ""
	}
	return toolName(m.Params)
}

// toolName extracts params.name from a tools/call request without disturbing
// the rest of the message. A miss returns "" — the caller still records the
// method, just without the tool-name refinement.
func toolName(params json.RawMessage) string {
	if len(params) == 0 {
		return ""
	}
	var p struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return ""
	}
	return p.Name
}
