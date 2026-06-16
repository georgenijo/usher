package broker

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/georgenijo/usher/internal/identity"
	"github.com/georgenijo/usher/internal/mcp"
)

// outboundCtx builds a Context as the outbound pump would, with the inflight
// map pre-loaded so the response can be correlated.
func outboundCtx(inflight *InflightMap) *Context {
	return &Context{Identity: identity.New(), Backend: "test", Dir: Outbound, Inflight: inflight}
}

// fatAXResult returns the raw JSON of a tools/call result whose text content is
// a large AX snapshot (image item first, text item second — the cua-driver SOM
// layout), and the marshaled mcp.Message for id. The returned message has its
// Raw set to the full wire bytes, exactly as Conn.Read would produce it, so
// pass-through tests can assert byte-for-byte Raw preservation.
func fatAXResult(t *testing.T, id string) *mcp.Message {
	t.Helper()
	var sb strings.Builder
	sb.WriteString("window_id=5 pid=1234 size=2560x1440 elements=120\n\n")
	sb.WriteString("- [0] AXWindow \"Calculator\"\n")
	sb.WriteString("  - AXStaticText = \"81\" (Edit field)\n")
	for i := 1; i <= 120; i++ {
		sb.WriteString("  - [")
		sb.WriteString(itoa(i))
		sb.WriteString("] AXButton (Button number ")
		sb.WriteString(itoa(i))
		sb.WriteString(") [id=b")
		sb.WriteString(itoa(i))
		sb.WriteString(" actions=[press]]\n")
	}
	axText := sb.String()
	if len(axText) <= DefaultTrimThreshold {
		t.Fatalf("test fixture not large enough: %d bytes", len(axText))
	}

	result := map[string]any{
		"content": []any{
			map[string]any{"type": "image", "data": "QkFTRTY0UE5H", "mimeType": "image/png"},
			map[string]any{"type": "text", "text": axText},
		},
		"structuredContent": map[string]any{"element_count": 120},
	}
	rb, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return withWireRaw(t, &mcp.Message{JSONRPC: "2.0", ID: json.RawMessage(id), Result: rb})
}

// withWireRaw populates m.Raw with the full marshaled message bytes, mimicking a
// message that came off the wire via Conn.Read (where Raw holds the exact line).
// Pass-through code paths must forward those bytes verbatim, so tests use the
// returned wire bytes to assert Raw is preserved byte-for-byte.
func withWireRaw(t *testing.T, m *mcp.Message) *mcp.Message {
	t.Helper()
	wire, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	m.Raw = wire
	return m
}

func TestTrimStage_TrimsLargeGetWindowState(t *testing.T) {
	f := NewInflightMap()
	f.Record("7", InflightEntry{Method: "tools/call", ToolName: "get_window_state"})
	m := fatAXResult(t, "7")
	origLen := len(m.Result)

	out, err := NewTrimStage().Process(outboundCtx(f), m)
	if err != nil {
		t.Fatal(err)
	}
	if out.Raw != nil {
		t.Error("Raw must be cleared after a modifying trim so Write re-encodes")
	}
	if len(out.Result) >= origLen {
		t.Errorf("result did not shrink: %d -> %d", origLen, len(out.Result))
	}

	// Still valid JSON-RPC and the digest is in the text item.
	var res toolResult
	if err := json.Unmarshal(out.Result, &res); err != nil {
		t.Fatalf("trimmed result is not valid JSON: %v", err)
	}
	if len(res.Content) != 2 {
		t.Fatalf("content length changed: %d", len(res.Content))
	}
	// Image item (content[0]) is untouched.
	var img map[string]any
	_ = json.Unmarshal(res.Content[0], &img)
	if img["type"] != "image" || img["data"] != "QkFTRTY0UE5H" {
		t.Errorf("image content item was altered: %v", img)
	}
	// Text item (content[1]) is now the digest.
	var ct contentText
	_ = json.Unmarshal(res.Content[1], &ct)
	if !strings.HasPrefix(ct.Text, "BUTTONS (act by element_index):") {
		t.Errorf("text item is not a digest:\n%s", ct.Text)
	}
	if !strings.Contains(ct.Text, "[1] AXButton") {
		t.Error("digest lost its actionable element lines")
	}
	// 80-element cap: the AXWindow line plus 79 of the 120 buttons. Strip the
	// header line, keep the element lines between it and the DISPLAY section.
	section := ct.Text[:strings.Index(ct.Text, "\n\nDISPLAY:")]
	section = strings.TrimPrefix(section, "BUTTONS (act by element_index):\n")
	elemLines := strings.Split(section, "\n")
	if len(elemLines) != 80 {
		t.Errorf("expected 80 capped BUTTONS lines, got %d", len(elemLines))
	}
	if c := strings.Count(section, "AXButton"); c != 79 {
		t.Errorf("expected 79 button lines under the 80-element cap, got %d", c)
	}
}

func TestTrimStage_PassThruToolsList(t *testing.T) {
	f := NewInflightMap()
	f.Record("2", InflightEntry{Method: "tools/list"})
	// A large tools/list result that even contains the word AXWindow in a
	// schema description must NOT be trimmed.
	raw := json.RawMessage(`{"tools":[{"name":"get_window_state","description":"Walk the AXWindow tree ` +
		strings.Repeat("x", 5000) + `","inputSchema":{"type":"object"}}]}`)
	m := withWireRaw(t, &mcp.Message{JSONRPC: "2.0", ID: json.RawMessage("2"), Result: raw})
	origRaw := append([]byte(nil), m.Raw...)
	orig := append([]byte(nil), m.Result...)

	out, err := NewTrimStage().Process(outboundCtx(f), m)
	if err != nil {
		t.Fatal(err)
	}
	// Raw must survive byte-for-byte so Conn.Write forwards the exact bytes; a
	// stage that wrongly cleared Raw on a tools/list pass-through is caught here.
	if string(out.Raw) != string(origRaw) {
		t.Errorf("tools/list Raw must pass through byte-identical:\n got: %s\nwant: %s", out.Raw, origRaw)
	}
	if string(out.Result) != string(orig) {
		t.Error("tools/list result was modified")
	}
}

func TestTrimStage_PassThruToolsListRawIdentical(t *testing.T) {
	// End-to-end byte-identity: a Read'd tools/list message (Raw set) survives
	// the stage unchanged so Conn.Write forwards the exact original bytes.
	f := NewInflightMap()
	f.Record("2", InflightEntry{Method: "tools/list"})
	wire := []byte(`{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"click","inputSchema":{}}]}}`)
	var m mcp.Message
	if err := json.Unmarshal(wire, &m); err != nil {
		t.Fatal(err)
	}
	m.Raw = append([]byte(nil), wire...)

	out, err := NewTrimStage().Process(outboundCtx(f), &m)
	if err != nil {
		t.Fatal(err)
	}
	if string(out.Raw) != string(wire) {
		t.Errorf("Raw bytes changed:\n got: %s\nwant: %s", out.Raw, wire)
	}
}

func TestTrimStage_PassThruSmallResult(t *testing.T) {
	f := NewInflightMap()
	f.Record("3", InflightEntry{Method: "tools/call", ToolName: "click"})
	// A small tools/call result (a click ack) — under the threshold, untouched.
	// Note the AXWindow marker is present: only size keeps it under the threshold.
	raw := json.RawMessage(`{"content":[{"type":"text","text":"clicked [5] AXWindow ok"}]}`)
	m := withWireRaw(t, &mcp.Message{JSONRPC: "2.0", ID: json.RawMessage("3"), Result: raw})
	origRaw := append([]byte(nil), m.Raw...)
	orig := append([]byte(nil), m.Result...)

	out, err := NewTrimStage().Process(outboundCtx(f), m)
	if err != nil {
		t.Fatal(err)
	}
	if string(out.Raw) != string(origRaw) || string(out.Result) != string(orig) {
		t.Error("small result must pass through unchanged (Raw byte-identical)")
	}
}

func TestTrimStage_PassThruLargeNonAX(t *testing.T) {
	f := NewInflightMap()
	f.Record("4", InflightEntry{Method: "tools/call", ToolName: "read_log"})
	// Large, but no AXWindow marker — not an AX snapshot, so leave it alone.
	big := strings.Repeat("log line\n", 1000)
	raw, _ := json.Marshal(map[string]any{
		"content": []any{map[string]any{"type": "text", "text": big}},
	})
	m := withWireRaw(t, &mcp.Message{JSONRPC: "2.0", ID: json.RawMessage("4"), Result: raw})
	origRaw := append([]byte(nil), m.Raw...)
	orig := append([]byte(nil), m.Result...)

	out, err := NewTrimStage().Process(outboundCtx(f), m)
	if err != nil {
		t.Fatal(err)
	}
	if string(out.Raw) != string(origRaw) || string(out.Result) != string(orig) {
		t.Error("large non-AX result must pass through unchanged (Raw byte-identical)")
	}
}

func TestTrimStage_PassThruUncorrelated(t *testing.T) {
	// A response whose id was never recorded (e.g. initialize result handled
	// before any inflight wiring, or a backend-initiated message) must pass.
	f := NewInflightMap()
	m := fatAXResult(t, "99")
	origRaw := append([]byte(nil), m.Raw...)
	orig := append([]byte(nil), m.Result...)

	out, err := NewTrimStage().Process(outboundCtx(f), m)
	if err != nil {
		t.Fatal(err)
	}
	if string(out.Raw) != string(origRaw) || string(out.Result) != string(orig) {
		t.Error("uncorrelated response must pass through unchanged (Raw byte-identical)")
	}
}

func TestTrimStage_NotOnInbound(t *testing.T) {
	f := NewInflightMap()
	f.Record("7", InflightEntry{Method: "tools/call"})
	m := fatAXResult(t, "7")
	origRaw := append([]byte(nil), m.Raw...)
	orig := append([]byte(nil), m.Result...)

	ctx := &Context{Identity: identity.New(), Backend: "test", Dir: Inbound, Inflight: f}
	out, err := NewTrimStage().Process(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if string(out.Raw) != string(origRaw) || string(out.Result) != string(orig) {
		t.Error("inbound messages must never be trimmed (Raw byte-identical)")
	}
}

func TestTrimStage_NilInflightSafe(t *testing.T) {
	// A Context without an inflight map (defensive) passes everything through.
	m := fatAXResult(t, "7")
	origRaw := append([]byte(nil), m.Raw...)
	orig := append([]byte(nil), m.Result...)
	ctx := &Context{Identity: identity.New(), Backend: "test", Dir: Outbound}
	out, err := NewTrimStage().Process(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if string(out.Raw) != string(origRaw) || string(out.Result) != string(orig) {
		t.Error("nil inflight must be a safe pass-through (Raw byte-identical)")
	}
}

func TestTrimStage_ConsumeEvenWhenNotTrimming(t *testing.T) {
	// Correlation entries are consumed on the matching response regardless of
	// whether trimming fires, so the map cannot leak.
	f := NewInflightMap()
	f.Record("3", InflightEntry{Method: "tools/call", ToolName: "click"})
	raw := json.RawMessage(`{"content":[{"type":"text","text":"ok"}]}`)
	m := &mcp.Message{JSONRPC: "2.0", ID: json.RawMessage("3"), Result: raw}
	if _, err := NewTrimStage().Process(outboundCtx(f), m); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.Consume("3"); ok {
		t.Error("entry should have been consumed by the trim stage even without a rewrite")
	}
}
