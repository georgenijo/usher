// Package mcpserver is a homegrown, hermetic stdio MCP server: a small, in-repo
// backend usher can front when an external MCP server (e.g. an npx-launched one)
// is unavailable. It speaks real MCP — initialize, notifications/initialized,
// tools/list, tools/call — over newline-delimited JSON-RPC on stdin/stdout, the
// exact framing the broker's backends use (internal/mcp.Conn).
//
// It is a guaranteed-distinct backend TYPE for proving usher's heterogeneous
// support: cua-driver is computer-use (macOS APIs); this is pure local compute
// with zero network and instant start. Its tools are echo (string round-trip),
// add (two numbers -> sum) and now (RFC3339 timestamp). Everything is pure and
// local; `now` is the only nondeterministic tool (it reads the clock) and is
// still hermetic. The server writes ONLY framed JSON-RPC to stdout; anything
// diagnostic goes to stderr so a client never sees a non-protocol byte.
package mcpserver

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/georgenijo/usher/internal/mcp"
)

// protocolVersion is the MCP revision this server implements. It matches what
// usher's probe and the other backends advertise (2024-11-05), so the handshake
// is uniform across the registered set.
const protocolVersion = "2024-11-05"

// serverName/serverVersion identify this backend in the initialize result. The
// name is also what usher namespaces its tools under (e.g. "mcpserver__add").
const (
	serverName    = "mcpserver"
	serverVersion = "0.1.0"
)

// Run drives the server loop over an arbitrary reader/writer pair so it is
// testable without a real process (drive it over an io.Pipe). In production it
// is called with os.Stdin/os.Stdout. It returns nil on a clean EOF (the client
// half-closed), and a non-nil error only on an unrecoverable write failure — a
// malformed line from the client is answered with a JSON-RPC error, not a
// teardown, so one bad frame never kills the session.
func Run(in io.Reader, out io.Writer) error {
	conn := mcp.NewConn(in, out)
	for {
		m, err := conn.Read()
		if err != nil {
			// EOF (clean shutdown) or a decode error on a single line. Either way
			// the connection is no longer usable for framed JSON-RPC, so exit
			// cleanly: the broker treats a closed backend as a normal teardown.
			if err == io.EOF {
				return nil
			}
			return nil
		}
		if err := dispatch(conn, m); err != nil {
			return err
		}
	}
}

// dispatch handles one inbound message. Requests (with an id) get exactly one
// reply; notifications (no id) get none. The write error is the only thing
// surfaced to Run — a logic error in a tool is reported in-band as an MCP tool
// error (isError:true), never by tearing down the connection.
func dispatch(conn *mcp.Conn, m *mcp.Message) error {
	switch m.Method {
	case "initialize":
		return conn.Write(reply(m.ID, initializeResult()))

	case "notifications/initialized":
		// A notification carries no id and expects no response. The MCP lifecycle
		// sends it after the client has seen the initialize result; we simply
		// acknowledge by doing nothing, which is correct.
		return nil

	case "tools/list":
		return conn.Write(reply(m.ID, toolsListResult()))

	case "tools/call":
		return conn.Write(handleToolCall(m))

	default:
		// Any other REQUEST (e.g. ping) must still get a response so a client that
		// probes never hangs; a non-request (notification we don't recognize) is
		// dropped. An empty result object is a valid JSON-RPC result.
		if m.IsRequest() {
			return conn.Write(reply(m.ID, json.RawMessage(`{}`)))
		}
		return nil
	}
}

// reply builds a JSON-RPC 2.0 success response carrying the request's id and the
// given result object. Raw is left nil so mcp.Conn.Write marshals it.
func reply(id json.RawMessage, result json.RawMessage) *mcp.Message {
	return &mcp.Message{
		JSONRPC: "2.0",
		ID:      append(json.RawMessage(nil), id...),
		Result:  result,
	}
}

// initializeResult is the MCP initialize response: a protocol version, the
// server's tools capability, and serverInfo. usher's probe and the supervisor's
// handshake both require a non-empty result here.
func initializeResult() json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"protocolVersion": protocolVersion,
		"capabilities":    map[string]any{"tools": map[string]any{}},
		"serverInfo":      map[string]any{"name": serverName, "version": serverVersion},
	})
	return b
}

// toolsListResult is the tools/list response: the three tools with JSON-Schema
// input shapes. The schemas are deliberately explicit (type/properties/required)
// so a client (and usher's trim stage, which forwards them byte-for-byte) sees a
// well-formed contract for each tool.
func toolsListResult() json.RawMessage {
	tools := []map[string]any{
		{
			"name":        "echo",
			"description": "Echo the input text back unchanged.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"text": map[string]any{"type": "string", "description": "the text to echo"},
				},
				"required": []string{"text"},
			},
		},
		{
			"name":        "add",
			"description": "Add two numbers and return the sum.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"a": map[string]any{"type": "number", "description": "the first addend"},
					"b": map[string]any{"type": "number", "description": "the second addend"},
				},
				"required": []string{"a", "b"},
			},
		},
		{
			"name":        "now",
			"description": "Return the current time as an RFC3339 timestamp.",
			"inputSchema": map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
	}
	b, _ := json.Marshal(map[string]any{"tools": tools})
	return b
}

// handleToolCall parses params {name, arguments}, dispatches to the named tool,
// and wraps the outcome as an MCP tool result {content:[{type:"text",text}]}.
// An unknown tool or a bad argument is reported in-band with isError:true (a
// valid MCP result), so the client sees a tool failure rather than a transport
// error. The clock is the now() tool's only nondeterminism.
func handleToolCall(m *mcp.Message) *mcp.Message {
	var params struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(m.Params, &params); err != nil {
		return reply(m.ID, toolError(fmt.Sprintf("invalid tools/call params: %v", err)))
	}

	switch params.Name {
	case "echo":
		var args struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(params.Arguments, &args); err != nil {
			return reply(m.ID, toolError(fmt.Sprintf("invalid arguments for echo: %v", err)))
		}
		return reply(m.ID, toolText(args.Text))

	case "add":
		var args struct {
			A float64 `json:"a"`
			B float64 `json:"b"`
		}
		if err := json.Unmarshal(params.Arguments, &args); err != nil {
			return reply(m.ID, toolError(fmt.Sprintf("invalid arguments for add: %v", err)))
		}
		return reply(m.ID, toolText(formatNumber(args.A+args.B)))

	case "now":
		// No arguments; ignore any provided. RFC3339 is unambiguous and parseable.
		return reply(m.ID, toolText(time.Now().UTC().Format(time.RFC3339)))

	default:
		return reply(m.ID, toolError(fmt.Sprintf("unknown tool %q", params.Name)))
	}
}

// toolText wraps a string as a successful MCP tool result.
func toolText(text string) json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"content": []any{map[string]any{"type": "text", "text": text}},
	})
	return b
}

// toolError wraps a message as an MCP tool result flagged isError:true — the
// MCP-idiomatic way to report a tool-level failure (distinct from a JSON-RPC
// protocol error). The client still gets a well-formed result it can read.
func toolError(msg string) json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"content": []any{map[string]any{"type": "text", "text": msg}},
		"isError": true,
	})
	return b
}

// formatNumber renders a float result without a trailing ".0" for whole numbers
// (so add(2,3) is "5", not "5.000000") while keeping fractional precision. It
// uses the shortest representation that round-trips ('g' with -1 precision).
func formatNumber(f float64) string {
	return fmt.Sprintf("%v", f)
}
