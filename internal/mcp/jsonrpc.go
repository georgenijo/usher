// Package mcp implements the minimal JSON-RPC 2.0 framing the Model Context
// Protocol uses over a stdio transport: one complete JSON object per line.
package mcp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

// Message is a single JSON-RPC 2.0 message — request, response, or
// notification. The broker forwards messages verbatim, so the original bytes
// are retained in Raw; transforming stages set Raw to nil to force re-encoding.
type Message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`

	// Raw is the exact bytes read off the wire, used for verbatim forwarding.
	// Stages that mutate the message must clear it (m.Raw = nil) so Write
	// re-marshals from the struct fields.
	Raw []byte `json:"-"`
}

// IDString renders the JSON-RPC id for logging (a number, string, or "" for
// notifications). It is not unquoted — "1" and "\"abc\"" both round-trip.
func (m *Message) IDString() string {
	if len(m.ID) == 0 {
		return ""
	}
	return string(m.ID)
}

// IsRequest reports whether the message is a call expecting a response.
func (m *Message) IsRequest() bool { return m.Method != "" && len(m.ID) > 0 }

// IsNotification reports whether the message is a fire-and-forget notification.
func (m *Message) IsNotification() bool { return m.Method != "" && len(m.ID) == 0 }

// IsResponse reports whether the message is a result/error for a prior request.
func (m *Message) IsResponse() bool {
	return m.Method == "" && (len(m.Result) > 0 || len(m.Error) > 0)
}

// Conn is a bidirectional newline-delimited JSON-RPC channel over an
// io.Reader/io.Writer pair. Writes are serialized; reads are not (drive each
// direction from a single goroutine).
type Conn struct {
	r  *bufio.Reader
	w  io.Writer
	mu sync.Mutex
}

// NewConn wraps a reader/writer pair. The read buffer is sized for large AX-tree
// payloads, which routinely exceed the default 4 KiB.
func NewConn(r io.Reader, w io.Writer) *Conn {
	return &Conn{r: bufio.NewReaderSize(r, 1<<20), w: w}
}

// Read returns the next message. Blank lines are skipped. A final line without a
// trailing newline is still returned; the following Read surfaces io.EOF.
func (c *Conn) Read() (*Message, error) {
	for {
		line, err := c.r.ReadBytes('\n')
		trimmed := bytes.TrimRight(line, "\r\n")
		if len(trimmed) == 0 {
			if err != nil {
				return nil, err
			}
			continue
		}
		var m Message
		if jerr := json.Unmarshal(trimmed, &m); jerr != nil {
			return nil, fmt.Errorf("decode jsonrpc: %w", jerr)
		}
		m.Raw = append([]byte(nil), trimmed...)
		return &m, nil
	}
}

// Write emits a single newline-delimited message. If Raw is set the bytes are
// forwarded verbatim; otherwise the struct is marshaled.
func (c *Conn) Write(m *Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	b := m.Raw
	if b == nil {
		var err error
		if b, err = json.Marshal(m); err != nil {
			return fmt.Errorf("encode jsonrpc: %w", err)
		}
	}
	if _, err := c.w.Write(b); err != nil {
		return err
	}
	_, err := c.w.Write([]byte{'\n'})
	return err
}
