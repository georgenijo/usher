package control

// sse.go is the control plane's live channel: GET /api/events is a Server-Sent
// Events stream that subscribes to the broker's event Hub and writes each event
// to the client as it happens. SSE is stdlib (text/event-stream + Flusher) — no
// websocket library — which is exactly why the design picked it: the browser's
// built-in EventSource consumes it with no JS dependency.
//
// The Hub is drop-oldest, so a frozen browser tab (a slow reader) loses
// intermediate events but never back-pressures the broker pump — the hot-path
// latency guarantee. On reconnect the client re-reads /api/backends (and the
// initial snapshot frame below) to reconcile whatever it missed.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/georgenijo/usher/internal/broker"
)

// sseSubscribeBuf is the per-subscriber Hub buffer depth for an SSE client. Deep
// enough to ride out a brief browser stall before drop-oldest kicks in, small
// enough to bound memory per connected tab.
const sseSubscribeBuf = 256

// keepaliveInterval is how often the stream emits an SSE comment line so an idle
// connection (no events flowing) is not reaped by an intermediary, and a dead
// client is noticed on the next write.
const keepaliveInterval = 15 * time.Second

// handleEvents serves GET /api/events as an SSE stream. It writes the SSE
// headers, sends an initial "snapshot" frame so a fresh tab paints current state
// immediately, then forwards every Hub event as `event: <type>\ndata: <json>\n\n`
// until the client disconnects (its request context is cancelled) or the daemon
// shuts down. The Hub subscription is always cancelled on return, so a closed tab
// frees its subscriber slot.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	// A nil bus (a bare test server with no daemon) still serves the snapshot frame
	// and keepalives; it just never delivers events.
	var ch <-chan broker.Event
	var cancel func() = func() {}
	if s.bus != nil {
		ch, cancel = s.bus.Subscribe(sseSubscribeBuf)
	}
	defer cancel()

	// Initial snapshot so the UI reconciles on connect/reconnect without waiting for
	// the next delta. It carries the current backend list and live connections.
	writeSSEFrame(w, "snapshot", s.snapshotJSON())
	flusher.Flush()

	ticker := time.NewTicker(keepaliveInterval)
	defer ticker.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return // client closed the tab → unsubscribe (deferred cancel)
		case <-ticker.C:
			// SSE comment line: keeps the connection warm and surfaces a dead client.
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case e, open := <-ch:
			if !open {
				return // hub closed the subscription (daemon shutdown)
			}
			body, err := broker.MarshalEvent(e)
			if err != nil {
				continue // a single un-marshalable event is skipped, not fatal
			}
			writeSSEFrame(w, broker.Kind(e), body)
			flusher.Flush()
		}
	}
}

// snapshotJSON renders the initial-frame payload: the current backend pool plus
// the live connections, so a connecting client paints full state from one frame.
func (s *Server) snapshotJSON() []byte {
	var backends []broker.BackendStatus
	if s.sv != nil {
		backends = s.sv.Snapshot()
	}
	if backends == nil {
		backends = []broker.BackendStatus{}
	}
	payload := struct {
		Backends    []broker.BackendStatus `json:"backends"`
		Connections []ConnInfo             `json:"connections"`
	}{backends, s.reg.snapshot()}
	body, err := json.Marshal(payload)
	if err != nil {
		return []byte(`{"backends":[],"connections":[]}`)
	}
	return body
}

// writeSSEFrame writes one SSE event frame: an event name line and a single data
// line carrying the JSON body, terminated by the blank line that ends the frame.
// data is assumed single-line JSON (our events marshal without embedded newlines);
// a Marshal never emits a bare newline, so one data: line is always sufficient.
func writeSSEFrame(w io.Writer, event string, data []byte) {
	// Errors are ignored here; the caller's next Flush (or the select's write) will
	// observe a dead client and return.
	fmt.Fprintf(w, "event: %s\n", event)
	_, _ = w.Write([]byte("data: "))
	_, _ = w.Write(data)
	_, _ = w.Write([]byte("\n\n"))
}
