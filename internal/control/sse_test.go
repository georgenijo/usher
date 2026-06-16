package control

// sse_test.go exercises GET /api/events end to end over a real httptest server:
// the SSE headers, the initial snapshot frame, delivery of a published event, and
// subscriber cleanup when the client disconnects.

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/georgenijo/usher/internal/broker"
)

// sseReader wraps an SSE response body in ONE background line reader so the test
// can pull frames one at a time without spawning a fresh goroutine per frame
// (which would race for lines on the shared stream). frame() returns the next
// non-keepalive event frame, with a deadline so a stalled stream fails fast.
type sseReader struct {
	t     *testing.T
	lines chan string
	errc  chan error
}

func newSSEReader(t *testing.T, body *bufio.Reader) *sseReader {
	t.Helper()
	r := &sseReader{t: t, lines: make(chan string, 64), errc: make(chan error, 1)}
	go func() {
		for {
			s, err := body.ReadString('\n')
			if err != nil {
				r.errc <- err
				return
			}
			r.lines <- strings.TrimRight(s, "\r\n")
		}
	}()
	return r
}

// frame reads lines until a complete `event:`/`data:` frame terminates on a blank
// line, skipping `:` keepalive comments.
func (r *sseReader) frame() (event, data string) {
	r.t.Helper()
	deadline := time.After(5 * time.Second)
	var ev, dat string
	for {
		select {
		case s := <-r.lines:
			switch {
			case s == "" && (ev != "" || dat != ""):
				return ev, dat
			case s == "", strings.HasPrefix(s, ":"):
				// blank separator or keepalive comment — ignore.
			case strings.HasPrefix(s, "event: "):
				ev = strings.TrimPrefix(s, "event: ")
			case strings.HasPrefix(s, "data: "):
				dat = strings.TrimPrefix(s, "data: ")
			}
		case err := <-r.errc:
			r.t.Fatalf("read SSE frame: %v", err)
		case <-deadline:
			r.t.Fatal("timed out reading SSE frame")
			return "", ""
		}
	}
}

// TestSSE_DeliversEventThenCleansUp opens the SSE stream, reads the snapshot
// frame, publishes a backend.state event and reads it back, then closes the
// client and asserts the server-side subscription is torn down (the Hub's
// subscriber count returns to its baseline).
func TestSSE_DeliversEventThenCleansUp(t *testing.T) {
	bus := broker.NewHub()
	srv := New(bus, nil, nil)

	hs := httptest.NewServer(srv.Handler())
	defer hs.Close()

	baseSubs := bus.SubscriberCount()

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "GET", hs.URL+"/api/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("open SSE: %v", err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("SSE content-type = %q, want text/event-stream", ct)
	}

	sr := newSSEReader(t, bufio.NewReader(resp.Body))

	// First frame is the initial snapshot.
	ev, data := sr.frame()
	if ev != "snapshot" {
		t.Fatalf("first SSE event = %q, want snapshot (data=%s)", ev, data)
	}
	if !strings.Contains(data, "backends") {
		t.Fatalf("snapshot frame missing backends: %s", data)
	}

	// The server must have a live subscriber now.
	waitFor(t, func() bool { return bus.SubscriberCount() == baseSubs+1 })

	// Publish an event; it must arrive on the stream as `event: backend.state`.
	bus.Emit(broker.BackendStateEvent{TS: time.Now(), Backend: "cua", From: "stopped", To: "starting"})
	ev, data = sr.frame()
	if ev != "backend.state" {
		t.Fatalf("delivered event = %q, want backend.state (data=%s)", ev, data)
	}
	if !strings.Contains(data, `"type":"backend.state"`) || !strings.Contains(data, `"to":"starting"`) {
		t.Fatalf("event payload unexpected: %s", data)
	}

	// Disconnect: cancel the request context and close the body. The server's
	// handler sees the request context cancel and unsubscribes from the Hub.
	cancel()
	resp.Body.Close()
	waitFor(t, func() bool { return bus.SubscriberCount() == baseSubs })
}

// TestSSE_SnapshotWithNilBus asserts the snapshot frame arrives even with a nil
// bus (a bare server), proving the stream is functional without any events to
// deliver.
func TestSSE_SnapshotWithNilBus(t *testing.T) {
	srv := New(nil, nil, nil)
	hs := httptest.NewServer(srv.Handler())
	defer hs.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", hs.URL+"/api/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("open SSE: %v", err)
	}
	defer resp.Body.Close()

	sr := newSSEReader(t, bufio.NewReader(resp.Body))
	ev, data := sr.frame()
	if ev != "snapshot" {
		t.Fatalf("nil-bus stream first event = %q, want snapshot (data=%s)", ev, data)
	}
}
