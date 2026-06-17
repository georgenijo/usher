package main

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/georgenijo/usher/internal/mcp"
)

// runSyntheticClient does a REAL MCP session over conn (a unix-socket connection
// to the usher daemon in the broker arm, or a private cua-driver child's stdio in
// the direct arm) and stays CONNECTED for the whole run, so the backend's memory
// is genuinely real and not a momentary spike:
//
//  1. initialize -> wait for the result (the handshake gate);
//  2. notifications/initialized;
//  3. a repeated tools/call get_screen_size loop on the callEvery tick until ctx
//     is cancelled.
//
// get_screen_size is the deliberate choice: it is in the broker's SAFE tool class
// (read-only), exists on cua-driver, and the fake test backend echoes it, so the
// gate never blocks it and it genuinely round-trips to the real backend.
//
// The stream is never corrupted: a single goroutine writes (here), and a second
// goroutine drains responses (drain), so a slow backend can never wedge the
// writer and partial frames are never interleaved. mcp.NewConn gives byte-for-byte
// the same framing the broker uses.
func runSyntheticClient(ctx context.Context, conn *mcp.Conn, callEvery time.Duration) error {
	// 1. initialize, then block for the result so we don't start calling before
	// the backend has handshaked (the daemon/backend gates on this).
	if err := conn.Write(&mcp.Message{
		JSONRPC: "2.0",
		ID:      json.RawMessage(`1`),
		Method:  "initialize",
		Params:  json.RawMessage(`{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"loadtest","version":"0"}}`),
	}); err != nil {
		return err
	}
	if _, err := conn.Read(); err != nil {
		return err
	}

	// 2. notifications/initialized (a notification: no reply expected).
	if err := conn.Write(&mcp.Message{JSONRPC: "2.0", Method: "notifications/initialized"}); err != nil {
		return err
	}

	// Drain responses concurrently so a slow backend never blocks the call loop's
	// writer (the response frames are not asserted on — the point is to keep the
	// session live and the backend exercised).
	go drain(ctx, conn)

	// 3. repeated tools/call get_screen_size until ctx is cancelled.
	t := time.NewTicker(callEvery)
	defer t.Stop()
	id := 1
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			id++
			if err := conn.Write(&mcp.Message{
				JSONRPC: "2.0",
				ID:      json.RawMessage(strconv.Itoa(id)),
				Method:  "tools/call",
				Params:  json.RawMessage(`{"name":"get_screen_size","arguments":{}}`),
			}); err != nil {
				return err // peer closed: end this client cleanly
			}
		}
	}
}

// drain reads and discards response frames until ctx is cancelled or the conn
// closes. It exists only to keep the connection's read side moving so the writer
// never blocks on a full pipe; nothing here asserts on the responses.
func drain(ctx context.Context, conn *mcp.Conn) {
	for {
		if ctx.Err() != nil {
			return
		}
		if _, err := conn.Read(); err != nil {
			return // EOF / closed conn: the session is over
		}
	}
}
