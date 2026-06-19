package control

// metrics.go is the control plane's GET /metrics surface: a cheap, plaintext
// counter dump for scraping (Prometheus text exposition format — "key value"
// lines, stdlib only, NO client library). It is fed by Watch, ONE more Hub
// subscriber alongside connRegistry and resourceState, off the SAME structured
// events the broker already emits as messages cross the front desk. It therefore
// never touches the forwarding hot path and never mutates, drops, or reorders a
// message: the MCP stream is byte-identical whether or not anything scrapes
// /metrics. A dropped event (the Hub is drop-oldest) at worst undercounts by one
// — the counters are a cheap observability readout, not a billing ledger.
//
// Counters are plain atomic int64s incremented from the single Watch goroutine
// and read from the HTTP handler, so a scrape is wait-free and never contends
// with the bus reader. "Active connections" is a gauge derived from the
// open/close delta; "backends configured" is read live from the supervisor at
// scrape time (it is config, not an event stream).

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/georgenijo/usher/internal/broker"
)

// metricsState holds the broker counters the /metrics endpoint exposes. Every
// field is an atomic int64 so the bus-fed Watch goroutine can increment while an
// HTTP scrape reads, with no lock on either side. The numbers are READ-ONLY
// observations of events the broker already emits — incrementing one never feeds
// back into the forwarding path.
type metricsState struct {
	// messagesInbound counts client→backend requests that were forwarded (one per
	// RequestEvent). A gated tool-call is dropped before forwarding and emits a
	// GateBlockEvent instead, so it lands in blockedCalls, not here.
	messagesInbound atomic.Int64
	// messagesOutbound counts backend→client responses that were forwarded (one
	// per ResponseEvent).
	messagesOutbound atomic.Int64
	// blockedCalls counts destructive tool-calls GateStage refused by policy (one
	// per GateBlockEvent).
	blockedCalls atomic.Int64
	// connsOpened / connsClosed are monotonic lifetime totals; their difference is
	// the active-connection gauge. Tracking both (rather than a single signed
	// gauge) keeps each increment a simple Add and exposes the lifetime totals too.
	connsOpened atomic.Int64
	connsClosed atomic.Int64
}

// newMetricsState returns a zeroed counter set.
func newMetricsState() *metricsState { return &metricsState{} }

// Watch consumes the Hub and folds each relevant event into the counters,
// mirroring connRegistry.Watch and resourceState.Watch. It returns when ctx is
// cancelled or the hub closes the subscription. A nil bus makes it a no-op so a
// bare test server (no daemon behind it) can skip it.
func (ms *metricsState) Watch(ctx context.Context, bus *broker.Hub) {
	if bus == nil {
		return
	}
	ch, cancel := bus.Subscribe(256)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-ch:
			if !ok {
				return
			}
			ms.apply(e)
		}
	}
}

// apply folds one event into the counters. Only the message/block/conn events
// move a counter; every other event type (resource samples, lock, backend state)
// is ignored here — the other Hub subscribers consume those. Each increment is a
// single atomic Add, so apply never blocks the bus reader.
func (ms *metricsState) apply(e broker.Event) {
	switch e.(type) {
	case broker.RequestEvent:
		ms.messagesInbound.Add(1)
	case broker.ResponseEvent:
		ms.messagesOutbound.Add(1)
	case broker.GateBlockEvent:
		ms.blockedCalls.Add(1)
	case broker.ConnOpenEvent:
		ms.connsOpened.Add(1)
	case broker.ConnCloseEvent:
		ms.connsClosed.Add(1)
	}
}

// render writes the counters as Prometheus text exposition format ("key value"
// lines with a HELP/TYPE preamble per metric). backendsConfigured is passed in
// because it is config read live at scrape time, not an event-stream counter.
// The output is plain ASCII with a trailing newline, parseable by any scraper or
// a one-line `grep`.
func (ms *metricsState) render(backendsConfigured int) string {
	inbound := ms.messagesInbound.Load()
	outbound := ms.messagesOutbound.Load()
	blocked := ms.blockedCalls.Load()
	opened := ms.connsOpened.Load()
	closed := ms.connsClosed.Load()
	active := opened - closed
	if active < 0 {
		active = 0 // defensive: a dropped ConnOpen could in theory underflow
	}

	var b strings.Builder
	metric := func(name, help, typ string, val int64) {
		fmt.Fprintf(&b, "# HELP %s %s\n", name, help)
		fmt.Fprintf(&b, "# TYPE %s %s\n", name, typ)
		fmt.Fprintf(&b, "%s %d\n", name, val)
	}

	metric("usher_messages_total",
		"MCP messages forwarded across the broker, by direction.",
		"counter", inbound+outbound)
	// The per-direction breakdown rides labelled samples under the same metric
	// name, the Prometheus convention; a "key value" scraper still reads each line.
	fmt.Fprintf(&b, "usher_messages_by_direction_total{direction=\"inbound\"} %d\n", inbound)
	fmt.Fprintf(&b, "usher_messages_by_direction_total{direction=\"outbound\"} %d\n", outbound)

	metric("usher_blocked_calls_total",
		"Destructive tool-calls refused by the gate policy.",
		"counter", blocked)
	metric("usher_active_connections",
		"Agent connections currently open (opened minus closed).",
		"gauge", active)
	metric("usher_connections_opened_total",
		"Agent connections opened over the daemon's lifetime.",
		"counter", opened)
	metric("usher_connections_closed_total",
		"Agent connections closed over the daemon's lifetime.",
		"counter", closed)
	metric("usher_backends_configured",
		"Backends declared in the loaded config.",
		"gauge", int64(backendsConfigured))

	return b.String()
}
