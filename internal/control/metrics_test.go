package control

// metrics_test.go drives GET /metrics with net/http/httptest: it asserts the
// endpoint answers 200 with parseable Prometheus text lines, and that the
// counters reflect simulated activity published on the SAME event bus the broker
// feeds at runtime. The metrics observer is one Hub subscriber, so the test
// publishes events and waits for the async fold (mirroring the connections test).

import (
	"bufio"
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/georgenijo/usher/internal/broker"
)

// parseMetrics reads a Prometheus text body into a name→value map, skipping the
// "# HELP"/"# TYPE" comment lines. Labelled samples (name{...} value) are keyed
// by their full "name{...}" head so a test can assert a specific labelled series.
func parseMetrics(t *testing.T, body string) map[string]float64 {
	t.Helper()
	out := make(map[string]float64)
	sc := bufio.NewScanner(strings.NewReader(body))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// A metric line is "<key> <value>"; split on the LAST space so a key with a
		// "{label=...}" suffix (which has no spaces inside for our output) stays whole.
		i := strings.LastIndexByte(line, ' ')
		if i < 0 {
			t.Fatalf("unparseable metric line %q (no value)", line)
		}
		key := strings.TrimSpace(line[:i])
		v, err := strconv.ParseFloat(strings.TrimSpace(line[i+1:]), 64)
		if err != nil {
			t.Fatalf("metric line %q has non-numeric value: %v", line, err)
		}
		out[key] = v
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan metrics body: %v", err)
	}
	return out
}

// TestMetrics_EmptyBeforeAnyActivity verifies GET /metrics answers 200 with
// well-formed, zeroed counters before anything has happened — a fresh scrape gets
// parseable lines, not an error or an empty body.
func TestMetrics_EmptyBeforeAnyActivity(t *testing.T) {
	srv := New(broker.NewHub(), nil, nil)

	rec := doReq(t, srv, "GET", "/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("GET /metrics content-type = %q, want text/plain", ct)
	}
	m := parseMetrics(t, rec.Body.String())
	for _, name := range []string{
		"usher_messages_total",
		"usher_blocked_calls_total",
		"usher_active_connections",
		"usher_backends_configured",
	} {
		if _, ok := m[name]; !ok {
			t.Fatalf("metric %q missing from output:\n%s", name, rec.Body.String())
		}
		if m[name] != 0 {
			t.Fatalf("metric %q = %v before any activity, want 0", name, m[name])
		}
	}
}

// TestMetrics_ReflectsActivity publishes simulated broker events on the bus and
// asserts the counters move to match: requests/responses by direction, gate
// blocks, and the active-connection gauge (opened minus closed). Backends
// configured comes from the supervisor wired by testServer.
func TestMetrics_ReflectsActivity(t *testing.T) {
	srv, bus, cleanup := testServer(t, []string{"click"})
	defer cleanup()

	// testServer starts only the registry watcher; start the metrics observer here
	// (as TestResources_RollsUpByRole does for res.Watch). The observer subscribes
	// asynchronously, so wait until its subscription is live before emitting — the
	// drop-oldest Hub would otherwise lose events published before it exists.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.metrics.Watch(ctx, bus)
	waitFor(t, func() bool { return bus.SubscriberCount() >= 2 })

	// Simulate 3 inbound requests, 2 outbound responses, 1 gate block, and a
	// connection that opens twice and closes once (active gauge → 1).
	now := time.Now()
	for i := 0; i < 3; i++ {
		bus.Emit(broker.RequestEvent{TS: now, ConnID: "c1", Backend: "fake", Tool: "click", RPCID: "1"})
	}
	for i := 0; i < 2; i++ {
		bus.Emit(broker.ResponseEvent{TS: now, ConnID: "c1", Backend: "fake", RPCID: "1"})
	}
	bus.Emit(broker.GateBlockEvent{TS: now, Tool: "delete_everything", ConnID: "c1"})
	bus.Emit(broker.ConnOpenEvent{TS: now, ConnID: "c1", PID: 100, Backend: "fake"})
	bus.Emit(broker.ConnOpenEvent{TS: now, ConnID: "c2", PID: 101, Backend: "fake"})
	bus.Emit(broker.ConnCloseEvent{TS: now, ConnID: "c1", Reason: "client-eof"})

	var m map[string]float64
	waitFor(t, func() bool {
		rec := doReq(t, srv, "GET", "/metrics")
		if rec.Code != http.StatusOK {
			return false
		}
		m = parseMetrics(t, rec.Body.String())
		// Wait until every fold has landed (the active gauge is the last domino).
		return m["usher_messages_total"] == 5 && m["usher_active_connections"] == 1
	})

	if got := m["usher_messages_total"]; got != 5 {
		t.Fatalf("usher_messages_total = %v, want 5 (3 in + 2 out)", got)
	}
	if got := m[`usher_messages_by_direction_total{direction="inbound"}`]; got != 3 {
		t.Fatalf("inbound messages = %v, want 3", got)
	}
	if got := m[`usher_messages_by_direction_total{direction="outbound"}`]; got != 2 {
		t.Fatalf("outbound messages = %v, want 2", got)
	}
	if got := m["usher_blocked_calls_total"]; got != 1 {
		t.Fatalf("usher_blocked_calls_total = %v, want 1", got)
	}
	if got := m["usher_connections_opened_total"]; got != 2 {
		t.Fatalf("usher_connections_opened_total = %v, want 2", got)
	}
	if got := m["usher_connections_closed_total"]; got != 1 {
		t.Fatalf("usher_connections_closed_total = %v, want 1", got)
	}
	if got := m["usher_active_connections"]; got != 1 {
		t.Fatalf("usher_active_connections = %v, want 1 (2 opened - 1 closed)", got)
	}
	// Backends configured is read live from the supervisor (one fake backend).
	if got := m["usher_backends_configured"]; got != 1 {
		t.Fatalf("usher_backends_configured = %v, want 1", got)
	}
}
