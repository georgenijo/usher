package broker

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/georgenijo/usher/internal/config"
)

// fakeBackendCommand returns an argv that re-execs the test binary as the fake
// MCP server (fakeBackendMain, driven by USHER_FAKE_* env), advertising the
// given tools. The same /bin/sh -c wrapper the socket/fanout tests use, so each
// child gets its own identity without polluting the test process env.
func fakeBackendCommand(t *testing.T, name string, tools []string) []string {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	script := "USHER_FAKE_BACKEND=1 USHER_FAKE_NAME=" + name +
		" USHER_FAKE_TOOLS=" + joinComma(tools) + ` exec "$0"`
	return []string{"/bin/sh", "-c", script, self}
}

// supervisorWith builds a supervisor over a single fake backend plus a hub the
// test can subscribe to for BackendState assertions. The returned cancel tears
// the pool's ctx down.
func supervisorWith(t *testing.T, name string, tools []string) (*BackendSupervisor, *Hub, context.CancelFunc) {
	t.Helper()
	cfg := &config.Config{
		Backends: []config.Backend{{
			Name:      name,
			Transport: "stdio",
			Command:   fakeBackendCommand(t, name, tools),
			Default:   true,
		}},
	}
	bus := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	sv := NewSupervisor(ctx, cfg, bus)
	return sv, bus, cancel
}

// collectStates subscribes to the hub and returns a func that drains the
// captured BackendState transitions ("from→to") for the named backend. It
// captures asynchronously so it never back-pressures Emit.
func collectStates(t *testing.T, bus *Hub, backend string) (drain func() []string) {
	t.Helper()
	ch, cancel := bus.Subscribe(256)
	var mu sync.Mutex
	var seen []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		for e := range ch {
			if ev, ok := e.(BackendStateEvent); ok && ev.Backend == backend {
				mu.Lock()
				seen = append(seen, ev.From+"→"+ev.To)
				mu.Unlock()
			}
		}
	}()
	return func() []string {
		cancel()
		<-done
		mu.Lock()
		defer mu.Unlock()
		out := make([]string, len(seen))
		copy(out, seen)
		return out
	}
}

// TestSupervisor_LazyStartOnFirstCall is the come-live core: a freshly built
// pool holds the backend StateStopped (no child), and the FIRST EnsureLive starts
// it, runs the handshake, and returns it live with its tools/list cached.
func TestSupervisor_LazyStartOnFirstCall(t *testing.T) {
	sv, _, cancel := supervisorWith(t, "fake", []string{"click", "type_text"})
	defer cancel()

	// Before any call: stopped, no child.
	snap := sv.Snapshot()
	if len(snap) != 1 || snap[0].State != "stopped" {
		t.Fatalf("pre-call snapshot = %+v, want one stopped backend", snap)
	}

	mb, err := sv.EnsureLive("fake")
	if err != nil {
		t.Fatalf("EnsureLive: %v", err)
	}
	if mb == nil {
		t.Fatal("EnsureLive returned nil backend")
	}

	snap = sv.Snapshot()
	if snap[0].State != "live" {
		t.Fatalf("post-call state = %q, want live", snap[0].State)
	}
	if snap[0].ToolCount != 2 {
		t.Fatalf("toolCount = %d, want 2 (cached tools/list)", snap[0].ToolCount)
	}
	if snap[0].StartedAt.IsZero() {
		t.Fatal("startedAt is zero for a live backend")
	}

	// Cached handshake artifacts are populated.
	if len(mb.initResult) == 0 {
		t.Error("initResult not cached after handshake")
	}
	if len(mb.toolsResult) == 0 {
		t.Error("toolsResult not cached after handshake")
	}
	_ = sv.Stop("fake")
}

// TestSupervisor_EnsureLiveIdempotent verifies a second EnsureLive returns the
// SAME live backend without spawning a second child (no transition past the
// first come-live).
func TestSupervisor_EnsureLiveIdempotent(t *testing.T) {
	sv, bus, cancel := supervisorWith(t, "fake", []string{"click"})
	defer cancel()
	drain := collectStates(t, bus, "fake")

	mb1, err := sv.EnsureLive("fake")
	if err != nil {
		t.Fatalf("first EnsureLive: %v", err)
	}
	mb2, err := sv.EnsureLive("fake")
	if err != nil {
		t.Fatalf("second EnsureLive: %v", err)
	}
	if mb1 != mb2 {
		t.Fatal("EnsureLive returned different backends; expected the same shared child")
	}
	_ = sv.Stop("fake")

	states := drain()
	// Exactly one come-live (stopped→starting→live), then the stop transitions.
	wantPrefix := []string{"stopped→starting", "starting→live"}
	if len(states) < 2 || states[0] != wantPrefix[0] || states[1] != wantPrefix[1] {
		t.Fatalf("state transitions = %v, want they begin with %v (one come-live only)", states, wantPrefix)
	}
}

// TestSupervisor_StateEventsInOrder asserts the full lifecycle publishes its
// transitions in order: stopped→starting→live on come-live, then
// live→stopping→stopped on Stop.
func TestSupervisor_StateEventsInOrder(t *testing.T) {
	sv, bus, cancel := supervisorWith(t, "fake", []string{"click"})
	defer cancel()
	drain := collectStates(t, bus, "fake")

	if _, err := sv.EnsureLive("fake"); err != nil {
		t.Fatalf("EnsureLive: %v", err)
	}
	if err := sv.Stop("fake"); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	want := []string{"stopped→starting", "starting→live", "live→stopping", "stopping→stopped"}
	got := drain()
	if len(got) != len(want) {
		t.Fatalf("transitions = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("transition[%d] = %q, want %q (full: %v)", i, got[i], want[i], got)
		}
	}
}

// TestSupervisor_CoalescedConcurrentStarts is the no-double-spawn guarantee:
// under a burst of N concurrent first-callers, exactly one child is started and
// exactly one come-live transition is published; all callers get the same live
// backend. Run under -race to catch a torn state flip.
func TestSupervisor_CoalescedConcurrentStarts(t *testing.T) {
	sv, bus, cancel := supervisorWith(t, "fake", []string{"click"})
	defer cancel()
	drain := collectStates(t, bus, "fake")

	const n = 32
	var wg sync.WaitGroup
	var mu sync.Mutex
	handles := make(map[*managedBackend]struct{})
	var errCount atomic.Int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mb, err := sv.EnsureLive("fake")
			if err != nil {
				errCount.Add(1)
				return
			}
			mu.Lock()
			handles[mb] = struct{}{}
			mu.Unlock()
		}()
	}
	wg.Wait()

	if errCount.Load() != 0 {
		t.Fatalf("%d concurrent EnsureLive calls errored", errCount.Load())
	}
	if len(handles) != 1 {
		t.Fatalf("%d distinct backends returned; expected exactly one shared child", len(handles))
	}
	_ = sv.Stop("fake")

	states := drain()
	// Count come-live starts: a starting→live transition must appear exactly once.
	live := 0
	starting := 0
	for _, s := range states {
		if s == "starting→live" {
			live++
		}
		if s == "stopped→starting" {
			starting++
		}
	}
	if starting != 1 || live != 1 {
		t.Fatalf("come-live transitions: stopped→starting=%d, starting→live=%d; want exactly 1 each (no double-spawn). full: %v", starting, live, states)
	}
}

// TestSupervisor_Restart brings a backend live, restarts it, and verifies it is
// live again and the transitions cover stop then start.
func TestSupervisor_Restart(t *testing.T) {
	sv, bus, cancel := supervisorWith(t, "fake", []string{"click", "type_text", "scroll"})
	defer cancel()
	drain := collectStates(t, bus, "fake")

	if _, err := sv.EnsureLive("fake"); err != nil {
		t.Fatalf("EnsureLive: %v", err)
	}
	if err := sv.Restart("fake"); err != nil {
		t.Fatalf("Restart: %v", err)
	}

	snap := sv.Snapshot()
	if snap[0].State != "live" {
		t.Fatalf("post-restart state = %q, want live", snap[0].State)
	}
	if snap[0].ToolCount != 3 {
		t.Fatalf("post-restart toolCount = %d, want 3", snap[0].ToolCount)
	}
	_ = sv.Stop("fake")

	states := drain()
	// The restart must include a stop (…→stopping→stopped) followed by a fresh
	// come-live (stopped→starting→live). Assert both substrings appear in order.
	if !containsSeq(states, []string{"live→stopping", "stopping→stopped", "stopped→starting", "starting→live"}) {
		t.Fatalf("restart transitions = %v, want a stop then a fresh come-live", states)
	}
}

// TestSupervisor_FailedStartSurfacesAndRecovers verifies a backend whose command
// cannot handshake goes StateFailed with the error surfaced, and that a later
// EnsureLive against a fixed command is NOT possible (config is immutable) — but
// a retry against the same bad command re-attempts and fails again cleanly,
// proving StateFailed is a valid retry trigger (no permanent wedge).
func TestSupervisor_FailedStartSurfacesAndRecovers(t *testing.T) {
	// A command that exits immediately produces EOF on the initialize read.
	cfg := &config.Config{
		Backends: []config.Backend{{
			Name:      "broken",
			Transport: "stdio",
			Command:   []string{"/bin/sh", "-c", "exit 0"},
		}},
	}
	bus := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sv := NewSupervisor(ctx, cfg, bus)
	drain := collectStates(t, bus, "broken")

	_, err := sv.EnsureLive("broken")
	if err == nil {
		t.Fatal("EnsureLive on a backend that cannot handshake returned nil error")
	}

	snap := sv.Snapshot()
	if snap[0].State != "failed" {
		t.Fatalf("state after failed start = %q, want failed", snap[0].State)
	}
	if snap[0].Err == "" {
		t.Fatal("failed backend snapshot carries no error")
	}

	// A second EnsureLive must RE-ATTEMPT (failed is a valid trigger), not return
	// the cached error forever. It fails again, but the retry path ran.
	_, err2 := sv.EnsureLive("broken")
	if err2 == nil {
		t.Fatal("retry EnsureLive unexpectedly succeeded against a still-broken command")
	}

	states := drain()
	// Two start attempts: each is stopped/failed→starting→failed.
	startingToFailed := 0
	for _, s := range states {
		if s == "starting→failed" {
			startingToFailed++
		}
	}
	if startingToFailed != 2 {
		t.Fatalf("starting→failed transitions = %d, want 2 (retry re-attempted). full: %v", startingToFailed, states)
	}
}

// TestSupervisor_UnknownBackend verifies EnsureLive on an unconfigured name is a
// clear error, not a panic.
func TestSupervisor_UnknownBackend(t *testing.T) {
	sv, _, cancel := supervisorWith(t, "fake", []string{"click"})
	defer cancel()
	if _, err := sv.EnsureLive("nope"); err == nil {
		t.Fatal("EnsureLive on an unconfigured backend returned nil error")
	}
}

// TestSupervisor_StopWhenStopped is a no-op: stopping an already-stopped backend
// must not error or transition.
func TestSupervisor_StopWhenStopped(t *testing.T) {
	sv, bus, cancel := supervisorWith(t, "fake", []string{"click"})
	defer cancel()
	drain := collectStates(t, bus, "fake")
	if err := sv.Stop("fake"); err != nil {
		t.Fatalf("Stop on stopped backend: %v", err)
	}
	// Give the (no) event a moment to not arrive, then drain.
	time.Sleep(10 * time.Millisecond)
	if states := drain(); len(states) != 0 {
		t.Fatalf("Stop on a stopped backend emitted transitions %v, want none", states)
	}
}

// containsSeq reports whether want appears as an ordered (not necessarily
// contiguous) subsequence of got.
func containsSeq(got, want []string) bool {
	i := 0
	for _, g := range got {
		if i < len(want) && g == want[i] {
			i++
		}
	}
	return i == len(want)
}
