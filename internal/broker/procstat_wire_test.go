package broker

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/procstat"
)

// TestBackendStatus_SurfacesLivePID asserts a live backend's Snapshot row carries
// the shared child's real OS pid (so the resource sampler can attribute its
// RSS/CPU), and that a stopped backend's pid is zero (the "not running" signal
// the sampler treats as no target).
func TestBackendStatus_SurfacesLivePID(t *testing.T) {
	sv, _, cancel := supervisorWith(t, "fake", []string{"click"})
	defer cancel()

	// Stopped: pid zero.
	if snap := sv.Snapshot(); snap[0].PID != 0 {
		t.Fatalf("stopped backend pid = %d, want 0", snap[0].PID)
	}

	if _, err := sv.EnsureLive("fake"); err != nil {
		t.Fatalf("EnsureLive: %v", err)
	}
	defer sv.Stop("fake")

	snap := sv.Snapshot()
	if snap[0].State != "live" {
		t.Fatalf("state = %q, want live", snap[0].State)
	}
	if snap[0].PID <= 0 {
		t.Fatalf("live backend pid = %d, want > 0 (the shared child's OS pid)", snap[0].PID)
	}
	// The reported pid must be a real, sampleable process: sampling it returns an
	// alive row (this is the same per-process path the daemon's sampler uses).
	ss, err := procstat.New(time.Second, nil).SampleNowOf(snap[0].PID)
	if err != nil {
		t.Fatalf("sampling the live backend pid: %v", err)
	}
	if !ss.Alive {
		t.Errorf("live backend pid %d sampled not alive: %+v", snap[0].PID, ss)
	}
}

// TestFeedSamplerFromSnapshot_TracksBackendPIDs drives the daemon's backend-pid
// feeder against a real supervisor: once the backend comes live, the feeder must
// register its pid as a RoleBackend target; once it stops, the feeder must drop
// that pid. This is the wiring that keeps "total backend RSS" pinned to the live
// children — the broker-vs-direct thesis.
func TestFeedSamplerFromSnapshot_TracksBackendPIDs(t *testing.T) {
	cfg := &config.Config{
		Backends: []config.Backend{{
			Name:      "fake",
			Transport: "stdio",
			Command:   fakeBackendCommand(t, "fake", []string{"click"}),
			Default:   true,
		}},
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.sv = newSupervisorForBroker(ctx, b)
	defer b.sv.StopAll()

	sampler := procstat.New(20*time.Millisecond, nil)
	sampler.Set(procstat.Target{PID: os.Getpid(), Role: procstat.RoleBroker, Label: "broker"})
	go b.feedSamplerFromSnapshot(ctx, sampler)

	// Bring the backend live; the feeder should pick up its pid within a few ticks.
	if _, err := b.sv.EnsureLive("fake"); err != nil {
		t.Fatalf("EnsureLive: %v", err)
	}
	livePID := b.sv.Snapshot()[0].PID
	if livePID <= 0 {
		t.Fatalf("live backend pid = %d, want > 0", livePID)
	}

	if !waitForTarget(sampler, livePID, procstat.RoleBackend, time.Second) {
		t.Fatalf("feeder did not register live backend pid %d as a backend target", livePID)
	}

	// Stop the backend; the feeder should drop the pid.
	if err := b.sv.Stop("fake"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !waitForNoTarget(sampler, livePID, time.Second) {
		t.Fatalf("feeder did not drop stopped backend pid %d", livePID)
	}
}

// TestFeedSamplerFromBus_ClientTargets drives the client-pid feeder via the bus:
// a ConnOpenEvent registers the agent pid as a RoleClient target, a ConnCloseEvent
// drops it. A pid shared by two connections (the load-test case, all synthetic
// clients in one harness process) is only removed once BOTH connections close, so
// an active sibling is never dropped from the sample.
func TestFeedSamplerFromBus_ClientTargets(t *testing.T) {
	b, err := New(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sampler := procstat.New(time.Hour, nil) // never auto-ticks; we drive the feeder by hand
	go b.feedSamplerFromBus(ctx, sampler)

	// Wait for the feeder to register as a bus subscriber before emitting, so the
	// events are not dropped on the floor (Emit is fire-and-forget, no replay).
	waitForSubscriber(t, b.bus)

	// Two connections sharing one pid (the broker-arm reality), plus one distinct.
	b.bus.Emit(ConnOpenEvent{ConnID: "c1", PID: 4242, Backend: "fake"})
	b.bus.Emit(ConnOpenEvent{ConnID: "c2", PID: 4242, Backend: "fake"})
	b.bus.Emit(ConnOpenEvent{ConnID: "c3", PID: 9999, Backend: "fake"})

	if !waitForTarget(sampler, 4242, procstat.RoleClient, time.Second) {
		t.Fatal("ConnOpen did not register shared client pid 4242")
	}
	if !waitForTarget(sampler, 9999, procstat.RoleClient, time.Second) {
		t.Fatal("ConnOpen did not register distinct client pid 9999")
	}

	// Close one of the two connections on the shared pid: it must STAY (sibling open).
	b.bus.Emit(ConnCloseEvent{ConnID: "c1", Reason: "client-eof"})
	// Closing the distinct pid's only connection drops it.
	b.bus.Emit(ConnCloseEvent{ConnID: "c3", Reason: "client-eof"})
	if !waitForNoTarget(sampler, 9999, time.Second) {
		t.Fatal("ConnClose did not drop the distinct client pid 9999")
	}
	// The shared pid is still watched because c2 is still open.
	if !targetPresent(sampler, 4242) {
		t.Fatal("shared pid 4242 was dropped while a sibling connection is still open")
	}

	// Close the last connection on the shared pid: now it drops.
	b.bus.Emit(ConnCloseEvent{ConnID: "c2", Reason: "client-eof"})
	if !waitForNoTarget(sampler, 4242, time.Second) {
		t.Fatal("ConnClose of the last connection did not drop shared pid 4242")
	}
}

// targetPresent reports whether pid is currently in the sampler's watch set.
func targetPresent(s *procstat.Sampler, pid int) bool {
	for _, t := range s.Targets() {
		if t.PID == pid {
			return true
		}
	}
	return false
}

// waitForTarget polls the sampler's watch set until pid appears with role, or the
// deadline passes.
func waitForTarget(s *procstat.Sampler, pid int, role procstat.Role, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for _, t := range s.Targets() {
			if t.PID == pid && t.Role == role {
				return true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// waitForNoTarget polls until pid is no longer in the sampler's watch set.
func waitForNoTarget(s *procstat.Sampler, pid int, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		found := false
		for _, t := range s.Targets() {
			if t.PID == pid {
				found = true
				break
			}
		}
		if !found {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
