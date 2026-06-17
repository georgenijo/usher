package procstat

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"
)

// TestParsePS_TableDriven exercises the parser against synthetic ps-shaped bytes
// (no process spawned), covering the three documented traps: comm-with-spaces,
// leading-space numeric alignment, a missing pid (Alive:false), and a malformed
// short line that must be skipped rather than crash.
func TestParsePS_TableDriven(t *testing.T) {
	targets := []Target{
		{PID: 100, Role: RoleBroker, Label: "broker"},
		{PID: 200, Role: RoleBackend, Label: "cua#0"},
		{PID: 300, Role: RoleClient, Label: "client-0"}, // intentionally absent from out
	}
	// pid 100: normal. pid 200: comm with spaces + leading-padded numeric columns.
	// pid 300: no row (exited). A garbage line is interleaved to prove it is skipped.
	out := []byte("" +
		"100 10240 1.5 usher\n" +
		"  200   20480   0.0 Google Chrome Helper\n" +
		"garbage line with no numeric pid\n")

	got := parsePS(out, targets)
	if len(got) != len(targets) {
		t.Fatalf("got %d samples, want %d (one per target, in order)", len(got), len(targets))
	}

	// pid 100 — role/label/metrics intact.
	if s := got[0]; !s.Alive || s.PID != 100 || s.RSSKB != 10240 || s.CPUPct != 1.5 ||
		s.Role != string(RoleBroker) || s.Label != "broker" || s.Comm != "usher" {
		t.Errorf("pid 100 sample = %+v, want alive broker row rss=10240 cpu=1.5 comm=usher", s)
	}
	// pid 200 — comm-with-spaces recovered, leading-space columns parsed cleanly.
	if s := got[1]; !s.Alive || s.RSSKB != 20480 || s.Comm != "Google Chrome Helper" ||
		s.Role != string(RoleBackend) {
		t.Errorf("pid 200 sample = %+v, want alive backend row rss=20480 comm=%q", s, "Google Chrome Helper")
	}
	// pid 300 — no row ⇒ Alive:false, zeroed metrics, role/label preserved.
	if s := got[2]; s.Alive || s.RSSKB != 0 || s.Role != string(RoleClient) || s.Label != "client-0" {
		t.Errorf("pid 300 sample = %+v, want dead client row with zeroed metrics", s)
	}
}

// TestSampleOnce_SelfPID is the canonical non-flaky check: sampling the test's
// own pid must return exactly one alive sample with positive RSS, correctly
// role-tagged. It runs a real `ps`, so it also validates the exec + parse path
// end to end.
func TestSampleOnce_SelfPID(t *testing.T) {
	self := os.Getpid()
	samples, err := sampleOnce([]Target{{PID: self, Role: RoleBroker, Label: "self"}})
	if err != nil {
		t.Fatalf("sampleOnce: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("got %d samples, want 1", len(samples))
	}
	s := samples[0]
	if s.PID != self {
		t.Errorf("PID = %d, want %d (the test's own pid)", s.PID, self)
	}
	if !s.Alive {
		t.Errorf("self pid reported not alive: %+v", s)
	}
	if s.RSSKB <= 0 {
		t.Errorf("RSSKB = %d, want > 0 (a running process has resident memory)", s.RSSKB)
	}
	if s.Role != string(RoleBroker) {
		t.Errorf("Role = %q, want %q", s.Role, RoleBroker)
	}
}

// TestSampleOnce_DeadPID spawns a child, waits for it to exit, then samples its
// (now stale) pid plus the live test pid. The dead pid must report Alive:false
// while the live pid reports Alive:true — the sampler tolerates a missing pid
// without erroring. Sampling the test's own spawned child is non-flaky.
func TestSampleOnce_DeadPID(t *testing.T) {
	cmd := exec.Command("true") // exits immediately
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	deadPID := cmd.Process.Pid
	if err := cmd.Wait(); err != nil {
		t.Fatalf("wait child: %v", err)
	}

	self := os.Getpid()
	samples, err := sampleOnce([]Target{
		{PID: self, Role: RoleBroker, Label: "self"},
		{PID: deadPID, Role: RoleBackend, Label: "dead-child"},
	})
	if err != nil {
		t.Fatalf("sampleOnce: %v", err)
	}
	if len(samples) != 2 {
		t.Fatalf("got %d samples, want 2", len(samples))
	}
	byPID := map[int]ProcSample{}
	for _, s := range samples {
		byPID[s.PID] = s
	}
	if s, ok := byPID[self]; !ok || !s.Alive {
		t.Errorf("live self pid sample = %+v (ok=%v), want alive", s, ok)
	}
	if s, ok := byPID[deadPID]; !ok || s.Alive {
		t.Errorf("dead child pid sample = %+v (ok=%v), want Alive:false (role/label preserved)", s, ok)
	} else if s.Role != string(RoleBackend) || s.Label != "dead-child" {
		t.Errorf("dead child sample lost its role/label: %+v", s)
	}
}

// TestSampleOnce_AllDead asserts the all-pids-dead path (ps exits non-zero)
// returns all-Alive:false samples, not an error — the teardown sweep case.
func TestSampleOnce_AllDead(t *testing.T) {
	cmd := exec.Command("true")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	deadPID := cmd.Process.Pid
	_ = cmd.Wait()

	samples, err := sampleOnce([]Target{{PID: deadPID, Role: RoleBackend, Label: "gone"}})
	if err != nil {
		t.Fatalf("sampleOnce with only a dead pid must not error, got: %v", err)
	}
	if len(samples) != 1 || samples[0].Alive {
		t.Errorf("samples = %+v, want one Alive:false row", samples)
	}
}

// TestSampler_SetRemoveAndTick drives the Sampler end to end: register two
// targets (the test pid twice under different roles is not valid since the map
// is pid-keyed, so we use the test pid + its parent pid), run one tick, and
// assert the sink received a slice covering the watched pids. A zero pid is
// ignored by Set. Remove drops a target. Sampling real, live pids of the test
// process tree keeps it non-flaky.
func TestSampler_SetRemoveAndTick(t *testing.T) {
	self := os.Getpid()
	parent := os.Getppid()

	got := make(chan []ProcSample, 4)
	s := New(10*time.Millisecond, func(ss []ProcSample) {
		select {
		case got <- ss:
		default:
		}
	})

	s.Set(Target{PID: self, Role: RoleBroker, Label: "broker"})
	s.Set(Target{PID: parent, Role: RoleClient, Label: "parent"})
	s.Set(Target{PID: 0, Role: RoleBackend, Label: "ignored"}) // pid 0 ignored

	if n := len(s.Targets()); n != 2 {
		t.Fatalf("watched targets = %d, want 2 (pid 0 must be ignored)", n)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	select {
	case ss := <-got:
		// The tick must cover the watched pids and tag the self pid as broker.
		foundSelf := false
		for _, x := range ss {
			if x.PID == self {
				foundSelf = true
				if x.Role != string(RoleBroker) || !x.Alive {
					t.Errorf("self sample = %+v, want alive broker", x)
				}
			}
		}
		if !foundSelf {
			t.Errorf("tick %+v did not include the watched self pid %d", ss, self)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no sample tick within 2s")
	}

	// Remove the parent target; the next tick must no longer include it.
	s.Remove(parent)
	if n := len(s.Targets()); n != 1 {
		t.Fatalf("after Remove, watched targets = %d, want 1", n)
	}
}
