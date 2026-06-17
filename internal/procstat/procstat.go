// Package procstat is usher's per-process resource sampler. Given a set of
// role-tagged PIDs (the broker's own pid, each LIVE backend child, and each
// connected client), it batches a SINGLE `ps` call per tick to read each pid's
// resident memory (RSS) and CPU%, tags every row with the role it plays in the
// topology, and hands the slice to a sink (the daemon emits it as a
// ResourceSampleEvent onto the bus).
//
// The one hard invariant is PER-PROCESS, NEVER SYSTEM-TOTAL: the sampler only
// ever reads the pids it was explicitly given, and the dashboard sums those rows
// by role (total backend RSS vs total client RSS) — it never reads a machine
// total. That is the whole point of the broker-vs-direct load test: with the
// broker, total backend RSS is one cua child no matter how many clients connect;
// without it, total backend RSS grows ~N×. Only a per-pid, role-tagged sampler
// can show that, and a system-total number would actively hide it.
//
// The package is dep-free (stdlib + os/exec only) and imports nothing of usher's
// so it can never close an import cycle with broker: the broker's
// ResourceSampleEvent carries its OWN ProcStat struct and the daemon converts
// []ProcSample → []broker.ProcStat at the emit site.
package procstat

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Role is the part a sampled process plays in the topology. Tagging each pid
// lets the dashboard roll up "all backend RSS" separately from "all client RSS"
// — the broker-vs-direct headline — without ever reading a machine total.
type Role string

const (
	// RoleClient is a connected agent (the synthetic load-test client, or any
	// real agent dialing the daemon).
	RoleClient Role = "client"
	// RoleBroker is the usher daemon process itself (os.Getpid in the daemon).
	RoleBroker Role = "broker"
	// RoleBackend is a downstream MCP server child (a cua-driver process). One in
	// the broker arm (shared); N in the direct arm (1:1).
	RoleBackend Role = "backend"
)

// Target is one process the sampler watches, tagged with its role and a human
// label ("broker", "cua#3", "client-07", a backend name). The sampler keys its
// watch set by pid, so re-registering the same pid is idempotent.
type Target struct {
	PID   int
	Role  Role
	Label string
}

// ProcSample is one process's resources at one tick. RSSKB is resident memory in
// KILOBYTES (the macOS `ps -o rss=` unit — kept in KB end-to-end so the UI
// divides by 1024 exactly once to show MB). CPUPct is the ps-reported %cpu.
// Alive is false when ps had no row for the pid (it exited between ticks), in
// which case the metric fields are zero — the caller sees the death rather than
// a silent gap.
type ProcSample struct {
	PID    int     `json:"pid"`
	Role   string  `json:"role"`
	Label  string  `json:"label"`
	RSSKB  int     `json:"rssKB"`
	CPUPct float64 `json:"cpuPct"`
	Comm   string  `json:"comm"`
	Alive  bool    `json:"alive"`
}

// psRow is one parsed `ps` line's metric fields, keyed by pid in the parse map.
type psRow struct {
	rss  int
	cpu  float64
	comm string
}

// parsePS turns raw `ps -o pid=,rss=,%cpu=,comm=` output into one ProcSample per
// requested target, in the SAME order as targets. A pid with no row in out (it
// exited mid-tick) yields Alive:false with zeroed metrics. Splitting parsing out
// from the exec lets it be unit-tested against synthetic ps-shaped bytes with no
// process spawned, and isolates the three parsing traps:
//
//   - comm may contain spaces ("Google Chrome Helper"): comm is the LAST column,
//     so we join fields[3:] to recover the full name;
//   - ps right-aligns numeric columns with LEADING spaces — strings.Fields
//     collapses runs of whitespace, so there is no left-pad off-by-one;
//   - a malformed/short line (fewer than 4 fields, or a non-numeric pid) is
//     skipped rather than crashing the tick.
func parsePS(out []byte, targets []Target) []ProcSample {
	byPID := make(map[int]psRow, len(targets))
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}
		rss, _ := strconv.Atoi(fields[1])
		cpu, _ := strconv.ParseFloat(fields[2], 64)
		comm := strings.Join(fields[3:], " ") // comm last; may contain spaces
		byPID[pid] = psRow{rss: rss, cpu: cpu, comm: comm}
	}
	samples := make([]ProcSample, len(targets))
	for i, t := range targets {
		if r, ok := byPID[t.PID]; ok {
			samples[i] = ProcSample{
				PID: t.PID, Role: string(t.Role), Label: t.Label,
				RSSKB: r.rss, CPUPct: r.cpu, Comm: r.comm, Alive: true,
			}
		} else {
			samples[i] = ProcSample{PID: t.PID, Role: string(t.Role), Label: t.Label, Alive: false}
		}
	}
	return samples
}

// deadSamples returns an all-Alive:false sample slice for targets, used when ps
// exits non-zero because NONE of the requested pids exist (the normal teardown
// case, not a harness error).
func deadSamples(targets []Target) []ProcSample {
	samples := make([]ProcSample, len(targets))
	for i, t := range targets {
		samples[i] = ProcSample{PID: t.PID, Role: string(t.Role), Label: t.Label, Alive: false}
	}
	return samples
}

// sampleOnce runs ONE `ps` over the given pids (a single batched exec — the only
// process-stat syscall surface) and returns a sample per requested pid. macOS
// `ps -o rss=` reports KILOBYTES; we keep KB end-to-end. Headerless columns (the
// trailing `=` on each -o spec) mean the output has no header row to skip, and
// comm is placed LAST so a name with spaces does not shift the numeric columns.
//
// ps exits non-zero only when NONE of the requested pids exist; that is the
// all-dead teardown case, returned as all-Alive:false samples rather than an
// error, so a final post-run sweep still reports. Any other exec failure is a
// real error returned to the caller (the Run loop logs and skips it).
func sampleOnce(targets []Target) ([]ProcSample, error) {
	if len(targets) == 0 {
		return nil, nil
	}
	csv := make([]string, len(targets))
	for i, t := range targets {
		csv[i] = strconv.Itoa(t.PID)
	}
	out, err := exec.Command("ps",
		"-o", "pid=,rss=,%cpu=,comm=",
		"-p", strings.Join(csv, ",")).Output()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			// ps found none of the pids (all exited) — not a harness error.
			return deadSamples(targets), nil
		}
		return nil, fmt.Errorf("ps: %w", err)
	}
	return parsePS(out, targets), nil
}

// Sampler periodically samples a dynamic, role-tagged target set and pushes each
// tick to a sink. The target set is mutable (clients attach/detach, backends
// come live and die) behind a mutex; one ps batch per tick covers whatever is
// registered AT THAT tick. The sink (typically "emit a ResourceSampleEvent")
// must not block: it runs on the sampler goroutine.
type Sampler struct {
	mu       sync.Mutex
	targets  map[int]Target // keyed by pid (idempotent re-register)
	interval time.Duration
	sink     func([]ProcSample)
}

// New returns a Sampler that ticks every interval and pushes each tick's slice
// to sink. A non-positive interval defaults to one second; a nil sink makes Run
// a no-op (it still ticks but drops the samples), which keeps callers nil-safe.
func New(interval time.Duration, sink func([]ProcSample)) *Sampler {
	if interval <= 0 {
		interval = time.Second
	}
	return &Sampler{
		targets:  make(map[int]Target),
		interval: interval,
		sink:     sink,
	}
}

// Set registers (or updates) a target by pid. Idempotent: re-Setting a pid that
// is already watched just refreshes its role/label. A pid of 0 (a backend that
// is not live) is ignored, so callers can Set a Snapshot row unconditionally.
func (s *Sampler) Set(t Target) {
	if t.PID <= 0 {
		return
	}
	s.mu.Lock()
	s.targets[t.PID] = t
	s.mu.Unlock()
}

// Remove stops watching a pid (a client disconnected, a backend left live). A
// pid that is not watched is a no-op.
func (s *Sampler) Remove(pid int) {
	s.mu.Lock()
	delete(s.targets, pid)
	s.mu.Unlock()
}

// Targets returns the currently-watched pids' roles, for tests and a future "N
// watched" readout. It is a snapshot copy, safe to read without the lock.
func (s *Sampler) Targets() []Target {
	return s.snapshotTargets()
}

// snapshotTargets copies the watch set to a slice so a tick's ps batch iterates
// without holding the lock across the exec.
func (s *Sampler) snapshotTargets() []Target {
	s.mu.Lock()
	out := make([]Target, 0, len(s.targets))
	for _, t := range s.targets {
		out = append(out, t)
	}
	s.mu.Unlock()
	return out
}

// SampleNow takes one immediate sample of the current target set, outside the
// tick loop. The daemon's feeders and tests use it to read a fresh tick on
// demand without waiting for the next interval; it is the exported single-shot
// form of what Run does each tick.
func (s *Sampler) SampleNow() ([]ProcSample, error) {
	return sampleOnce(s.snapshotTargets())
}

// SampleNowOf samples a SINGLE pid right now (untagged: role/label empty),
// independent of the watch set. It is a thin convenience over sampleOnce for
// callers that just want to confirm one known pid is alive and read its RSS —
// e.g. verifying a freshly-live backend child is sampleable. Per-process by
// construction: it reads only the pid it was given.
func (s *Sampler) SampleNowOf(pid int) (ProcSample, error) {
	ss, err := sampleOnce([]Target{{PID: pid}})
	if err != nil {
		return ProcSample{}, err
	}
	if len(ss) == 0 {
		return ProcSample{PID: pid}, nil
	}
	return ss[0], nil
}

// Run ticks until ctx is cancelled. Each tick snapshots the targets, runs ONE ps
// batch over them, and pushes the slice to the sink. A ps error is skipped (a
// transient exec failure must not kill the sampler — the next tick retries); an
// empty target set is skipped without an exec. It only ever samples the pids it
// was given — nothing system-wide is read.
func (s *Sampler) Run(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tgts := s.snapshotTargets()
			if len(tgts) == 0 {
				continue
			}
			samples, err := sampleOnce(tgts)
			if err != nil {
				continue // transient ps failure; retry next tick
			}
			if s.sink != nil {
				s.sink(samples)
			}
		}
	}
}
