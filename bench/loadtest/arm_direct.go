package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/georgenijo/usher/internal/backend"
	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/procstat"
)

// armDirect runs the 1:1 NO-BROKER arm: n synthetic agents each SPAWN THEIR OWN
// real cua-driver child (the configured backend command) and speak MCP directly
// to it. The harness OWNS every child pid and samples each, so total backend RSS
// = N×cua — the spike the broker eliminates.
//
// EVERY spawned child is reaped on exit, triple-guarded:
//   - backend.Stdio.Start uses exec.CommandContext(ctx), so cancelling runCtx
//     alone kills every child even on a panic before the defer runs;
//   - a defer loops over ALL children and Close()s (Kill+Wait) each one,
//     unconditionally, ignoring individual errors so one failure can't strand the
//     rest;
//   - a final post-run liveness sweep asserts every child pid is gone and returns
//     an error naming any survivor, so a leak is loud rather than silent.
//
// The harness samples this arm itself (its own procstat.Sampler) because there is
// no daemon in arm B to attribute pids; broker self here is the harness process,
// labelled honestly as "harness".
func armDirect(ctx context.Context, opts options, n int) (result *armResult, err error) {
	// config.Load returns the built-in Default (cua as the sole backend) when the
	// file is absent, so the direct arm works on a fresh machine with no config.
	cfg, lerr := config.Load(opts.configPath)
	if lerr != nil {
		return nil, lerr
	}
	be := cfg.ResolveBackend("")
	if be == nil {
		return nil, fmt.Errorf("no default backend configured")
	}
	// Allow the CLI to override the backend command (e.g. --cua /path/to/cua-driver).
	// Copy the value first so overriding Command never mutates the loaded config's
	// slice element.
	if len(opts.cuaCommand) > 0 {
		beCopy := *be
		beCopy.Command = append([]string(nil), opts.cuaCommand...)
		be = &beCopy
	}
	envExtra, eerr := config.EnvForBackend(be)
	if eerr != nil {
		return nil, fmt.Errorf("resolve backend env: %w", eerr)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The harness samples this arm. broker self = the harness; each child is a
	// backend pid. The sink records every tick so the LAST one is the reported row.
	var (
		mu   sync.Mutex
		last []procstat.ProcSample
	)
	sampler := procstat.New(opts.sampleEvery, func(ss []procstat.ProcSample) {
		mu.Lock()
		last = ss
		mu.Unlock()
	})
	sampler.Set(procstat.Target{PID: os.Getpid(), Role: procstat.RoleBroker, Label: "harness"})

	// Register the child slice + its reaper BEFORE the spawn loop, so a spawn error
	// mid-loop still tears down every already-started child (the defer is live).
	var (
		children []*backend.Stdio
		wg       sync.WaitGroup
	)
	defer func() {
		for _, ch := range children {
			_ = ch.Close() // Kill+Wait every child, unconditionally
		}
	}()

	for i := 0; i < n; i++ {
		sb := backend.NewStdio(be.Name, be.Command, envExtra)
		if serr := sb.Start(runCtx); serr != nil {
			cancel()
			wg.Wait()
			return nil, fmt.Errorf("spawn cua-driver child %d/%d: %w", i+1, n, serr)
		}
		children = append(children, sb)
		sampler.Set(procstat.Target{PID: sb.PID(), Role: procstat.RoleBackend, Label: fmt.Sprintf("cua#%d", i+1)})
		wg.Add(1)
		go func(conn *backend.Stdio) {
			defer wg.Done()
			_ = runSyntheticClient(runCtx, conn.Conn(), opts.callEvery)
		}(sb)
	}

	// Drive the sampler for the hold window so each child's memory is measured
	// while it is genuinely exercised.
	sampleCtx, sampleCancel := context.WithTimeout(runCtx, opts.duration)
	sampler.Run(sampleCtx) // blocks until the window ends
	sampleCancel()

	cancel()  // stop the client loops
	wg.Wait() // let them exit

	mu.Lock()
	procs := last
	mu.Unlock()

	r := &armResult{arm: "direct", clients: n, procs: procs}
	r.rollup()

	// Tear the children down NOW (the defer also would, but we want the post-run
	// liveness sweep to run AFTER they are killed so it proves zero survivors).
	for _, ch := range children {
		_ = ch.Close()
	}
	children = nil // the defer becomes a no-op; we already reaped

	if survivors := liveChildPIDs(pidsOf(r.procs)); len(survivors) > 0 {
		return r, fmt.Errorf("leak: %d cua-driver child(ren) still alive after teardown: %v", len(survivors), survivors)
	}
	return r, nil
}

// pidsOf extracts the backend-role pids from a sample slice, for the post-run
// liveness sweep.
func pidsOf(procs []procstat.ProcSample) []int {
	var pids []int
	for _, p := range procs {
		if p.Role == string(procstat.RoleBackend) {
			pids = append(pids, p.PID)
		}
	}
	return pids
}

// liveChildPIDs sweeps the given pids with ONE ps batch and returns any that are
// still alive — the leak guard. An empty input or all-dead result returns nil.
func liveChildPIDs(pids []int) []int {
	if len(pids) == 0 {
		return nil
	}
	s := procstat.New(time.Second, nil)
	var alive []int
	for _, pid := range pids {
		samp, err := s.SampleNowOf(pid)
		if err == nil && samp.Alive {
			alive = append(alive, pid)
		}
	}
	return alive
}
