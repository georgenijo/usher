// Command loadtest is usher's live broker-vs-direct load test. It proves the
// broker's core value — a shared backend pool instead of one backend child per
// client — by measuring PER-PROCESS resource usage, never a system total.
//
// Two arms, the same synthetic MCP clients in each:
//
//	broker  N agents DIAL the running usher daemon's unix socket and are
//	        multiplexed onto ONE shared cua child. Total backend RSS = 1×cua no
//	        matter how large N grows — the flat line that is the whole thesis.
//	        Resources are read from the daemon's own /api/resources sampler.
//	direct  N agents each SPAWN THEIR OWN real cua-driver child (the 1:1,
//	        no-broker model). Total backend RSS = N×cua — the spike the broker
//	        eliminates. The harness owns and samples every child pid, and reaps
//	        every one on exit (no leaks).
//
// Each synthetic client does a REAL MCP session: initialize -> initialized ->
// repeated tools/call get_screen_size, held connected for the run so the backend
// is genuinely exercised and its memory is real.
//
// Usage:
//
//	go run ./bench/loadtest --arm broker|direct|both [--clients N] [--sweep]
//	    [--seconds S] [--call D] [--sample D] [--socket PATH] [--cua PATH]
//
// The broker arm requires a running daemon started WITH the resource sampler on:
//
//	USHER_SAMPLE=1 usher start --prewarm
//
// All spawned processes and connections are cleaned up on exit and on Ctrl-C.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/georgenijo/usher/internal/config"
)

// options is the resolved CLI configuration for one loadtest run.
type options struct {
	arm         string        // "broker" | "direct" | "both"
	clients     int           // N (the fan-out, default 15)
	sweep       bool          // run 1..N and print the growth curve
	duration    time.Duration // how long to hold clients connected per run
	callEvery   time.Duration // tools/call cadence per client
	sampleEvery time.Duration // resource-sampling cadence
	socket      string        // usher unix socket (broker arm)
	configPath  string        // usher config.json (direct arm: resolves the backend)
	cuaCommand  []string      // override the cua-driver argv (direct arm)
}

func main() {
	opts, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadtest:", err)
		os.Exit(2)
	}

	// Ctrl-C / SIGTERM cancels the run context, which both stops the client loops
	// AND (via exec.CommandContext in the direct arm) kills every spawned child —
	// the first line of the no-leak guarantee.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, opts, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "loadtest:", err)
		os.Exit(1)
	}
}

// parseFlags resolves the CLI flags into options, applying defaults and a few
// sanity floors (clients >= 1, positive durations).
func parseFlags(args []string) (options, error) {
	fs := flag.NewFlagSet("loadtest", flag.ContinueOnError)
	arm := fs.String("arm", "both", "which arm to run: broker | direct | both")
	clients := fs.Int("clients", 15, "number of synthetic client-agents (N)")
	sweep := fs.Bool("sweep", false, "run 1..N and print the per-client growth curve")
	seconds := fs.Float64("seconds", 6, "how long to hold clients connected per run (seconds)")
	call := fs.Duration("call", 500*time.Millisecond, "tools/call cadence per client")
	sample := fs.Duration("sample", time.Second, "resource-sampling cadence")
	socket := fs.String("socket", config.SocketPath(), "usher unix socket (broker arm)")
	cfgPath := fs.String("config", config.DefaultPath(), "usher config.json (direct arm backend resolution)")
	cua := fs.String("cua", "", "override the cua-driver command, space-separated (direct arm); empty uses the configured backend")
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}

	a := strings.ToLower(strings.TrimSpace(*arm))
	switch a {
	case "broker", "direct", "both":
	default:
		return options{}, fmt.Errorf("--arm must be broker|direct|both, got %q", a)
	}
	if *clients < 1 {
		return options{}, fmt.Errorf("--clients must be >= 1, got %d", *clients)
	}
	if *seconds <= 0 {
		return options{}, fmt.Errorf("--seconds must be > 0, got %v", *seconds)
	}
	if *call <= 0 || *sample <= 0 {
		return options{}, fmt.Errorf("--call and --sample must be > 0")
	}

	opts := options{
		arm:         a,
		clients:     *clients,
		sweep:       *sweep,
		duration:    time.Duration(*seconds * float64(time.Second)),
		callEvery:   *call,
		sampleEvery: *sample,
		socket:      *socket,
		configPath:  *cfgPath,
	}
	if c := strings.Fields(*cua); len(c) > 0 {
		opts.cuaCommand = c
	}
	return opts, nil
}

// run drives the requested arm(s) and prints the per-PID tables, per-role totals,
// and the broker-vs-direct headline (or the growth curve in --sweep). It returns
// the first arm error (e.g. a missing daemon for the broker arm, or a child leak
// in the direct arm).
func run(ctx context.Context, opts options, out io.Writer) error {
	if opts.sweep {
		return runSweep(ctx, opts, out)
	}

	var brokerRes, directRes *armResult
	if opts.arm == "broker" || opts.arm == "both" {
		r, err := armBroker(ctx, opts, opts.clients)
		if err != nil {
			return err
		}
		r.printTable(out)
		brokerRes = r
	}
	if opts.arm == "direct" || opts.arm == "both" {
		r, err := armDirect(ctx, opts, opts.clients)
		if r != nil {
			r.printTable(out)
			directRes = r
		}
		if err != nil {
			return err
		}
	}
	printHeadline(out, brokerRes, directRes)
	return nil
}

// runSweep runs n = 1..clients for each requested arm and prints the growth curve
// (direct climbs ~linearly in backend RSS, broker stays flat). The arms run
// sequentially so the machine is never doubly loaded.
func runSweep(ctx context.Context, opts options, out io.Writer) error {
	if opts.arm == "broker" || opts.arm == "both" {
		var curve []*armResult
		for n := 1; n <= opts.clients; n++ {
			if ctx.Err() != nil {
				break
			}
			r, err := armBroker(ctx, opts, n)
			if err != nil {
				return err
			}
			curve = append(curve, r)
		}
		printSweepCurve(out, "broker", curve)
	}
	if opts.arm == "direct" || opts.arm == "both" {
		var curve []*armResult
		for n := 1; n <= opts.clients; n++ {
			if ctx.Err() != nil {
				break
			}
			r, err := armDirect(ctx, opts, n)
			if r != nil {
				curve = append(curve, r)
			}
			if err != nil {
				return err
			}
		}
		printSweepCurve(out, "direct", curve)
	}
	return nil
}
