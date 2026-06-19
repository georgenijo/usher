// Command usher is the MCP broker — a front desk every agent talks to instead
// of wiring each tool itself. It routes calls to backends, runs them through a
// middleware pipeline (trim, arbitrate, gate, audit), and forwards verbatim by
// default. This is the #14 skeleton: a working stdio proxy with identity and
// audit; the pipeline's substantive stages are wired but pass-through.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/georgenijo/usher/internal/audit"
	"github.com/georgenijo/usher/internal/backend"
	"github.com/georgenijo/usher/internal/broker"
	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/control"
	"github.com/georgenijo/usher/internal/keychain"
	"github.com/georgenijo/usher/internal/mcp"
	"github.com/georgenijo/usher/internal/mcpserver"
)

// version is the build version. It is a var (not const) so the release build
// can stamp the real tag via ldflags: -X main.version={{.Version}} (#21). The
// default below is what a plain `go build` / `go install` reports.
var version = "0.0.1-dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "version", "--version", "-v":
		fmt.Printf("usher %s\n", version)
	case "serve":
		err = cmdServe(os.Args[2:])
	case "mcpserver":
		// Homegrown hermetic stdio MCP server (echo/add/now). It is a guaranteed
		// distinct backend TYPE usher can front; register it with:
		//   usher backend add mcpserver -- /abs/path/to/usher mcpserver
		err = mcpserver.Run(os.Stdin, os.Stdout)
	case "backend":
		err = cmdBackend(os.Args[2:])
	case "config":
		err = cmdConfig(os.Args[2:])
	case "doctor":
		err = cmdDoctor(os.Args[2:])
	case "gate":
		err = cmdGate(os.Args[2:])
	case "start":
		err = cmdStart(os.Args[2:])
	case "stop":
		err = cmdStop(os.Args[2:])
	case "status":
		err = cmdStatus(os.Args[2:])
	case "ui":
		err = cmdUI(os.Args[2:])
	case "install":
		err = cmdInstall(os.Args[2:])
	case "uninstall":
		err = cmdUninstall(os.Args[2:])
	case "completion":
		err = cmdCompletion(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "usher: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "usher:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usher — MCP broker (front desk)

usage:
  usher serve [--backend NAME]      proxy stdio MCP to a backend (the daemon)
  usher serve --socket [--backend N] listen on the Unix socket + loopback control UI
                                    (backends start lazily on first client; --prewarm starts eager)
  usher serve --all                 aggregate ALL backends (namespaced tools)
  usher serve --backends cua,fs     aggregate the named backends
  usher serve --quiet               suppress info/lifecycle logs (errors + blocked + audit stay)
  usher serve --verbose             full-detail logging
  usher mcpserver                   run the homegrown hermetic MCP server (echo/add/now)
                                    register it: usher backend add mcpserver -- <usher path> mcpserver
  usher start [--backend NAME]      launch the daemon in the background
  usher stop                        stop the background daemon
  usher status [--json]             print daemon status (running/stopped/stale + UI url)
  usher ui                          open the control-plane dashboard in the browser
  usher install [--backend NAME]    install + load the launchd LaunchAgent
  usher uninstall                   unload + remove the launchd LaunchAgent
  usher backend list [--json]       show registered backends
  usher backend show NAME           inspect one backend in detail
  usher backend add NAME -- CMD...  register a stdio backend
  usher backend remove NAME         deregister a backend (alias: rm; purges auth=env Keychain keys)
  usher backend rename OLD NEW      rename a backend + migrate its Keychain secrets
  usher backend probe NAME          re-run the initialize handshake against a backend
  usher backend export [--out FILE] write all backends as JSON (no secrets) to stdout/FILE
  usher backend import [--force] F  add backends from JSON F (handshake-validated; --force overwrites)
  usher config check                validate config.json (no daemon); exits non-zero on error
  usher config init [--force]       scaffold a starter config.json (--force overwrites)
  usher doctor                      health-probe every registered backend (table; exit !=0 if any fail)
  usher completion bash|zsh|fish    print a shell completion script to stdout
  usher gate block TOOL             add a bare tool name to the block-list
  usher gate unblock TOOL           allow-list a bare tool name (override; always wins)
  usher gate list                   show the effective block-list + allow-list
  usher version

control-plane UI (served by serve --socket / start):
  --ui-port N    bind the dashboard on 127.0.0.1:N (overrides config.uiAddr)
  --ui-off       disable the dashboard (serve MCP only)
  USHER_UI_ADDR  loopback host:port override (validated; rejects routable hosts)
  config.json: "uiAddr": "127.0.0.1:7187", "uiOff": false

backend add flags:
  --transport stdio|http   transport (http is validated-but-stubbed)
  --auth none|env|inherit|oauth  auth strategy (default inherit)
  --env KEY                env var whose secret is read into the Keychain (repeatable, auth=env)
  --default                make this the default backend

state dir: `+config.StateDir()+`
`)
}

// cmdServe runs the stdio proxy: stdin/stdout is the agent, routed to a backend.
// By default it is the legacy 1:1 proxy (--backend NAME). --all (or --backends
// a,b,c) instead aggregates several backends behind one connection, merging
// their tools/list with namespaced names and routing tools/call by prefix (#17).
func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	backendName := fs.String("backend", "", "backend to route to (default: configured default)")
	all := fs.Bool("all", false, "aggregate ALL configured backends behind one connection")
	backends := fs.String("backends", "", "comma-separated backends to aggregate (e.g. cua,fs)")
	socket := fs.Bool("socket", false, "listen on the Unix socket in the state dir (daemon foreground)")
	uiPort := fs.Int("ui-port", 0, "control-plane UI port on 127.0.0.1 (0: config or built-in default)")
	uiOff := fs.Bool("ui-off", false, "disable the control-plane web UI (serve MCP only)")
	prewarm := fs.Bool("prewarm", false, "bring the default backend live at daemon start instead of lazily on the first client")
	quiet := fs.Bool("quiet", false, "suppress info/lifecycle log lines (errors, gate-blocked, and core audit still emit)")
	verbose := fs.Bool("verbose", false, "full-detail logging (default verbosity plus any debug lines)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *quiet && *verbose {
		return fmt.Errorf("--quiet and --verbose are mutually exclusive")
	}

	cfg, err := config.Load(config.DefaultPath())
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Map the verbosity flags to the audit level; default is normal (the
	// historical behavior). Only Infof lifecycle lines are gated (#log-verbosity).
	level := audit.LevelNormal
	switch {
	case *quiet:
		level = audit.LevelQuiet
	case *verbose:
		level = audit.LevelVerbose
	}

	b, err := broker.NewWithLevel(cfg, level)
	if err != nil {
		return err
	}

	// Socket mode is the always-on daemon: listen in the state dir and proxy many
	// concurrent connections, each through its own pipeline pair (#20). It is
	// additive — the stdio paths below are unchanged. (Multi-backend aggregation
	// over the socket is a future combination; --socket routes to one backend.)
	if *socket {
		ln, err := listenUnix(config.SocketPath())
		if err != nil {
			return err
		}
		// Maintain a PID file for `usher status`, removed on clean shutdown.
		_ = writePID(config.PidPath(), os.Getpid())
		defer removePID(config.PidPath())
		fmt.Fprintf(os.Stderr, "usher: listening on %s (pid %d)\n", config.SocketPath(), os.Getpid())

		// Bring up the loopback control plane (REST + SSE + embedded UI) on the SAME
		// shared backend pool the socket accept loop drives, so the UI's
		// Start/Stop/Restart and a lazy come-live move one state machine. Build the
		// supervisor here, hand it to the control server, and start the
		// connection-level audit subscriber (the SSE stream is the bus's other
		// reader). A control-plane bind failure is a warning, not fatal: the broker
		// still serves MCP without the UI.
		//
		// The UI can be turned off entirely (--ui-off or config.UIOff): the daemon
		// still serves MCP over the socket but never binds the HTTP listener.
		sv := b.EnsureSupervisor(ctx)
		b.StartAuditSubscriber(ctx) // connection-level audit becomes a bus subscriber
		if uiDisabled(cfg, *uiOff) {
			fmt.Fprintln(os.Stderr, "usher: control plane disabled (--ui-off)")
		} else {
			ui := control.New(b.Bus(), sv, cfg)
			ui.SetAddr(uiAddr(cfg, *uiPort))
			if uiLn, lerr := ui.Listen(); lerr != nil {
				fmt.Fprintf(os.Stderr, "usher: warning: control plane not started: %v\n", lerr)
			} else {
				go func() {
					if serr := ui.Serve(ctx, uiLn); serr != nil {
						fmt.Fprintf(os.Stderr, "usher: control plane stopped: %v\n", serr)
					}
				}()
				// Record the ACTUAL bound URL so `usher status`/`usher ui` (separate
				// processes that can't see this run's --ui-port) report it exactly.
				_ = os.WriteFile(config.UIURLPath(), []byte("http://"+uiLn.Addr().String()+"\n"), 0o644)
				defer os.Remove(config.UIURLPath())
				fmt.Fprintf(os.Stderr, "usher: UI on http://%s\n", uiLn.Addr())
			}
		}

		return b.ServeSocket(ctx, *backendName, ln, *prewarm)
	}

	// Multi-backend aggregation is opt-in and additive; the default path is the
	// unchanged single-backend ServeStdio.
	if *all || *backends != "" {
		names := splitBackends(*backends) // nil when --all alone -> "every configured"
		return b.ServeMulti(ctx, names, os.Stdin, os.Stdout)
	}
	return b.ServeStdio(ctx, *backendName, os.Stdin, os.Stdout)
}

// uiAddr resolves the control-plane listen address with this precedence, highest
// first: the --ui-port flag (port>0, on 127.0.0.1), the USHER_UI_ADDR env
// override, config.UIAddr, then the package default. The control server still
// validates the result is loopback before binding, so an override that names a
// routable host fails closed. cfg may be nil (status reads it lazily).
func uiAddr(cfg *config.Config, port int) string {
	if port > 0 {
		return fmt.Sprintf("127.0.0.1:%d", port)
	}
	if a := os.Getenv(control.EnvAddr); a != "" {
		return a
	}
	if cfg != nil && cfg.UIAddr != "" {
		return cfg.UIAddr
	}
	return control.DefaultAddr
}

// uiDisabled reports whether the control-plane UI is off, honoring the --ui-off
// flag (force off for this run) over config.UIOff (the persistent default).
func uiDisabled(cfg *config.Config, flagOff bool) bool {
	if flagOff {
		return true
	}
	return cfg != nil && cfg.UIOff
}

// uiURL is the dashboard URL the daemon serves, used by `usher ui` and
// `usher status`. A running daemon records the address it ACTUALLY bound in the
// state dir (config.UIURLPath), so a run started with --ui-port is reported
// exactly even though this process can't see that flag; when the file is absent
// (no daemon, or an older one) it falls back to resolving config + env.
func uiURL(cfg *config.Config) string {
	if b, err := os.ReadFile(config.UIURLPath()); err == nil {
		if s := strings.TrimSpace(string(b)); s != "" {
			return s
		}
	}
	return "http://" + uiAddr(cfg, 0)
}

// cmdUI opens the control-plane dashboard in the default browser via macOS
// `open`. It prints the resolved URL first (so the user has it even if `open`
// is unavailable), then refuses early when the UI is disabled in config and
// nudges the user when the daemon does not appear to be running — but still
// opens, since the daemon may be starting or bound elsewhere.
func cmdUI(args []string) error {
	fs := flag.NewFlagSet("ui", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(config.DefaultPath())
	if err != nil {
		return err
	}
	if uiDisabled(cfg, false) {
		return fmt.Errorf("control-plane UI is disabled in config (uiOff=true); enable it (remove uiOff) or run: usher serve --socket")
	}

	url := uiURL(cfg)
	fmt.Println(url)

	// Advisory: a not-running daemon means nothing is listening yet. Don't fail —
	// the user may be about to start it, or it may be bound under launchd.
	if pid, perr := readPID(config.PidPath()); perr != nil || !processAlive(pid) {
		fmt.Fprintln(os.Stderr, "usher: warning: daemon does not appear to be running; start it with: usher start")
	}

	return openBrowser(url)
}

// openBrowser opens url in the default browser using the macOS `open` command
// (stdlib-only; no x/browser dependency). A non-macOS host or a missing `open`
// surfaces a clear error so the printed URL remains the fallback.
func openBrowser(url string) error {
	cmd := exec.Command("/usr/bin/open", url)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("open %s: %w (open it manually)", url, err)
	}
	return nil
}

// splitBackends parses a "cua,fs" list into names, trimming blanks. An empty
// string yields nil so ServeMulti aggregates every configured backend.
func splitBackends(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// cmdBackend handles the backend control subcommands.
func cmdBackend(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: usher backend <list|show|add|remove|rename|probe|export|import> ...")
	}
	switch args[0] {
	case "list":
		return backendList(args[1:])
	case "show":
		return backendShow(args[1:])
	case "add":
		return backendAdd(args[1:])
	case "remove", "rm":
		return backendRemove(args[1:])
	case "rename":
		return backendRename(args[1:])
	case "probe":
		return backendProbe(args[1:])
	case "export":
		return backendExport(args[1:])
	case "import":
		return backendImport(args[1:])
	default:
		return fmt.Errorf("unknown backend subcommand %q (want list|show|add|remove|rename|probe|export|import)", args[0])
	}
}

// cmdConfig handles the config control subcommands. The dispatch mirrors
// cmdBackend so adding `config edit`/`config show` later is a one-line case.
func cmdConfig(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: usher config <check|init> ...")
	}
	switch args[0] {
	case "check":
		return configCheck(args[1:])
	case "init":
		return configInit(args[1:])
	default:
		return fmt.Errorf("unknown config subcommand %q (want check|init)", args[0])
	}
}

// configCheck validates config.json without starting the daemon. The validation
// logic lives in internal/config (CheckFile); this wiring stays thin: load,
// print the report, and exit non-zero when there is any ERROR finding.
func configCheck(args []string) error {
	fs := flag.NewFlagSet("config check", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	path := config.DefaultPath()
	res, err := config.CheckFile(path)
	if err != nil {
		return err
	}
	fmt.Printf("checking %s\n", path)
	var sb strings.Builder
	res.Report(&sb)
	fmt.Print(sb.String())
	if res.HasError() {
		// Surface a non-zero exit without the duplicate "usher: ..." prefix main
		// adds for ordinary errors — the report already explains the failures.
		os.Exit(1)
	}
	return nil
}

// configInit scaffolds a starter config.json at config.DefaultPath(). It refuses
// to clobber an existing config unless --force is passed. The written JSON has no
// comments (encoding/json can't emit them), so the next-steps hint goes to stderr
// while the path written goes to stdout.
func configInit(args []string) error {
	fs := flag.NewFlagSet("config init", flag.ContinueOnError)
	force := fs.Bool("force", false, "overwrite an existing config")
	if err := fs.Parse(args); err != nil {
		return err
	}

	path := config.DefaultPath()
	if err := config.Init(path, *force); err != nil {
		if errors.Is(err, config.ErrConfigExists) {
			return fmt.Errorf("%w (pass --force to overwrite)", err)
		}
		return err
	}

	fmt.Println(path)
	fmt.Fprintf(os.Stderr, `wrote starter config (empty backends list).

next steps:
  usher backend add NAME -- COMMAND...   register a backend
  usher backend list                     show registered backends
  usher serve --socket                   start the daemon
`)
	return nil
}

// backendRemove deregisters the backend named in args[0] (alias `rm`). It loads
// the config, removes the entry, and — for auth=env backends — purges each of
// the backend's namespaced Keychain secrets (service usher.<name>, account = the
// env-var name). It prints exactly what was removed. A missing backend is a
// clear error before anything is touched.
//
// The --yes flag exists for scripting symmetry (and a future confirm prompt);
// removal is non-interactive today, so it is accepted and ignored.
func backendRemove(args []string) error {
	fs := flag.NewFlagSet("backend remove", flag.ContinueOnError)
	yes := fs.Bool("yes", false, "skip confirmation (no-op today; reserved for scripting)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	_ = *yes
	rest := fs.Args()
	if len(rest) != 1 || rest[0] == "" {
		return fmt.Errorf("usage: usher backend remove [--yes] NAME")
	}
	name := rest[0]

	path := config.DefaultPath()
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}

	// Snapshot the backend before removing it so we know its auth strategy and
	// EnvKeys for the Keychain purge below; error clearly if it is absent.
	be := cfg.ResolveBackend(name)
	if be == nil {
		return fmt.Errorf("no backend named %q (run: usher backend list)", name)
	}
	auth, envKeys := be.Auth, append([]string(nil), be.EnvKeys...)

	if !cfg.Remove(name) {
		// ResolveBackend already matched, so this should not happen; guard anyway.
		return fmt.Errorf("no backend named %q", name)
	}
	if err := cfg.Save(path); err != nil {
		return err
	}
	fmt.Printf("removed backend %q from %s\n", name, path)

	// Purge the backend's Keychain secrets only for auth=env. Keys are namespaced
	// to the backend (service usher.<name>), so deleting them cannot touch another
	// backend's secrets. keychain.Delete is idempotent — a missing item is fine —
	// but a real Keychain error is surfaced (config is already saved, so this is
	// reported, not rolled back).
	if auth == "env" {
		for _, k := range envKeys {
			if err := keychainDelete(name, k); err != nil {
				return fmt.Errorf("purge Keychain secret %s/%s: %w", name, k, err)
			}
			fmt.Printf("purged Keychain secret %s (service usher.%s)\n", k, name)
		}
	}
	return nil
}

// backendListJSON is the machine-readable shape of one registered backend, a
// stable subset of config.Backend chosen for tooling (--json). It is a distinct
// type so the on-disk config schema can evolve without silently changing the CLI
// contract, and so secrets never leak: only the env-var NAMES (envKeys) appear,
// never values (which live in the Keychain).
type backendListJSON struct {
	Name      string   `json:"name"`
	Transport string   `json:"transport"`
	Auth      string   `json:"auth"`
	Command   []string `json:"command,omitempty"`
	EnvKeys   []string `json:"envKeys,omitempty"`
	Default   bool     `json:"default"`
}

func backendList(args []string) error {
	fs := flag.NewFlagSet("backend list", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "emit a JSON array of backends instead of a table")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(config.DefaultPath())
	if err != nil {
		return err
	}

	// --json: a JSON array of backends for scripting. Always emit an array (never
	// `null`) so consumers can iterate unconditionally even with no backends.
	if *asJSON {
		out := make([]backendListJSON, 0, len(cfg.Backends))
		for _, be := range cfg.Backends {
			out = append(out, backendListJSON{
				Name:      be.Name,
				Transport: be.Transport,
				Auth:      be.Auth,
				Command:   be.Command,
				EnvKeys:   be.EnvKeys,
				Default:   be.Default,
			})
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tTRANSPORT\tAUTH\tDEFAULT\tCOMMAND")
	for _, be := range cfg.Backends {
		def := ""
		if be.Default {
			def = "*"
		}
		name := be.Name
		if be.Disabled {
			name += " (disabled)"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%v\n", name, be.Transport, be.Auth, def, be.Command)
	}
	return w.Flush()
}

// backendShow renders a detailed, read-only view of a single registered backend:
// its name, transport, auth strategy, and (for stdio) the command + args. For
// auth=env it lists each EnvKey NAME and whether the secret is present in the
// Keychain (service usher.<name>) as "set" / "MISSING". The secret VALUE is
// NEVER printed. Keychain lookups degrade gracefully: an unavailable Keychain
// is reported as "unknown" rather than failing the command.
func backendShow(args []string) error {
	if len(args) != 1 || args[0] == "" {
		return fmt.Errorf("usage: usher backend show NAME")
	}
	name := args[0]
	cfg, err := config.Load(config.DefaultPath())
	if err != nil {
		return err
	}
	// Match by exact name only — unlike ResolveBackend, "show" never falls back
	// to the default backend, so an absent name is an unambiguous error.
	var be *config.Backend
	for i := range cfg.Backends {
		if cfg.Backends[i].Name == name {
			be = &cfg.Backends[i]
			break
		}
	}
	if be == nil {
		return fmt.Errorf("no backend named %q", name)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintf(w, "Name:\t%s\n", be.Name)
	fmt.Fprintf(w, "Transport:\t%s\n", be.Transport)
	fmt.Fprintf(w, "Auth:\t%s\n", be.Auth)
	fmt.Fprintf(w, "Default:\t%v\n", be.Default)
	if be.Transport == "stdio" {
		if len(be.Command) > 0 {
			fmt.Fprintf(w, "Command:\t%s\n", be.Command[0])
			if len(be.Command) > 1 {
				fmt.Fprintf(w, "Args:\t%s\n", strings.Join(be.Command[1:], " "))
			}
		} else {
			fmt.Fprintf(w, "Command:\t(none)\n")
		}
	}
	if be.Auth == "env" {
		if len(be.EnvKeys) == 0 {
			fmt.Fprintf(w, "Env keys:\t(none)\n")
		}
		for i, k := range be.EnvKeys {
			label := "Env keys:"
			if i > 0 {
				label = ""
			}
			// Report presence WITHOUT exposing the value: a found secret is
			// "set", an absent one "MISSING", and a Keychain we cannot read
			// (e.g. unavailable in the test/CI environment) "unknown".
			status := "set"
			if _, err := keychainGet(be.Name, k); errors.Is(err, keychain.ErrNotFound) {
				status = "MISSING"
			} else if err != nil {
				status = "unknown"
			}
			fmt.Fprintf(w, "%s\t%s (%s)\n", label, k, status)
		}
	}
	return w.Flush()
}

// envFlags is a repeatable --env KEY collector. Each KEY names an environment
// variable whose secret value is stored in the Keychain (auth=env).
type envFlags []string

func (e *envFlags) String() string { return strings.Join(*e, ",") }
func (e *envFlags) Set(v string) error {
	if v == "" {
		return fmt.Errorf("--env key must be non-empty")
	}
	*e = append(*e, v)
	return nil
}

// backendAdd parses
//
//	NAME [--transport T] [--auth A] [--env KEY]... [--default] -- CMD...
//
// and registers the backend per the full #32 contract: transport x auth, with
// auth=env secrets stored in the macOS Keychain (only the var NAMES land in
// config.json), then a handshake probe to prove the backend speaks MCP.
func backendAdd(args []string) error {
	sep := -1
	for i, a := range args {
		if a == "--" {
			sep = i
			break
		}
	}
	if sep == -1 {
		return fmt.Errorf("usage: usher backend add NAME [--transport T] [--auth A] [--env KEY]... [--default] -- COMMAND...")
	}
	head, cmd := args[:sep], args[sep+1:]
	if len(head) == 0 {
		return fmt.Errorf("backend name required")
	}
	name := head[0]

	var envKeys envFlags
	fs := flag.NewFlagSet("backend add", flag.ContinueOnError)
	auth := fs.String("auth", "inherit", "auth strategy: none|env|inherit|oauth")
	transport := fs.String("transport", "stdio", "transport: stdio|http")
	makeDefault := fs.Bool("default", false, "make this the default backend")
	fs.Var(&envKeys, "env", "env var whose secret is stored in the Keychain (repeatable, auth=env)")
	if err := fs.Parse(head[1:]); err != nil {
		return err
	}
	if len(cmd) == 0 {
		return fmt.Errorf("command required after --")
	}

	// Transport: stdio is fully supported; http is recognized but not yet
	// implemented (validated-but-stubbed — no silent acceptance).
	switch *transport {
	case "stdio":
		// supported
	case "http":
		return fmt.Errorf("transport %q recognized but not yet implemented (stdio only); http registration is stubbed pending the http transport", *transport)
	default:
		return fmt.Errorf("unknown transport %q (want stdio|http)", *transport)
	}

	// Auth: validate the strategy and reconcile it with --env.
	switch *auth {
	case "none", "inherit":
		if len(envKeys) > 0 {
			return fmt.Errorf("--env is only valid with --auth env (got --auth %s)", *auth)
		}
	case "env":
		if len(envKeys) == 0 {
			return fmt.Errorf("--auth env requires at least one --env KEY")
		}
	case "oauth":
		return fmt.Errorf("auth %q not yet supported (env|inherit|none only)", *auth)
	default:
		return fmt.Errorf("unknown auth %q (want none|env|inherit|oauth)", *auth)
	}

	// For auth=env, prompt for and store each secret in the Keychain BEFORE
	// writing config, so a failed/aborted prompt leaves no half-registered
	// backend. The secret value never touches config.json — only the key name.
	if *auth == "env" {
		for _, k := range envKeys {
			secret, err := readSecret(fmt.Sprintf("Enter value for %s (input hidden): ", k))
			if err != nil {
				return fmt.Errorf("read secret for %s: %w", k, err)
			}
			if secret == "" {
				return fmt.Errorf("secret for %s was empty; aborting", k)
			}
			if err := keychain.Set(name, k, secret); err != nil {
				return fmt.Errorf("store secret for %s in Keychain: %w", k, err)
			}
		}
	}

	path := config.DefaultPath()
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	be := config.Backend{
		Name:      name,
		Transport: *transport,
		Command:   cmd,
		Auth:      *auth,
		EnvKeys:   []string(envKeys),
	}

	skipped, err := validateAndSaveBackend(cfg, path, be, *makeDefault)
	if err != nil {
		return fmt.Errorf("backend %q handshake failed, not registered: %w\n  (fix the command/key, then re-run: usher backend add %s ...)", name, err, name)
	}
	if skipped {
		return nil // advisory already printed
	}
	fmt.Printf("registered backend %q -> %v (transport=%s, auth=%s, handshake: ok)\n", name, cmd, *transport, *auth)
	return nil
}

// validateAndSaveBackend is the canonical handshake-validate-BEFORE-save gate
// shared by `backend add` and `backend import`. It adds be to cfg, probes the
// MCP handshake, and only persists cfg to path on success — a handshake failure
// is fatal and leaves the on-disk config untouched (the caller wraps the error
// with a command-specific hint).
//
// The lone advisory exception is when the auth env cannot be resolved yet (e.g.
// a key not yet in the Keychain, an OAuth flow still pending). That is a
// legitimately pre-install state, not a broken backend, so we register without
// probing, print a verify-later hint, and report skipped=true so the caller
// suppresses its own success line.
func validateAndSaveBackend(cfg *config.Config, path string, be config.Backend, makeDefault bool) (skipped bool, err error) {
	cfg.Add(be, makeDefault)

	// Resolve auth env against a transient copy that is added to cfg but NOT yet
	// saved.
	probeBe := cfg.ResolveBackend(be.Name)
	envExtra, err := config.EnvForBackend(probeBe)
	if err != nil {
		// Auth env not yet resolvable: register without probing (advisory).
		if serr := cfg.Save(path); serr != nil {
			return false, serr
		}
		fmt.Fprintf(os.Stderr, "usher: warning: cannot resolve auth env: %v\n", err)
		fmt.Fprintf(os.Stderr, "usher: backend %q registered but probe skipped; verify with: usher backend probe %s\n", be.Name, be.Name)
		return true, nil
	}
	probeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := probeBackend(probeCtx, probeBe, envExtra); err != nil {
		// Handshake failed: refuse to register and leave config untouched.
		return false, err
	}

	// Handshake ok — now persist.
	if err := cfg.Save(path); err != nil {
		return false, err
	}
	return false, nil
}

// Keychain indirections used by backendRename, backendRemove, and backendShow so
// tests can substitute an in-memory store and never touch the real login
// Keychain. Production wires the real /usr/bin/security-backed implementations
// from internal/keychain.
var (
	keychainGet    = keychain.Get
	keychainSet    = keychain.Set
	keychainDelete = keychain.Delete
)

// backendRename renames an already-registered backend from OLD to NEW and, when
// the backend's auth strategy is env, migrates its secrets in the Keychain from
// service usher.<old> to service usher.<new> (read → write → delete the old).
//
//	usher backend rename OLD NEW
//
// It refuses when OLD is absent or NEW already exists, and saves the config only
// after the rename is applied in memory. A Keychain read that fails for one key
// is a warning (the rename continues for the rest) rather than a hard abort, so
// a single missing/locked item doesn't leave the config and the Keychain
// permanently out of step.
func backendRename(args []string) error {
	if len(args) != 2 || args[0] == "" || args[1] == "" {
		return fmt.Errorf("usage: usher backend rename OLD NEW")
	}
	oldName, newName := args[0], args[1]
	if oldName == newName {
		return fmt.Errorf("OLD and NEW are the same name %q; nothing to rename", oldName)
	}

	path := config.DefaultPath()
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}

	// Locate OLD and guard NEW in one pass so we fail before mutating anything.
	src := -1
	for i := range cfg.Backends {
		switch cfg.Backends[i].Name {
		case oldName:
			src = i
		case newName:
			return fmt.Errorf("backend %q already exists; choose a different NEW name", newName)
		}
	}
	if src == -1 {
		return fmt.Errorf("no backend named %q", oldName)
	}

	be := &cfg.Backends[src]

	// Migrate Keychain secrets BEFORE renaming the config key, so each read still
	// targets service usher.<old>. auth=env is the only strategy with secrets; all
	// others migrate nothing. A failed read for one key is a warning (skip it and
	// continue) so one bad item can't abort the whole rename.
	var migrated []string
	if be.Auth == "env" {
		for _, k := range be.EnvKeys {
			secret, err := keychainGet(oldName, k)
			if err != nil {
				fmt.Fprintf(os.Stderr, "usher: warning: keychain read %s/%s failed, skipping migration of this key: %v\n", oldName, k, err)
				continue
			}
			if err := keychainSet(newName, k, secret); err != nil {
				fmt.Fprintf(os.Stderr, "usher: warning: keychain write %s/%s failed, skipping migration of this key: %v\n", newName, k, err)
				continue
			}
			if err := keychainDelete(oldName, k); err != nil {
				fmt.Fprintf(os.Stderr, "usher: warning: keychain delete %s/%s failed (secret copied to %s, stale item remains): %v\n", oldName, k, newName, err)
			}
			migrated = append(migrated, k)
		}
	}

	// Apply the rename and persist. Save writes atomically via config.Save.
	be.Name = newName
	if err := cfg.Save(path); err != nil {
		return err
	}

	fmt.Printf("renamed backend %q -> %q\n", oldName, newName)
	for _, k := range migrated {
		fmt.Printf("  migrated keychain secret %s: usher.%s -> usher.%s\n", k, oldName, newName)
	}
	return nil
}

// backendProbe re-runs the initialize handshake against an already-registered
// backend so the user can verify it after fixing a key or finishing an install.
func backendProbe(args []string) error {
	if len(args) != 1 || args[0] == "" {
		return fmt.Errorf("usage: usher backend probe NAME")
	}
	name := args[0]
	cfg, err := config.Load(config.DefaultPath())
	if err != nil {
		return err
	}
	be := cfg.ResolveBackend(name)
	if be == nil {
		return fmt.Errorf("no backend named %q", name)
	}
	if be.Transport != "stdio" {
		return fmt.Errorf("backend %q: transport %q cannot be probed yet (stdio only)", be.Name, be.Transport)
	}
	envExtra, err := config.EnvForBackend(be)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := probeBackend(ctx, be, envExtra); err != nil {
		return fmt.Errorf("backend %q handshake failed: %w", name, err)
	}
	fmt.Printf("backend %q handshake: ok\n", name)
	return nil
}

// backendExport writes every registered backend as a JSON array to stdout (or to
// --out FILE). Each entry carries name, transport, auth, command/args, and the
// env key NAMES — the same fields that live in config.json. Secret VALUES are
// never exported: they live exclusively in the Keychain (config.Backend has no
// field for them), so the marshaled portable record cannot leak a secret.
func backendExport(args []string) error {
	fs := flag.NewFlagSet("backend export", flag.ContinueOnError)
	out := fs.String("out", "", "write to FILE instead of stdout")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(config.DefaultPath())
	if err != nil {
		return err
	}

	// Marshal config.Backend records: they have no secret field, so name/transport/
	// auth/command/envKeys (names only) are exactly what gets written. Default is
	// intentionally cleared — "default" is a property of one machine's setup, not a
	// portable attribute of the backend; import never sets a default either.
	backends := make([]config.Backend, len(cfg.Backends))
	copy(backends, cfg.Backends)
	for i := range backends {
		backends[i].Default = false
	}
	b, err := json.MarshalIndent(backends, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')

	if *out == "" {
		_, err := os.Stdout.Write(b)
		return err
	}
	if err := os.WriteFile(*out, b, 0o644); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "usher: exported %d backend(s) to %s\n", len(backends), *out)
	return nil
}

// backendImport reads a JSON array produced by `backend export` and registers
// each backend, HANDSHAKE-VALIDATING before saving (the same gate as
// `backend add` — a backend that does not speak MCP is refused). On a name
// collision with an already-registered backend it skips the entry unless
// --force is set. The import is incremental: each backend that passes its
// handshake is persisted, so a later failure does not roll back earlier ones,
// and the failing backend is left out of config.json.
func backendImport(args []string) error {
	fs := flag.NewFlagSet("backend import", flag.ContinueOnError)
	force := fs.Bool("force", false, "overwrite a backend whose name already exists")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) != 1 || rest[0] == "" {
		return fmt.Errorf("usage: usher backend import [--force] FILE")
	}
	file := rest[0]

	raw, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	var incoming []config.Backend
	if err := json.Unmarshal(raw, &incoming); err != nil {
		return fmt.Errorf("parse %s: %w (expected a JSON array from `usher backend export`)", file, err)
	}

	path := config.DefaultPath()
	var imported, skipped, failed int
	for _, be := range incoming {
		if be.Name == "" {
			return fmt.Errorf("import: entry with empty name in %s", file)
		}

		// Reload per entry so each successful import is observed by the next
		// collision check and the on-disk config grows incrementally.
		cfg, err := config.Load(path)
		if err != nil {
			return err
		}
		if existing := cfg.ResolveBackend(be.Name); existing != nil && !*force {
			fmt.Fprintf(os.Stderr, "usher: skipping %q: a backend with that name already exists (use --force to overwrite)\n", be.Name)
			skipped++
			continue
		}

		// Default is never imported — it is a per-machine property; importing must
		// not silently steal the default away from an existing backend.
		be.Default = false
		// Handshake-validate before saving (reuses the add gate); never makeDefault.
		advisory, verr := validateAndSaveBackend(cfg, path, be, false)
		if verr != nil {
			fmt.Fprintf(os.Stderr, "usher: skipping %q: handshake failed: %v\n", be.Name, verr)
			failed++
			continue
		}
		if advisory {
			// Registered but probe skipped (auth env not yet resolvable); the helper
			// already printed the verify-later hint.
			imported++
			continue
		}
		fmt.Fprintf(os.Stderr, "usher: imported %q (handshake: ok)\n", be.Name)
		imported++
	}

	fmt.Fprintf(os.Stderr, "usher: import complete: %d imported, %d skipped, %d failed\n", imported, skipped, failed)
	if failed > 0 {
		return fmt.Errorf("%d backend(s) failed to import", failed)
	}
	return nil
}

// cmdDoctor health-probes every registered backend and prints a status table —
// the scriptable companion to `backend probe` (which targets one). It reuses the
// shared probeBackendDetail engine: spawn, initialize (timed), tools/list. Each
// backend is probed concurrently under its own ~10s deadline so one slow/hung
// backend doesn't serialize the whole run. The process exits non-zero if any
// backend fails, so `usher doctor && deploy` works in scripts.
func cmdDoctor(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	timeout := fs.Duration("timeout", 10*time.Second, "per-backend handshake timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(config.DefaultPath())
	if err != nil {
		return err
	}
	if len(cfg.Backends) == 0 {
		fmt.Println("no backends registered (add one: usher backend add NAME -- CMD...)")
		return nil
	}

	type row struct {
		name    string
		ok      bool
		latency time.Duration
		tools   int
		errMsg  string
	}
	rows := make([]row, len(cfg.Backends))

	var wg sync.WaitGroup
	for i := range cfg.Backends {
		be := &cfg.Backends[i]
		rows[i].name = be.Name
		// http (and other) transports can't be stdio-probed yet — mark, don't spawn.
		if be.Transport != "stdio" {
			rows[i].errMsg = fmt.Sprintf("transport %q not probeable (stdio only)", be.Transport)
			continue
		}
		envExtra, eerr := config.EnvForBackend(be)
		if eerr != nil {
			rows[i].errMsg = eerr.Error()
			continue
		}
		wg.Add(1)
		go func(i int, be *config.Backend, envExtra []string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), *timeout)
			defer cancel()
			res, perr := probeBackendDetail(ctx, be, envExtra)
			if perr != nil {
				rows[i].errMsg = perr.Error()
				return
			}
			rows[i].ok = true
			rows[i].latency = res.Latency
			rows[i].tools = res.Tools
		}(i, be, envExtra)
	}
	wg.Wait()

	failed := 0
	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "BACKEND\tSTATUS\tLATENCY\tTOOLS\tERROR")
	for _, r := range rows {
		if r.ok {
			fmt.Fprintf(w, "%s\tok\t%s\t%d\t\n", r.name, r.latency.Round(time.Millisecond), r.tools)
			continue
		}
		failed++
		fmt.Fprintf(w, "%s\tFAIL\t-\t-\t%s\n", r.name, r.errMsg)
	}
	if err := w.Flush(); err != nil {
		return err
	}

	if failed > 0 {
		return fmt.Errorf("%d of %d backend(s) failed health probe", failed, len(rows))
	}
	return nil
}

// probeBackend spawns the backend, completes the MCP initialize handshake, and
// tears it down. It is the canonical validation gate for backend add/probe: on
// success the caller can trust the command runs and speaks MCP. The context's
// deadline (and exec.CommandContext under it) kills a backend that hangs or
// never answers, so a long-lived daemon cannot wedge the probe. It is a thin
// wrapper over probeBackendDetail (which add/probe don't need the metrics of).
func probeBackend(ctx context.Context, be *config.Backend, envExtra []string) error {
	_, err := probeBackendDetail(ctx, be, envExtra)
	return err
}

// probeResult carries the measured outcome of a backend health probe: the
// initialize round-trip latency and the tool count reported by tools/list.
type probeResult struct {
	Latency time.Duration // initialize request -> response round-trip
	Tools   int           // tools advertised by tools/list (0 if it declines)
}

// probeBackendDetail spawns the backend, completes the MCP initialize handshake,
// then lists its tools — measuring initialize latency and counting tools along
// the way. It is the shared engine behind `backend add`/`backend probe` (which
// only care that err == nil) and `usher doctor` (which renders the metrics).
// A backend that hangs is bounded by the context deadline, never blocking Read
// forever. The handshake itself is never mutated — this is a plain MCP client.
func probeBackendDetail(ctx context.Context, be *config.Backend, envExtra []string) (probeResult, error) {
	var res probeResult

	sb := backend.NewStdio(be.Name, be.Command, envExtra)
	if err := sb.Start(ctx); err != nil {
		return res, fmt.Errorf("start: %w", err)
	}
	defer sb.Close()

	conn := sb.Conn()

	// Read responses on a goroutine so a backend that never replies is bounded by
	// the context deadline rather than blocking Read forever. One reader serves
	// the whole probe sequence (initialize, then tools/list).
	//
	// read skips server-initiated traffic and returns only the response whose id
	// matches wantID. A backend may emit a notification (server-everything sends
	// notifications/tools/list_changed right after initialize) or a server→client
	// request before answering ours; a naive single Read would mistake one of
	// those for the response — reporting "not an MCP server" or 0 tools (the bug
	// fixed for the broker handshake in commit 7524818). The whole loop, including
	// each Read, stays bounded by the context deadline.
	type readResult struct {
		m   *mcp.Message
		err error
	}
	read := func(wantID json.RawMessage) (*mcp.Message, error) {
		for {
			ch := make(chan readResult, 1)
			go func() {
				m, err := conn.Read()
				ch <- readResult{m, err}
			}()
			var r readResult
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case r = <-ch:
			}
			if r.err != nil {
				return nil, r.err
			}
			// Skip server-initiated traffic: notifications (no id) and server
			// requests (method + id) are not our response. A reply carries no
			// method and an id; one bearing our id is the answer (even an empty
			// result, so the "not an MCP server" check below still fires fast).
			if r.m.Method != "" {
				continue
			}
			if len(r.m.ID) == 0 {
				continue
			}
			if bytes.Equal(bytes.TrimSpace(r.m.ID), bytes.TrimSpace(wantID)) {
				return r.m, nil
			}
			// A response with a different id (out-of-order) — keep waiting.
		}
	}

	params := json.RawMessage(`{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"usher","version":"` + version + `"}}`)
	req := &mcp.Message{
		JSONRPC: "2.0",
		ID:      json.RawMessage("1"),
		Method:  "initialize",
		Params:  params,
	}
	start := time.Now()
	if err := conn.Write(req); err != nil {
		return res, fmt.Errorf("write initialize: %w", err)
	}

	m, err := read(req.ID)
	if err != nil {
		if ctx.Err() != nil {
			return res, fmt.Errorf("initialize: %w", ctx.Err())
		}
		return res, fmt.Errorf("read initialize response: %w", err)
	}
	res.Latency = time.Since(start)
	if len(m.Error) > 0 {
		return res, fmt.Errorf("initialize error: %s", m.Error)
	}
	if len(m.Result) == 0 {
		return res, fmt.Errorf("initialize returned no result (not an MCP server?)")
	}

	// The backend speaks MCP. Send notifications/initialized so it can finish its
	// handshake cleanly.
	_ = conn.Write(&mcp.Message{JSONRPC: "2.0", Method: "notifications/initialized"})

	// Count tools via tools/list. A backend without a tools capability may reply
	// with an error or no result; that's not a probe failure (the handshake is
	// what proves MCP), so we leave Tools at 0 and return success.
	listReq := &mcp.Message{
		JSONRPC: "2.0",
		ID:      json.RawMessage("2"),
		Method:  "tools/list",
		Params:  json.RawMessage(`{}`),
	}
	if err := conn.Write(listReq); err != nil {
		return res, nil // handshake already proved MCP; report what we have
	}
	lm, err := read(listReq.ID)
	if err != nil || lm == nil || len(lm.Error) > 0 || len(lm.Result) == 0 {
		return res, nil
	}
	var listResult struct {
		Tools []json.RawMessage `json:"tools"`
	}
	if json.Unmarshal(lm.Result, &listResult) == nil {
		res.Tools = len(listResult.Tools)
	}
	return res, nil
}

// readSecret reads a single secret line from the terminal with echo disabled, so
// the value is not shown and does not land in shell history. Echo is toggled via
// `stty` (POSIX, stdlib-only — no golang.org/x/term dependency). When stdin is
// not a TTY (e.g. piped input), echo control is skipped and the line is read
// directly, which lets `printf '%s\n' "$SECRET" | usher backend add ...` work in
// scripts without leaking to a terminal.
func readSecret(prompt string) (string, error) {
	fmt.Fprint(os.Stderr, prompt)

	restore := disableEcho()
	defer restore()

	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	fmt.Fprintln(os.Stderr) // newline the suppressed Enter didn't echo
	if err != nil && line == "" {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// disableEcho turns off terminal echo for the duration of a secret read and
// returns a restore func. It is a no-op (and the restore a no-op) when stdin is
// not an interactive terminal, so piped/non-TTY input still works.
func disableEcho() func() {
	if !isTerminal(os.Stdin) {
		return func() {}
	}
	if err := sttyEcho(false); err != nil {
		return func() {} // best effort: if stty fails the read still works (echoed)
	}
	return func() { _ = sttyEcho(true) }
}

// sttyEcho toggles terminal echo via the stty(1) command, wired to the current
// /dev/tty so it affects the controlling terminal even when stdin is redirected.
func sttyEcho(on bool) error {
	arg := "-echo"
	if on {
		arg = "echo"
	}
	cmd := exec.Command("/bin/stty", arg)
	cmd.Stdin = os.Stdin
	return cmd.Run()
}

// isTerminal reports whether f is an interactive terminal. It uses the
// stat-mode character-device heuristic (stdlib-only): a TTY is a character
// device, whereas a pipe or regular file is not.
func isTerminal(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}
