// Command usher is the MCP broker — a front desk every agent talks to instead
// of wiring each tool itself. It routes calls to backends, runs them through a
// middleware pipeline (trim, arbitrate, gate, audit), and forwards verbatim by
// default. This is the #14 skeleton: a working stdio proxy with identity and
// audit; the pipeline's substantive stages are wired but pass-through.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
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
  usher status                      print daemon status (running/stopped/stale + UI url)
  usher ui                          open the control-plane dashboard in the browser
  usher install [--backend NAME]    install + load the launchd LaunchAgent
  usher uninstall                   unload + remove the launchd LaunchAgent
  usher backend list                show registered backends
  usher backend add NAME -- CMD...  register a stdio backend
  usher backend probe NAME          re-run the initialize handshake against a backend
  usher config check                validate config.json (no daemon); exits non-zero on error
  usher config init [--force]       scaffold a starter config.json (--force overwrites)
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
		return fmt.Errorf("usage: usher backend <list|add|probe> ...")
	}
	switch args[0] {
	case "list":
		return backendList()
	case "add":
		return backendAdd(args[1:])
	case "probe":
		return backendProbe(args[1:])
	default:
		return fmt.Errorf("unknown backend subcommand %q (want list|add|probe)", args[0])
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

func backendList() error {
	cfg, err := config.Load(config.DefaultPath())
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tTRANSPORT\tAUTH\tDEFAULT\tCOMMAND")
	for _, be := range cfg.Backends {
		def := ""
		if be.Default {
			def = "*"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%v\n", be.Name, be.Transport, be.Auth, def, be.Command)
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
	cfg.Add(be, *makeDefault)

	// Handshake-validate-BEFORE-save: the done criterion is "refuse to register
	// if the handshake fails (with a clear message)." So we probe first and only
	// persist a backend that actually speaks MCP — a handshake failure is fatal
	// and the config is left untouched.
	//
	// The lone advisory exception is when the auth env cannot be resolved yet
	// (e.g. a key not yet in the Keychain, an OAuth flow still pending). That is a
	// legitimately pre-install state, not a broken backend, so we register
	// without probing and tell the user how to verify later. We resolve against a
	// transient copy that is added to cfg but NOT yet saved.
	probeBe := cfg.ResolveBackend(name)
	envExtra, err := config.EnvForBackend(probeBe)
	if err != nil {
		// Auth env not yet resolvable: register without probing (advisory).
		if err := cfg.Save(path); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "usher: warning: cannot resolve auth env: %v\n", err)
		fmt.Fprintf(os.Stderr, "usher: backend %q registered but probe skipped; verify with: usher backend probe %s\n", name, name)
		return nil
	}
	probeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := probeBackend(probeCtx, probeBe, envExtra); err != nil {
		// Handshake failed: refuse to register and leave config untouched.
		return fmt.Errorf("backend %q handshake failed, not registered: %w\n  (fix the command/key, then re-run: usher backend add %s ...)", name, err, name)
	}

	// Handshake ok — now persist.
	if err := cfg.Save(path); err != nil {
		return err
	}
	fmt.Printf("registered backend %q -> %v (transport=%s, auth=%s, handshake: ok)\n", name, cmd, *transport, *auth)
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

// probeBackend spawns the backend, completes the MCP initialize handshake, and
// tears it down. It is the canonical validation gate for backend add/probe: on
// success the caller can trust the command runs and speaks MCP. The context's
// deadline (and exec.CommandContext under it) kills a backend that hangs or
// never answers, so a long-lived daemon cannot wedge the probe.
func probeBackend(ctx context.Context, be *config.Backend, envExtra []string) error {
	sb := backend.NewStdio(be.Name, be.Command, envExtra)
	if err := sb.Start(ctx); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	defer sb.Close()

	conn := sb.Conn()

	params := json.RawMessage(`{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"usher","version":"` + version + `"}}`)
	req := &mcp.Message{
		JSONRPC: "2.0",
		ID:      json.RawMessage("1"),
		Method:  "initialize",
		Params:  params,
	}
	if err := conn.Write(req); err != nil {
		return fmt.Errorf("write initialize: %w", err)
	}

	// Read responses on a goroutine so a backend that never replies is bounded by
	// the context deadline rather than blocking Read forever.
	type readResult struct {
		m   *mcp.Message
		err error
	}
	ch := make(chan readResult, 1)
	go func() {
		m, err := conn.Read()
		ch <- readResult{m, err}
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("initialize: %w", ctx.Err())
	case r := <-ch:
		if r.err != nil {
			return fmt.Errorf("read initialize response: %w", r.err)
		}
		if len(r.m.Error) > 0 {
			return fmt.Errorf("initialize error: %s", r.m.Error)
		}
		if len(r.m.Result) == 0 {
			return fmt.Errorf("initialize returned no result (not an MCP server?)")
		}
	}

	// The backend speaks MCP. Send notifications/initialized so it can finish its
	// handshake cleanly, then close. tools/list is exercised at serve time.
	_ = conn.Write(&mcp.Message{JSONRPC: "2.0", Method: "notifications/initialized"})
	return nil
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
