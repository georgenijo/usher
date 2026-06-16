// Command usher is the MCP broker — a front desk every agent talks to instead
// of wiring each tool itself. It routes calls to backends, runs them through a
// middleware pipeline (trim, arbitrate, gate, audit), and forwards verbatim by
// default. This is the #14 skeleton: a working stdio proxy with identity and
// audit; the pipeline's substantive stages are wired but pass-through.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"

	"github.com/georgenijo/usher/internal/broker"
	"github.com/georgenijo/usher/internal/config"
)

const version = "0.0.1-dev"

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
	case "backend":
		err = cmdBackend(os.Args[2:])
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
  usher backend list                show registered backends
  usher backend add NAME -- CMD...  register a stdio backend
  usher version

state dir: `+config.StateDir()+`
`)
}

// cmdServe runs the stdio proxy: stdin/stdout is the agent, routed to a backend.
func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	backendName := fs.String("backend", "", "backend to route to (default: configured default)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(config.DefaultPath())
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	b, err := broker.New(cfg)
	if err != nil {
		return err
	}
	return b.ServeStdio(ctx, *backendName, os.Stdin, os.Stdout)
}

// cmdBackend handles the backend control subcommands.
func cmdBackend(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: usher backend <list|add> ...")
	}
	switch args[0] {
	case "list":
		return backendList()
	case "add":
		return backendAdd(args[1:])
	default:
		return fmt.Errorf("unknown backend subcommand %q (want list|add)", args[0])
	}
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

// backendAdd parses `NAME [--auth A] [--transport T] [--default] -- CMD...`.
func backendAdd(args []string) error {
	sep := -1
	for i, a := range args {
		if a == "--" {
			sep = i
			break
		}
	}
	if sep == -1 {
		return fmt.Errorf("usage: usher backend add NAME [--auth A] [--default] -- COMMAND...")
	}
	head, cmd := args[:sep], args[sep+1:]
	if len(head) == 0 {
		return fmt.Errorf("backend name required")
	}
	name := head[0]

	fs := flag.NewFlagSet("backend add", flag.ContinueOnError)
	auth := fs.String("auth", "inherit", "auth strategy: none|env|inherit|oauth")
	transport := fs.String("transport", "stdio", "transport: stdio|http")
	makeDefault := fs.Bool("default", false, "make this the default backend")
	if err := fs.Parse(head[1:]); err != nil {
		return err
	}
	if len(cmd) == 0 {
		return fmt.Errorf("command required after --")
	}
	if *transport != "stdio" {
		return fmt.Errorf("transport %q not supported yet (stdio only)", *transport)
	}

	path := config.DefaultPath()
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	cfg.Add(config.Backend{
		Name:      name,
		Transport: *transport,
		Command:   cmd,
		Auth:      *auth,
	}, *makeDefault)
	if err := cfg.Save(path); err != nil {
		return err
	}
	fmt.Printf("registered backend %q -> %v (auth=%s)\n", name, cmd, *auth)
	return nil
}
