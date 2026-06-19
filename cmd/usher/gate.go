package main

// `usher gate` manages the gate's block/allow lists in config.json without
// hand-editing JSON (#18). The block-list ADDS to the broker's built-in
// DefaultBlockedTools; the allow-list is the operator's accepted-risk override
// that always wins (see broker.policyFromConfig / Policy.blocks). Tool names are
// BARE, never namespaced — the single-backend pump carries no prefix and the
// fanout strips the namespace before the pipeline — so a name containing "__" is
// refused here, matching the invariant GateStage relies on.

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/georgenijo/usher/internal/broker"
	"github.com/georgenijo/usher/internal/config"
)

// cmdGate dispatches the gate subcommands.
func cmdGate(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: usher gate <block|unblock|list> ...")
	}
	switch args[0] {
	case "block":
		return gateBlock(args[1:])
	case "unblock":
		return gateUnblock(args[1:])
	case "list":
		return gateList(args[1:])
	default:
		return fmt.Errorf("unknown gate subcommand %q (want block|unblock|list)", args[0])
	}
}

// gateBlock appends a bare tool name to cfg.BlockedTools (idempotent), refusing a
// namespaced name. The added name unions with the built-in DefaultBlockedTools at
// serve time, so the gate refuses tools/call to it unless it is allow-listed.
func gateBlock(args []string) error {
	fs := flag.NewFlagSet("gate block", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	tool, err := gateToolArg(fs.Args())
	if err != nil {
		return err
	}

	path := config.DefaultPath()
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	if contains(cfg.BlockedTools, tool) {
		fmt.Printf("%q already in block-list\n", tool)
		return nil
	}
	cfg.BlockedTools = append(cfg.BlockedTools, tool)
	if err := cfg.Save(path); err != nil {
		return err
	}
	fmt.Printf("blocked %q\n", tool)
	return nil
}

// gateUnblock appends a bare tool name to cfg.AllowedTools (idempotent), refusing
// a namespaced name. The allow-list ALWAYS wins, so this overrides a built-in or
// configured block for that tool — the "I've accepted the risk" escape hatch.
func gateUnblock(args []string) error {
	fs := flag.NewFlagSet("gate unblock", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	tool, err := gateToolArg(fs.Args())
	if err != nil {
		return err
	}

	path := config.DefaultPath()
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	if contains(cfg.AllowedTools, tool) {
		fmt.Printf("%q already in allow-list\n", tool)
		return nil
	}
	cfg.AllowedTools = append(cfg.AllowedTools, tool)
	if err := cfg.Save(path); err != nil {
		return err
	}
	fmt.Printf("unblocked %q (allow-list override)\n", tool)
	return nil
}

// gateList prints the effective block-list (built-in DefaultBlockedTools UNION
// cfg.BlockedTools, marking which entries are built-in) and the allow-list. It is
// read-only.
func gateList(args []string) error {
	fs := flag.NewFlagSet("gate list", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(config.DefaultPath())
	if err != nil {
		return err
	}

	builtin := make(map[string]struct{}, len(broker.DefaultBlockedTools))
	for _, t := range broker.DefaultBlockedTools {
		builtin[t] = struct{}{}
	}
	// Effective block-list = built-ins UNION configured, de-duplicated.
	effective := append([]string(nil), broker.DefaultBlockedTools...)
	for _, t := range cfg.BlockedTools {
		if _, ok := builtin[t]; !ok {
			effective = append(effective, t)
		}
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "BLOCKED\tSOURCE")
	for _, t := range effective {
		src := "config"
		if _, ok := builtin[t]; ok {
			src = "built-in"
		}
		fmt.Fprintf(w, "%s\t%s\n", t, src)
	}
	if err := w.Flush(); err != nil {
		return err
	}

	fmt.Println()
	allowed := append([]string(nil), cfg.AllowedTools...)
	sort.Strings(allowed)
	if len(allowed) == 0 {
		fmt.Println("ALLOWED (override): (none)")
	} else {
		fmt.Printf("ALLOWED (override): %s\n", strings.Join(allowed, ", "))
	}
	return nil
}

// gateToolArg pulls the single bare tool-name argument from the parsed args,
// rejecting a missing/extra arg and any namespaced name (containing "__"). The
// namespace check enforces the BARE-name invariant the gate keys on.
func gateToolArg(args []string) (string, error) {
	if len(args) != 1 || strings.TrimSpace(args[0]) == "" {
		return "", fmt.Errorf("exactly one tool name required")
	}
	tool := args[0]
	if strings.Contains(tool, "__") {
		return "", fmt.Errorf("tool name %q is namespaced; gate lists use BARE tool names (no \"__\")", tool)
	}
	return tool, nil
}

// contains reports whether s contains v (for idempotent list appends).
func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
