// Shell completion generator: `usher completion <bash|zsh|fish>` prints a static
// completion script to stdout that completes usher's top-level subcommands and
// the `backend` subcommands. It is pure stdlib (text/template-free string
// building) — no completion library and no third-party dependency, in keeping
// with usher's stdlib-only constraint. The scripts are static: they encode the
// command list at build time rather than shelling back into usher, so completion
// stays fast and works even before the daemon is up.
package main

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// topCommands is the canonical list of usher's top-level subcommands, kept in
// sync with the switch in main() (#completion). It drives every shell's
// completion script so all three agree on the same surface.
var topCommands = []string{
	"serve",
	"mcpserver",
	"backend",
	"start",
	"stop",
	"status",
	"ui",
	"install",
	"uninstall",
	"version",
	"help",
	"completion",
}

// backendSubcommands mirrors cmdBackend's switch (list|add|probe).
var backendSubcommands = []string{
	"list",
	"add",
	"probe",
}

// completionShells is the set of shells the generator supports. It also seeds the
// `usher completion` argument completion so completion completes itself.
var completionShells = []string{"bash", "zsh", "fish"}

// cmdCompletion implements `usher completion <shell>`: it writes the chosen
// shell's completion script to stdout, or returns a clear error for a missing or
// unsupported shell so the user knows which shells are available.
func cmdCompletion(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: usher completion <%s>", strings.Join(completionShells, "|"))
	}
	if len(args) > 1 {
		return fmt.Errorf("usher completion takes exactly one shell argument (got %d)", len(args))
	}
	return writeCompletion(os.Stdout, args[0])
}

// writeCompletion writes the completion script for shell to w. It is split out
// from cmdCompletion so tests can capture the script without touching os.Stdout.
// An unsupported shell is reported with the list of shells that are supported.
func writeCompletion(w io.Writer, shell string) error {
	var script string
	switch shell {
	case "bash":
		script = bashCompletion()
	case "zsh":
		script = zshCompletion()
	case "fish":
		script = fishCompletion()
	default:
		return fmt.Errorf("unsupported shell %q (want %s)", shell, strings.Join(completionShells, "|"))
	}
	_, err := io.WriteString(w, script)
	return err
}

// bashCompletion builds a bash completion function. It completes the top-level
// commands at position 1, and the backend subcommands after `usher backend`.
func bashCompletion() string {
	var b strings.Builder
	b.WriteString("# bash completion for usher\n")
	b.WriteString("# install: usher completion bash > /usr/local/etc/bash_completion.d/usher\n")
	b.WriteString("#     or:  source <(usher completion bash)\n")
	b.WriteString("_usher() {\n")
	b.WriteString("    local cur prev words cword\n")
	b.WriteString("    COMPREPLY=()\n")
	b.WriteString(`    cur="${COMP_WORDS[COMP_CWORD]}"` + "\n")
	b.WriteString(`    prev="${COMP_WORDS[COMP_CWORD-1]}"` + "\n")
	b.WriteString("    local commands=\"" + strings.Join(topCommands, " ") + "\"\n")
	b.WriteString("    local backend_subcommands=\"" + strings.Join(backendSubcommands, " ") + "\"\n")
	b.WriteString("    local completion_shells=\"" + strings.Join(completionShells, " ") + "\"\n")
	b.WriteString("\n")
	b.WriteString("    if [ \"$COMP_CWORD\" -eq 1 ]; then\n")
	b.WriteString(`        COMPREPLY=( $(compgen -W "$commands" -- "$cur") )` + "\n")
	b.WriteString("        return 0\n")
	b.WriteString("    fi\n")
	b.WriteString("\n")
	b.WriteString("    case \"${COMP_WORDS[1]}\" in\n")
	b.WriteString("        backend)\n")
	b.WriteString("            if [ \"$COMP_CWORD\" -eq 2 ]; then\n")
	b.WriteString(`                COMPREPLY=( $(compgen -W "$backend_subcommands" -- "$cur") )` + "\n")
	b.WriteString("            fi\n")
	b.WriteString("            ;;\n")
	b.WriteString("        completion)\n")
	b.WriteString("            if [ \"$COMP_CWORD\" -eq 2 ]; then\n")
	b.WriteString(`                COMPREPLY=( $(compgen -W "$completion_shells" -- "$cur") )` + "\n")
	b.WriteString("            fi\n")
	b.WriteString("            ;;\n")
	b.WriteString("    esac\n")
	b.WriteString("    return 0\n")
	b.WriteString("}\n")
	b.WriteString("complete -F _usher usher\n")
	return b.String()
}

// zshCompletion builds a zsh completion function using _describe/_arguments-free
// _values so it stays simple and dependency-light. It completes top-level
// commands, then dispatches to backend / completion subcommand lists.
func zshCompletion() string {
	var b strings.Builder
	b.WriteString("#compdef usher\n")
	b.WriteString("# zsh completion for usher\n")
	b.WriteString("# install: usher completion zsh > \"${fpath[1]}/_usher\"\n")
	b.WriteString("#     or:  source <(usher completion zsh)\n")
	b.WriteString("_usher() {\n")
	b.WriteString("    local -a commands backend_subcommands completion_shells\n")
	b.WriteString("    commands=(" + zshQuoteList(topCommands) + ")\n")
	b.WriteString("    backend_subcommands=(" + zshQuoteList(backendSubcommands) + ")\n")
	b.WriteString("    completion_shells=(" + zshQuoteList(completionShells) + ")\n")
	b.WriteString("\n")
	b.WriteString("    if (( CURRENT == 2 )); then\n")
	b.WriteString("        _describe 'usher command' commands\n")
	b.WriteString("        return\n")
	b.WriteString("    fi\n")
	b.WriteString("\n")
	b.WriteString("    case \"${words[2]}\" in\n")
	b.WriteString("        backend)\n")
	b.WriteString("            if (( CURRENT == 3 )); then\n")
	b.WriteString("                _describe 'backend subcommand' backend_subcommands\n")
	b.WriteString("            fi\n")
	b.WriteString("            ;;\n")
	b.WriteString("        completion)\n")
	b.WriteString("            if (( CURRENT == 3 )); then\n")
	b.WriteString("                _describe 'shell' completion_shells\n")
	b.WriteString("            fi\n")
	b.WriteString("            ;;\n")
	b.WriteString("    esac\n")
	b.WriteString("}\n")
	b.WriteString("_usher \"$@\"\n")
	return b.String()
}

// fishCompletion builds fish completion via `complete` directives. Top-level
// commands complete only when no subcommand is yet present; backend / completion
// subcommands complete only after their respective command word.
func fishCompletion() string {
	var b strings.Builder
	b.WriteString("# fish completion for usher\n")
	b.WriteString("# install: usher completion fish > ~/.config/fish/completions/usher.fish\n")
	b.WriteString("\n")
	b.WriteString("# Helper: true when usher has no subcommand word yet.\n")
	b.WriteString("function __usher_no_subcommand\n")
	b.WriteString("    set -l cmd (commandline -opc)\n")
	b.WriteString("    test (count $cmd) -eq 1\n")
	b.WriteString("end\n")
	b.WriteString("\n")
	b.WriteString("# Top-level subcommands.\n")
	for _, c := range topCommands {
		b.WriteString("complete -c usher -n __usher_no_subcommand -f -a " + c + "\n")
	}
	b.WriteString("\n")
	b.WriteString("# backend subcommands.\n")
	for _, c := range backendSubcommands {
		b.WriteString("complete -c usher -n '__fish_seen_subcommand_from backend' -f -a " + c + "\n")
	}
	b.WriteString("\n")
	b.WriteString("# completion shells.\n")
	for _, c := range completionShells {
		b.WriteString("complete -c usher -n '__fish_seen_subcommand_from completion' -f -a " + c + "\n")
	}
	return b.String()
}

// zshQuoteList renders names as a space-separated list of single-quoted words for
// a zsh array literal, e.g. 'serve' 'backend'.
func zshQuoteList(names []string) string {
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = "'" + n + "'"
	}
	return strings.Join(quoted, " ")
}
