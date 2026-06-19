package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestWriteCompletionPerShell asserts every supported shell emits a non-empty
// script that mentions the key subcommands (a sanity check that the static
// command list actually made it into the generated script). It does not assert
// shell syntax — that is the shell's job — only that the surface is present.
func TestWriteCompletionPerShell(t *testing.T) {
	// Representative key names that must appear in every shell's script.
	wantSubstrings := []string{"serve", "backend", "completion", "version", "add", "probe"}

	for _, shell := range completionShells {
		t.Run(shell, func(t *testing.T) {
			var buf bytes.Buffer
			if err := writeCompletion(&buf, shell); err != nil {
				t.Fatalf("writeCompletion(%s) = %v, want nil", shell, err)
			}
			out := buf.String()
			if strings.TrimSpace(out) == "" {
				t.Fatalf("writeCompletion(%s) produced an empty script", shell)
			}
			for _, sub := range wantSubstrings {
				if !strings.Contains(out, sub) {
					t.Errorf("%s script missing key subcommand %q", shell, sub)
				}
			}
		})
	}
}

// TestWriteCompletionUnsupportedShell asserts an unknown shell errors clearly and
// the error names the shells that are supported.
func TestWriteCompletionUnsupportedShell(t *testing.T) {
	var buf bytes.Buffer
	err := writeCompletion(&buf, "powershell")
	if err == nil {
		t.Fatal("writeCompletion(powershell) = nil, want error")
	}
	for _, shell := range completionShells {
		if !strings.Contains(err.Error(), shell) {
			t.Errorf("error %q does not mention supported shell %q", err, shell)
		}
	}
	if buf.Len() != 0 {
		t.Errorf("unsupported shell wrote %d bytes, want 0", buf.Len())
	}
}

// TestCmdCompletionArgs covers the argument handling: a missing shell and too
// many shells both error; exactly one supported shell succeeds.
func TestCmdCompletionArgs(t *testing.T) {
	if err := cmdCompletion(nil); err == nil {
		t.Error("cmdCompletion(nil) = nil, want error (missing shell)")
	}
	if err := cmdCompletion([]string{"bash", "zsh"}); err == nil {
		t.Error("cmdCompletion(two shells) = nil, want error")
	}
	if err := cmdCompletion([]string{"bash"}); err != nil {
		t.Errorf("cmdCompletion(bash) = %v, want nil", err)
	}
}

// TestCompletionCoversAllTopCommands guards against drift: every top-level
// command listed in the generator must appear in each shell's emitted script, so
// adding a command to topCommands without breaking completion is caught here.
func TestCompletionCoversAllTopCommands(t *testing.T) {
	for _, shell := range completionShells {
		var buf bytes.Buffer
		if err := writeCompletion(&buf, shell); err != nil {
			t.Fatalf("writeCompletion(%s) = %v", shell, err)
		}
		out := buf.String()
		for _, cmd := range topCommands {
			if !strings.Contains(out, cmd) {
				t.Errorf("%s script missing top-level command %q", shell, cmd)
			}
		}
	}
}
