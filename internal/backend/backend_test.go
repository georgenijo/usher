package backend

import (
	"context"
	"strings"
	"testing"
)

// TestStdioEnvExtraJSON injects an env var and has the child emit it as a JSON
// line so the MCP Conn can decode it, asserting the value landed in the child's
// environment. /bin/sh stands in for an MCP backend; only the env-injection
// wiring is under test.
func TestStdioEnvExtraJSON(t *testing.T) {
	// Emit {"result":"<value of FOO>"} as one newline-delimited JSON object.
	sb := NewStdio("envtest",
		[]string{"/bin/sh", "-c", `printf '{"jsonrpc":"2.0","result":"%s"}\n' "$FOO"`},
		[]string{"FOO=injected-value"},
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sb.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer sb.Close()

	m, err := sb.Conn().Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got := strings.Trim(string(m.Result), `"`); got != "injected-value" {
		t.Errorf("child saw FOO=%q, want %q (env injection failed)", got, "injected-value")
	}
}

// TestStdioNoEnvExtraInherits confirms that a nil envExtra leaves the child
// inheriting the parent environment (the auth=inherit/none default): a var set
// in the parent process is visible to the child.
func TestStdioNoEnvExtraInherits(t *testing.T) {
	t.Setenv("USHER_INHERIT_PROBE", "yes")
	sb := NewStdio("inherittest",
		[]string{"/bin/sh", "-c", `printf '{"jsonrpc":"2.0","result":"%s"}\n' "$USHER_INHERIT_PROBE"`},
		nil, // inherit/none default
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sb.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer sb.Close()

	m, err := sb.Conn().Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got := strings.Trim(string(m.Result), `"`); got != "yes" {
		t.Errorf("child saw USHER_INHERIT_PROBE=%q, want %q (parent env not inherited)", got, "yes")
	}
}
