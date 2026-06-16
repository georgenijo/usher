// Package backend connects the broker to a downstream MCP server. The skeleton
// ships the stdio transport: spawn a child process and speak newline-delimited
// JSON-RPC over its stdin/stdout. (http transport is a later phase.)
package backend

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/georgenijo/usher/internal/mcp"
)

// Backend is a live connection to one downstream MCP server.
type Backend interface {
	Name() string
	Conn() *mcp.Conn
	CloseStdin() error
	Close() error
}

// Stdio runs an MCP server as a child process and bridges its stdio.
type Stdio struct {
	name  string
	argv  []string
	cmd   *exec.Cmd
	stdin io.WriteCloser
	conn  *mcp.Conn
}

// NewStdio prepares (does not start) a stdio backend from an argv.
func NewStdio(name string, argv []string) *Stdio {
	return &Stdio{name: name, argv: argv}
}

// Start launches the child process and wires up the JSON-RPC connection. The
// child's stderr is forwarded to usher's stderr so backend logs stay visible.
func (s *Stdio) Start(ctx context.Context) error {
	if len(s.argv) == 0 {
		return fmt.Errorf("backend %q has no command", s.name)
	}
	s.cmd = exec.CommandContext(ctx, s.argv[0], s.argv[1:]...)
	s.cmd.Stderr = os.Stderr

	stdout, err := s.cmd.StdoutPipe()
	if err != nil {
		return err
	}
	s.stdin, err = s.cmd.StdinPipe()
	if err != nil {
		return err
	}
	if err := s.cmd.Start(); err != nil {
		return fmt.Errorf("start %q: %w", s.argv[0], err)
	}
	s.conn = mcp.NewConn(stdout, s.stdin)
	return nil
}

// Name returns the backend's registered name.
func (s *Stdio) Name() string { return s.name }

// Conn is the JSON-RPC channel to the backend (valid after Start).
func (s *Stdio) Conn() *mcp.Conn { return s.conn }

// CloseStdin signals end-of-input to the backend (half-close) so it can flush
// and exit cleanly without being killed.
func (s *Stdio) CloseStdin() error {
	if s.stdin == nil {
		return nil
	}
	return s.stdin.Close()
}

// Close terminates the child process. Used for hard teardown (ctx cancel); the
// normal path is CloseStdin followed by the child exiting on its own.
func (s *Stdio) Close() error {
	if s.cmd == nil || s.cmd.Process == nil {
		return nil
	}
	_ = s.cmd.Process.Kill()
	_, err := s.cmd.Process.Wait()
	return err
}
