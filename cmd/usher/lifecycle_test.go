package main

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/georgenijo/usher/internal/config"
)

// captureStdout runs fn with os.Stdout redirected to a pipe and returns what was
// written, so a test can assert on a command's printed output.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = orig }()

	fn()
	_ = w.Close()
	out, _ := io.ReadAll(r)
	return string(out)
}

// TestCmdStatus_Stopped: with an isolated state dir and no PID file, status must
// report "stopped" without error.
func TestCmdStatus_Stopped(t *testing.T) {
	t.Setenv("USHER_STATE_DIR", t.TempDir())
	out := captureStdout(t, func() {
		if err := cmdStatus(nil); err != nil {
			t.Errorf("cmdStatus = %v, want nil", err)
		}
	})
	if !strings.Contains(out, "stopped") {
		t.Errorf("cmdStatus output = %q, want it to contain \"stopped\"", out)
	}
}

// TestCmdStatus_Running: a PID file naming THIS process (which is alive) must
// report "running" with the socket path.
func TestCmdStatus_Running(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)
	if err := writePID(config.PidPath(), os.Getpid()); err != nil {
		t.Fatal(err)
	}
	out := captureStdout(t, func() {
		if err := cmdStatus(nil); err != nil {
			t.Errorf("cmdStatus = %v, want nil", err)
		}
	})
	if !strings.Contains(out, "running") {
		t.Errorf("cmdStatus output = %q, want it to contain \"running\"", out)
	}
	if !strings.Contains(out, config.SocketPath()) {
		t.Errorf("cmdStatus output = %q, want it to contain the socket path %q", out, config.SocketPath())
	}
}

// TestCmdStatus_Stale: a PID file naming a process that cannot be alive (PID
// chosen to be almost certainly dead) must report a stale pid file.
func TestCmdStatus_Stale(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)
	// A very large PID that is essentially never live.
	if err := writePID(config.PidPath(), 2147480000); err != nil {
		t.Fatal(err)
	}
	out := captureStdout(t, func() {
		if err := cmdStatus(nil); err != nil {
			t.Errorf("cmdStatus = %v, want nil", err)
		}
	})
	if !strings.Contains(out, "stale") {
		t.Errorf("cmdStatus output = %q, want it to mention a stale pid file", out)
	}
}

// TestPIDFileRoundTrip writes then reads a PID file, asserting the value
// survives, that a missing file reports os.ErrNotExist, and removePID clears it.
func TestPIDFileRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usher.pid")

	if _, err := readPID(path); !os.IsNotExist(err) {
		t.Errorf("readPID(missing) err = %v, want os.ErrNotExist", err)
	}
	if err := writePID(path, 4242); err != nil {
		t.Fatalf("writePID: %v", err)
	}
	got, err := readPID(path)
	if err != nil {
		t.Fatalf("readPID: %v", err)
	}
	if got != 4242 {
		t.Errorf("readPID = %d, want 4242", got)
	}
	removePID(path)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("after removePID, file still exists (err=%v)", err)
	}
}

// TestListenUnix_StaleFile: a leftover socket file from a prior crash must be
// removed so listenUnix succeeds, and the returned listener must accept a dial.
func TestListenUnix_StaleFile(t *testing.T) {
	dir, err := os.MkdirTemp("", "usher-listen")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "u.sock")

	// Simulate a stale socket file from a previous unclean shutdown.
	if err := os.WriteFile(sock, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}

	ln, err := listenUnix(sock)
	if err != nil {
		t.Fatalf("listenUnix over a stale file = %v, want nil", err)
	}
	defer ln.Close()

	// The returned listener is functional: a dial connects.
	accepted := make(chan struct{}, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			_ = c.Close()
			accepted <- struct{}{}
		}
	}()
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial fresh listener: %v", err)
	}
	_ = c.Close()
	<-accepted

	// The socket file is mode 0600 (owner-only), per the local-only ethos.
	info, err := os.Stat(sock)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("socket perm = %o, want 600", perm)
	}
}
