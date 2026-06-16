//go:build darwin

package identity

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

// TestPeerPID_FromUnixConn dials a Unix socket in-process and asserts PeerPID on
// the accepted server-side *net.UnixConn returns the dialing process's PID. Since
// the dialer is this test process, the peer PID must equal os.Getpid().
func TestPeerPID_FromUnixConn(t *testing.T) {
	dir, err := os.MkdirTemp("", "usher-peerpid")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "p.sock")

	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			accepted <- nil
			return
		}
		accepted <- c
	}()

	dialer, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer dialer.Close()

	server := <-accepted
	if server == nil {
		t.Fatal("accept failed")
	}
	defer server.Close()

	got := PeerPID(server)
	if want := os.Getpid(); got != want {
		t.Errorf("PeerPID(accepted conn) = %d, want %d (the dialing process)", got, want)
	}
}

// TestPeerPID_NilFallback asserts the stdio path (nil conn) and a non-Unix conn
// both fall back to os.Getpid(), so ServeStdio's identity is unchanged.
func TestPeerPID_NilFallback(t *testing.T) {
	if got, want := PeerPID(nil), os.Getpid(); got != want {
		t.Errorf("PeerPID(nil) = %d, want %d", got, want)
	}
	// A TCP conn is a net.Conn but not *net.UnixConn: must also fall back.
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	if got, want := PeerPID(c1), os.Getpid(); got != want {
		t.Errorf("PeerPID(non-unix conn) = %d, want %d", got, want)
	}
}
