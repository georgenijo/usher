// Package identity stamps each broker connection with a stable id at connect
// time. In the stdio skeleton the agent spawns `usher serve`, so one process ==
// one connection and PID identifies the caller. As an always-on Unix-socket
// daemon (#20), the caller is the socket peer, so NewForConn reads the peer
// credentials (LOCAL_PEERPID on macOS) off the accepted connection instead.
package identity

import (
	"crypto/rand"
	"encoding/hex"
	"net"
	"os"
	"time"
)

// Identity describes one connected agent for the lifetime of its connection.
type Identity struct {
	ID        string    // short random hex, unique per connection
	PID       int       // caller process id
	StartedAt time.Time // connect time
}

// New mints an identity for the local stdio path, where the agent spawns
// `usher serve` and thus shares its own process — PID is os.Getpid().
func New() Identity {
	return Identity{
		ID:        randID(),
		PID:       os.Getpid(),
		StartedAt: time.Now(),
	}
}

// NewForConn mints an identity for a freshly accepted socket connection, reading
// the caller's PID from the connection's peer credentials. A nil conn (the stdio
// path) or a non-Unix conn falls back to os.Getpid via PeerPID.
func NewForConn(c net.Conn) Identity {
	return Identity{
		ID:        randID(),
		PID:       PeerPID(c),
		StartedAt: time.Now(),
	}
}

// randID returns a short random hex id, unique per connection.
func randID() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
