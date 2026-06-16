// Package identity stamps each broker connection with a stable id at connect
// time. In the stdio skeleton the agent spawns `usher serve`, so one process ==
// one connection and PID identifies the caller. When usher becomes an always-on
// Unix-socket daemon (#20), New is replaced by a peer-credential lookup
// (SO_PEERCRED / LOCAL_PEERPID) read off the accepted socket.
package identity

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"time"
)

// Identity describes one connected agent for the lifetime of its connection.
type Identity struct {
	ID        string    // short random hex, unique per connection
	PID       int       // caller process id
	StartedAt time.Time // connect time
}

// New mints an identity for a freshly accepted connection.
func New() Identity {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return Identity{
		ID:        hex.EncodeToString(b[:]),
		PID:       os.Getpid(),
		StartedAt: time.Now(),
	}
}
