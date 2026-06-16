//go:build !darwin

package identity

import (
	"net"
	"os"
)

// PeerPID on non-darwin platforms (Linux/CI) has no LOCAL_PEERPID equivalent
// wired up yet, so it falls back to os.Getpid(). This keeps the package building
// and the socket path functional everywhere; the real peer-credential lookup is
// the darwin build's job. (SO_PEERCRED on Linux is a future addition.)
func PeerPID(net.Conn) int { return os.Getpid() }
