//go:build darwin

package identity

import (
	"net"
	"os"
	"syscall"
)

// macOS AF_UNIX peer-credential constants. They are stable numeric literals from
// /usr/include/sys/un.h (available since macOS 10.8) but are NOT exported by
// Go's syscall package, so we name them here. The build tag isolates them to
// darwin; if Apple ever changed them (historically they have not) only this file
// is affected.
const (
	solLocal     = 0 // SOL_LOCAL: the AF_UNIX protocol option level
	localPeerPID = 1 // LOCAL_PEERPID: getsockopt option for the peer's PID
)

// PeerPID returns the process id of the connection's peer. For a *net.UnixConn
// it reads LOCAL_PEERPID off the live socket fd via SyscallConn().Control — no
// fd duplication (unlike net.UnixConn.File()) and no cgo or x/sys dependency.
// A nil conn, a non-Unix conn, or any syscall failure falls back to os.Getpid().
func PeerPID(c net.Conn) int {
	uc, ok := c.(*net.UnixConn)
	if !ok {
		return os.Getpid()
	}
	rc, err := uc.SyscallConn()
	if err != nil {
		return os.Getpid()
	}
	var pid int
	// Control runs the closure with the non-blocking fd live; it is never
	// duplicated and the conn stays usable afterward.
	_ = rc.Control(func(fd uintptr) {
		if v, err := syscall.GetsockoptInt(int(fd), solLocal, localPeerPID); err == nil {
			pid = v
		}
	})
	if pid <= 0 {
		return os.Getpid()
	}
	return pid
}
