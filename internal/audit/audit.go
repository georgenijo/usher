// Package audit is the broker's append-only record of every message crossing
// the front desk. The skeleton logs to a writer (stderr by default); a later
// phase points it at a file under the state dir and adds structured records.
package audit

import (
	"io"
	"log"

	"github.com/georgenijo/usher/internal/identity"
)

// Logger formats audit lines. It is safe for concurrent use via the embedded
// *log.Logger.
type Logger struct {
	l *log.Logger
}

// New returns a Logger writing to w.
func New(w io.Writer) *Logger {
	return &Logger{l: log.New(w, "usher ", log.LstdFlags|log.Lmicroseconds)}
}

// Connect records a new agent connection bound to a backend.
func (a *Logger) Connect(id identity.Identity, backend string) {
	a.l.Printf("connect id=%s pid=%d backend=%s", id.ID, id.PID, backend)
}

// Disconnect records the end of a connection.
func (a *Logger) Disconnect(id identity.Identity, reason string) {
	a.l.Printf("disconnect id=%s reason=%s", id.ID, reason)
}

// ConnectID records a new connection from its id/pid/backend directly, rather
// than from a full Identity. The event-bus audit subscriber uses it: it receives
// a ConnOpenEvent (which already carries these fields) and cannot reconstruct the
// original Identity, so it logs the same line from the raw fields.
func (a *Logger) ConnectID(id string, pid int, backend string) {
	a.l.Printf("connect id=%s pid=%d backend=%s", id, pid, backend)
}

// DisconnectID records the end of a connection from its id directly, the
// event-bus counterpart to Disconnect (which takes a full Identity).
func (a *Logger) DisconnectID(id, reason string) {
	a.l.Printf("disconnect id=%s reason=%s", id, reason)
}

// Message records one message crossing the broker. dir is a human label
// ("client→backend" / "backend→client"); method and msgID may be empty.
func (a *Logger) Message(id, dir, method, msgID string, nbytes int) {
	a.l.Printf("msg id=%s %s method=%q rpc-id=%s bytes=%d", id, dir, method, msgID, nbytes)
}

// Errorf records a pipeline or transport error without tearing down the link.
func (a *Logger) Errorf(id, format string, args ...any) {
	a.l.Printf("error id=%s "+format, append([]any{id}, args...)...)
}

// Infof records an informational lifecycle line (prewarm, backend state
// transitions, sampler start/stop) WITHOUT the error framing of Errorf, so a
// healthy stopped→starting→live no longer reads as an "error" on the daemon's
// stderr. tag is a subsystem label ("supervisor", "loadtest", "procstat")
// rather than a connection id.
func (a *Logger) Infof(tag, format string, args ...any) {
	a.l.Printf("info tag=%s "+format, append([]any{tag}, args...)...)
}
