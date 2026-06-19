// Package audit is the broker's append-only record of every message crossing
// the front desk. It logs to a writer; the broker drives it with an
// io.MultiWriter over stderr AND a size-rotated file under the state dir (see
// FileSink in file.go). Structured records remain a later phase.
package audit

import (
	"io"
	"log"

	"github.com/georgenijo/usher/internal/identity"
)

// Level controls the stderr verbosity of the Logger. It gates ONLY the
// informational Infof lifecycle lines; errors, gate-blocked/security lines, and
// the core per-message audit record always emit regardless of level so a quiet
// daemon never hides a refusal or a transport failure (#log-verbosity).
type Level int

const (
	// LevelQuiet suppresses Infof lifecycle lines (prewarm, backend state
	// transitions, sampler start/stop) while keeping errors and the core audit.
	LevelQuiet Level = iota
	// LevelNormal is the default: every line emits, the historical behavior.
	LevelNormal
	// LevelVerbose is the full-detail level. It currently emits exactly what
	// LevelNormal does (the format is fixed by spec); it exists so a future
	// debug-only line can gate on it without another signature change.
	LevelVerbose
)

// Logger formats audit lines. It is safe for concurrent use via the embedded
// *log.Logger.
type Logger struct {
	l     *log.Logger
	level Level
}

// New returns a Logger writing to w at the default (normal) verbosity — the
// historical behavior where every line emits.
func New(w io.Writer) *Logger {
	return NewLevel(w, LevelNormal)
}

// NewLevel returns a Logger writing to w at the given verbosity. Only Infof is
// gated by level; Errorf, the core Message audit, and connect/disconnect lines
// always emit.
func NewLevel(w io.Writer, level Level) *Logger {
	return &Logger{l: log.New(w, "usher ", log.LstdFlags|log.Lmicroseconds), level: level}
}

// Level reports the Logger's configured verbosity.
func (a *Logger) Level() Level { return a.level }

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
//
// Infof is the ONLY level-gated line: at LevelQuiet it is suppressed so a quiet
// daemon's stderr carries only errors, gate-blocked/security lines, and the core
// per-message audit. Errorf and Message are never gated.
func (a *Logger) Infof(tag, format string, args ...any) {
	if a.level < LevelNormal {
		return
	}
	a.l.Printf("info tag=%s "+format, append([]any{tag}, args...)...)
}
