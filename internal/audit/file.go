package audit

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Default rotation knobs for the file sink. A zero config value falls back to
// these so zero-config (the built-in Default config) still gets a sensible
// rotating log under the state dir.
const (
	// DefaultMaxBytes is the size the active audit.log may reach before the sink
	// rotates it to audit.log.1 and starts a fresh file. 10 MB keeps a single
	// file cheap to tail while still holding a meaningful run of wire activity.
	DefaultMaxBytes int64 = 10 << 20 // 10 MiB

	// DefaultKeep is how many rotated files (audit.log.1 .. audit.log.K) the sink
	// retains; anything older is pruned on the next rollover.
	DefaultKeep = 5
)

// FileSink is a thread-safe io.Writer that appends audit lines to a file and
// rotates it once it crosses maxBytes. On rollover it shifts audit.log →
// audit.log.1, audit.log.1 → audit.log.2, …, drops the oldest beyond keep, then
// reopens a fresh active file. It is pure stdlib (os, sync, path/filepath) so the
// broker's zero-dependency constraint holds.
//
// The broker writes audit output to an io.MultiWriter(os.Stderr, sink): stderr
// behaviour is unchanged and the file is an additional, durable copy under the
// state dir (the phase the package comment promised).
type FileSink struct {
	mu       sync.Mutex
	path     string // active log path (e.g. <state>/audit.log)
	maxBytes int64  // rotate when size would exceed this
	keep     int    // number of rotated files to retain

	f    *os.File // current active file handle (lazily opened)
	size int64    // bytes written to the active file so far
}

// NewFileSink opens (creating if needed) an appending audit log at path and
// returns a sink that rotates at maxBytes, keeping the last keep rotated files.
// A non-positive maxBytes or keep falls back to the package defaults. The parent
// directory is created if missing.
func NewFileSink(path string, maxBytes int64, keep int) (*FileSink, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	if keep <= 0 {
		keep = DefaultKeep
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("audit: create log dir: %w", err)
	}
	s := &FileSink{path: path, maxBytes: maxBytes, keep: keep}
	if err := s.open(); err != nil {
		return nil, err
	}
	return s, nil
}

// open attaches to the active file (append mode) and records its current size so
// the running byte count survives a daemon restart onto an existing log.
func (s *FileSink) open() error {
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("audit: open log: %w", err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("audit: stat log: %w", err)
	}
	s.f = f
	s.size = fi.Size()
	return nil
}

// Write appends p to the active log, rotating first when the write would push
// the file past maxBytes. It satisfies io.Writer and is safe for concurrent use.
func (s *FileSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Rotate before writing so a single record never straddles two files and the
	// active file's size stays bounded by maxBytes + one record. Skip when the
	// file is still empty so an oversized lone record can't spin forever.
	if s.size > 0 && s.size+int64(len(p)) > s.maxBytes {
		if err := s.rotate(); err != nil {
			return 0, err
		}
	}
	n, err := s.f.Write(p)
	s.size += int64(n)
	return n, err
}

// rotate closes the active file, shifts the numbered backups up by one (pruning
// any beyond keep), renames the active file to audit.log.1, and reopens a fresh
// active file. The caller holds s.mu.
func (s *FileSink) rotate() error {
	if err := s.f.Close(); err != nil {
		return fmt.Errorf("audit: close on rotate: %w", err)
	}
	// Drop the oldest retained file, then walk down shifting each backup up one
	// index: audit.log.(K-1) → audit.log.K, …, audit.log.1 → audit.log.2.
	if err := os.Remove(s.numbered(s.keep)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("audit: prune oldest: %w", err)
	}
	for i := s.keep - 1; i >= 1; i-- {
		from, to := s.numbered(i), s.numbered(i+1)
		if err := os.Rename(from, to); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("audit: shift backup %d: %w", i, err)
		}
	}
	if err := os.Rename(s.path, s.numbered(1)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("audit: rotate active: %w", err)
	}
	return s.open()
}

// numbered is the path of the i-th rotated backup (audit.log.i).
func (s *FileSink) numbered(i int) string {
	return fmt.Sprintf("%s.%d", s.path, i)
}

// Close flushes and releases the active file handle. Safe to call once at
// daemon shutdown; further Writes will fail.
func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}
