package audit

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestFileSinkRotation drives the sink past a tiny threshold and asserts it
// rolls the active file to audit.log.1 and keeps no more than K backups.
func TestFileSinkRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")

	const keep = 3
	// 10-byte records against a 25-byte threshold: every third write triggers a
	// rollover (size 20 + 10 > 25), so many writes produce many rotations and
	// exercise the keep/prune path.
	const maxBytes = 25
	const recLen = 10

	s, err := NewFileSink(path, maxBytes, keep)
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}
	rec := make([]byte, recLen)
	for i := range rec {
		rec[i] = 'x'
	}
	for i := 0; i < 40; i++ {
		if _, err := s.Write(rec); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The active log must exist.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("active log missing: %v", err)
	}
	// At least one rollover must have happened (audit.log.1 present).
	if _, err := os.Stat(path + ".1"); err != nil {
		t.Fatalf("expected rollover to audit.log.1: %v", err)
	}
	// Only keep backups may survive; audit.log.(keep+1) must be gone.
	if _, err := os.Stat(fmt.Sprintf("%s.%d", path, keep+1)); !os.IsNotExist(err) {
		t.Fatalf("expected no backup beyond keep=%d, got err=%v", keep, err)
	}
	// Count the backups that exist; must be <= keep.
	got := 0
	for i := 1; i <= keep+5; i++ {
		if _, err := os.Stat(fmt.Sprintf("%s.%d", path, i)); err == nil {
			got++
		}
	}
	if got > keep {
		t.Fatalf("retained %d backups, want <= %d", got, keep)
	}
	if got != keep {
		t.Fatalf("after many rotations expected exactly keep=%d backups, got %d", keep, got)
	}
}

// TestFileSinkDefaults confirms non-positive knobs fall back to package defaults
// and a small write does not rotate.
func TestFileSinkDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")

	s, err := NewFileSink(path, 0, 0)
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}
	defer s.Close()
	if s.maxBytes != DefaultMaxBytes {
		t.Fatalf("maxBytes = %d, want default %d", s.maxBytes, DefaultMaxBytes)
	}
	if s.keep != DefaultKeep {
		t.Fatalf("keep = %d, want default %d", s.keep, DefaultKeep)
	}
	if _, err := s.Write([]byte("small line\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := os.Stat(path + ".1"); !os.IsNotExist(err) {
		t.Fatalf("small write must not rotate, but audit.log.1 exists: %v", err)
	}
}

// TestFileSinkReopenKeepsSize confirms reopening an existing log resumes its
// byte count rather than overwriting, so size-based rotation survives restart.
func TestFileSinkReopenKeepsSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")

	s1, err := NewFileSink(path, 100, 2)
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}
	if _, err := s1.Write([]byte("0123456789")); err != nil { // 10 bytes
		t.Fatalf("write: %v", err)
	}
	s1.Close()

	s2, err := NewFileSink(path, 100, 2)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if s2.size != 10 {
		t.Fatalf("reopened size = %d, want 10 (resumed append)", s2.size)
	}
}
