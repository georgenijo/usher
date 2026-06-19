package audit

import (
	"bytes"
	"strings"
	"testing"
)

// TestInfof asserts the info-level lifecycle line carries the "info" level and a
// "tag=" subsystem label (not the "error id=" framing of Errorf), so a healthy
// transition no longer reads as an error on the daemon's stderr.
func TestInfof(t *testing.T) {
	var buf bytes.Buffer
	a := New(&buf)
	a.Infof("supervisor", "backend %q state %s→%s", "cua", "stopped", "live")

	got := buf.String()
	for _, want := range []string{
		"info tag=supervisor",
		`backend "cua" state stopped→live`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("Infof output %q missing %q", got, want)
		}
	}
	if strings.Contains(got, "error ") {
		t.Errorf("Infof output %q must not carry the error framing", got)
	}
}

// TestErrorf keeps the error framing distinct from Infof so genuine errors are
// still recognisable as such.
func TestErrorf(t *testing.T) {
	var buf bytes.Buffer
	a := New(&buf)
	a.Errorf("conn-1", "pipeline blew up: %v", "boom")

	got := buf.String()
	if !strings.Contains(got, "error id=conn-1") {
		t.Errorf("Errorf output %q missing error framing", got)
	}
}

// TestLevelDefault asserts New keeps the historical behavior: every line emits,
// Infof included, so existing daemons see no change in verbosity.
func TestLevelDefault(t *testing.T) {
	var buf bytes.Buffer
	a := New(&buf)
	if got := a.Level(); got != LevelNormal {
		t.Fatalf("New level = %v, want LevelNormal", got)
	}
	a.Infof("supervisor", "prewarm: bringing %q live", "cua")
	if !strings.Contains(buf.String(), "info tag=supervisor") {
		t.Errorf("default level dropped Infof: %q", buf.String())
	}
}

// TestLevelQuietSuppressesInfof is the core of #log-verbosity: at LevelQuiet the
// informational Infof lifecycle line is suppressed while Errorf, the gate-blocked
// security line, and the core per-message audit still emit. A quiet daemon must
// never hide a refusal or a transport failure.
func TestLevelQuietSuppressesInfof(t *testing.T) {
	var buf bytes.Buffer
	a := NewLevel(&buf, LevelQuiet)

	// Infof is gated out.
	a.Infof("supervisor", "backend %q state %s→%s", "cua", "stopped", "live")
	if buf.Len() != 0 {
		t.Errorf("LevelQuiet must suppress Infof, got %q", buf.String())
	}

	// Errorf still emits.
	a.Errorf("conn-1", "pipeline blew up: %v", "boom")
	if !strings.Contains(buf.String(), "error id=conn-1") {
		t.Errorf("LevelQuiet dropped Errorf: %q", buf.String())
	}

	// The core per-message audit still emits (this is the security/forwarding
	// record — never gated by verbosity).
	buf.Reset()
	a.Message("conn-1", "client→backend", "tools/call", "7", 128)
	if !strings.Contains(buf.String(), "msg id=conn-1") {
		t.Errorf("LevelQuiet dropped the core Message audit: %q", buf.String())
	}
}

// TestLevelVerboseEmitsInfof asserts --verbose keeps the informational lines (it
// is at least as loud as normal); the message format is unchanged by spec.
func TestLevelVerboseEmitsInfof(t *testing.T) {
	var buf bytes.Buffer
	a := NewLevel(&buf, LevelVerbose)
	a.Infof("procstat", "per-process resource sampler on")
	if !strings.Contains(buf.String(), "info tag=procstat") {
		t.Errorf("LevelVerbose dropped Infof: %q", buf.String())
	}
}
