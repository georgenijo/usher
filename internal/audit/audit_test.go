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
