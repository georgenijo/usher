package main

import (
	"encoding/xml"
	"strings"
	"testing"
)

// TestRenderPlist asserts the LaunchAgent plist renders to well-formed XML, names
// the binary in ProgramArguments, and includes/excludes the --backend pair based
// on whether a backend was given. This catches a template bug at test time rather
// than as an opaque launchctl error.
func TestRenderPlist(t *testing.T) {
	t.Setenv("USHER_STATE_DIR", t.TempDir())

	cases := []struct {
		name        string
		backend     string
		wantBackend bool
	}{
		{"with backend", "cua", true},
		{"without backend", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := renderPlist("/usr/local/bin/usher", tc.backend)
			if err != nil {
				t.Fatalf("renderPlist: %v", err)
			}

			// Must be well-formed XML.
			var doc struct {
				XMLName xml.Name `xml:"plist"`
			}
			if err := xml.Unmarshal(out, &doc); err != nil {
				t.Fatalf("rendered plist not valid XML: %v\n%s", err, out)
			}

			s := string(out)
			if !strings.Contains(s, "<string>/usr/local/bin/usher</string>") {
				t.Errorf("plist missing binary path:\n%s", s)
			}
			if !strings.Contains(s, "<string>--socket</string>") {
				t.Errorf("plist missing --socket arg:\n%s", s)
			}
			if !strings.Contains(s, "<string>"+launchdLabel+"</string>") {
				t.Errorf("plist missing label %q:\n%s", launchdLabel, s)
			}
			hasBackend := strings.Contains(s, "<string>--backend</string>")
			if hasBackend != tc.wantBackend {
				t.Errorf("plist --backend present = %v, want %v:\n%s", hasBackend, tc.wantBackend, s)
			}
			if tc.wantBackend && !strings.Contains(s, "<string>"+tc.backend+"</string>") {
				t.Errorf("plist missing backend name %q:\n%s", tc.backend, s)
			}
		})
	}
}
