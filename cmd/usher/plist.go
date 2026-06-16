// launchd LaunchAgent plist generation (#20). The plist is rendered from a
// text/template (stdlib-only, no plist library) and validated by round-tripping
// through encoding/xml so a template bug surfaces at install time rather than as
// an opaque launchctl error. KeepAlive=true asks launchd to restart the daemon
// on crash; ThrottleInterval=3 prevents a tight crash loop.
package main

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"text/template"
)

// plistTemplate is the LaunchAgent plist. The ProgramArguments point at the
// absolute binary path and run `serve --socket`, optionally pinned to a backend.
// HOME and PATH are carried into the agent's environment so a child backend that
// relies on them (e.g. cua-driver under ~/.local/bin) resolves correctly.
const plistTemplate = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>{{.Label}}</string>
	<key>ProgramArguments</key>
	<array>
		<string>{{.Binary}}</string>
		<string>serve</string>
		<string>--socket</string>
{{- if .Backend}}
		<string>--backend</string>
		<string>{{.Backend}}</string>
{{- end}}
	</array>
	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<true/>
	<key>ThrottleInterval</key>
	<integer>3</integer>
	<key>EnvironmentVariables</key>
	<dict>
		<key>HOME</key>
		<string>{{.Home}}</string>
		<key>PATH</key>
		<string>{{.Path}}</string>
	</dict>
	<key>StandardOutPath</key>
	<string>{{.OutLog}}</string>
	<key>StandardErrorPath</key>
	<string>{{.ErrLog}}</string>
</dict>
</plist>
`

// plistData is the template's binding.
type plistData struct {
	Label   string
	Binary  string
	Backend string
	Home    string
	Path    string
	OutLog  string
	ErrLog  string
}

// renderPlist produces the LaunchAgent plist bytes for the given binary path and
// optional backend. It validates the result parses as XML before returning so a
// malformed template never reaches launchctl.
func renderPlist(binary, backend string) ([]byte, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("home dir: %w", err)
	}
	path := os.Getenv("PATH")
	if path == "" {
		path = "/usr/bin:/bin:/usr/sbin:/sbin"
	}

	data := plistData{
		Label:   launchdLabel,
		Binary:  binary,
		Backend: backend,
		Home:    home,
		Path:    path,
		OutLog:  filepath.Join(logsDir(), "usher.out.log"),
		ErrLog:  filepath.Join(logsDir(), "usher.err.log"),
	}

	tmpl, err := template.New("plist").Parse(plistTemplate)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, err
	}

	// Validate the rendered plist is well-formed XML. This catches a template bug
	// (an unescaped value, a broken tag) at install time, not as a cryptic
	// launchctl failure later.
	if err := xml.Unmarshal(buf.Bytes(), new(struct {
		XMLName xml.Name `xml:"plist"`
	})); err != nil {
		return nil, fmt.Errorf("rendered plist is not valid XML: %w", err)
	}
	return buf.Bytes(), nil
}
