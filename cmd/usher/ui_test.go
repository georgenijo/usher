package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/control"
)

// TestUIAddrPrecedence pins the resolution order for the control-plane listen
// address: the --ui-port flag wins, then USHER_UI_ADDR, then config.UIAddr, then
// the built-in default. A flag port always renders on 127.0.0.1 so it can never
// bind a routable host.
func TestUIAddrPrecedence(t *testing.T) {
	cases := []struct {
		name   string
		cfg    *config.Config
		port   int
		env    string // value for USHER_UI_ADDR; "" leaves it unset
		want   string
		setEnv bool
	}{
		{name: "default when nothing set", cfg: &config.Config{}, want: control.DefaultAddr},
		{name: "nil config falls back to default", cfg: nil, want: control.DefaultAddr},
		{name: "config addr honored", cfg: &config.Config{UIAddr: "127.0.0.1:9001"}, want: "127.0.0.1:9001"},
		{name: "env overrides config", cfg: &config.Config{UIAddr: "127.0.0.1:9001"}, env: "127.0.0.1:9002", setEnv: true, want: "127.0.0.1:9002"},
		{name: "flag port wins over env and config", cfg: &config.Config{UIAddr: "127.0.0.1:9001"}, env: "127.0.0.1:9002", setEnv: true, port: 9003, want: "127.0.0.1:9003"},
		{name: "flag port wins over config alone", cfg: &config.Config{UIAddr: "127.0.0.1:9001"}, port: 9004, want: "127.0.0.1:9004"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setEnv {
				t.Setenv(control.EnvAddr, tc.env)
			} else {
				// Ensure a leaked real env var doesn't perturb the default cases.
				t.Setenv(control.EnvAddr, "")
			}
			if got := uiAddr(tc.cfg, tc.port); got != tc.want {
				t.Errorf("uiAddr(%+v, %d) = %q, want %q", tc.cfg, tc.port, got, tc.want)
			}
		})
	}
}

// TestUIDisabled covers the off switch: the --ui-off flag forces the UI off even
// when config leaves it on, and config.UIOff disables it as the persistent
// default. A nil config is treated as "not disabled".
func TestUIDisabled(t *testing.T) {
	cases := []struct {
		name    string
		cfg     *config.Config
		flagOff bool
		want    bool
	}{
		{"nil config not disabled", nil, false, false},
		{"default not disabled", &config.Config{}, false, false},
		{"flag forces off", &config.Config{}, true, true},
		{"config off honored", &config.Config{UIOff: true}, false, true},
		{"flag off over config on", &config.Config{UIOff: false}, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := uiDisabled(tc.cfg, tc.flagOff); got != tc.want {
				t.Errorf("uiDisabled(%+v, %v) = %v, want %v", tc.cfg, tc.flagOff, got, tc.want)
			}
		})
	}
}

// TestUIURL asserts the dashboard URL is the http:// loopback form, defaulting
// to the built-in address and honoring a config override.
func TestUIURL(t *testing.T) {
	// Isolate the state dir so uiURL can't read a real ~/.usher/usher.ui left by a
	// running daemon, which would shadow the config value this test asserts.
	t.Setenv("USHER_STATE_DIR", t.TempDir())
	t.Setenv(control.EnvAddr, "")
	if got, want := uiURL(&config.Config{}), "http://"+control.DefaultAddr; got != want {
		t.Errorf("uiURL(default) = %q, want %q", got, want)
	}
	if got, want := uiURL(&config.Config{UIAddr: "127.0.0.1:9099"}), "http://127.0.0.1:9099"; got != want {
		t.Errorf("uiURL(custom) = %q, want %q", got, want)
	}
}

// TestCmdUIDisabledRefuses: when the UI is disabled in config, `usher ui` must
// refuse with a clear error rather than open a browser at an address nothing is
// serving.
func TestCmdUIDisabledRefuses(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)
	cfg := &config.Config{Backends: config.Default().Backends, UIOff: true}
	if err := cfg.Save(filepath.Join(dir, "config.json")); err != nil {
		t.Fatal(err)
	}
	err := cmdUI(nil)
	if err == nil {
		t.Fatal("cmdUI with uiOff=true = nil, want a refusal error")
	}
	if !strings.Contains(err.Error(), "disabled") {
		t.Errorf("cmdUI error = %q, want it to mention the UI is disabled", err)
	}
}

// TestCmdStatusRunningPrintsUI: a running daemon reports the dashboard URL it
// recorded in the state dir (config.UIURLPath), and reports ui=off when no URL
// file is present (the daemon ran with --ui-off / the UI failed to bind).
func TestCmdStatusRunningPrintsUI(t *testing.T) {
	// UI on: the daemon wrote its bound URL; status echoes it exactly (including a
	// non-default port, proving status reflects the runtime address not config).
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)
	if err := writePID(config.PidPath(), os.Getpid()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config.UIURLPath(), []byte("http://127.0.0.1:7191\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	out := captureStdout(t, func() {
		if err := cmdStatus(nil); err != nil {
			t.Errorf("cmdStatus = %v", err)
		}
	})
	if !strings.Contains(out, "ui=http://127.0.0.1:7191") {
		t.Errorf("status output = %q, want it to echo the recorded UI URL", out)
	}

	// UI off: no URL file → status prints ui=off even though the daemon is running.
	dir2 := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir2)
	if err := writePID(config.PidPath(), os.Getpid()); err != nil {
		t.Fatal(err)
	}
	out = captureStdout(t, func() {
		if err := cmdStatus(nil); err != nil {
			t.Errorf("cmdStatus = %v", err)
		}
	})
	if !strings.Contains(out, "ui=off") {
		t.Errorf("status output (no UI file) = %q, want it to contain ui=off", out)
	}
}
