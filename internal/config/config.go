// Package config defines usher's on-disk configuration: the set of registered
// backends and audit settings. The skeleton uses JSON to stay stdlib-only and
// offline-buildable; TOML is a candidate once `usher backend add` is the only
// writer and humans rarely hand-edit (tracked with the #32 registration path).
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/georgenijo/usher/internal/keychain"
)

// Backend is one registered MCP server behind the broker.
type Backend struct {
	Name      string   `json:"name"`
	Transport string   `json:"transport"`         // stdio|http (http: validated-but-stubbed, #32)
	Command   []string `json:"command,omitempty"` // argv for stdio transport
	Auth      string   `json:"auth"`              // none|env|inherit|oauth (#32)

	// EnvKeys are the names of the environment variables this backend's secrets
	// occupy (auth=env only). The VALUES live exclusively in the macOS Keychain
	// (service "usher.<name>", account = the var name) and are NEVER written to
	// config.json — only the names appear on disk.
	EnvKeys []string `json:"envKeys,omitempty"`

	Default bool `json:"default,omitempty"`

	// Disabled, when true, removes the backend from multi-backend enumeration
	// ("usher serve --all") so it is never spawned, while leaving it registered.
	// This is SELECTION metadata only — it does not touch the pipeline or message
	// handling. omitempty keeps existing configs (and false here) byte-for-byte
	// unchanged on disk.
	Disabled bool `json:"disabled,omitempty"`
}

// Config is the whole usher state file.
type Config struct {
	Backends []Backend `json:"backends"`

	// TrimThreshold is the per-text-item size (in bytes) above which the broker's
	// trim stage compacts an oversized AX-tree digest. Zero (the unset default)
	// means use the broker's built-in DefaultTrimThreshold.
	TrimThreshold int `json:"trimThreshold,omitempty"`

	// LockTTLSeconds is how long a per-window write-lock may be held before the
	// broker reclaims it from a backend that never answered (#16). Zero (unset)
	// means use the broker's built-in default.
	LockTTLSeconds int `json:"lockTtlSeconds,omitempty"`

	// LockWaitSeconds is how long a contended writer waits for a busy window
	// before the broker refuses the call with a JSON-RPC error (#16). Zero
	// (unset) means use the broker's built-in default.
	LockWaitSeconds int `json:"lockWaitSeconds,omitempty"`

	// BlockedTools ADDS to the broker's built-in destructive-tool set: extra
	// BARE tool names the gate refuses (#18). Names are bare (e.g. "kill_app",
	// "drag"), never namespaced. Omitted/empty leaves only the built-in defaults.
	BlockedTools []string `json:"blockedTools,omitempty"`

	// AllowedTools OVERRIDES the gate: a bare tool name here is forwarded even if
	// it is in the built-in or configured block-list (#18). This is the config
	// escape hatch for an operator who has accepted the risk of a destructive
	// tool; the env override USHER_ALLOW_TOOLS adds to this set at serve time.
	AllowedTools []string `json:"allowedTools,omitempty"`

	// UIAddr is the loopback host:port the daemon's control-plane web UI binds
	// (e.g. "127.0.0.1:7187"). Empty leaves the built-in default address. The
	// daemon validates it is loopback before binding, so a routable host fails
	// closed. The --ui-port flag and the USHER_UI_ADDR env var override this at
	// serve time; this is the persistent default.
	UIAddr string `json:"uiAddr,omitempty"`

	// UIOff disables the control-plane web UI entirely: the daemon serves MCP
	// over the socket but never binds the HTTP listener. The --ui-off flag forces
	// this for a single run; this is the persistent default.
	UIOff bool `json:"uiOff,omitempty"`
}

// EnvAllowTools is the environment variable that allow-lists destructive tools
// past the gate at serve time without editing config.json (#18). Its value is a
// comma-separated list of BARE tool names; each is unioned into the policy's
// allow-list, so a matching block is overridden for that run only.
const EnvAllowTools = "USHER_ALLOW_TOOLS"

// EnvSampleResources gates the daemon's per-process resource sampler: when set
// to a truthy value ("1", "true") the socket daemon attributes RSS/CPU per pid
// (broker self, shared backend children, connected clients) and emits a
// ResourceSampleEvent per tick on the bus so the RESOURCES dashboard panel and
// the broker-vs-direct load test can watch the shared-pool flat line. Off by
// default so a normal daemon spawns no extra `ps` calls and is byte-for-byte
// unchanged. The load harness always sets it.
const EnvSampleResources = "USHER_SAMPLE"

// LockTTL is the configured write-lock lease as a Duration, or zero when unset
// (the broker then applies its built-in default). A non-positive value is unset.
func (c *Config) LockTTL() time.Duration {
	if c.LockTTLSeconds <= 0 {
		return 0
	}
	return time.Duration(c.LockTTLSeconds) * time.Second
}

// LockWait is the configured contended-writer wait as a Duration, or zero when
// unset (the broker then applies its built-in default).
func (c *Config) LockWait() time.Duration {
	if c.LockWaitSeconds <= 0 {
		return 0
	}
	return time.Duration(c.LockWaitSeconds) * time.Second
}

// StateDir is usher's single state directory (config, socket, audit log).
// Override with USHER_STATE_DIR for tests and isolated runs.
func StateDir() string {
	if d := os.Getenv("USHER_STATE_DIR"); d != "" {
		return d
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".usher"
	}
	return filepath.Join(home, ".usher")
}

// DefaultPath is the config file location inside the state dir.
func DefaultPath() string { return filepath.Join(StateDir(), "config.json") }

// SocketPath is the Unix-domain socket the always-on daemon listens on (#20).
// It lives inside the single state dir so every lifecycle command, the accept
// loop, and the launchd plist generator agree on one location.
func SocketPath() string { return filepath.Join(StateDir(), "usher.sock") }

// PidPath is the daemon's PID file inside the state dir (#20). Its presence plus
// a process-liveness check is how `usher status` distinguishes running from
// stopped from stale, without a flock dependency.
func PidPath() string { return filepath.Join(StateDir(), "usher.pid") }

// UIURLPath is the file the daemon writes with the dashboard URL it actually
// bound, so `usher status` and `usher ui` (separate processes that cannot see
// the daemon's runtime --ui-port flag) report the live address rather than
// re-deriving it from config. It is removed on clean shutdown alongside the PID
// file; an absent file means the daemon never bound the UI (e.g. --ui-off).
func UIURLPath() string { return filepath.Join(StateDir(), "usher.ui") }

// cuaCommand is the default hands backend: the Cua Driver MCP server.
func cuaCommand() []string {
	home, err := os.UserHomeDir()
	bin := "cua-driver"
	if err == nil {
		bin = filepath.Join(home, ".local", "bin", "cua-driver")
	}
	return []string{bin, "mcp"}
}

// Default is the config used when no file exists yet: GhostHands' hands (Cua)
// as the sole, default backend.
func Default() *Config {
	return &Config{
		Backends: []Backend{{
			Name:      "cua",
			Transport: "stdio",
			Command:   cuaCommand(),
			Auth:      "inherit",
			Default:   true,
		}},
	}
}

// Load reads the config at path, or returns the built-in Default when the file
// does not exist. A malformed file is an error (don't silently mask it).
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return Default(), nil
	}
	if err != nil {
		return nil, err
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &c, nil
}

// Save writes the config to path, creating the state dir if needed.
func (c *Config) Save(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// ResolveBackend returns the backend named name, or the default backend when
// name is empty. Returns nil if nothing matches.
func (c *Config) ResolveBackend(name string) *Backend {
	if name == "" {
		for i := range c.Backends {
			if c.Backends[i].Default {
				return &c.Backends[i]
			}
		}
		if len(c.Backends) > 0 {
			return &c.Backends[0]
		}
		return nil
	}
	for i := range c.Backends {
		if c.Backends[i].Name == name {
			return &c.Backends[i]
		}
	}
	return nil
}

// Add registers a backend, replacing any existing one with the same name. When
// makeDefault is set, the new backend becomes the sole default.
func (c *Config) Add(b Backend, makeDefault bool) {
	if makeDefault {
		for i := range c.Backends {
			c.Backends[i].Default = false
		}
		b.Default = true
	}
	for i := range c.Backends {
		if c.Backends[i].Name == b.Name {
			c.Backends[i] = b
			return
		}
	}
	c.Backends = append(c.Backends, b)
}

// keychainGet is the indirection used by EnvForBackend to read secrets. It is a
// variable so tests can substitute an in-memory store without touching the real
// Keychain; production always uses keychain.Get.
var keychainGet = keychain.Get

// EnvForBackend resolves the KEY=VALUE environment additions a backend's child
// process needs, according to its auth strategy. It is called at SERVE time
// (just before spawning the backend), never at add time.
//
//	none, inherit, "" → nil: the child inherits the parent environment unchanged
//	                         (exec.Cmd.Env=nil already does this). The distinction
//	                         is declarative: "inherit" means the backend relies on
//	                         the parent's env (HOME/PATH/…); "none" means it is
//	                         self-contained. Both inject nothing here.
//	env               → reads each EnvKeys name from the Keychain and returns
//	                    KEY=<secret> pairs; a missing key is a clear error.
//	oauth             → reserved; returns an error (not yet supported).
func EnvForBackend(be *Backend) ([]string, error) {
	switch be.Auth {
	case "", "none", "inherit":
		return nil, nil
	case "env":
		out := make([]string, 0, len(be.EnvKeys))
		for _, k := range be.EnvKeys {
			v, err := keychainGet(be.Name, k)
			if errors.Is(err, keychain.ErrNotFound) {
				return nil, fmt.Errorf("backend %q: secret %q not in Keychain (run: usher backend add %s --auth env --env %s -- ...)", be.Name, k, be.Name, k)
			}
			if err != nil {
				return nil, fmt.Errorf("backend %q: keychain get %q: %w", be.Name, k, err)
			}
			out = append(out, k+"="+v)
		}
		return out, nil
	case "oauth":
		return nil, fmt.Errorf("backend %q: auth=oauth not yet supported", be.Name)
	default:
		return nil, fmt.Errorf("backend %q: unknown auth strategy %q (want none|env|inherit|oauth)", be.Name, be.Auth)
	}
}
