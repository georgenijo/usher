// Package config defines usher's on-disk configuration: the set of registered
// backends and audit settings. The skeleton uses JSON to stay stdlib-only and
// offline-buildable; TOML is a candidate once `usher backend add` is the only
// writer and humans rarely hand-edit (tracked with the #32 registration path).
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Backend is one registered MCP server behind the broker.
type Backend struct {
	Name      string   `json:"name"`
	Transport string   `json:"transport"`         // "stdio" (http: future)
	Command   []string `json:"command,omitempty"` // argv for stdio transport
	Auth      string   `json:"auth"`              // none|env|inherit|oauth (#32)
	Default   bool     `json:"default,omitempty"`
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
}

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
