package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/georgenijo/usher/internal/keychain"
)

// TestLockDurations: configured second-counts convert to Durations; an unset or
// non-positive value reports zero so the broker applies its built-in default.
func TestLockDurations(t *testing.T) {
	cases := []struct {
		name     string
		ttlSec   int
		waitSec  int
		wantTTL  time.Duration
		wantWait time.Duration
	}{
		{"unset falls back to zero", 0, 0, 0, 0},
		{"custom values honored", 45, 3, 45 * time.Second, 3 * time.Second},
		{"negative treated as unset", -5, -1, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Config{LockTTLSeconds: tc.ttlSec, LockWaitSeconds: tc.waitSec}
			if got := c.LockTTL(); got != tc.wantTTL {
				t.Errorf("LockTTL() = %v, want %v", got, tc.wantTTL)
			}
			if got := c.LockWait(); got != tc.wantWait {
				t.Errorf("LockWait() = %v, want %v", got, tc.wantWait)
			}
		})
	}
}

// TestStateDirPaths asserts SocketPath and PidPath live inside the state dir
// with the expected filenames, and that the USHER_STATE_DIR override is honored
// (so tests and isolated runs never touch the real ~/.usher).
func TestStateDirPaths(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)

	if got, want := StateDir(), dir; got != want {
		t.Fatalf("StateDir() = %q, want %q", got, want)
	}
	if got, want := SocketPath(), filepath.Join(dir, "usher.sock"); got != want {
		t.Errorf("SocketPath() = %q, want %q", got, want)
	}
	if got, want := PidPath(), filepath.Join(dir, "usher.pid"); got != want {
		t.Errorf("PidPath() = %q, want %q", got, want)
	}
	if got, want := DefaultPath(), filepath.Join(dir, "config.json"); got != want {
		t.Errorf("DefaultPath() = %q, want %q", got, want)
	}
}

// TestEnvForBackend covers the auth-strategy → env-additions mapping. The env
// strategy is exercised with an in-memory keychainGet so the test never touches
// the real Keychain.
func TestEnvForBackend(t *testing.T) {
	// Swap in a fake Keychain store for the duration of this test.
	store := map[string]string{ // key = name + "\x00" + account
		"db\x00ANTHROPIC_API_KEY": "sk-test",
		"db\x00OPENAI_API_KEY":    "sk-openai",
	}
	orig := keychainGet
	keychainGet = func(name, account string) (string, error) {
		if v, ok := store[name+"\x00"+account]; ok {
			return v, nil
		}
		return "", keychain.ErrNotFound
	}
	t.Cleanup(func() { keychainGet = orig })

	cases := []struct {
		name    string
		be      Backend
		want    []string
		wantErr bool
	}{
		{"inherit yields nil", Backend{Name: "cua", Auth: "inherit"}, nil, false},
		{"none yields nil", Backend{Name: "x", Auth: "none"}, nil, false},
		{"empty auth yields nil (legacy config)", Backend{Name: "x", Auth: ""}, nil, false},
		{
			"env injects keychain secrets",
			Backend{Name: "db", Auth: "env", EnvKeys: []string{"ANTHROPIC_API_KEY", "OPENAI_API_KEY"}},
			[]string{"ANTHROPIC_API_KEY=sk-test", "OPENAI_API_KEY=sk-openai"},
			false,
		},
		{
			"env with no keys yields empty (not nil) slice",
			Backend{Name: "db", Auth: "env"},
			[]string{},
			false,
		},
		{
			"env missing key errors",
			Backend{Name: "db", Auth: "env", EnvKeys: []string{"NOPE"}},
			nil, true,
		},
		{"oauth is reserved", Backend{Name: "x", Auth: "oauth"}, nil, true},
		{"unknown auth errors", Backend{Name: "x", Auth: "weird"}, nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := EnvForBackend(&tc.be)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("EnvForBackend(%+v) = %v, want error", tc.be, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("EnvForBackend(%+v) unexpected error: %v", tc.be, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("EnvForBackend(%+v) = %v, want %v", tc.be, got, tc.want)
			}
		})
	}
}

// TestConfigRoundTrip: a backend carrying EnvKeys survives a Save/Load cycle and
// EnvKeys is preserved, while no secret value is ever serialized (only names).
func TestConfigRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	// The sentinel secret value: it is the kind of string that must NEVER reach
	// config.json. We assert below that it does not appear in the raw bytes.
	const secretValue = "sk-test"

	in := &Config{
		Backends: []Backend{
			{Name: "cua", Transport: "stdio", Command: []string{"cua-driver", "mcp"}, Auth: "inherit", Default: true},
			{Name: "db", Transport: "stdio", Command: []string{"db-mcp"}, Auth: "env", EnvKeys: []string{"ANTHROPIC_API_KEY"}},
		},
		TrimThreshold: 2048,
		UIAddr:        "127.0.0.1:9000",
		UIOff:         true,
	}
	if err := in.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}
	out, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round-trip mismatch:\n in = %+v\nout = %+v", in, out)
	}

	db := out.ResolveBackend("db")
	if db == nil {
		t.Fatal("ResolveBackend(db) = nil")
	}
	if !reflect.DeepEqual(db.EnvKeys, []string{"ANTHROPIC_API_KEY"}) {
		t.Errorf("EnvKeys = %v, want [ANTHROPIC_API_KEY]", db.EnvKeys)
	}

	// Read the raw bytes and assert the secret VALUE never landed on disk. The
	// Config struct has no field that holds a secret, but this explicit check
	// catches a future regression that accidentally serialized one. The key NAME
	// is expected to be present; the value is not.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if strings.Contains(string(raw), secretValue) {
		t.Errorf("config.json contains secret value %q; secrets must live only in the Keychain:\n%s", secretValue, raw)
	}
	if !strings.Contains(string(raw), "ANTHROPIC_API_KEY") {
		t.Errorf("config.json missing the env key NAME; envKeys should be serialized:\n%s", raw)
	}
}

// TestBackendDisabledRoundTrip: the Disabled flag survives a Save/Load cycle when
// true, and is OMITTED from the on-disk bytes when false (omitempty) so existing
// configs that never set it are byte-for-byte unchanged.
func TestBackendDisabledRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	in := &Config{
		Backends: []Backend{
			{Name: "cua", Transport: "stdio", Command: []string{"cua-driver", "mcp"}, Auth: "inherit", Default: true},
			{Name: "fs", Transport: "stdio", Command: []string{"fs-mcp"}, Auth: "none", Disabled: true},
		},
	}
	if err := in.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}
	out, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round-trip mismatch:\n in = %+v\nout = %+v", in, out)
	}

	fs := out.ResolveBackend("fs")
	if fs == nil || !fs.Disabled {
		t.Errorf("fs.Disabled = %v, want true", fs)
	}

	// The enabled backend ("cua", Disabled=false) must NOT serialize a "disabled"
	// key — omitempty keeps pre-existing configs unchanged on disk.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(raw), `"disabled": true`) {
		t.Errorf("config.json missing the disabled flag for fs:\n%s", raw)
	}
	if strings.Count(string(raw), "disabled") != 1 {
		t.Errorf("config.json has more than one \"disabled\" key; false should be omitted:\n%s", raw)
	}
}
