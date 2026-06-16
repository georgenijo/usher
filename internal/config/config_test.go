package config

import (
	"path/filepath"
	"reflect"
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

	in := &Config{
		Backends: []Backend{
			{Name: "cua", Transport: "stdio", Command: []string{"cua-driver", "mcp"}, Auth: "inherit", Default: true},
			{Name: "db", Transport: "stdio", Command: []string{"db-mcp"}, Auth: "env", EnvKeys: []string{"ANTHROPIC_API_KEY"}},
		},
		TrimThreshold: 2048,
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
}
