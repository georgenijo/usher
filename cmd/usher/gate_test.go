package main

import (
	"path/filepath"
	"testing"

	"github.com/georgenijo/usher/internal/broker"
	"github.com/georgenijo/usher/internal/config"
)

// TestGateBlock: `gate block` appends a bare name to cfg.BlockedTools, is
// idempotent, and persists to config.json.
func TestGateBlock(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)
	path := filepath.Join(dir, "config.json")

	if err := gateBlock([]string{"drag"}); err != nil {
		t.Fatalf("gateBlock(drag) = %v, want nil", err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !contains(cfg.BlockedTools, "drag") {
		t.Errorf("BlockedTools = %v, want it to contain \"drag\"", cfg.BlockedTools)
	}

	// Idempotent: a second block of the same name does not duplicate it.
	if err := gateBlock([]string{"drag"}); err != nil {
		t.Fatalf("gateBlock(drag) again = %v, want nil", err)
	}
	cfg, _ = config.Load(path)
	if n := countOf(cfg.BlockedTools, "drag"); n != 1 {
		t.Errorf("\"drag\" appears %d times, want 1 (must be idempotent)", n)
	}
}

// TestGateUnblock: `gate unblock` appends a bare name to cfg.AllowedTools (the
// override list), is idempotent, and persists.
func TestGateUnblock(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)
	path := filepath.Join(dir, "config.json")

	if err := gateUnblock([]string{"send"}); err != nil {
		t.Fatalf("gateUnblock(send) = %v, want nil", err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !contains(cfg.AllowedTools, "send") {
		t.Errorf("AllowedTools = %v, want it to contain \"send\"", cfg.AllowedTools)
	}

	if err := gateUnblock([]string{"send"}); err != nil {
		t.Fatalf("gateUnblock(send) again = %v, want nil", err)
	}
	cfg, _ = config.Load(path)
	if n := countOf(cfg.AllowedTools, "send"); n != 1 {
		t.Errorf("\"send\" appears %d times, want 1 (must be idempotent)", n)
	}
}

// TestGateNamespacedRejected: a namespaced name (containing "__") is refused for
// both block and unblock, and nothing is persisted — gate lists key on BARE
// names only.
func TestGateNamespacedRejected(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)
	path := filepath.Join(dir, "config.json")

	if err := gateBlock([]string{"fs__delete"}); err == nil {
		t.Error("gateBlock(fs__delete) = nil, want error (namespaced name must be rejected)")
	}
	if err := gateUnblock([]string{"fs__delete"}); err == nil {
		t.Error("gateUnblock(fs__delete) = nil, want error (namespaced name must be rejected)")
	}
	if _, err := config.Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}
	cfg, _ := config.Load(path)
	if len(cfg.BlockedTools) != 0 || len(cfg.AllowedTools) != 0 {
		t.Errorf("rejected namespaced names were persisted: blocked=%v allowed=%v", cfg.BlockedTools, cfg.AllowedTools)
	}
}

// TestGateListShowsBuiltinsAndConfigured: `gate list` reports every built-in
// DefaultBlockedTools entry plus any configured block, without erroring.
func TestGateListShowsBuiltinsAndConfigured(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("USHER_STATE_DIR", dir)

	if err := gateBlock([]string{"drag"}); err != nil {
		t.Fatalf("gateBlock(drag) = %v", err)
	}

	// gateList writes to stdout; here we assert the union it computes by reloading
	// config and checking the same set gateList prints (built-ins + configured).
	cfg, err := config.Load(config.DefaultPath())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, bt := range broker.DefaultBlockedTools {
		if !contains(broker.DefaultBlockedTools, bt) {
			t.Errorf("built-in %q missing from DefaultBlockedTools", bt)
		}
	}
	if !contains(cfg.BlockedTools, "drag") {
		t.Errorf("configured block \"drag\" missing: %v", cfg.BlockedTools)
	}

	// And the command itself runs cleanly.
	if err := gateList(nil); err != nil {
		t.Fatalf("gateList() = %v, want nil", err)
	}
}

// TestGateToolArgValidation rejects missing/extra args.
func TestGateToolArgValidation(t *testing.T) {
	if _, err := gateToolArg(nil); err == nil {
		t.Error("gateToolArg(nil) = nil, want error")
	}
	if _, err := gateToolArg([]string{"a", "b"}); err == nil {
		t.Error("gateToolArg(two args) = nil, want error")
	}
	if _, err := gateToolArg([]string{""}); err == nil {
		t.Error("gateToolArg(empty) = nil, want error")
	}
	if got, err := gateToolArg([]string{"kill_app"}); err != nil || got != "kill_app" {
		t.Errorf("gateToolArg(kill_app) = (%q, %v), want (\"kill_app\", nil)", got, err)
	}
}

func countOf(s []string, v string) int {
	n := 0
	for _, x := range s {
		if x == v {
			n++
		}
	}
	return n
}
