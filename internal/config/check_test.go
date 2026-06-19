package config

import (
	"strings"
	"testing"

	"github.com/georgenijo/usher/internal/keychain"
)

// stubKeychain replaces keychainGet with an in-memory map for the duration of a
// test, so the validator's auth=env lookups never touch the real Keychain. A key
// present in have resolves; anything else reports ErrNotFound.
func stubKeychain(t *testing.T, have map[string]bool) {
	t.Helper()
	orig := keychainGet
	keychainGet = func(name, account string) (string, error) {
		if have[name+"/"+account] {
			return "secret", nil
		}
		return "", keychain.ErrNotFound
	}
	t.Cleanup(func() { keychainGet = orig })
}

// findingsBySeverity counts errors and warnings in a result.
func findingsBySeverity(r *CheckResult) (errs, warns int) {
	for _, f := range r.Findings {
		if f.Severity == Error {
			errs++
		} else {
			warns++
		}
	}
	return
}

// hasFinding reports whether any finding's message contains substr.
func hasFinding(r *CheckResult, substr string) bool {
	for _, f := range r.Findings {
		if strings.Contains(f.Msg, substr) {
			return true
		}
	}
	return false
}

// TestCheckBytes_GoodConfig: a well-formed config with every auth=env secret
// present in the Keychain produces no errors (warnings allowed to be zero too).
func TestCheckBytes_GoodConfig(t *testing.T) {
	stubKeychain(t, map[string]bool{"gh/GITHUB_TOKEN": true})
	good := `{
	  "backends": [
	    {"name": "cua", "transport": "stdio", "command": ["cua-driver", "mcp"], "auth": "inherit", "default": true},
	    {"name": "gh", "transport": "http", "auth": "env", "envKeys": ["GITHUB_TOKEN"]}
	  ],
	  "blockedTools": ["kill_app", "drag"],
	  "allowedTools": ["screenshot"],
	  "uiAddr": "127.0.0.1:7187"
	}`
	r := CheckBytes([]byte(good))
	if r.HasError() {
		t.Fatalf("good config flagged errors: %+v", r.Findings)
	}
	if errs, _ := findingsBySeverity(r); errs != 0 {
		t.Fatalf("expected 0 errors, got %d: %+v", errs, r.Findings)
	}
}

// TestCheckBytes_BadTransport: an unrecognized transport is an ERROR.
func TestCheckBytes_BadTransport(t *testing.T) {
	stubKeychain(t, nil)
	cfg := `{"backends":[{"name":"x","transport":"grpc","auth":"inherit","command":["x"]}]}`
	r := CheckBytes([]byte(cfg))
	if !r.HasError() {
		t.Fatal("bad transport should error")
	}
	if !hasFinding(r, "invalid transport") {
		t.Fatalf("missing transport finding: %+v", r.Findings)
	}
}

// TestCheckBytes_BadAuth: an unrecognized auth strategy is an ERROR.
func TestCheckBytes_BadAuth(t *testing.T) {
	stubKeychain(t, nil)
	cfg := `{"backends":[{"name":"x","transport":"stdio","auth":"magic","command":["x"]}]}`
	r := CheckBytes([]byte(cfg))
	if !r.HasError() {
		t.Fatal("bad auth should error")
	}
	if !hasFinding(r, "invalid auth") {
		t.Fatalf("missing auth finding: %+v", r.Findings)
	}
}

// TestCheckBytes_StdioMissingCommand: a stdio backend with no command is an ERROR.
func TestCheckBytes_StdioMissingCommand(t *testing.T) {
	stubKeychain(t, nil)
	cfg := `{"backends":[{"name":"x","transport":"stdio","auth":"inherit"}]}`
	r := CheckBytes([]byte(cfg))
	if !r.HasError() {
		t.Fatal("stdio with no command should error")
	}
	if !hasFinding(r, "no command") {
		t.Fatalf("missing command finding: %+v", r.Findings)
	}
}

// TestCheckBytes_NamespacedBlockEntry: a namespaced name in blockedTools or
// allowedTools is an ERROR (per CLAUDE.md these MUST be bare).
func TestCheckBytes_NamespacedBlockEntry(t *testing.T) {
	stubKeychain(t, nil)
	cfg := `{"backends":[{"name":"x","transport":"stdio","auth":"inherit","command":["x"]},
	  {"name":"y","transport":"stdio","auth":"inherit","command":["y"]}],
	  "blockedTools":["cua__kill_app"],
	  "allowedTools":["fs__delete"]}`
	r := CheckBytes([]byte(cfg))
	if !r.HasError() {
		t.Fatal("namespaced block/allow entry should error")
	}
	if !hasFinding(r, "kill_app") || !hasFinding(r, "delete") {
		t.Fatalf("expected bare-name suggestions for both lists: %+v", r.Findings)
	}
	if errs, _ := findingsBySeverity(r); errs != 2 {
		t.Fatalf("expected 2 namespaced errors, got %d: %+v", errs, r.Findings)
	}
}

// TestCheckBytes_MissingKeychainKey: an auth=env backend whose secret is absent
// from the Keychain is a WARNING, not an error (it may be installed later).
func TestCheckBytes_MissingKeychainKey(t *testing.T) {
	stubKeychain(t, nil) // nothing in the keychain
	cfg := `{"backends":[{"name":"gh","transport":"http","auth":"env","envKeys":["GITHUB_TOKEN"]}]}`
	r := CheckBytes([]byte(cfg))
	if r.HasError() {
		t.Fatalf("missing keychain key should warn, not error: %+v", r.Findings)
	}
	_, warns := findingsBySeverity(r)
	if warns == 0 || !hasFinding(r, "not found in Keychain") {
		t.Fatalf("expected a missing-keychain warning: %+v", r.Findings)
	}
}

// TestCheckBytes_UnknownTopLevelKey: an unrecognized top-level key warns.
func TestCheckBytes_UnknownTopLevelKey(t *testing.T) {
	stubKeychain(t, nil)
	cfg := `{"backends":[{"name":"x","transport":"stdio","auth":"inherit","command":["x"]}],
	  "blockedTool":["typo"]}`
	r := CheckBytes([]byte(cfg))
	if r.HasError() {
		t.Fatalf("unknown key should warn, not error: %+v", r.Findings)
	}
	if !hasFinding(r, "unknown top-level key") {
		t.Fatalf("expected unknown-key warning: %+v", r.Findings)
	}
}

// TestCheckBytes_InvalidJSON: malformed JSON is a single fatal error.
func TestCheckBytes_InvalidJSON(t *testing.T) {
	r := CheckBytes([]byte(`{not json`))
	if !r.HasError() {
		t.Fatal("invalid JSON should error")
	}
	if len(r.Findings) != 1 || !hasFinding(r, "invalid JSON") {
		t.Fatalf("expected a single invalid-JSON finding: %+v", r.Findings)
	}
}

// TestCheckBytes_AuthEnvNoKeys: auth=env with no envKeys declared is an ERROR.
func TestCheckBytes_AuthEnvNoKeys(t *testing.T) {
	stubKeychain(t, nil)
	cfg := `{"backends":[{"name":"gh","transport":"http","auth":"env"}]}`
	r := CheckBytes([]byte(cfg))
	if !r.HasError() {
		t.Fatal("auth=env with no envKeys should error")
	}
	if !hasFinding(r, "no envKeys") {
		t.Fatalf("expected no-envKeys finding: %+v", r.Findings)
	}
}

// TestCheckBytes_DuplicateBackend: two backends with the same name is an ERROR.
func TestCheckBytes_DuplicateBackend(t *testing.T) {
	stubKeychain(t, nil)
	cfg := `{"backends":[
	  {"name":"x","transport":"stdio","auth":"inherit","command":["x"]},
	  {"name":"x","transport":"stdio","auth":"inherit","command":["x"]}]}`
	r := CheckBytes([]byte(cfg))
	if !hasFinding(r, "duplicate backend name") {
		t.Fatalf("expected duplicate-name error: %+v", r.Findings)
	}
}

// TestReport: the rendered report shows OK for a clean config and a count
// summary otherwise.
func TestReport(t *testing.T) {
	stubKeychain(t, nil)
	clean := CheckBytes([]byte(`{"backends":[{"name":"x","transport":"stdio","auth":"inherit","command":["x"]}]}`))
	var sb strings.Builder
	clean.Report(&sb)
	if !strings.Contains(sb.String(), "config OK") {
		t.Fatalf("clean report should say OK: %q", sb.String())
	}

	bad := CheckBytes([]byte(`{"backends":[{"name":"x","transport":"grpc","auth":"inherit","command":["x"]}]}`))
	sb.Reset()
	bad.Report(&sb)
	if !strings.Contains(sb.String(), "error(s)") {
		t.Fatalf("bad report should summarize errors: %q", sb.String())
	}
}
