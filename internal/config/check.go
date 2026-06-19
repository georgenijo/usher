// check.go validates a usher config.json without starting the daemon, so an
// operator can sanity-check a hand-edited file before launching the broker. The
// logic lives here (not in cmd) so it is unit-testable; `usher config check`
// keeps only the thin wiring that loads the file and prints the report.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/georgenijo/usher/internal/keychain"
)

// Severity classifies a finding. Only Error fails the check; a Warning is
// advisory (e.g. an unknown key, a secret missing from the Keychain).
type Severity int

const (
	// Warning is advisory: the config is usable but something looks off.
	Warning Severity = iota
	// Error means the config is invalid; `usher config check` exits non-zero.
	Error
)

func (s Severity) String() string {
	if s == Error {
		return "ERROR"
	}
	return "WARN"
}

// Finding is one problem the validator reports. Where points at the offending
// location (e.g. `backends[0] "cua"`) so a long config is navigable.
type Finding struct {
	Severity Severity
	Where    string
	Msg      string
}

// CheckResult is the outcome of validating a config: the findings in report
// order plus a convenience flag.
type CheckResult struct {
	Findings []Finding
}

// add appends a finding.
func (r *CheckResult) add(sev Severity, where, format string, args ...any) {
	r.Findings = append(r.Findings, Finding{Severity: sev, Where: where, Msg: fmt.Sprintf(format, args...)})
}

// HasError reports whether any finding is an Error (so the caller exits
// non-zero). Warnings never set this.
func (r *CheckResult) HasError() bool {
	for _, f := range r.Findings {
		if f.Severity == Error {
			return true
		}
	}
	return false
}

// validTransports / validAuths mirror the accepted values that config.go and
// `usher backend add` enforce. Keep them in sync with EnvForBackend's switch and
// backendAdd's transport/auth validation.
var validTransports = map[string]bool{"stdio": true, "http": true}
var validAuths = map[string]bool{"none": true, "env": true, "inherit": true, "oauth": true}

// knownTopLevelKeys is the set of recognized config.json top-level keys, matching
// the json tags on Config. An unrecognized key is a Warning (likely a typo) — it
// is silently dropped by encoding/json, so flagging it is the only signal an
// operator gets that a setting is not taking effect.
var knownTopLevelKeys = map[string]bool{
	"backends":        true,
	"trimThreshold":   true,
	"lockTtlSeconds":  true,
	"lockWaitSeconds": true,
	"blockedTools":    true,
	"allowedTools":    true,
	"uiAddr":          true,
	"uiOff":           true,
}

// CheckFile reads the config at path and validates it, returning the findings.
// A file that does not exist is reported (an Error) rather than treated as the
// built-in default — `config check` is about the on-disk file, not the fallback.
// A parse failure is a single fatal Error (no point validating gibberish).
func CheckFile(path string) (*CheckResult, error) {
	r := &CheckResult{}
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		r.add(Error, path, "config file does not exist (the broker would use the built-in default)")
		return r, nil
	}
	if err != nil {
		return nil, err
	}
	return CheckBytes(b), nil
}

// CheckBytes validates raw config.json bytes. It is the unit-testable core:
// CheckFile is the thin I/O wrapper around it.
func CheckBytes(b []byte) *CheckResult {
	r := &CheckResult{}

	// JSON must parse at all. A malformed file is fatal — return immediately so we
	// don't emit a wall of spurious findings off a zero-value Config.
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		r.add(Error, "config", "invalid JSON: %v", err)
		return r
	}

	// Warn on unknown top-level keys: encoding/json drops them silently, so a
	// typo like "blockedTool" would otherwise take effect as nothing.
	checkUnknownKeys(r, b)

	checkBackends(r, &c)
	checkToolLists(r, &c)

	return r
}

// checkUnknownKeys decodes into a generic map to find top-level keys that are not
// recognized Config fields. It only inspects the top level (matching CLAUDE.md's
// "unknown top-level keys" requirement); nested unknowns are out of scope.
func checkUnknownKeys(r *CheckResult, b []byte) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		// Not an object (e.g. a JSON array/scalar). Unmarshal into Config above
		// already flagged or accepted it; nothing more to do here.
		return
	}
	// Sort for deterministic report order.
	keys := make([]string, 0, len(raw))
	for k := range raw {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !knownTopLevelKeys[k] {
			r.add(Warning, "config", "unknown top-level key %q (ignored by usher — check for a typo)", k)
		}
	}
}

// checkBackends validates each backend's transport/auth/command and, for
// auth=env, that the named EnvKeys resolve in the Keychain.
func checkBackends(r *CheckResult, c *Config) {
	if len(c.Backends) == 0 {
		r.add(Warning, "backends", "no backends configured")
		return
	}
	seen := map[string]bool{}
	for i := range c.Backends {
		be := &c.Backends[i]
		where := fmt.Sprintf("backends[%d] %q", i, be.Name)

		if be.Name == "" {
			r.add(Error, where, "backend has no name")
		} else if seen[be.Name] {
			r.add(Error, where, "duplicate backend name %q", be.Name)
		}
		seen[be.Name] = true

		if !validTransports[be.Transport] {
			r.add(Error, where, "invalid transport %q (want stdio|http)", be.Transport)
		}
		if !validAuths[be.Auth] {
			r.add(Error, where, "invalid auth %q (want none|env|inherit|oauth)", be.Auth)
		}

		// stdio backends must carry a command to spawn.
		if be.Transport == "stdio" && len(be.Command) == 0 {
			r.add(Error, where, "stdio backend has no command")
		}

		checkBackendEnv(r, be, where)
	}
}

// checkBackendEnv handles the auth=env keychain lookups and cross-field rules.
func checkBackendEnv(r *CheckResult, be *Backend, where string) {
	switch be.Auth {
	case "env":
		if len(be.EnvKeys) == 0 {
			r.add(Error, where, "auth=env but no envKeys declared")
		}
		// Warn (not error) on a secret missing from the Keychain: the key may be
		// added later (the same pre-install state backendAdd tolerates), so failing
		// the whole check would be too strict.
		for _, k := range be.EnvKeys {
			_, err := keychainGet(be.Name, k)
			if errors.Is(err, keychain.ErrNotFound) {
				r.add(Warning, where, "envKey %q not found in Keychain (run: usher backend add %s --auth env --env %s -- ...)", k, be.Name, k)
			} else if err != nil {
				r.add(Warning, where, "envKey %q: keychain lookup failed: %v", k, err)
			}
		}
	default:
		// EnvKeys only make sense with auth=env; flag a stray declaration.
		if len(be.EnvKeys) > 0 {
			r.add(Warning, where, "envKeys declared but auth=%q (only auth=env reads them)", be.Auth)
		}
	}
}

// checkToolLists enforces that blockedTools/allowedTools hold only BARE tool
// names. A namespaced name ("backend__tool") never matches the gate's bare-name
// classification (see CLAUDE.md "Tool-name namespacing"), so it is silently inert
// — an ERROR, because the operator's intent (block/allow a tool) is not honored.
func checkToolLists(r *CheckResult, c *Config) {
	checkBareNames(r, "blockedTools", c.BlockedTools)
	checkBareNames(r, "allowedTools", c.AllowedTools)
}

func checkBareNames(r *CheckResult, field string, names []string) {
	for i, n := range names {
		where := fmt.Sprintf("%s[%d]", field, i)
		if strings.Contains(n, "__") {
			r.add(Error, where, "namespaced tool name %q — block/allow lists MUST use BARE names (e.g. %q)", n, bareName(n))
		}
		if strings.TrimSpace(n) == "" {
			r.add(Warning, where, "empty tool name")
		}
	}
}

// bareName strips a leading "<backend>__" namespace so the error message can
// suggest the correct bare form.
func bareName(n string) string {
	if i := strings.Index(n, "__"); i >= 0 {
		return n[i+2:]
	}
	return n
}

// Report renders the findings as a human-readable report and returns whether the
// check passed (no errors). It writes to w so cmd wiring stays trivial.
func (r *CheckResult) Report(w *strings.Builder) {
	var errs, warns int
	for _, f := range r.Findings {
		if f.Severity == Error {
			errs++
		} else {
			warns++
		}
		fmt.Fprintf(w, "  %-5s %s: %s\n", f.Severity, f.Where, f.Msg)
	}
	if errs == 0 && warns == 0 {
		w.WriteString("config OK: no problems found\n")
		return
	}
	fmt.Fprintf(w, "\n%d error(s), %d warning(s)\n", errs, warns)
}
