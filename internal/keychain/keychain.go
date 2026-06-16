// Package keychain stores and retrieves usher's backend secrets in the macOS
// login Keychain via /usr/bin/security. It is the single chokepoint for the
// security CLI so the rest of usher never shells out to it directly and tests
// can gate the (Keychain-touching) round-trip behind an env var.
//
// Coordinates: every secret is one generic-password item keyed by
//
//	service = "usher.<backend-name>"   (e.g. "usher.cua")
//	account = <env-var name>           (e.g. "ANTHROPIC_API_KEY")
//
// so one backend can hold several secrets (one item per env var), matching the
// security CLI's -s (service) / -a (account) model.
package keychain

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

// securityBin is the macOS security CLI. Hardcoded (no exec.LookPath): it always
// lives here on macOS and pinning the path avoids picking up a shadowing binary.
const securityBin = "/usr/bin/security"

// errSecItemNotFound is the security(1) exit status when the requested item is
// absent (errSecItemNotFound, -25300, surfaced by the CLI as exit code 44).
const errSecItemNotFound = 44

// ErrNotFound is returned by Get when no Keychain item exists for the
// backend/account pair. It is a sentinel so callers can errors.Is against it
// instead of matching the security CLI's stderr text.
var ErrNotFound = errors.New("keychain: item not found")

// service builds the Keychain service name for a backend.
func service(backend string) string { return "usher." + backend }

// Set stores (or updates) the secret for the named backend under account (the
// env-var name). It runs:
//
//	security add-generic-password -U -s usher.<backend> -a <account> -w <secret>
//
// -U updates the item in place when it already exists, so re-running
// `usher backend add` rotates the secret rather than failing.
//
// NOTE: the secret is passed as the -w argument and is therefore briefly visible
// in ps(1) while security runs. This is the documented trade-off for scripted
// use of the security CLI; the alternative (CGo into Security.framework) is
// excluded by usher's stdlib-only ethos.
func Set(backend, account, secret string) error {
	cmd := exec.Command(securityBin, "add-generic-password",
		"-U",
		"-s", service(backend),
		"-a", account,
		"-w", secret,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("keychain set %s/%s: %w: %s", backend, account, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// Get retrieves the secret for the backend/account pair. It runs:
//
//	security find-generic-password -s usher.<backend> -a <account> -w
//
// On success the password is on stdout with no trailing newline. When the item
// is absent (exit code 44) it returns ErrNotFound.
func Get(backend, account string) (string, error) {
	cmd := exec.Command(securityBin, "find-generic-password",
		"-s", service(backend),
		"-a", account,
		"-w",
	)
	out, err := cmd.Output()
	if err != nil {
		if exitCode(err) == errSecItemNotFound {
			return "", ErrNotFound
		}
		return "", fmt.Errorf("keychain get %s/%s: %w", backend, account, err)
	}
	return strings.TrimRight(string(out), "\n"), nil
}

// Delete removes the Keychain item for the backend/account pair. It is
// idempotent: a missing item (exit code 44) is not an error. It runs:
//
//	security delete-generic-password -s usher.<backend> -a <account>
func Delete(backend, account string) error {
	cmd := exec.Command(securityBin, "delete-generic-password",
		"-s", service(backend),
		"-a", account,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		if exitCode(err) == errSecItemNotFound {
			return nil
		}
		return fmt.Errorf("keychain delete %s/%s: %w: %s", backend, account, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// exitCode extracts the process exit code from an exec error, or -1 when the
// error is not an *exec.ExitError (e.g. the binary could not be launched).
func exitCode(err error) int {
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return -1
}
