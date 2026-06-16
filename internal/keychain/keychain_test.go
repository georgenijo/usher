package keychain

import (
	"errors"
	"os"
	"strconv"
	"testing"
	"time"
)

// Keychain round-trip tests touch the real login Keychain via the security CLI,
// which requires an unlocked Keychain and prompts for access on a fresh machine.
// They are therefore gated behind USHER_KEYCHAIN_TEST so CI (and a plain
// `go test ./...`) skips them; run them locally with:
//
//	USHER_KEYCHAIN_TEST=1 go test ./internal/keychain/
func requireKeychain(t *testing.T) {
	t.Helper()
	if os.Getenv("USHER_KEYCHAIN_TEST") == "" {
		t.Skip("set USHER_KEYCHAIN_TEST=1 to run Keychain round-trip tests")
	}
}

// uniqueBackend derives a per-run backend name so concurrent or repeated runs
// never collide on the same Keychain item.
func uniqueBackend(t *testing.T) string {
	t.Helper()
	return "test-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// TestSetGetRoundTrip: a stored secret reads back byte-for-byte, and a second
// Set with -U rotates the value in place rather than failing.
func TestSetGetRoundTrip(t *testing.T) {
	requireKeychain(t)
	backend := uniqueBackend(t)
	const account = "ANTHROPIC_API_KEY"
	t.Cleanup(func() { _ = Delete(backend, account) })

	if err := Set(backend, account, "sk-secret-1"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := Get(backend, account)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "sk-secret-1" {
		t.Fatalf("Get = %q, want %q", got, "sk-secret-1")
	}

	// -U update-in-place: re-Set rotates the secret.
	if err := Set(backend, account, "sk-secret-2"); err != nil {
		t.Fatalf("Set (rotate): %v", err)
	}
	got, err = Get(backend, account)
	if err != nil {
		t.Fatalf("Get (after rotate): %v", err)
	}
	if got != "sk-secret-2" {
		t.Fatalf("Get (after rotate) = %q, want %q", got, "sk-secret-2")
	}
}

// TestGetMissing: a never-stored item returns ErrNotFound (not a wrapped CLI
// error), so callers can branch on it.
func TestGetMissing(t *testing.T) {
	requireKeychain(t)
	backend := uniqueBackend(t)
	_, err := Get(backend, "NOPE")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing = %v, want ErrNotFound", err)
	}
}

// TestDeleteIdempotent: Delete removes the item, and a second Delete on the
// now-absent item is a no-op (nil).
func TestDeleteIdempotent(t *testing.T) {
	requireKeychain(t)
	backend := uniqueBackend(t)
	const account = "TOKEN"

	if err := Set(backend, account, "v"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := Delete(backend, account); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := Get(backend, account); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after Delete = %v, want ErrNotFound", err)
	}
	if err := Delete(backend, account); err != nil {
		t.Fatalf("second Delete (idempotent) = %v, want nil", err)
	}
}
