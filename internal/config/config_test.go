package config

import (
	"testing"
	"time"
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
