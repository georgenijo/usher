package broker

import (
	"strconv"
	"sync"
	"time"
)

// lock.go is the arbitration registry behind ArbitrateStage (#16): a per-window
// write-lock so two agents cannot drive the same window at once and corrupt each
// other's AX indices mid-flight. The locked design (see the project's
// TOMORROW.md) is deliberately small: a plain mutual-exclusion lock per window,
// NO RW-lock, NO global lock, NO priority/preemption. Reads never enter here.
//
// A lock is keyed by the target window (pid, window_id) parsed from a mutating
// tool-call's arguments. A whole-process lock (only kill_app needs it) uses the
// wildcardWindow sentinel so it serialises against every per-window lock on that
// pid as well — kill_app must not race a click in the same app.
//
// Three release paths converge on the same registry, all idempotent:
//   - the matching RESPONSE (the normal path; ArbitrateStage releases on the
//     outbound side, correlating via the #15 inflight map);
//   - a TTL lease (a backend that never answers, or a dropped response, must not
//     wedge a window forever);
//   - reclaim-on-death (a caller's connection ends with the lock still held).

// wildcardWindow is the window_id sentinel for a whole-process lock. The driver
// uses CGWindowID (u32) where 0 is invalid, so 0 can never be a real window and
// is safe to reuse as "the whole pid". A tool-call that carries a real window_id
// of 0 is malformed and is treated as windowless (no lock) by the caller.
const wildcardWindow = 0

// windowKey identifies the target of a write-lock. windowID == wildcardWindow
// means a whole-process lock spanning every window of pid.
type windowKey struct {
	pid      int64
	windowID int64
}

// isWildcard reports whether the key locks the whole process rather than a
// single window.
func (k windowKey) isWildcard() bool { return k.windowID == wildcardWindow }

// String renders the key for event/audit lines: "pid=<pid> window=<id>", with
// the wildcard window shown as "*" so a whole-process lock reads "pid=1 window=*".
func (k windowKey) String() string {
	win := strconv.FormatInt(k.windowID, 10)
	if k.isWildcard() {
		win = "*"
	}
	return "pid=" + strconv.FormatInt(k.pid, 10) + " window=" + win
}

// lease is the live state of one held lock. owner is the connection Identity.ID
// that holds it; acquiredAt anchors the TTL; token disambiguates successive
// holders of the same key so a stale release (a late response after the TTL
// already reclaimed and re-granted the lock) cannot free the new holder's lock.
type lease struct {
	owner      string
	token      uint64
	acquiredAt time.Time
}

// lockRegistry is the set of live per-window write-locks. It serialises
// contending writers on the same key and leaves different keys independent. Safe
// for concurrent use: the two pump goroutines of every connection touch it.
//
// Each key carries a 1-slot semaphore channel (held[key]) that is the actual
// mutual-exclusion primitive — a writer that finds it full waits on it with a
// bounded select, so contention serialises without a busy-loop and reads (which
// never call Acquire) are wholly unaffected. leases[key] records who holds it for
// release, TTL expiry, and reclaim-on-death.
type lockRegistry struct {
	mu     sync.Mutex
	held   map[windowKey]chan struct{}
	leases map[windowKey]lease
	ttl    time.Duration
	wait   time.Duration
	nextTk uint64
	now    func() time.Time // injectable clock for tests
}

// defaultLockTTL is how long a write-lock may be held before the registry
// reclaims it on the next contended Acquire. It bounds the damage of a backend
// that accepts a mutating call but never answers (so the release-on-response path
// never fires) — the window frees itself for the next writer rather than wedging.
const defaultLockTTL = 30 * time.Second

// defaultLockWait is how long a contended writer blocks for the holder to
// release before Acquire gives up and the stage returns a JSON-RPC busy error.
// It is bounded so a stuck holder cannot stall the waiter's whole connection.
const defaultLockWait = 5 * time.Second

// newLockRegistry returns an empty registry with the given TTL lease and bounded
// contended-wait. A zero ttl or wait falls back to the package default.
func newLockRegistry(ttl, wait time.Duration) *lockRegistry {
	if ttl <= 0 {
		ttl = defaultLockTTL
	}
	if wait <= 0 {
		wait = defaultLockWait
	}
	return &lockRegistry{
		held:   make(map[windowKey]chan struct{}),
		leases: make(map[windowKey]lease),
		ttl:    ttl,
		wait:   wait,
		now:    time.Now,
	}
}

// slot returns the 1-buffered semaphore channel for key, creating it on first
// use. Caller must hold r.mu. The channel models the lock: a value present means
// free; empty means held. It is created full (one value buffered) so the first
// Acquire succeeds immediately.
func (r *lockRegistry) slot(key windowKey) chan struct{} {
	ch, ok := r.held[key]
	if !ok {
		ch = make(chan struct{}, 1)
		ch <- struct{}{} // start free
		r.held[key] = ch
	}
	return ch
}

// acquireResult is why Acquire returned, so the caller can map it to either a
// forward (granted) or a JSON-RPC error (timedOut).
type acquireResult int

const (
	// acquired means the caller now holds the write-lock for key.
	acquired acquireResult = iota
	// timedOut means the lock stayed held past the bounded wait; the caller
	// should answer the agent with a "window busy" JSON-RPC error.
	timedOut
)

// Acquire takes the write-lock for key on behalf of owner, blocking up to the
// registry's bounded wait if another owner holds it. It returns the lease token
// the holder must present to Release. A held lock whose lease is older than the
// TTL is reclaimed (its slot refilled) before the wait, so a never-answered call
// cannot wedge a window indefinitely.
//
// Different keys are wholly independent: their semaphore channels are distinct,
// so a writer on window A never blocks a writer on window B. Reads never call
// Acquire, so they never block here.
func (r *lockRegistry) Acquire(key windowKey, owner string) (token uint64, result acquireResult) {
	r.mu.Lock()
	r.reapExpiredLocked()
	ch := r.slot(key)
	r.mu.Unlock()

	// Fast path and bounded wait share the same select; the timer is only armed
	// on contention. A free slot is taken immediately.
	timer := time.NewTimer(r.wait)
	defer timer.Stop()
	select {
	case <-ch:
		// Got the slot: record the lease under the lock.
		r.mu.Lock()
		r.nextTk++
		tk := r.nextTk
		r.leases[key] = lease{owner: owner, token: tk, acquiredAt: r.now()}
		r.mu.Unlock()
		return tk, acquired
	case <-timer.C:
		// Holder never released within the bounded wait. One last chance: a TTL
		// reclaim may have freed it right as we timed out — try a non-blocking
		// take before giving up, so a just-expired lock is granted not refused.
		r.mu.Lock()
		r.reapExpiredLocked()
		select {
		case <-ch:
			r.nextTk++
			tk := r.nextTk
			r.leases[key] = lease{owner: owner, token: tk, acquiredAt: r.now()}
			r.mu.Unlock()
			return tk, acquired
		default:
			r.mu.Unlock()
			return 0, timedOut
		}
	}
}

// Release frees key if it is still held by token. A mismatched or absent token
// is a no-op (true is returned only when this call actually freed the lock), so
// a late release after a TTL reclaim re-granted the lock to someone else cannot
// free the new holder, and a double release is harmless. This is the normal
// release-on-response path and is safe to call from the outbound pump.
func (r *lockRegistry) Release(key windowKey, token uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	ls, ok := r.leases[key]
	if !ok || ls.token != token {
		return false // already reclaimed/released, or a stale token
	}
	delete(r.leases, key)
	r.refillLocked(key)
	return true
}

// ReleaseOwner frees every lock currently held by owner — the reclaim-on-death
// path, called when a connection ends with locks still held. It returns the
// number of locks reclaimed (for the audit line). Independent of token, because
// a dead caller cannot present one.
func (r *lockRegistry) ReleaseOwner(owner string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for key, ls := range r.leases {
		if ls.owner == owner {
			delete(r.leases, key)
			r.refillLocked(key)
			n++
		}
	}
	return n
}

// reapExpiredLocked frees any lease older than the TTL. Caller must hold r.mu.
// It runs on every Acquire so an abandoned lock is reclaimed lazily — no
// background goroutine, no timer per lock.
func (r *lockRegistry) reapExpiredLocked() {
	cutoff := r.now().Add(-r.ttl)
	for key, ls := range r.leases {
		if ls.acquiredAt.Before(cutoff) {
			delete(r.leases, key)
			r.refillLocked(key)
		}
	}
}

// refillLocked returns the slot for key to the free state. Caller must hold
// r.mu. It is a non-blocking send: the buffer is size 1, so a slot that is
// somehow already free is left as-is rather than panicking on a full channel.
func (r *lockRegistry) refillLocked(key windowKey) {
	ch, ok := r.held[key]
	if !ok {
		return
	}
	select {
	case ch <- struct{}{}:
	default: // already free; nothing to do
	}
}
