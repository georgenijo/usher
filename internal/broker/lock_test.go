package broker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestLockRegistry_DifferentWindowsIndependent: two writers on distinct windows
// both acquire immediately — different keys never block each other.
func TestLockRegistry_DifferentWindowsIndependent(t *testing.T) {
	r := newLockRegistry(time.Minute, time.Minute)
	a := windowKey{pid: 1, windowID: 10}
	b := windowKey{pid: 1, windowID: 20}

	if _, res := r.Acquire(a, "owner-a"); res != acquired {
		t.Fatal("first window should acquire")
	}
	// A different window must not be blocked by the lock on a.
	if _, res := r.Acquire(b, "owner-b"); res != acquired {
		t.Fatal("a different window must acquire without blocking")
	}
}

// TestLockRegistry_SameWindowSerializes: two contexts contending on one window
// serialize — the second blocks until the first releases, then acquires.
func TestLockRegistry_SameWindowSerializes(t *testing.T) {
	r := newLockRegistry(time.Minute, time.Minute)
	key := windowKey{pid: 1, windowID: 10}

	tk1, res := r.Acquire(key, "owner-1")
	if res != acquired {
		t.Fatal("owner-1 should acquire the free window")
	}

	// owner-2 must block while owner-1 holds it; release after a short delay and
	// confirm owner-2 then proceeds.
	acquired2 := make(chan uint64, 1)
	go func() {
		tk2, res := r.Acquire(key, "owner-2")
		if res == acquired {
			acquired2 <- tk2
		} else {
			close(acquired2)
		}
	}()

	// owner-2 should still be waiting (lock held).
	select {
	case <-acquired2:
		t.Fatal("owner-2 acquired while owner-1 still held the window")
	case <-time.After(50 * time.Millisecond):
	}

	if !r.Release(key, tk1) {
		t.Fatal("owner-1 release should succeed")
	}
	select {
	case tk2, ok := <-acquired2:
		if !ok {
			t.Fatal("owner-2 failed to acquire after release")
		}
		if tk2 == tk1 {
			t.Error("successive holders must get distinct tokens")
		}
	case <-time.After(time.Second):
		t.Fatal("owner-2 did not acquire after owner-1 released")
	}
}

// TestLockRegistry_ContendedWaitTimesOut: when the holder never releases, a
// contended writer times out within the bounded wait and is refused.
func TestLockRegistry_ContendedWaitTimesOut(t *testing.T) {
	r := newLockRegistry(time.Minute, 30*time.Millisecond)
	key := windowKey{pid: 1, windowID: 10}

	if _, res := r.Acquire(key, "holder"); res != acquired {
		t.Fatal("holder should acquire")
	}
	start := time.Now()
	if _, res := r.Acquire(key, "waiter"); res != timedOut {
		t.Fatalf("waiter should time out, got %v", res)
	}
	if elapsed := time.Since(start); elapsed < 25*time.Millisecond {
		t.Errorf("waiter returned too fast (%v): wait was not bounded-honored", elapsed)
	}
}

// TestLockRegistry_ReleaseOwner reclaims every lock a dead caller held — the
// reclaim-on-death path. After reclamation the window is free for a new owner.
func TestLockRegistry_ReleaseOwner(t *testing.T) {
	r := newLockRegistry(time.Minute, 30*time.Millisecond)
	k1 := windowKey{pid: 1, windowID: 10}
	k2 := windowKey{pid: 2, windowID: 20}
	k3 := windowKey{pid: 3, windowID: 30}

	r.Acquire(k1, "dead")
	r.Acquire(k2, "dead")
	r.Acquire(k3, "alive")

	if n := r.ReleaseOwner("dead"); n != 2 {
		t.Fatalf("ReleaseOwner reclaimed %d, want 2", n)
	}
	// The dead owner's windows are now free.
	if _, res := r.Acquire(k1, "next"); res != acquired {
		t.Error("k1 should be free after the dead owner was reclaimed")
	}
	// The live owner's lock is untouched: a new writer on k3 still blocks.
	if _, res := r.Acquire(k3, "next"); res != timedOut {
		t.Error("k3 held by a live owner must not be reclaimed")
	}
}

// TestLockRegistry_TTLReclaim: a lease older than the TTL is reclaimed on the
// next contended Acquire, so a never-answered call cannot wedge a window.
func TestLockRegistry_TTLReclaim(t *testing.T) {
	r := newLockRegistry(50*time.Millisecond, 30*time.Millisecond)
	// Drive the clock manually so the test is deterministic and fast.
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	r.now = func() time.Time { return time.Unix(0, now.Load()) }

	key := windowKey{pid: 1, windowID: 10}
	if _, res := r.Acquire(key, "stuck"); res != acquired {
		t.Fatal("stuck owner should acquire")
	}
	// Advance the clock past the TTL; the stuck lease is now reclaimable.
	now.Add(int64(100 * time.Millisecond))

	if _, res := r.Acquire(key, "next"); res != acquired {
		t.Fatal("a lease older than the TTL must be reclaimed for the next writer")
	}
}

// TestLockRegistry_StaleReleaseNoOp: a release with a stale token (after the
// lease was reclaimed and re-granted) must not free the new holder's lock.
func TestLockRegistry_StaleReleaseNoOp(t *testing.T) {
	r := newLockRegistry(50*time.Millisecond, 30*time.Millisecond)
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	r.now = func() time.Time { return time.Unix(0, now.Load()) }
	key := windowKey{pid: 1, windowID: 10}

	tkOld, _ := r.Acquire(key, "old")
	now.Add(int64(100 * time.Millisecond)) // age past TTL
	tkNew, res := r.Acquire(key, "new")    // TTL reclaim + re-grant
	if res != acquired {
		t.Fatal("new owner should acquire after TTL reclaim")
	}

	// The old owner's late release must NOT free the new owner's lock.
	if r.Release(key, tkOld) {
		t.Error("a stale-token release must be a no-op")
	}
	// Confirm the new owner still holds it: a contender still times out.
	if _, res := r.Acquire(key, "contender"); res != timedOut {
		t.Error("new owner's lock was wrongly freed by the stale release")
	}
	// The legitimate release by the new owner frees it.
	if !r.Release(key, tkNew) {
		t.Error("new owner's own release should succeed")
	}
}

// TestLockRegistry_DoubleReleaseHarmless: releasing twice is a no-op the second
// time, so the release-on-response path and a reclaim cannot double-free.
func TestLockRegistry_DoubleReleaseHarmless(t *testing.T) {
	r := newLockRegistry(time.Minute, time.Minute)
	key := windowKey{pid: 1, windowID: 10}
	tk, _ := r.Acquire(key, "o")
	if !r.Release(key, tk) {
		t.Fatal("first release should succeed")
	}
	if r.Release(key, tk) {
		t.Error("second release of the same token must be a no-op")
	}
}

// TestLockRegistry_ConcurrentSameWindow: many goroutines hammer one window; the
// registry must grant exactly one holder at a time (run with -race). Each holder
// briefly observes the critical section; an overlap is a serialization bug.
func TestLockRegistry_ConcurrentSameWindow(t *testing.T) {
	r := newLockRegistry(time.Minute, time.Second)
	key := windowKey{pid: 1, windowID: 10}

	var inside atomic.Int32
	var maxObserved atomic.Int32
	var granted atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk, res := r.Acquire(key, "o"+itoa(i))
			if res != acquired {
				return // a timeout is acceptable under heavy contention
			}
			granted.Add(1)
			n := inside.Add(1)
			for {
				m := maxObserved.Load()
				if n <= m || maxObserved.CompareAndSwap(m, n) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			inside.Add(-1)
			r.Release(key, tk)
		}()
	}
	wg.Wait()

	if maxObserved.Load() > 1 {
		t.Errorf("write-lock allowed %d concurrent holders; must serialize to 1", maxObserved.Load())
	}
	if granted.Load() == 0 {
		t.Error("no goroutine ever acquired the lock")
	}
}
