// circuit_breaker_test.go — TDD specification for the 3-state CircuitBreaker.
//
// These tests are written BEFORE circuit_breaker.go and assert REAL state
// transitions over a CONTROLLED clock — not mock call counts. The breaker is
// pure domain logic (no I/O), so we drive time with an injectable now-function
// and assert the exact state after each event. This is the centerpiece test:
// every transition edge of the machine is exercised here.
//
// We test from INSIDE the package (package domain) because the breaker is an
// internal type with unexported fields we want to construct directly with a fake
// clock — no import cycle risk (the breaker has no port dependencies).
package domain

import (
	"testing"
	"time"
)

// fakeClock is a controllable time source. WHY inject the clock: the breaker's
// CLOSED→OPEN→HALF_OPEN edge is TIME-based (after reset_timeout). A test that
// slept for the real timeout would be slow and flaky; a fake clock makes the
// transition deterministic and instant.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time   { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// newTestBreaker builds a breaker with small thresholds and the fake clock.
//   failureThreshold=3  → 3 consecutive failures trips CLOSED→OPEN
//   successThreshold=2  → 2 consecutive HALF_OPEN successes close it
//   resetTimeout=10s    → OPEN waits 10s before allowing a HALF_OPEN probe
//   halfOpenMaxProbes=1 → only 1 in-flight probe allowed in HALF_OPEN
func newTestBreaker(clk *fakeClock) *CircuitBreaker {
	return newCircuitBreaker(breakerConfig{
		failureThreshold:  3,
		successThreshold:  2,
		resetTimeout:      10 * time.Second,
		halfOpenMaxProbes: 1,
	}, clk.now)
}

// TestBreaker_StartsClosed: a fresh breaker admits traffic. WHY assert the zero
// state explicitly: the whole machine assumes CLOSED is the entry state; if the
// zero value were OPEN, every backend would start quarantined.
func TestBreaker_StartsClosed(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	b := newTestBreaker(clk)
	if got := b.State(); got != CircuitClosed {
		t.Fatalf("new breaker state = %v, want CLOSED", got)
	}
	if !b.Allow() {
		t.Fatal("new breaker should Allow()")
	}
}

// TestBreaker_TripsOpenOnThreshold: failureThreshold CONSECUTIVE failures move
// CLOSED→OPEN, and an OPEN breaker fails fast (Allow=false). This is the core
// protective transition: stop hammering a sick backend.
func TestBreaker_TripsOpenOnThreshold(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	b := newTestBreaker(clk)

	// Two failures: still CLOSED (below threshold of 3). The breaker tolerates
	// transient blips — one failed call must not quarantine a backend.
	b.RecordFailure()
	b.RecordFailure()
	if got := b.State(); got != CircuitClosed {
		t.Fatalf("after 2 failures state = %v, want CLOSED (threshold 3)", got)
	}
	if !b.Allow() {
		t.Fatal("breaker below threshold should still Allow()")
	}

	// Third consecutive failure trips it OPEN.
	b.RecordFailure()
	if got := b.State(); got != CircuitOpen {
		t.Fatalf("after 3 failures state = %v, want OPEN", got)
	}
	if b.Allow() {
		t.Fatal("OPEN breaker must fail fast (Allow()=false)")
	}
}

// TestBreaker_SuccessResetsFailureCount: a success in CLOSED clears the
// consecutive-failure tally. WHY this matters — the trip condition is
// CONSECUTIVE failures, not lifetime failures; two failures, a success, then two
// more failures must NOT trip (the backend recovered between blips).
func TestBreaker_SuccessResetsFailureCount(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	b := newTestBreaker(clk)

	b.RecordFailure()
	b.RecordFailure()
	b.RecordSuccess() // resets the consecutive count to 0
	b.RecordFailure()
	b.RecordFailure()

	if got := b.State(); got != CircuitClosed {
		t.Fatalf("non-consecutive failures tripped breaker: state = %v, want CLOSED", got)
	}
}

// TestBreaker_OpenToHalfOpenAfterTimeout: an OPEN breaker rejects until
// resetTimeout elapses, then admits ONE probe (transitioning to HALF_OPEN). This
// is the recovery-attempt edge — the breaker doesn't stay open forever; it
// periodically tests whether the backend has healed.
func TestBreaker_OpenToHalfOpenAfterTimeout(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	b := newTestBreaker(clk)
	trip(b) // → OPEN

	// Just before the timeout: still failing fast.
	clk.advance(9 * time.Second)
	if b.Allow() {
		t.Fatal("OPEN breaker before reset_timeout must reject")
	}
	if got := b.State(); got != CircuitOpen {
		t.Fatalf("state before timeout = %v, want OPEN", got)
	}

	// After the timeout: the first Allow() admits a probe and flips to HALF_OPEN.
	clk.advance(2 * time.Second) // now 11s > 10s timeout
	if !b.Allow() {
		t.Fatal("after reset_timeout the first Allow() must admit a probe")
	}
	if got := b.State(); got != CircuitHalfOpen {
		t.Fatalf("after probe admitted state = %v, want HALF_OPEN", got)
	}
}

// TestBreaker_HalfOpenLimitsProbes: in HALF_OPEN only halfOpenMaxProbes requests
// are admitted concurrently. WHY: HALF_OPEN must send a TRICKLE, not a flood — if
// it admitted everything, a still-sick backend would be hammered the instant the
// timeout elapsed, defeating the purpose. The 2nd concurrent probe is rejected.
func TestBreaker_HalfOpenLimitsProbes(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	b := newTestBreaker(clk)
	trip(b)
	clk.advance(11 * time.Second)

	if !b.Allow() { // probe #1 admitted → HALF_OPEN, 1 probe in flight
		t.Fatal("first probe should be admitted")
	}
	if b.Allow() { // probe #2 rejected: max in-flight probes is 1
		t.Fatal("HALF_OPEN must cap concurrent probes (2nd should be rejected)")
	}
}

// TestBreaker_HalfOpenToClosedOnSuccesses: successThreshold consecutive probe
// successes in HALF_OPEN close the breaker (backend recovered). After each
// success the breaker frees a probe slot so the next probe can be admitted.
func TestBreaker_HalfOpenToClosedOnSuccesses(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	b := newTestBreaker(clk)
	trip(b)
	clk.advance(11 * time.Second)

	// Probe 1: admit, succeed. Still HALF_OPEN (need 2 successes to close).
	if !b.Allow() {
		t.Fatal("probe 1 should be admitted")
	}
	b.RecordSuccess()
	if got := b.State(); got != CircuitHalfOpen {
		t.Fatalf("after 1 success state = %v, want HALF_OPEN (need 2)", got)
	}

	// Probe 2: admit, succeed → CLOSED.
	if !b.Allow() {
		t.Fatal("probe 2 should be admitted after probe 1 freed its slot")
	}
	b.RecordSuccess()
	if got := b.State(); got != CircuitClosed {
		t.Fatalf("after 2 successes state = %v, want CLOSED", got)
	}
	if !b.Allow() {
		t.Fatal("recovered (CLOSED) breaker should Allow()")
	}
}

// TestBreaker_HalfOpenToOpenOnFailure: a SINGLE failure during HALF_OPEN flips
// straight back to OPEN and restarts the reset timer. The backend is still sick;
// don't keep probing immediately. This is the pessimistic edge that prevents a
// flapping backend from oscillating traffic.
func TestBreaker_HalfOpenToOpenOnFailure(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	b := newTestBreaker(clk)
	trip(b)
	clk.advance(11 * time.Second)

	if !b.Allow() { // → HALF_OPEN, probe in flight
		t.Fatal("probe should be admitted")
	}
	b.RecordFailure() // probe failed → back to OPEN
	if got := b.State(); got != CircuitOpen {
		t.Fatalf("after HALF_OPEN failure state = %v, want OPEN", got)
	}
	// And it fails fast again immediately (timer restarted).
	if b.Allow() {
		t.Fatal("re-OPENed breaker must reject immediately")
	}

	// The reset timer RESTARTED at the moment of re-opening, so we must wait the
	// full timeout AGAIN from here — proving the timer anchors to the LAST
	// transition, not the first.
	clk.advance(9 * time.Second)
	if b.Allow() {
		t.Fatal("re-OPENed breaker must reject until a fresh reset_timeout elapses")
	}
	clk.advance(2 * time.Second)
	if !b.Allow() {
		t.Fatal("after a fresh reset_timeout the re-OPENed breaker should probe again")
	}
}

// TestBreaker_Snapshot: the observability view reports the live state and failure
// count. The dashboards/observability RPCs read exactly this.
func TestBreaker_Snapshot(t *testing.T) {
	clk := &fakeClock{t: time.Unix(100, 0)}
	b := newTestBreaker(clk)
	b.RecordFailure()
	b.RecordFailure()

	snap := b.snapshot("fraud-detector", "v2")
	if snap.State != CircuitClosed {
		t.Fatalf("snapshot state = %v, want CLOSED", snap.State)
	}
	if snap.ConsecutiveFailures != 2 {
		t.Fatalf("snapshot failures = %d, want 2", snap.ConsecutiveFailures)
	}
	if snap.ModelName != "fraud-detector" || snap.Version != "v2" {
		t.Fatalf("snapshot identity = %s/%s, want fraud-detector/v2", snap.ModelName, snap.Version)
	}
}

// trip drives a fresh breaker to OPEN via failureThreshold (3) failures.
func trip(b *CircuitBreaker) {
	b.RecordFailure()
	b.RecordFailure()
	b.RecordFailure()
}
