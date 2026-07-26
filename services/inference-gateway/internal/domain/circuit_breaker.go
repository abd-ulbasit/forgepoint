// circuit_breaker.go — the 3-state Circuit Breaker, the resilience centerpiece.
//
// ============================================================================
// THE CIRCUIT BREAKER PATTERN (Nygard, "Release It!")
// ============================================================================
//
// A circuit breaker wraps a call to a fallible dependency and, after enough
// failures, "trips" — refusing further calls for a cooldown so it stops piling
// requests onto a sick backend (which only makes the outage worse and ties up
// the caller's resources waiting on doomed calls). After the cooldown it sends a
// few PROBES; if they succeed it closes again, if they fail it stays open. This
// is exactly Envoy's outlier detection / Hystrix / resilience4j / gobreaker.
//
// THE STATE MACHINE (this file IS the machine — read it alongside the test):
//
//		┌─────────┐  consecutive failures ≥ failureThreshold   ┌──────┐
//		│ CLOSED  │ ──────────────────────────────────────────►│ OPEN │
//		│ (normal)│ ◄── successes ≥ successThreshold ───┐       │(fast │
//		└─────────┘                                     │       │ fail)│
//		     ▲                                          │       └──────┘
//		     │                                   ┌──────────┐      │ after resetTimeout,
//		     │                                   │ HALF_OPEN│◄─────┘ first Allow() admits
//		     └────────── any probe failure ──────│ (probing)│        up to halfOpenMaxProbes
//		                  (→ OPEN, restart timer) └──────────┘
//
//	  CLOSED    — requests pass; count CONSECUTIVE failures; a success resets the
//	              count. failureThreshold consecutive failures → OPEN.
//	  OPEN      — Allow() returns false (fail fast) until resetTimeout elapses from
//	              the last transition; the first Allow() after that admits a probe
//	              and moves to HALF_OPEN.
//	  HALF_OPEN — admit at most halfOpenMaxProbes concurrent probes. Each success
//	              counts toward successThreshold (→ CLOSED); ANY failure → OPEN and
//	              restarts the timer (pessimistic: a flapping backend stays out).
//
// ============================================================================
// CONCURRENCY MODEL
// ============================================================================
//
// One breaker guards one backend (model+version) and is hit by every concurrent
// Predict for that backend across goroutines. All state mutation goes under a
// single Mutex — the critical sections are tiny (a few int compares), so a mutex
// is simpler and faster here than a lock-free scheme, and it makes the
// transitions atomically correct (no torn read of state+counters). The `-race`
// detector in the use-case tests exercises this.
//
// CLOCK INJECTION: the OPEN→HALF_OPEN edge is time-based. We inject a now()
// function so tests drive transitions with a fake clock (instant, deterministic)
// and production passes time.Now. This is the standard way to make time-based
// logic testable without sleeping.
// ============================================================================
package domain

import (
	"sync"
	"time"
)

// breakerConfig holds the (immutable) tuning knobs for one breaker. Defaults are
// applied by the registry; tests construct it directly with small values.
type breakerConfig struct {
	failureThreshold  int           // consecutive CLOSED failures that trip → OPEN
	successThreshold  int           // consecutive HALF_OPEN successes that → CLOSED
	resetTimeout      time.Duration // OPEN cooldown before a probe is admitted
	halfOpenMaxProbes int           // max concurrent probes admitted in HALF_OPEN
}

// defaultBreakerConfig is the production tuning. WHY these numbers:
//   - 5 consecutive failures: tolerant of transient blips (a single dropped
//     packet won't trip) yet trips well before a sustained outage saturates the
//     caller. A common industry default (resilience4j uses ~50% over a window;
//     we use a simpler consecutive-count which is easier to reason about and
//     explain).
//   - 30s reset: long enough that we don't hammer a recovering backend, short
//     enough that a recovered backend rejoins quickly.
//   - 2 successes to close: one lucky probe isn't proof of recovery; two
//     consecutive is a reasonable confidence bar without being slow to recover.
//   - 1 probe at a time: HALF_OPEN must trickle, not flood.
func defaultBreakerConfig() breakerConfig {
	return breakerConfig{
		failureThreshold:  5,
		successThreshold:  2,
		resetTimeout:      30 * time.Second,
		halfOpenMaxProbes: 1,
	}
}

// CircuitBreaker is a single backend's breaker. It is a pure domain type with no
// I/O — its only dependency is the injected clock. The BreakerRegistry port
// hands these out keyed by (model, version).
type CircuitBreaker struct {
	cfg breakerConfig
	now func() time.Time

	mu               sync.Mutex
	state            CircuitState
	consecutiveFails int       // CLOSED: failures since the last success/trip
	halfOpenSuccess  int       // HALF_OPEN: consecutive probe successes so far
	halfOpenInFlight int       // HALF_OPEN: probes currently admitted but not yet recorded
	lastTransition   time.Time // anchors the resetTimeout countdown
}

// newCircuitBreaker constructs a CLOSED breaker. Unexported: breakers are created
// by the registry (or tests), never ad hoc, so all share consistent config.
func newCircuitBreaker(cfg breakerConfig, now func() time.Time) *CircuitBreaker {
	return &CircuitBreaker{
		cfg:            cfg,
		now:            now,
		state:          CircuitClosed,
		lastTransition: now(),
	}
}

// State returns the breaker's current state. Takes the lock because state can be
// lazily advanced (OPEN→HALF_OPEN is time-driven and may be realized on read).
func (b *CircuitBreaker) State() CircuitState {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maybeHalfOpen()
	return b.state
}

// Allow reports whether a request may proceed AND reserves a probe slot if the
// breaker is (or just became) HALF_OPEN. It is the GATE the use-case calls before
// forwarding to the backend.
//
//	CLOSED    → always allow.
//	OPEN      → allow only if resetTimeout elapsed; the first such call flips to
//	            HALF_OPEN and reserves a probe slot.
//	HALF_OPEN → allow only while in-flight probes < halfOpenMaxProbes; each
//	            allowed call reserves a slot (released by RecordSuccess/Failure).
//
// WHY Allow reserves the slot (rather than a separate Acquire): keeping the
// check-and-reserve atomic under one lock prevents two goroutines from both
// passing the "< maxProbes" check and flooding a sick backend — the classic
// check-then-act race.
func (b *CircuitBreaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.maybeHalfOpen() // realize a pending OPEN→HALF_OPEN transition first

	switch b.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		// Still cooling down (maybeHalfOpen didn't flip us) → fail fast.
		return false
	case CircuitHalfOpen:
		if b.halfOpenInFlight < b.cfg.halfOpenMaxProbes {
			b.halfOpenInFlight++
			return true
		}
		return false
	default:
		return false
	}
}

// RecordSuccess reports that an admitted call succeeded.
//
//	CLOSED    → clear the consecutive-failure tally (the backend is healthy).
//	HALF_OPEN → free the probe slot, count the success; at successThreshold,
//	            transition to CLOSED (recovered).
func (b *CircuitBreaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case CircuitClosed:
		b.consecutiveFails = 0
	case CircuitHalfOpen:
		if b.halfOpenInFlight > 0 {
			b.halfOpenInFlight--
		}
		b.halfOpenSuccess++
		if b.halfOpenSuccess >= b.cfg.successThreshold {
			b.transitionTo(CircuitClosed)
		}
	}
}

// RecordFailure reports that an admitted call failed (ErrUpstream/ErrTimeout —
// NOT a client error like ErrInvalidInput, which the use-case never records).
//
//	CLOSED    → increment the consecutive-failure tally; at failureThreshold → OPEN.
//	HALF_OPEN → any failure → OPEN immediately and restart the cooldown timer
//	            (the backend is still sick).
func (b *CircuitBreaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case CircuitClosed:
		b.consecutiveFails++
		if b.consecutiveFails >= b.cfg.failureThreshold {
			b.transitionTo(CircuitOpen)
		}
	case CircuitHalfOpen:
		if b.halfOpenInFlight > 0 {
			b.halfOpenInFlight--
		}
		b.transitionTo(CircuitOpen)
	}
}

// maybeHalfOpen realizes the time-driven OPEN→HALF_OPEN edge. Called under the
// lock at the top of State/Allow so the transition happens lazily on the next
// access after resetTimeout — no background goroutine/timer per breaker (which
// would be wasteful at thousands of backends). This is the standard "check the
// clock on access" approach (gobreaker does the same).
func (b *CircuitBreaker) maybeHalfOpen() {
	if b.state == CircuitOpen && !b.now().Before(b.lastTransition.Add(b.cfg.resetTimeout)) {
		b.transitionTo(CircuitHalfOpen)
	}
}

// transitionTo moves to a new state and resets the per-state counters and the
// transition timestamp. Centralizing the reset here guarantees no stale counter
// leaks across states (e.g. a CLOSED failure count bleeding into HALF_OPEN).
// Caller must hold the lock.
func (b *CircuitBreaker) transitionTo(s CircuitState) {
	b.state = s
	b.lastTransition = b.now()
	b.consecutiveFails = 0
	b.halfOpenSuccess = 0
	b.halfOpenInFlight = 0
}

// snapshot returns the read-only observability view for (model, version).
func (b *CircuitBreaker) snapshot(modelName, version string) CircuitSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maybeHalfOpen()
	return CircuitSnapshot{
		ModelName:           modelName,
		Version:             version,
		State:               b.state,
		ConsecutiveFailures: b.consecutiveFails,
		LastTransitionAt:    b.lastTransition,
	}
}
