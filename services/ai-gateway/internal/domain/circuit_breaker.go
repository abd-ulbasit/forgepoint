// circuit_breaker.go — the 3-state Circuit Breaker, REUSED verbatim from the
// inference-gateway (the platform's canonical resilience primitive).
//
// ============================================================================
// THE CIRCUIT BREAKER PATTERN (Nygard, "Release It!")
// ============================================================================
//
// In the AI Gateway the breaker guards each PROVIDER (Ollama, the stub) rather
// than a model+version. The failover loop asks the breaker Allow() before calling
// a provider; on the provider's outcome it RecordSuccess/RecordFailure. A provider
// that keeps failing trips OPEN and the loop SKIPS straight to the next provider —
// that skip IS the failover. This is the identical machine the inference gateway
// uses for model backends (Envoy outlier detection / Hystrix / gobreaker).
//
// THE STATE MACHINE:
//
//	┌─────────┐  consecutive failures ≥ failureThreshold   ┌──────┐
//	│ CLOSED  │ ──────────────────────────────────────────►│ OPEN │
//	│ (normal)│ ◄── successes ≥ successThreshold ───┐       │(skip │
//	└─────────┘                                     │       │ this │
//	     ▲                                          │       │ prov)│
//	     │                                   ┌──────────┐   └──────┘
//	     │                                   │ HALF_OPEN│◄──── after resetTimeout,
//	     └────────── any probe failure ──────│ (probing)│      first Allow() admits
//	                  (→ OPEN, restart timer) └──────────┘      one probe
//
// CONCURRENCY: one breaker per provider, hit by every concurrent ChatCompletion.
// All mutation is under a single Mutex (tiny critical sections). CLOCK INJECTION:
// the OPEN→HALF_OPEN edge is time-based; now() is injected so tests drive it with a
// fake clock (instant, deterministic) and production passes time.Now.
// ============================================================================
package domain

import (
	"sync"
	"time"
)

// breakerConfig holds the immutable tuning for one breaker.
type breakerConfig struct {
	failureThreshold  int           // consecutive CLOSED failures that trip → OPEN
	successThreshold  int           // consecutive HALF_OPEN successes that → CLOSED
	resetTimeout      time.Duration // OPEN cooldown before a probe is admitted
	halfOpenMaxProbes int           // max concurrent probes admitted in HALF_OPEN
}

// defaultBreakerConfig is the production tuning (same shape as inference-gateway;
// tuned a touch lower on failures because a provider call is heavier than a predict
// and we want to fail over to a healthy backend quickly).
func defaultBreakerConfig() breakerConfig {
	return breakerConfig{
		failureThreshold:  3,
		successThreshold:  1,
		resetTimeout:      30 * time.Second,
		halfOpenMaxProbes: 1,
	}
}

// CircuitBreaker is a single provider's breaker — a pure domain type whose only
// dependency is the injected clock.
type CircuitBreaker struct {
	cfg breakerConfig
	now func() time.Time

	mu               sync.Mutex
	state            CircuitState
	consecutiveFails int
	halfOpenSuccess  int
	halfOpenInFlight int
	lastTransition   time.Time
}

func newCircuitBreaker(cfg breakerConfig, now func() time.Time) *CircuitBreaker {
	return &CircuitBreaker{
		cfg:            cfg,
		now:            now,
		state:          CircuitClosed,
		lastTransition: now(),
	}
}

// State returns the current state, realizing a pending OPEN→HALF_OPEN edge lazily.
func (b *CircuitBreaker) State() CircuitState {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maybeHalfOpen()
	return b.state
}

// Allow reports whether a call may proceed AND reserves a HALF_OPEN probe slot.
// CLOSED → always; OPEN → only after resetTimeout (first such call flips HALF_OPEN
// and reserves a probe); HALF_OPEN → only while in-flight probes < max. The
// check-and-reserve is atomic under one lock (no check-then-act race that would
// flood a sick provider).
func (b *CircuitBreaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.maybeHalfOpen()

	switch b.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
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

// RecordSuccess reports an admitted call succeeded.
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

// RecordFailure reports an admitted call failed. CLOSED → tally; at threshold → OPEN.
// HALF_OPEN → any failure → OPEN and restart the cooldown (provider still sick).
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

// maybeHalfOpen realizes the time-driven OPEN→HALF_OPEN edge lazily on access (no
// per-breaker timer goroutine). Caller must hold the lock.
func (b *CircuitBreaker) maybeHalfOpen() {
	if b.state == CircuitOpen && !b.now().Before(b.lastTransition.Add(b.cfg.resetTimeout)) {
		b.transitionTo(CircuitHalfOpen)
	}
}

// transitionTo moves state and resets per-state counters + the transition clock.
// Caller must hold the lock.
func (b *CircuitBreaker) transitionTo(s CircuitState) {
	b.state = s
	b.lastTransition = b.now()
	b.consecutiveFails = 0
	b.halfOpenSuccess = 0
	b.halfOpenInFlight = 0
}
