// breaker_registry.go — one breaker per PROVIDER (the failover quarantine unit).
//
// Breakers are PER provider: Ollama's breaker can be OPEN (it's cold/refused)
// while the stub's stays CLOSED, so the failover loop skips the sick one and the
// healthy one serves — exactly the canary-quarantine property the inference
// gateway gets per (model,version), here per provider. This in-memory registry is
// pure domain (no I/O); a Redis-backed cross-replica variant could implement the
// same shape later for multi-replica failure sharing. For a single replica (and
// tests) in-memory is correct and fastest.
package domain

import (
	"sync"
	"time"
)

// BreakerTuning is the exported knob surface the wiring/tests pass in. Zero fields
// fall back to defaultBreakerConfig() so a caller overrides only what it cares about.
type BreakerTuning struct {
	FailureThreshold  int
	SuccessThreshold  int
	ResetTimeout      time.Duration
	HalfOpenMaxProbes int
}

// BreakerRegistry hands out the per-provider breaker. The use-case asks for the
// breaker guarding a provider before calling it.
type BreakerRegistry interface {
	// Get returns the breaker for kind, creating it CLOSED on first use.
	Get(kind ProviderKind) *CircuitBreaker
	// State returns kind's current circuit state (for ListProviders).
	State(kind ProviderKind) CircuitState
}

type breakerRegistry struct {
	cfg breakerConfig
	now func() time.Time

	mu       sync.RWMutex
	breakers map[ProviderKind]*CircuitBreaker
}

// NewBreakerRegistry builds the in-memory registry. now is injected for testable
// time (production passes time.Now; a nil now defaults to time.Now).
func NewBreakerRegistry(t BreakerTuning, now func() time.Time) BreakerRegistry {
	cfg := defaultBreakerConfig()
	if t.FailureThreshold > 0 {
		cfg.failureThreshold = t.FailureThreshold
	}
	if t.SuccessThreshold > 0 {
		cfg.successThreshold = t.SuccessThreshold
	}
	if t.ResetTimeout > 0 {
		cfg.resetTimeout = t.ResetTimeout
	}
	if t.HalfOpenMaxProbes > 0 {
		cfg.halfOpenMaxProbes = t.HalfOpenMaxProbes
	}
	if now == nil {
		now = time.Now
	}
	return &breakerRegistry{
		cfg:      cfg,
		now:      now,
		breakers: make(map[ProviderKind]*CircuitBreaker),
	}
}

// Get returns the breaker for kind, lazily creating it CLOSED. Fast path takes a
// read lock (the common case: breaker exists); the rare first-sighting upgrades to
// a write lock with a double-check (the check-then-act race).
func (r *breakerRegistry) Get(kind ProviderKind) *CircuitBreaker {
	r.mu.RLock()
	b, ok := r.breakers[kind]
	r.mu.RUnlock()
	if ok {
		return b
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if b, ok = r.breakers[kind]; ok {
		return b
	}
	b = newCircuitBreaker(r.cfg, r.now)
	r.breakers[kind] = b
	return b
}

// State returns kind's circuit state without forcing a breaker to exist on read —
// an unseen provider is CLOSED (it has taken no failures). We DON'T create the
// breaker here: ListProviders shouldn't mutate the registry as a side effect of a
// read (it would also race with the lazy-create double-check). A never-called
// provider reporting CLOSED is exactly right (nothing has gone wrong yet).
func (r *breakerRegistry) State(kind ProviderKind) CircuitState {
	r.mu.RLock()
	b, ok := r.breakers[kind]
	r.mu.RUnlock()
	if !ok {
		return CircuitClosed
	}
	return b.State()
}
