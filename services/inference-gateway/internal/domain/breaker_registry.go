// breaker_registry.go — the in-process BreakerRegistry: one breaker per backend.
//
// ============================================================================
// WHY A REGISTRY (and why it's the DEFAULT, in-memory adapter living in domain)
// ============================================================================
//
// Breakers are PER (model, version) — v2's breaker can be OPEN while v1 stays
// CLOSED, which is exactly what protects you during a bad canary (a sick new
// version is quarantined without taking down the stable one). The registry is
// the lazy factory + lookup that hands the use-case the right breaker for the
// backend the splitter chose, creating it CLOSED on first use.
//
// This in-memory registry is a legitimate DOMAIN object, not infrastructure: it
// holds pure CircuitBreaker values and a mutex, no I/O. It implements the
// BreakerRegistry PORT so the use-case depends on the interface. In a
// multi-replica deployment a Redis-backed registry (sharing failure counts
// across replicas so one replica's observation trips the breaker for all) would
// implement the same port; for a single replica (and for tests) this in-memory
// one is correct and fastest. Keeping the simple impl in the domain means the
// service runs fully without any external breaker store.
//
// CONCURRENCY: Get is on the hot path (every Predict). We use an RWMutex: the
// common case (breaker already exists) takes only a read lock; the rare case
// (first sighting of a backend) upgrades to a write lock with a double-check to
// avoid creating two breakers for the same key in a race.
// ============================================================================
package domain

import (
	"sort"
	"sync"
	"time"
)

// BreakerTuning is the public, exported tuning the wiring (main.go / tests) pass
// in. It mirrors the unexported breakerConfig but is the stable external knob
// surface (so callers don't touch internal field names). Zero fields fall back
// to production defaults in NewBreakerRegistry.
type BreakerTuning struct {
	FailureThreshold  int
	SuccessThreshold  int
	ResetTimeout      time.Duration
	HalfOpenMaxProbes int
}

// breakerRegistry is the in-memory BreakerRegistry implementation.
type breakerRegistry struct {
	cfg breakerConfig
	now func() time.Time

	mu       sync.RWMutex
	breakers map[breakerKey]*CircuitBreaker
}

// breakerKey identifies a backend. A struct key (vs a "model|version" string)
// avoids any delimiter-collision ambiguity and is a cheap comparable map key.
type breakerKey struct {
	model   string
	version string
}

// NewBreakerRegistry builds an in-memory registry. Unspecified (zero) tuning
// fields fall back to defaultBreakerConfig() so a caller can override just the
// knobs it cares about. now is injected for testable time (production: time.Now).
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
		breakers: make(map[breakerKey]*CircuitBreaker),
	}
}

// Get returns the breaker for (model, version), creating it CLOSED on first use.
func (r *breakerRegistry) Get(model, version string) *CircuitBreaker {
	key := breakerKey{model: model, version: version}

	// Fast path: read lock, breaker already exists (the overwhelming common case).
	r.mu.RLock()
	b, ok := r.breakers[key]
	r.mu.RUnlock()
	if ok {
		return b
	}

	// Slow path: create it under a write lock, double-checking in case another
	// goroutine created it between our RUnlock and Lock (the check-then-act race).
	r.mu.Lock()
	defer r.mu.Unlock()
	if b, ok = r.breakers[key]; ok {
		return b
	}
	b = newCircuitBreaker(r.cfg, r.now)
	r.breakers[key] = b
	return b
}

// Snapshot returns a stable, sorted read-only view of all breakers (optionally
// filtered by model) for the observability RPCs. Sorted so ListCircuitStates is
// deterministic across calls (stable pagination, stable dashboards).
func (r *breakerRegistry) Snapshot(modelFilter string) []CircuitSnapshot {
	r.mu.RLock()
	keys := make([]breakerKey, 0, len(r.breakers))
	for k := range r.breakers {
		if modelFilter == "" || k.model == modelFilter {
			keys = append(keys, k)
		}
	}
	r.mu.RUnlock()

	sort.Slice(keys, func(i, j int) bool {
		if keys[i].model != keys[j].model {
			return keys[i].model < keys[j].model
		}
		return keys[i].version < keys[j].version
	})

	out := make([]CircuitSnapshot, 0, len(keys))
	for _, k := range keys {
		// Re-fetch under read lock per key; snapshot() takes the breaker's own lock.
		r.mu.RLock()
		b := r.breakers[k]
		r.mu.RUnlock()
		if b != nil {
			out = append(out, b.snapshot(k.model, k.version))
		}
	}
	return out
}
