// provider_registry.go — the ordered set of enabled providers + routing logic.
//
// ============================================================================
// THE REGISTRY: routing + failover ordering in one place
// ============================================================================
//
// The registry holds the enabled providers IN FAILOVER ORDER (the order they were
// registered = the order the loop tries them). It answers two questions the
// use-case asks:
//
//  1. Candidates(pinned, model) → the ordered list of providers to attempt for
//     this request. If the caller PINNED a provider, that one is tried first (and,
//     if it's the only match, alone). Otherwise the full default order, filtered
//     to providers that can serve the requested model.
//  2. Snapshot(breakers) → the read-only view for ListProviders, each provider's
//     kind/name/models + its live circuit state.
//
// WHY the registry lives in the domain (not internal/providers): it is pure
// routing POLICY (ordering, model-matching, pin-handling) over the Provider port —
// no I/O. The concrete Ollama/Stub adapters live in internal/providers and are
// REGISTERED into this domain registry at wiring time. Keeping the policy in the
// domain means the failover ORDER and the pin semantics are unit-testable with the
// stub alone, no HTTP.
package domain

// ProviderRegistry holds the enabled providers in failover order. It is immutable
// after construction (built once at wiring), so it needs no lock: Candidates and
// Snapshot are pure reads over a fixed slice, safe for concurrent use.
type ProviderRegistry struct {
	ordered []Provider // failover order: index 0 is the preferred provider.
	byKind  map[ProviderKind]Provider
}

// NewProviderRegistry builds the registry from providers in FAILOVER ORDER (first =
// preferred). A later duplicate kind overwrites the earlier in the byKind index but
// keeps its position in the ordered slice (registration order is the contract;
// callers should not register a kind twice).
func NewProviderRegistry(providers ...Provider) *ProviderRegistry {
	byKind := make(map[ProviderKind]Provider, len(providers))
	for _, p := range providers {
		byKind[p.Kind()] = p
	}
	return &ProviderRegistry{ordered: providers, byKind: byKind}
}

// Candidates returns the ordered providers to attempt for a request.
//
//	PINNED (pinned != Unspecified): the caller opted OUT of routing and named a
//	  provider. Return ONLY that provider (no failover to others — honoring the pin
//	  means "use this or fail", which is what a pin is FOR: reproducibility, A/B,
//	  cost control). If the pinned kind isn't registered, return empty (→ ErrNoProvider).
//	UNPINNED: return the full default order, filtered to providers that can serve
//	  the requested model (a provider with an empty Models() serves any model).
//
// WHY a pin disables failover: failover silently substituting a DIFFERENT model
// would violate the caller's explicit choice (they may be measuring that exact
// backend). The unpinned path is where failover is desired and applied.
func (r *ProviderRegistry) Candidates(pinned ProviderKind, model string) []Provider {
	if pinned != ProviderKindUnspecified {
		if p, ok := r.byKind[pinned]; ok && providerServes(p, model) {
			return []Provider{p}
		}
		return nil
	}
	out := make([]Provider, 0, len(r.ordered))
	for _, p := range r.ordered {
		if providerServes(p, model) {
			out = append(out, p)
		}
	}
	return out
}

// providerServes reports whether p can serve model. A provider with no declared
// models (Models() empty) is a wildcard that serves any model name (the Ollama
// adapter, which forwards any model string to the backend, and the stub, which
// answers anything deterministically). A provider WITH a model list serves only a
// listed model; an empty requested model matches a wildcard provider (the gateway
// applies that provider's default model).
func providerServes(p Provider, model string) bool {
	models := p.Models()
	if len(models) == 0 {
		return true // wildcard provider
	}
	if model == "" {
		return true // let the provider apply its default
	}
	for _, m := range models {
		if m == model {
			return true
		}
	}
	return false
}

// Snapshot returns the read-only view for ListProviders, attaching each provider's
// live circuit state from the breaker registry. Order matches the failover order so
// an operator reads the list as "preferred first".
func (r *ProviderRegistry) Snapshot(breakers BreakerRegistry) []ProviderSnapshot {
	out := make([]ProviderSnapshot, 0, len(r.ordered))
	for _, p := range r.ordered {
		state := CircuitClosed
		if breakers != nil {
			state = breakers.State(p.Kind())
		}
		out = append(out, ProviderSnapshot{
			Kind:         p.Kind(),
			Name:         p.Kind().String(),
			Enabled:      true, // a registered provider is enabled by definition.
			Models:       p.Models(),
			CircuitState: state,
		})
	}
	return out
}
