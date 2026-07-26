package events

import "context"

// ============================================================================
// MonitorResolver — the AUTHORIZED-BINDING port (model name → monitor binding)
// ============================================================================
//
// THE PROBLEM IT SOLVES: an InferenceCompleted / ModelPromoted event carries NO
// tenancy and NO monitor reference (events are decoupled from the monitor's data
// model). But the domain's data-plane methods need an AUTHORIZED binding:
//   - ObserveInference needs MonitorID + OwnerTeam stamped on the observation.
//   - ResetBaselineFromPromotion needs the OwnerTeam to reuse the team-scoped path.
// The domain must NOT derive a tenant from untrusted event data (a cross-tenant
// footgun — see the InferenceObservation doc in window.go). So the ADAPTER
// resolves the binding from the monitor's OWN store BEFORE calling the domain.
//
// WHY A SEPARATE PORT (not domain.MonitorRepository.GetByModel): GetByModel is
// (ownerTeam, modelName) — it REQUIRES the team, which the event doesn't have.
// What the consumer needs is the inverse lookup "given only a model name, which
// monitor (and whose team) governs it?". That is an events-plane concern, so the
// port is declared HERE (the consumer owns the port it depends on) and the
// production adapter implements it with a `SELECT id, owner_team FROM monitors
// WHERE model_name = $1 AND state != deleted` over the SAME monitors table the
// repository writes. Tests inject a fake.
//
// THE MODEL-NAME UNIQUENESS CAVEAT (an honest tradeoff):
//   Monitors are keyed by (owner_team, model_name), so a model name is NOT globally
//   unique — team-a/"fraud" and team-b/"fraud" are different monitors. An inference
//   event names only the model ("fraud") plus the served version + api_key_id; it
//   does NOT carry the team. The CORRECT long-term resolution joins the api_key_id
//   → team (via the auth/registry mapping) to disambiguate. For THIS adapter we
//   model the resolution as a single binding per model name and document the
//   limitation: in the common case a model name maps to one monitored model; the
//   multi-tenant-same-name disambiguation is a follow-up (it needs an api_key→team
//   resolver that is itself another service's concern). The port returns found=false
//   when no monitor governs the model — the safe default (ACK + drop, never fold an
//   unmonitored model). This keeps the cross-tenant SAFETY property (we never
//   fabricate a team) while being honest that same-name disambiguation is deferred.

// MonitorBinding is the authorized (monitor id, owner team) pair the adapter
// resolves for a model name before invoking a domain data-plane method.
type MonitorBinding struct {
	MonitorID string // the monitor governing this model (domain stamps it on the observation)
	OwnerTeam string // the tenancy fact (server-authoritative, from the monitor row)
}

// MonitorResolver resolves a model name to its monitor binding. found=false means
// no monitor governs the model (the event is ACKed and dropped). An error means a
// transient lookup failure (the handler NAKs → JetStream redelivers).
type MonitorResolver interface {
	// ResolveByModel returns the (monitor id, owner team) for a model name, or
	// found=false when the model is not monitored. The production adapter scopes the
	// query to non-deleted monitors so a soft-deleted monitor stops folding traffic.
	ResolveByModel(ctx context.Context, modelName string) (binding MonitorBinding, found bool, err error)
}
