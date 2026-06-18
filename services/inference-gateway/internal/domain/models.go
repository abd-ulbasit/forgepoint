// Package domain is the innermost ring of Clean Architecture for the Inference
// Gateway. It holds the canonical business types and the RESILIENCE CORE of the
// service: the circuit breaker, the traffic splitter, the route table, and the
// Predict use-case that orchestrates them.
//
// ============================================================================
// CLEAN ARCHITECTURE — DOMAIN LAYER (zero framework imports)
// ============================================================================
//
// This package imports ONLY the Go standard library and github.com/google/uuid.
// It has NO imports of gRPC, NATS, database drivers, or generated proto code.
// That is what makes the resilience patterns here independently unit-testable
// (a breaker transition is pure arithmetic over a clock) and lets the same logic
// run in any transport context.
//
//	Dependency rule (arrows point inward — nothing inside points out):
//
//	  cmd/server/main.go         (wires everything)
//	     ↓
//	  internal/handler           (proto ↔ domain conversion)        ─┐
//	     ↓                                                            │ both
//	  internal/domain  ◄──────── internal/repository/redis (adapter) ─┘ implement
//	  (THIS PACKAGE)   ◄──────── internal/events (NATS adapter)         the PORTS
//	  stdlib + uuid only          defined HERE (ports.go)
//
// WHY pure Go types instead of the proto-generated messages: using *.pb.go types
// as business objects couples the breaker math to the wire schema, drags grpc-go
// into a unit test that only checks a state transition, and makes the domain
// unrunnable where proto isn't available. The handler is the ONLY place proto ↔
// domain conversion happens (the anti-corruption layer in DDD terms).
//
// ============================================================================
// WHAT THIS SERVICE IS (the API Gateway + resilience stack, for ML inference)
// ============================================================================
//
// The gateway is the single front door for online prediction traffic. A client
// calls a MODEL NAME; the gateway decides WHICH VERSION serves (traffic split /
// canary), PROTECTS the backends (circuit breaker, rate limit, bulkhead), calls
// the chosen model-serving backend, records the outcome, and emits the canonical
// InferenceCompleted/Failed event. Real-world analogues: Envoy/Istio weighted
// clusters + outlier detection + local rate limiting; SageMaker production
// variants; KServe canary traffic percentage.
package domain

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// ============================================================================
// BASIS POINTS — the canary weight unit
// ============================================================================

// TotalWeightBps is 100% expressed in basis points (per-10,000). Every routable
// model's ACTIVE targets must have weight_bps summing to exactly this.
//
// WHY basis points (integers) and not float percentages: integer bps express a
// fine-grained canary (0.5% = 50 bps) EXACTLY, and a set of weights that must
// sum to a fixed total never suffers float rounding drift (0.1 + 0.2 != 0.3).
// This is the same reason finance quotes in bps. It also makes the weighted
// split a clean integer-modulo operation (see TrafficSplitter) with no FP
// determinism worries across platforms.
const TotalWeightBps = 10000

// ============================================================================
// TARGET STATUS — a routable backend's lifecycle (mirrors the proto enum)
// ============================================================================

// TargetStatus is the lifecycle state of one RouteTarget. SERVER-authoritative:
// clients propose weights, the gateway owns status. A bare bool "active" is
// insufficient — DRAINING (let in-flight finish, take no new traffic) is what
// makes a graceful rollback/promotion possible, mirroring Envoy/K8s endpoint
// draining. UNHEALTHY is set by the circuit breaker, never by a client.
type TargetStatus int

const (
	// TargetStatusUnspecified is the zero value; a target must be given a real
	// status before it can route. Treated as "not eligible".
	TargetStatusUnspecified TargetStatus = iota
	// TargetStatusActive receives traffic per its weight_bps.
	TargetStatusActive
	// TargetStatusDraining takes NO new traffic (effective weight 0) but lets
	// in-flight requests complete. Used during rollback/promotion before removal.
	TargetStatusDraining
	// TargetStatusUnhealthy is quarantined by the circuit breaker (backend sick).
	// The gateway sets this automatically; it is not client-settable.
	TargetStatusUnhealthy
)

// Eligible reports whether a target may receive NEW traffic. Only ACTIVE targets
// are eligible; DRAINING and UNHEALTHY get an effective weight of zero. The
// traffic splitter and route invariants both lean on this single definition so
// "what counts as routable" lives in exactly one place.
func (s TargetStatus) Eligible() bool { return s == TargetStatusActive }

// ============================================================================
// ROUTE TARGET — one candidate version behind a model name
// ============================================================================

// RouteTarget points the gateway at one model VERSION's serving backend, with
// the traffic weight that version should receive.
//
// SECURITY — Endpoint is SERVER-AUTHORITATIVE. It is resolved from the
// ModelDeployed event the gateway consumed, NEVER supplied by an API client.
// Accepting a client-supplied endpoint would be a textbook SSRF hole: the
// gateway would forward traffic (and credentials) to an arbitrary host the
// caller names. See Route.ApplyDeployed / UpsertRoute handling in the impl.
type RouteTarget struct {
	Version   string       // version label, e.g. "v3" — combined with the model name identifies a backend
	Endpoint  string       // serving DNS, e.g. "iris-v3.fp-models.svc:9090". SERVER-resolved (anti-SSRF).
	WeightBps int          // traffic share in basis points (0..10000)
	Status    TargetStatus // SERVER-authoritative lifecycle
	IsStable  bool         // true = the current stable (non-canary) target; false candidates are canaries
}

// ============================================================================
// ROUTE — the routing-table row for one model name
// ============================================================================

// Route is the gateway's indirection for one public model name: the list of
// candidate versions and how traffic splits among them. This indirection is the
// whole point of a gateway — the client's contract ("predict with fraud-detector")
// stays stable while we shift traffic between v1/v2/v3 underneath.
//
// HOW IT'S POPULATED: in the common case the table is driven by EVENTS, not API
// writes. The gateway consumes ModelDeployed/Undeployed/Promoted/Archived and
// mutates routes via the methods below. The admin RPCs (UpsertRoute /
// SetTrafficSplit / DeleteRoute) are the break-glass manual control plane.
type Route struct {
	// OwnerTeam is the TENANT that owns this route. It is the multi-tenancy
	// boundary of the whole routing plane: a route is addressed by the pair
	// (OwnerTeam, ModelName), never by ModelName alone. SERVER-authoritative — it
	// comes from the caller's verified Principal.Team (Predict / control plane) or
	// from the owning team carried on the ModelDeployed event (event reactions),
	// NEVER from a request field a caller could spoof.
	//
	// SECURITY (cross-tenant route IDOR fix): the routing table used to be a GLOBAL
	// namespace keyed only by ModelName. Any authenticated caller — regardless of
	// team — could read, repoint, or delete another team's route by guessing its
	// model name, or hijack a name collision (two teams both deploy "fraud"). The
	// auth interceptor authenticates the JWT but does NOT tenancy-authorize, so
	// authentication alone was not enough. Namespacing every route operation by
	// (team, model_name) — and returning NO_ROUTE (not a distinguishable error) on
	// a cross-team miss so there is no existence oracle — closes that hole: team A
	// literally cannot name team B's route because the store key is derived from A's
	// own claims. See routeKey below and the Predict/Get*/Upsert*/Delete* impls.
	OwnerTeam string
	ModelName string
	Targets   []RouteTarget
	UpdatedAt time.Time
}

// routeKey composes the (team, model) tenancy pair into the single string the
// RouteStore is keyed by. It is the linchpin of the multi-tenant IDOR fix: every
// store access (Get/Upsert/Delete) goes through this, so a route is reachable
// ONLY under its owning team's namespace and one team can never address another's
// route by model name.
//
// WHY compose into the existing string key (rather than widen the RouteStore port
// to take a team parameter): it keeps the port — and its in-memory + Redis
// adapters — unchanged, while making tenancy a property the DOMAIN enforces at the
// single point where it owns the key. The store stays a dumb keyed map; the
// business rule ("routes are per-team") lives in the layer that should own it.
//
// FORMAT: "<team>\x00<model>". The NUL separator can't appear in a team or model
// name (both are identifier-like tokens from claims / the registry), so the
// mapping is injective — ("a","b:c") and ("a:b","c") can never collide into the
// same key the way a plain ":" join could. An empty team yields a distinct
// "\x00<model>" namespace, so an unauthenticated/teamless principal can never
// alias a real tenant's route either.
func routeKey(team, modelName string) string {
	return team + "\x00" + modelName
}

// ActiveWeightSum returns the sum of weight_bps across ELIGIBLE (active) targets.
// Draining/unhealthy targets contribute 0 — their weight is intentionally not
// counted so a draining canary doesn't leave the split summing below 10000 from
// the splitter's point of view (the splitter only ever sees eligible targets).
func (r Route) ActiveWeightSum() int {
	sum := 0
	for _, t := range r.Targets {
		if t.Status.Eligible() {
			sum += t.WeightBps
		}
	}
	return sum
}

// IsServing reports whether the model is currently routable (has at least one
// eligible target). A non-serving model fails predicts with NO_ROUTE.
func (r Route) IsServing() bool {
	for _, t := range r.Targets {
		if t.Status.Eligible() {
			return true
		}
	}
	return false
}

// findTarget returns a pointer to the target with the given version, or nil.
// Unexported: callers mutate routes only through the named domain methods below
// so every mutation goes through one validated path.
//
// MUTATION DISCIPLINE: the pointer findTarget hands back aliases an element of
// r.Targets' backing array. The control-plane / event-reaction methods MUST NOT
// write through it when r was obtained from RouteStore.Get and will also be
// served to readers, because:
//   - ATOMICITY: writing in place before the weight-sum validates leaves a
//     REJECTED proposal committed to the stored route (validate-then-commit is
//     broken). See SetTrafficSplit/ApplyModelPromoted — they build a fresh
//     []RouteTarget via cloneTargets and only Upsert after validation.
//   - DATA RACE: the hot path (Predict → splitter.Pick) reads those same slice
//     elements concurrently with event reactions. Copy-on-write + atomic Upsert
//     means a reader always sees a fully-formed, immutable snapshot.
// findTarget therefore stays a pure read helper (selectTarget reads through it);
// no method writes through the returned pointer.
func (r *Route) findTarget(version string) *RouteTarget {
	for i := range r.Targets {
		if r.Targets[i].Version == version {
			return &r.Targets[i]
		}
	}
	return nil
}

// cloneTargets returns a DEEP COPY of the route's target slice: a new backing
// array with each RouteTarget value-copied. WHY this is the linchpin of both the
// atomicity and the race fixes:
//
//   - COPY-ON-WRITE for mutations: a control-plane/event method clones, mutates
//     the COPY, validates the copy, and only then Upserts. A rejected proposal
//     never touches the stored route — true validate-then-commit.
//   - SNAPSHOT ISOLATION for the store: the in-memory RouteStore adapter clones
//     on Get (and Upsert) so the Route a reader holds can never be mutated out
//     from under it by a concurrent writer. The per-element copy matters because
//     a plain `append`/slice-header copy still SHARES the backing array — the
//     exact aliasing that let a rejected SetTrafficSplit corrupt stored weights
//     and let Predict race the event consumers.
//
// RouteTarget is all value fields (no pointers/slices), so a per-element copy is
// a complete deep copy. nil in → nil out (preserves "no targets" exactly).
func (r Route) cloneTargets() []RouteTarget {
	if r.Targets == nil {
		return nil
	}
	out := make([]RouteTarget, len(r.Targets))
	copy(out, r.Targets) // element-wise value copy; RouteTarget has no reference fields
	return out
}

// ============================================================================
// CIRCUIT-BREAKER STATE — the 3-state machine label (mirrors the proto enum)
// ============================================================================

// CircuitState is the breaker's current mode. The full transition machine lives
// on CircuitBreaker (circuit_breaker.go); this is just the label other layers
// (handlers, dashboards) read.
//
//	┌─────────┐  failures ≥ threshold   ┌──────┐
//	│ CLOSED  │ ──────────────────────► │ OPEN │  (fail fast)
//	└─────────┘ ◄── successes ≥ ──┐     └──────┘
//	     ▲       success_threshold │        │ after reset_timeout
//	     │                    ┌──────────┐  │
//	     └──── any failure ───│ HALF_OPEN│◄─┘  (send limited probes)
//	                          └──────────┘
type CircuitState int

const (
	// CircuitClosed is normal operation: requests pass through, failures counted.
	CircuitClosed CircuitState = iota
	// CircuitOpen is tripped: requests fail fast without hitting the backend.
	CircuitOpen
	// CircuitHalfOpen is probing: a limited number of trial requests test recovery.
	CircuitHalfOpen
)

// String gives breakers a readable label in logs/metrics and test failures.
func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "CLOSED"
	case CircuitOpen:
		return "OPEN"
	case CircuitHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

// ============================================================================
// PREDICT INPUT / OUTPUT — the hot-path domain types
// ============================================================================

// Tensor is the domain's transport-agnostic view of one named tensor. The
// handler converts proto TensorData ↔ this; the domain only inspects shape/dtype
// for validation and forwards Data opaquely (zero-copy forwarding — the gateway
// never deserializes the numbers it doesn't need to read). DType/Shape are kept
// so the domain can length-validate (len(Data) == product(Shape) * elemSize)
// before forwarding, catching client mistakes at the edge.
type Tensor struct {
	Shape []int64
	DType string // e.g. "float32"; opaque label carried for validation/forwarding
	Data  []byte // densely packed, row-major; opaque to the gateway
}

// PredictInput is the validated, transport-free input to the Predict use-case.
// Note what is ABSENT and why (security defaults):
//   - No api_key_id / team / owner field. The billed principal and tenant come
//     from the verified auth claims (Principal), NEVER a request field — a
//     client-supplied principal would be account-takeover-for-billing (the
//     inference event meters against APIKeyID).
//   - VersionOverride is a PRIVILEGED escape hatch the handler authorizes before
//     it reaches here; ordinary traffic leaves it empty and is split by weight.
type PredictInput struct {
	ModelName       string            // required; the public model name to predict with
	Inputs          map[string]Tensor // named input tensors
	VersionOverride string            // optional privileged pin; empty = weighted split (the norm)
	IdempotencyKey  string            // optional; lets the gateway dedupe retries to avoid double-billing
}

// Principal is the caller identity, taken from the verified token by the handler
// and passed into the use-case. SERVER-authoritative end to end.
type Principal struct {
	APIKeyID string // the calling key's id (never the raw secret) — billed/metered against
	Team     string // tenant; used for the pre-flight quota check
}

// PredictOutput is the result the use-case returns. ServedVersion, RequestID and
// Latency are SERVER-authoritative — they reflect what the gateway CHOSE and
// OBSERVED, never what the client requested. The client asked for a model; it
// MUST be told which version actually answered (for A/B analysis, debugging, and
// to correlate with the InferenceCompleted event).
type PredictOutput struct {
	Outputs       map[string]Tensor
	ServedVersion string
	IsCanary      bool          // true if a non-stable (canary) version served — carried onto the event
	Latency       time.Duration // backend latency the gateway observed
	RequestID     string        // gateway-minted UUID; echoed to client AND onto the event
}

// ============================================================================
// MODEL-SERVER RESPONSE — what the backend port returns
// ============================================================================

// PredictResult is the model-serving backend's reply as the ModelServerClient
// port surfaces it. The use-case forwards Outputs and times the call itself; the
// port returns only the payload so timing/outcome classification stay in the
// domain (the breaker records based on the error, the latency is measured around
// the call).
type PredictResult struct {
	Outputs map[string]Tensor
}

// newRequestID mints the server-authoritative request id: a hand-rolled RFC 4122
// version-4 UUID from crypto/rand.
//
// WHY hand-roll it from crypto/rand instead of github.com/google/uuid: it keeps
// the DOMAIN layer dependency-free of even that small library. google/uuid
// implements database/sql's driver.Valuer/Scanner, so importing it transitively
// drags database/sql/driver into the domain's dependency graph — harmless in
// fact, but it trips the "no database imports in domain" purity check. Minting
// the UUID with 16 crypto/rand bytes + the standard version/variant bits gives an
// identical, equally-unguessable id with ZERO non-stdlib imports. The id is the
// client's correlation handle AND Billing's business idempotency key on the
// emitted event, so it must be globally unique without coordination — 122 bits of
// entropy guarantees that.
func newRequestID() string {
	var b [16]byte
	// crypto/rand.Read never returns a short read; it errors only on a broken
	// system entropy source. We panic on that — a gateway that cannot generate a
	// unique request id cannot safely bill/trace a prediction, so failing loudly
	// at the source is correct (this path is effectively unreachable in practice).
	if _, err := rand.Read(b[:]); err != nil {
		panic("inference: crypto/rand failed generating request id: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4 (random)
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	// Canonical 8-4-4-4-12 hyphenated form.
	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:])
}
