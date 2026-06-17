// ports.go — the PORTS (interfaces) the Inference Gateway domain depends on.
//
// ============================================================================
// WHY THESE INTERFACES LIVE IN THE DOMAIN (Hexagonal "consumer-owned ports")
// ============================================================================
//
// The domain is the CONSUMER of persistence, of the rate limiter, and of the
// model-serving backend. In Hexagonal Architecture the consumer owns the port;
// the adapter (Redis, the gRPC client to model-serving, the NATS publisher)
// implements it one layer out. Defining these in `internal/repository` instead
// would create domain → repository → domain (the port signatures reference
// domain types, and the domain service impl references the ports) — an import
// CYCLE Go rejects outright. So they live HERE; adapters import domain and
// implement them: a single inward arrow, no cycle.
//
//	domain  (defines InferenceService + these PORTS + the breaker/splitter) ← stdlib+uuid only
//	   ▲
//	   │ implements
//	redis adapter   model-serving gRPC client   NATS publisher   (all outer)
//
// INTERVIEW: "Where do the rate-limiter and backend-client interfaces belong?"
// With the side that USES them (the domain). The infrastructure conforms to the
// business contract, not the other way round.
// ============================================================================
package domain

import (
	"context"
	"time"
)

// ============================================================================
// RATE LIMITER PORT — token-bucket semantics
// ============================================================================

// RateLimiter is the port the Predict use-case calls to decide whether a request
// may proceed, realizing the RATE LIMITING pattern. The contract is TOKEN-BUCKET
// semantics, the standard for API rate limiting (Stripe, AWS API Gateway):
//
//	A bucket holds up to `burst` tokens and refills at `rate` tokens/sec. Each
//	request tries to take 1 token. Token available → Allow returns true (and the
//	token is consumed). Empty bucket → false (reject, 429). The bucket smooths
//	bursts (you can spend up to `burst` at once) while bounding the sustained
//	rate — superior to a fixed-window counter, which allows 2x burst at a window
//	boundary, and to a leaky bucket, which can't burst at all.
//
// WHY a PORT and not an in-domain implementation: the authoritative bucket state
// is shared across all gateway replicas, so it lives in Redis (atomic Lua
// INCR/refill) — infrastructure. The domain owns only the CONTRACT and the
// DECISION semantics. A pure in-memory token bucket also implements this port
// and is used in unit tests, proving the use-case wiring without Redis.
//
// IDEMPOTENCY/COST: each call attempts to consume exactly one token for `key`
// (the principal — api_key_id). The implementation must be atomic per key so two
// concurrent replicas can't both spend the last token (Redis Lua / WATCH).
type RateLimiter interface {
	// Allow attempts to consume one token for key. Returns true if a token was
	// available (request may proceed) and false if the bucket was empty (reject
	// with ErrRateLimited). An error is an INFRASTRUCTURE failure (Redis down):
	// the use-case decides the fail-open vs fail-closed policy, not the limiter.
	Allow(ctx context.Context, key string) (allowed bool, err error)
}

// ============================================================================
// MODEL-SERVER CLIENT PORT — the backend the gateway forwards to
// ============================================================================

// ModelServerClient is the port the use-case calls (THROUGH THE CIRCUIT BREAKER)
// to forward a prediction to a model-serving backend. The adapter is a gRPC
// client to a model-serving pod, dialed by the SERVER-RESOLVED endpoint from the
// route target (never a client-supplied address — anti-SSRF).
//
// WHY the endpoint is a parameter, not embedded in the client: one gateway
// process talks to MANY backends (one per version); the client multiplexes over
// endpoints. The use-case passes the endpoint the splitter chose.
//
// ERROR CONTRACT (drives the breaker): the adapter returns the domain sentinels
// the breaker understands — ErrTimeout when the call exceeds its deadline,
// ErrUpstream when the backend returns an error/garbage. Returning a typed
// sentinel (not a raw gRPC status) keeps the breaker and the use-case free of
// transport types — the adapter maps gRPC codes → domain errors at the boundary.
type ModelServerClient interface {
	// Predict forwards one inference to the backend at endpoint and returns its
	// outputs. The ctx carries the per-call deadline (the use-case sets it); a
	// deadline breach surfaces as ErrTimeout, a backend error as ErrUpstream.
	Predict(ctx context.Context, endpoint string, in PredictInput) (PredictResult, error)
}

// ============================================================================
// ROUTE STORE PORT — the routing table's persistence
// ============================================================================

// RouteStore is the port for the routing table. The authoritative copy is held
// in process (an in-memory map for hot-path lookups with no network on the
// critical path — exactly how Envoy holds its xDS-pushed config) and mirrored to
// Redis so a freshly-started replica can warm its table without replaying every
// past event. The adapter implements both halves; the domain sees one contract.
//
// CONCURRENCY: Get is on the hot path (every Predict) and Upsert/Delete are
// driven by events and admin RPCs — the adapter must make these safe for
// concurrent use (RWMutex over the in-memory map). The domain port simply
// promises that contract.
//
// SNAPSHOT ISOLATION (a contract, not a suggestion): Get MUST return a route the
// caller can read freely without holding any lock and that NO concurrent Upsert
// can mutate underneath it — i.e. a DEEP COPY (the Targets slice copied
// element-wise, not just a slice-header copy that shares the backing array). WHY
// this is part of the contract and not left to the adapter's discretion: the
// authoritative copy is in-memory and the hot path scans route.Targets while
// event consumers Upsert new splits on other goroutines. A shared backing array
// is a textbook data race (Predict's splitter reads WeightBps/Status while a
// promote/reweight writes them) AND it let a rejected control-plane write corrupt
// the stored route. Copy-on-read + atomic copy-on-write (Upsert replaces the map
// entry with the caller's already-validated Route) gives every reader a stable,
// immutable snapshot. The domain ALSO defends in depth (it never mutates a
// fetched Route in place — see SetTrafficSplit/ApplyModel* cloneTargets), so the
// two layers are belt-and-suspenders: either alone closes the race; together they
// make the invariant impossible to break by a future careless edit on one side.
type RouteStore interface {
	// Get returns the route for a model name. Returns ErrNoRoute if absent (the
	// use-case maps that straight to a NO_ROUTE failure). The returned Route is a
	// DEEP COPY (see SNAPSHOT ISOLATION above): callers may read it without locking
	// and a concurrent Upsert never mutates it.
	Get(ctx context.Context, modelName string) (Route, error)
	// Upsert creates or replaces a model's route (used by event consumers and
	// the admin control plane). The caller passes a fully-validated Route; the
	// adapter stores it atomically (swap the map entry under the write lock) so a
	// concurrent Get sees either the whole old route or the whole new one.
	Upsert(ctx context.Context, route Route) error
	// Delete removes a model from the table (ModelUndeployed/Archived, DeleteRoute).
	// Deleting an absent route is a safe no-op (idempotent consumers).
	Delete(ctx context.Context, modelName string) error
	// List returns a page of routes for the operator dashboard / CLI. pageSize is
	// already capped by the handler; the store honors the cursor.
	List(ctx context.Context, opts ListOptions) (routes []Route, nextToken string, err error)
}

// ListOptions carries cursor-based pagination. WHY cursor and not LIMIT/OFFSET:
// a cursor is stable under concurrent writes (the routing table mutates from
// events while an operator pages through it) where OFFSET can skip or duplicate
// rows when a row is inserted/removed between pages.
type ListOptions struct {
	PageSize  int    // 0 = use a default; the handler caps the max
	PageToken string // opaque cursor; empty = from the start
}

// ============================================================================
// QUOTA CHECKER PORT — the pre-flight billing gate
// ============================================================================

// QuotaChecker is the port the use-case calls FIRST (before spending a rate-limit
// token) to reject callers whose TEAM is over its billing quota. The state is an
// eventually-consistent cache (Redis) flipped to "blocked" by the QuotaExceeded
// event the gateway consumes off the bus, and flipped back when usage resets.
//
// WHY eventual consistency is fine here: a team that just crossed its quota might
// get a few more predicts through before the cache flips — acceptable for a soft
// usage cap (Billing reconciles the exact count from the metering events). The
// gateway is not the billing source of truth; it is a fast pre-flight reject so
// the platform stops doing unbillable work promptly. Distinct from the rate
// limiter: quota is a billing-period cap (slow, per-team), rate limiting is
// throughput shaping (fast, per-key).
//
// WHY team and not api_key_id: quota is metered at the TENANT (team) level — all
// of a team's keys share one quota. The team comes from the verified claims
// (Principal.Team), never a request field.
type QuotaChecker interface {
	// IsBlocked reports whether the team is currently over quota. A pure
	// boolean (no error): a cache miss / infra hiccup is treated as "not blocked"
	// (fail-open) inside the adapter — we do NOT want a transient Redis blip to
	// reject all traffic platform-wide. (Contrast the rate limiter, where the
	// use-case decides the policy; here the soft-cap nature makes fail-open the
	// right default and the adapter owns it.)
	IsBlocked(ctx context.Context, team string) bool
}

// ============================================================================
// BREAKER REGISTRY PORT — where per-backend breaker state lives
// ============================================================================

// BreakerRegistry hands out (and persists the shape of) the per-backend circuit
// breakers. Breakers are PER (model, version), not per model — v2's breaker can
// be OPEN while v1 stays CLOSED, which is exactly what protects you during a bad
// canary. The use-case asks the registry for the breaker guarding the chosen
// backend before forwarding.
//
// WHY a port and not just a map in the impl: in a multi-replica deployment the
// breaker's failure counts can be shared (so one replica's observation of a sick
// backend trips the breaker for all) via Redis — an infrastructure concern. The
// CircuitBreaker TYPE (circuit_breaker.go) is pure domain logic; the registry
// PORT is how the impl obtains/stores them. A simple in-memory registry also
// implements this port for single-replica deployments and tests.
type BreakerRegistry interface {
	// Get returns the breaker guarding (modelName, version), creating it in the
	// CLOSED state on first use. The returned *CircuitBreaker is the live object
	// the use-case calls Allow/RecordSuccess/RecordFailure on.
	Get(modelName, version string) *CircuitBreaker
	// Snapshot returns a read-only view of all breakers for the observability
	// RPCs (GetCircuitState / ListCircuitStates). Empty filter = all.
	Snapshot(modelNameFilter string) []CircuitSnapshot
}

// CircuitSnapshot is a read-only, point-in-time view of one breaker for the
// observability RPCs and dashboards. It turns an invisible failure mode ("why is
// v3 getting no traffic?") into an observable one.
type CircuitSnapshot struct {
	ModelName           string
	Version             string
	State               CircuitState
	ConsecutiveFailures int
	LastTransitionAt    time.Time
}

// ============================================================================
// EVENT PUBLISHER PORT — the async outcome of every prediction
// ============================================================================

// EventPublisher is the port the use-case calls to emit the canonical inference
// events (InferenceCompleted on success, InferenceFailed on failure). The
// adapter packs the domain payload into the NATS EventEnvelope and publishes to
// fp.inference.completed / fp.inference.failed.
//
// WHY publishing is a PORT (not a direct NATS call in the use-case): the domain
// must not import NATS. The use-case decides WHAT business fact occurred and
// hands a pure payload to the port; the adapter owns the envelope, subject,
// trace propagation, and at-least-once delivery. Publishing is best-effort and
// OFF the response path — a publish error must NOT fail a successful prediction
// (the client already has its answer); the impl logs and moves on.
//
// IMPLEMENTATION NOTE (what a later layer adds): the NATS adapter wraps each
// payload in EventEnvelope{id,type,source,timestamp,correlation_id,data},
// converts the domain summaries to events.v1 messages, and publishes idempotently
// (request_id is the business dedupe handle Billing keys on). The domain provides
// the payload; the adapter provides the transport guarantees.
type EventPublisher interface {
	// PublishCompleted emits InferenceCompleted (best-effort). An error is logged
	// by the caller, not returned to the client.
	PublishCompleted(ctx context.Context, ev InferenceCompleted) error
	// PublishFailed emits InferenceFailed (best-effort).
	PublishFailed(ctx context.Context, ev InferenceFailed) error
}

// FailureReason is the domain's resilience-failure taxonomy. It is the SAME set
// of values as events.v1.InferenceFailureReason; the handler/event adapter maps
// this 1:1 to the generated enum at the boundary. WHY mirror it in the domain
// rather than import the generated enum here: the domain must stay free of gen/go
// imports (purity rule), and this set is small, stable, and central to the
// breaker/use-case logic. The adapter owns the (trivial, total) mapping.
type FailureReason int

const (
	FailureReasonUnspecified FailureReason = iota
	FailureReasonNoRoute
	FailureReasonRateLimited
	FailureReasonBulkheadFull
	FailureReasonCircuitOpen
	FailureReasonUpstreamError
	FailureReasonTimeout
	FailureReasonInvalidInput
	FailureReasonQuotaExceeded
)

// InferenceCompleted is the domain payload for the success event. The adapter
// converts it to events.v1.InferenceCompleted. It carries IDs + the served
// version + latency — never raw tensors and never an end-user identity (PII
// discipline; only the api_key_id REFERENCE travels). The (statistical)
// prediction/feature summaries that the canonical event also carries are added
// by a later layer that computes them from the tensors; the use-case provides
// the routing/billing facts it alone knows.
type InferenceCompleted struct {
	RequestID   string        // gateway-minted; Billing's dedupe key + Monitor's join key
	ModelName   string        // the model that served
	Version     string        // the version that ACTUALLY served, post split
	APIKeyID    string        // billed principal (id only, never the secret)
	IsCanary    bool          // true when a non-stable target served
	Latency     time.Duration // observed backend latency
	CompletedAt time.Time     // gateway clock
}

// InferenceFailed is the domain payload for the failure event. It carries the
// failure CLASSIFICATION (which resilience pattern fired) — the thing that drives
// Monitor's error-rate signal and Notification's alerts — but no summaries (on
// failure there may be no prediction to summarize, and we avoid echoing a
// possibly-malformed input).
type InferenceFailed struct {
	RequestID string
	ModelName string
	Version   string // the version attempted, empty if it failed before selection (NO_ROUTE)
	APIKeyID  string
	Reason    FailureReason
	Message   string // human-readable, PII-free
	FailedAt  time.Time
}
