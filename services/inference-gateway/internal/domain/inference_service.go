// inference_service.go — the InferenceService interface: the primary port the
// handler layer reaches business logic through.
//
// ============================================================================
// THE SERVICE INTERFACE AS A "PORT" (the use-case contract)
// ============================================================================
//
// The handler depends on this ABSTRACTION, not on the concrete impl. That lets
// us swap implementations (the real resilience-stack impl vs. an in-memory stub)
// without touching handler code, and lets handler unit tests inject a mock
// service (no Redis, no backend). The domain owns this interface; the handler
// consumes it; the impl (inference_service_impl.go) lives in this same package
// because it IS business logic.
//
// The methods fall into three groups that mirror the proto's RPC categories:
//
//	DATA PLANE      — Predict (the hot path; the resilience stack runs here)
//	CONTROL PLANE   — UpsertRoute / SetTrafficSplit / DeleteRoute / Get*/List*
//	EVENT REACTIONS — ApplyModelDeployed/Undeployed/Promoted/Archived (the
//	                  routing table is driven by events, not API writes)
//
// Batch/stream predict and the circuit observability reads are intentionally a
// thin orchestration over Predict + the breaker registry; they are added when
// the handler/adapters land (noted in the impl). The centerpiece — the
// resilience CORE — is fully realized here at the domain level.
// ============================================================================
package domain

import "context"

// InferenceService is the primary domain interface for the gateway.
type InferenceService interface {
	// -----------------------------------------------------------------------
	// DATA PLANE
	// -----------------------------------------------------------------------

	// Predict runs ONE inference through the full resilience stack and returns
	// the outputs plus the version that served and the latency observed. It is
	// the orchestrator the whole service exists for. The ordered steps:
	//
	//   1. VALIDATE input (model name present, inputs non-empty, tensor lengths).
	//      Bad input → ErrInvalidInput (never counted against a backend).
	//   2. QUOTA pre-flight: if the principal's team is flagged over-quota in the
	//      cache, reject with ErrQuotaExceeded (cheap, before spending a token).
	//   3. RATE LIMIT: take one token from the principal's bucket. Empty →
	//      ErrRateLimited. (Token bucket — see RateLimiter port.)
	//   4. ROUTE lookup: fetch the model's Route. Not routable → ErrNoRoute.
	//   5. TRAFFIC SPLIT: pick the target version by weighted selection (or honor
	//      a privileged VersionOverride). The CLIENT does not choose the version.
	//   6. CIRCUIT BREAKER: ask the chosen backend's breaker for permission. OPEN
	//      (or no HALF_OPEN probe slot) → fail fast with ErrCircuitOpen, no
	//      backend call.
	//   7. FORWARD through the breaker: call ModelServerClient.Predict with a
	//      deadline; record success/failure on the breaker either way.
	//   8. RECORD OUTCOME: emit InferenceCompleted (success) or InferenceFailed
	//      (failure) via the EventPublisher — best-effort, off the response path.
	//
	// All SERVER-authoritative fields (RequestID, ServedVersion, IsCanary,
	// Latency) are set by the gateway from what it CHOSE and OBSERVED, never from
	// the request. Errors are the domain sentinels (errors.go), which the handler
	// maps to gRPC status codes and the publisher maps to InferenceFailureReason.
	Predict(ctx context.Context, p Principal, in PredictInput) (PredictOutput, error)

	// -----------------------------------------------------------------------
	// CONTROL PLANE (break-glass + the canary dial)
	// -----------------------------------------------------------------------

	// GetRoute returns the current routing-table entry for a model (operator view,
	// includes endpoints). ErrNoRoute if absent OR owned by a different team.
	//
	// TENANCY (cross-tenant IDOR fix): team comes from the caller's verified claims;
	// the route is looked up under (team, modelName). A route owned by another team
	// is reported as ErrNoRoute — identical to "doesn't exist" — so there is NO
	// existence oracle that would let team A confirm team B owns a given name.
	GetRoute(ctx context.Context, team, modelName string) (Route, error)

	// ListRoutes returns a page of the CALLER'S routing table (operator dashboard),
	// scoped to team — a team only ever sees its own routes.
	ListRoutes(ctx context.Context, team string, opts ListOptions) (routes []Route, nextToken string, err error)

	// UpsertRoute creates or replaces a model's route (break-glass manual control;
	// routes are normally event-driven). SECURITY: only version + weight_bps are
	// honored from each proposed target; the endpoint is SERVER-RESOLVED from the
	// existing route (anti-SSRF) and status/updated_at are server-owned. The
	// weights of ACTIVE targets must sum to 10000 → else ErrRouteValidation.
	// TENANCY: scoped to team — a caller can only create/replace routes UNDER its
	// own team, and endpoint resolution reads only that team's existing route.
	UpsertRoute(ctx context.Context, team, modelName string, proposed []ProposedTarget) (Route, error)

	// SetTrafficSplit adjusts ONLY the version weights of an existing route — the
	// canary dial (90/10 → 50/50 → 0/100). Each version must already exist in the
	// route and the new ACTIVE weights must sum to 10000, else ErrRouteValidation.
	// Narrow by design: a caller cannot smuggle in an endpoint/status change.
	// TENANCY: scoped to team; a cross-team model name resolves to ErrNoRoute.
	SetTrafficSplit(ctx context.Context, team, modelName string, weights []TrafficWeight) (Route, error)

	// DeleteRoute removes a model from routing entirely. Idempotent (deleting an
	// absent route is a no-op). Subsequent predicts get ErrNoRoute. TENANCY: scoped
	// to team — deleting a name the team does not own is a no-op against ANOTHER
	// team's route (it only deletes within the caller's own namespace).
	DeleteRoute(ctx context.Context, team, modelName string) error

	// -----------------------------------------------------------------------
	// RESILIENCE OBSERVABILITY
	// -----------------------------------------------------------------------

	// CircuitStates returns a snapshot of breaker states (optional model filter)
	// for GetCircuitState / ListCircuitStates. Read-only.
	CircuitStates(modelNameFilter string) []CircuitSnapshot

	// -----------------------------------------------------------------------
	// EVENT REACTIONS (the routing table is a pure reactor on the control bus)
	// -----------------------------------------------------------------------

	// ApplyModelDeployed ADDS or updates a route target from a ModelDeployed
	// event. The endpoint and initial weight come from the EVENT (server-resolved
	// by the deploy saga), never a client. New non-stable targets are canaries.
	ApplyModelDeployed(ctx context.Context, ev ModelDeployed) error

	// ApplyModelUndeployed REMOVES a route target (a saga removed a version from
	// serving). If it was the model's last target the route becomes non-serving.
	ApplyModelUndeployed(ctx context.Context, ev ModelUndeployed) error

	// ApplyModelPromoted repoints traffic to a newly-promoted PRODUCTION version
	// (100% to it, mark it stable) and tears down the auto-demoted old one.
	ApplyModelPromoted(ctx context.Context, ev ModelPromoted) error

	// ApplyModelArchived DROPS the model's route entirely (the model is going away).
	ApplyModelArchived(ctx context.Context, ev ModelArchived) error
}

// ============================================================================
// CONTROL-PLANE INPUT TYPES (deliberately narrow — anti mass-assignment)
// ============================================================================

// ProposedTarget is what a caller may propose on UpsertRoute: a version and a
// weight, NOTHING ELSE. Endpoint/status/updated_at are server-owned. WHY a
// dedicated type and not RouteTarget: accepting a full RouteTarget would let a
// caller set the endpoint (SSRF) or force a status — exactly the mass-assignment
// vulnerability this type closes by construction.
type ProposedTarget struct {
	Version   string
	WeightBps int
}

// TrafficWeight is the minimal (version, weight) payload for the canary dial.
// Like ProposedTarget it deliberately omits endpoint/status so SetTrafficSplit
// cannot be used to smuggle in a backend change.
type TrafficWeight struct {
	Version   string
	WeightBps int
}

// ============================================================================
// EVENT-REACTION INPUT TYPES (the domain shapes behind the consumed events)
// ============================================================================
//
// These mirror the business-relevant fields of the events.v1 messages the
// gateway CONSUMES. The events adapter unpacks the proto and calls the matching
// Apply* method with one of these — keeping the domain free of generated types
// while still modeling the exact operation each event triggers.

// ModelDeployed → fp.pipelines.model.deployed. Adds/updates a target.
//
// OwnerTeam is the TENANT the deploy belongs to — the team the resulting route is
// created UNDER. It is server-authoritative (the deploy saga derives it from the
// model's owning team in the registry, never from a client) and is the value that
// namespaces the route so a later Predict from that team — and only that team —
// can reach it. A deploy with an empty owner team lands in the "" namespace, which
// is intentionally unreachable by any authenticated tenant (see routeKey).
type ModelDeployed struct {
	OwnerTeam string // the team that owns the deployed model — namespaces the route (anti cross-tenant IDOR)
	ModelName string
	Version   string
	Endpoint  string // SERVER-resolved by the saga — the only trusted source of a backend address
	WeightBps int    // initial traffic share (a canary typically deploys small, e.g. 1000)
}

// ModelUndeployed → fp.pipelines.model.undeployed. Removes a target. OwnerTeam
// namespaces which team's route the target is removed from — an undeploy for
// team A must not touch team B's identically-named model.
type ModelUndeployed struct {
	OwnerTeam string
	ModelName string
	Version   string
	Reason    string // "rollback"/"superseded"/"scale_to_zero"/"teardown" — for logging, not logic
}

// ModelPromoted → fp.models.promoted. Repoint traffic to the new production
// version; tear down the auto-demoted prior one. OwnerTeam scopes the promotion to
// the owning team's route.
type ModelPromoted struct {
	OwnerTeam      string
	ModelName      string
	Version        string // the newly-promoted production version
	DemotedVersion string // the auto-demoted prior production version (empty if none)
}

// ModelArchived → fp.models.archived. Drop the whole route. OwnerTeam scopes the
// drop to the owning team's namespace.
type ModelArchived struct {
	OwnerTeam string
	ModelName string
}
