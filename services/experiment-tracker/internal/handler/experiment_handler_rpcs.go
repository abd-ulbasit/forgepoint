// experiment_handler_rpcs.go holds the per-RPC method bodies for
// ExperimentHandler.
//
// The scaffold (experiment_handler.go) embeds
// UnimplementedExperimentTrackerServiceServer and holds the
// domain.ExperimentService field. This file OVERRIDES each RPC with a real
// implementation. Once a method is defined here on *ExperimentHandler, Go's
// method-set resolution prefers it over the embedded Unimplemented base — so
// every RPC below is live, and only RPCs we have NOT written fall through to
// Unimplemented (none remain for this service; all 15 are here).
//
// ============================================================================
// THE FOUR-STEP HANDLER CONTRACT (every method follows it)
// ============================================================================
//
//  1. NIL-SVC GUARD. A binary may be wired with svc==nil during the scaffold /
//     pre-repo phase (main.go passes nil until the Postgres + NATS adapters
//     land). Calling a nil interface's method panics, so every method first
//     checks h.svc==nil and returns codes.Unimplemented — the SAME code the
//     embedded base would return, so a half-wired binary behaves identically to
//     the scaffold instead of crashing. (All RPCs here are unary; the streaming
//     guard form — returning only the status error — is noted in the scaffold
//     for the day a server-streaming RPC is added.)
//
//  2. VALIDATE THE REQUEST. Reject malformed input with codes.InvalidArgument
//     and a CLEAN message (no internal detail, no echo of secrets). The handler
//     fails fast at the transport boundary so the domain only ever sees
//     well-formed input. The domain ALSO validates (defense in depth), but the
//     handler gives the client a precise gRPC code for shape errors.
//
//  3. CONVERT proto -> domain, lifting the SERVER-AUTHORITATIVE identity
//     (domain.Actor{UserID, Team}) from the auth interceptor's claims
//     (grpcutil.ClaimsFromContext) — NEVER from client-supplied request fields.
//     The client cannot be trusted to say "I am user X on team Y"; the validated
//     token says who they are. Owner/team/ids/timestamps/status are all derived
//     server-side, so the domain input structs do not even carry those fields
//     (the mass-assignment guard, enforced by the type system).
//
//  4. CALL the domain service, MAP its sentinel errors -> precise gRPC status
//     codes via toStatusError, and CONVERT the domain result -> proto.
//
// ============================================================================
// WHY THE PROTO RPC IS UpdateRunStatus BUT THE DOMAIN METHOD IS FinishRun
// ============================================================================
//
// The wire contract exposes UpdateRunStatus(run_id, status). The domain models
// the SAME operation as FinishRun(FinishRunInput{RunID, TargetStatus}) because
// the only legal status change is a transition to a TERMINAL state (the run-
// status state machine: RUNNING is the only non-terminal state, and a run is
// born RUNNING via StartRun — see domain/models.go). The handler is the
// anti-corruption boundary that reconciles the two vocabularies: it converts the
// proto RunStatus enum to the domain RunStatus and calls FinishRun. The domain
// rejects a non-terminal / illegal target with ErrInvalidStatusTransition, which
// maps to FailedPrecondition.
package handler

import (
	"context"
	"errors"
	"math"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	experimentv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/experiment/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ============================================================================
// PAGINATION CONSTANTS (mirror the proto's documented caps)
// ============================================================================
//
// WHY two tiers: list RPCs return whole rows (experiments/runs), so a small cap
// (20/100) bounds memory. Metric history returns tiny (key,value,step,ts)
// points, so a far higher cap (1000/5000) is safe and is what charts actually
// want. Both caps are SERVER-enforced (clamped here AND re-capped in the domain)
// so a client can never request an unbounded page.
const (
	// defaultPageSize / maxPageSize apply to ListExperiments and ListRuns.
	defaultPageSize = 20
	maxPageSize     = 100

	// defaultMetricPageSize / maxMetricPageSize apply to GetMetricHistory.
	defaultMetricPageSize = 1000
	maxMetricPageSize     = 5000

	// maxCompareRuns bounds CompareRuns. Comparing hundreds of full curves at
	// once is a payload/DoS hazard. This mirrors the proto's documented bound and
	// the domain's MaxCompareRuns; the handler rejects early so the client gets a
	// precise InvalidArgument before any repository fan-out begins.
	maxCompareRuns = 20
)

// ============================================================================
// IDENTITY + AUTHORIZATION HELPERS — the security spine
// ============================================================================
//
// Two separable questions, two helpers (do not conflate them):
//
//   - AUTHENTICATION ("who are you?")  -> actorFromContext  -> Unauthenticated
//   - AUTHORIZATION  ("may you?")      -> requireScope      -> PermissionDenied
//
// gRPC has DISTINCT codes for these and a correct client reacts differently:
// Unauthenticated means "(re)acquire a token"; PermissionDenied means "your token
// is valid but lacks the capability — ask an admin", and retrying with the SAME
// token is futile. Keeping them apart is the contract, not cosmetics.

// experiment-tracker scope vocabulary. These names match the capability model the
// proto documents (CreateExperiment "// Requires experiments:write") and the
// auth service's example scopes (services/auth/internal/domain/models.go:
// ["models:read", "experiments:write"]). Two tiers only — reads vs. mutations:
//
//   - scopeRead  gates every read RPC (Get*/List*/GetMetricHistory/CompareRuns).
//   - scopeWrite gates every mutating RPC (Create/Update/Archive, the run
//     lifecycle, and all ingestion).
//
// WHY a writer also satisfies reads (see requireReadScope): a principal trusted to
// CREATE experiments and LOG metrics can obviously READ them; forcing both scopes
// on every writer is friction with no security gain. So write IMPLIES read here.
// We deliberately do NOT model a separate finer-grained run:write tier: in this
// service runs are an inseparable part of "writing experiment data", so a single
// experiments:write capability keeps the model honest and the token small. (If a
// future requirement needs run-only writers, add scopeRunWrite and accept it in
// the run RPCs — the helper composition below makes that a one-line change.)
//
// ROLE vs SCOPE — the bug this fixes (broken access control, the OTHER way):
//
//	Two distinct credential shapes flow through grpcutil.Claims (set by the auth
//	service's validator, services/auth/internal/authn/validator.go):
//	  - A human JWT (what the UI/BFF mints at Login) carries Role (the role NAME,
//	    e.g. "admin"/"engineer"/"viewer") and an EMPTY Scopes slice. Scopes are a
//	    JWT non-feature here by design: the auth service resolves a JWT's
//	    permissions FRESH from its role at CheckPermission time and deliberately
//	    does NOT stuff per-resource scopes into the token (see Login in
//	    services/auth/internal/domain/auth_service_impl.go — "the role NAME, not
//	    its permission list, goes in the token").
//	  - An API key carries Scopes (the per-key grants) and may carry a Role too.
//
//	The ORIGINAL gate checked ONLY claims.Scopes. That silently denied EVERY human
//	JWT caller — including the bootstrap ADMIN whose role is the {*,*} wildcard
//	grant (services/auth/migrations/001_init_auth_schema.up.sql) — because a JWT's
//	Scopes is always empty. Symptom: the UI's GET /runs (→ ListRuns) returned
//	PermissionDenied even for an admin, making the Experiments page unusable.
//
//	THE FIX: authorization is satisfied by EITHER the role OR the scope. The admin
//	role is a platform-wide superuser (the {*,*} grant), so it subsumes every
//	experiment-tracker capability — read and write — exactly as the billing
//	service treats claims.Role == "admin" as the authoritative admin signal at the
//	handler boundary (services/billing/internal/handler/billing_handler_rpcs.go).
//	Non-admin callers still need the matching experiments:read / experiments:write
//	scope (e.g. an API key), so this does NOT over-open the surface — it only stops
//	denying the legitimately-privileged admin role the scope check could never see.
const (
	scopeRead  = "experiments:read"
	scopeWrite = "experiments:write"

	// roleAdmin is the platform superuser role NAME the auth service seeds with the
	// {*,*} wildcard permission (full platform access). A caller bearing this role
	// satisfies every authz gate in this service. We match the role by NAME — not
	// by re-deriving its permissions — because the JWT only carries the name and the
	// auth service is the source of truth for what "admin" can do; duplicating the
	// permission grammar here would be a second place to keep in sync. (Mirrors
	// billing's adminRole constant so admin recognition is identical platform-wide.)
	roleAdmin = "admin"
)

// actorFromContext lifts the SERVER-AUTHORITATIVE caller identity from the auth
// interceptor's claims and packages it as a domain.Actor, ALSO returning the raw
// claims so the caller can run an authorization (scope) check.
//
// SECURITY: this is the ONE place the handler establishes "who is calling".
// Every RPC routes through it, so the tenancy boundary (Actor.Team) and ownership
// (Actor.UserID) are derived from the validated token in exactly one auditable
// spot — never from a request field. If claims are absent (the call bypassed the
// interceptor) or carry no user/team, we fail CLOSED with Unauthenticated: no
// identity means no authority. Team is required because it is the tenancy
// boundary; a caller with no team cannot be correctly isolated.
//
// It returns the *grpcutil.Claims (never nil on the success path) precisely so the
// scope check is a separate, explicit step at each call site — authentication and
// authorization stay visibly distinct.
func actorFromContext(ctx context.Context) (domain.Actor, *grpcutil.Claims, error) {
	claims, ok := grpcutil.ClaimsFromContext(ctx)
	if !ok || claims == nil || claims.UserID == "" {
		return domain.Actor{}, nil, status.Error(codes.Unauthenticated, "missing authentication")
	}
	if claims.Team == "" {
		// Team scopes every read/write; without it we cannot enforce isolation, so
		// reject rather than silently operate in an undefined tenant.
		return domain.Actor{}, nil, status.Error(codes.Unauthenticated, "authenticated caller has no team")
	}
	return domain.Actor{UserID: claims.UserID, Team: claims.Team}, claims, nil
}

// hasScope reports whether the claims carry the named scope.
//
// FAIL-CLOSED: a nil claims set has NO scopes, so the caller is treated as
// unprivileged. This is the opposite of a rate limiter's fail-open default — for
// authorization the risk of guessing wrong is privilege escalation, so we deny by
// default. (Mirrors inference-gateway's hasScope so the whole platform enforces
// authz identically.)
func hasScope(claims *grpcutil.Claims, scope string) bool {
	if claims == nil {
		return false
	}
	for _, s := range claims.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

// hasAdminRole reports whether the caller bears the platform admin role.
//
// FAIL-CLOSED like hasScope: nil claims (a call that bypassed the interceptor)
// are NOT admin. The role string comes from the validated token's Role claim, so
// a client cannot self-assert "admin" — the auth service set it from the user's
// assigned role at Login, and the JWT signature protects it from tampering.
func hasAdminRole(claims *grpcutil.Claims) bool {
	return claims != nil && claims.Role == roleAdmin
}

// requireScope returns a PermissionDenied status iff the caller holds NONE of the
// accepted scopes (an "any-of" check, so a higher tier can subsume a lower one)
// AND is not the admin role.
//
// ADMIN SHORT-CIRCUIT: the admin role is the {*,*} platform superuser, so it
// satisfies every gate before any scope is consulted. This is what lets a human
// admin JWT — which carries Role="admin" but an EMPTY Scopes slice (see the
// ROLE vs SCOPE note on the scope constants) — use read and write RPCs. Without
// it, the scope-only check would deny the admin (and every JWT human) outright.
//
// MESSAGE HYGIENE: the error names the REQUIRED capability so a client can
// self-diagnose, but NEVER echoes the caller's actual granted scopes — leaking the
// token's contents back would tell a probing caller exactly what else the
// principal can (or cannot) do. We name what is needed, not what is held.
func requireScope(claims *grpcutil.Claims, accepted ...string) error {
	// Admin subsumes all experiment-tracker capabilities — check it FIRST so the
	// platform superuser never trips the scope gate.
	if hasAdminRole(claims) {
		return nil
	}
	for _, s := range accepted {
		if hasScope(claims, s) {
			return nil
		}
	}
	// Report the FIRST (canonical/least-privilege) accepted scope as the one to
	// obtain. Listing alternatives is unnecessary noise for the common two-tier
	// case and risks implying the caller's current grants.
	want := ""
	if len(accepted) > 0 {
		want = accepted[0]
	}
	return status.Errorf(codes.PermissionDenied, "caller lacks required scope %q", want)
}

// requireWriteScope authorizes a MUTATING RPC: the caller must hold
// experiments:write.
func requireWriteScope(claims *grpcutil.Claims) error {
	return requireScope(claims, scopeWrite)
}

// requireReadScope authorizes a READ RPC: experiments:read OR experiments:write
// (write implies read — a writer can always read what it can write).
func requireReadScope(claims *grpcutil.Claims) error {
	return requireScope(claims, scopeRead, scopeWrite)
}

// ============================================================================
// PAGINATION HELPER
// ============================================================================

// listOptionsFromProto normalizes a proto PaginationRequest into a domain
// ListOptions, clamping page_size to [1, max] with a default when 0.
//
// WHY CLAMP (not reject) an over-large page_size: a client asking for "as much as
// possible" should get the maximum we serve, not an error — this is Google's
// AIP-158 guidance. A NEGATIVE page_size, however, is a clear client bug (it can
// only come from a malformed request), so we surface it as InvalidArgument. The
// returned bool reports validity; callers turn false into that status.
func listOptionsFromProto(p *commonv1.PaginationRequest, def, max int) (domain.ListOptions, bool) {
	if p == nil {
		return domain.ListOptions{PageSize: def}, true
	}
	size := int(p.GetPageSize())
	if size < 0 {
		return domain.ListOptions{}, false
	}
	switch {
	case size == 0:
		size = def
	case size > max:
		size = max // clamp, per AIP-158
	}
	return domain.ListOptions{PageSize: size, PageToken: p.GetPageToken()}, true
}

// ============================================================================
// EXPERIMENTS
// ============================================================================

// CreateExperiment creates a named grouping of runs. owner_id/team come from the
// caller's claims (NOT the request) — the mass-assignment guard: a client must
// not be able to create an experiment "owned" by someone else or in another
// team. The proto request therefore carries only name/description/tags.
func (h *ExperimentHandler) CreateExperiment(ctx context.Context, req *experimentv1.CreateExperimentRequest) (*experimentv1.CreateExperimentResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	actor, claims, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// AUTHORIZE: mutating RPC -> experiments:write. Checked AFTER authentication
	// (we know who you are) but BEFORE any validation or domain call, so an
	// under-scoped caller is rejected with PermissionDenied without the domain ever
	// being reached.
	if err := requireWriteScope(claims); err != nil {
		return nil, err
	}

	// VALIDATE: name is the human handle and the uniqueness key within a team, so
	// it is required. We do NOT enforce length/charset here — that is a business
	// rule the domain owns (ErrValidation); the handler guarantees presence so the
	// domain never sees an obviously-unusable empty name.
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	exp, err := h.svc.CreateExperiment(ctx, actor, domain.CreateExperimentInput{
		Name:        req.GetName(),
		Description: req.GetDescription(),
		Tags:        req.GetTags(),
	})
	if err != nil {
		return nil, toStatusError(err)
	}

	return &experimentv1.CreateExperimentResponse{
		Experiment: experimentToProto(exp),
	}, nil
}

// GetExperiment fetches one experiment, scoped to the caller's team. A missing
// experiment AND one belonging to another team both map to NotFound (the domain
// returns the same ErrExperimentNotFound for both) so existence cannot be probed
// across tenants.
func (h *ExperimentHandler) GetExperiment(ctx context.Context, req *experimentv1.GetExperimentRequest) (*experimentv1.GetExperimentResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	actor, claims, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// AUTHORIZE: read RPC -> experiments:read (or write, which implies read).
	if err := requireReadScope(claims); err != nil {
		return nil, err
	}

	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	exp, err := h.svc.GetExperiment(ctx, actor, req.GetId())
	if err != nil {
		return nil, toStatusError(err)
	}

	return &experimentv1.GetExperimentResponse{
		Experiment: experimentToProto(exp),
	}, nil
}

// ListExperiments returns a team-scoped page. team_filter only NARROWS within
// the caller's permitted team — it can never widen visibility, so we do NOT pass
// it as the tenancy boundary (that is always Actor.Team). The domain ignores any
// attempt to list another team. include_archived toggles soft-deleted rows.
func (h *ExperimentHandler) ListExperiments(ctx context.Context, req *experimentv1.ListExperimentsRequest) (*experimentv1.ListExperimentsResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	actor, claims, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// AUTHORIZE: read RPC -> experiments:read (or write, which implies read).
	if err := requireReadScope(claims); err != nil {
		return nil, err
	}

	opts, ok := listOptionsFromProto(req.GetPagination(), defaultPageSize, maxPageSize)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "page_size must not be negative")
	}

	exps, nextToken, err := h.svc.ListExperiments(ctx, actor, req.GetIncludeArchived(), opts)
	if err != nil {
		return nil, toStatusError(err)
	}

	out := make([]*experimentv1.Experiment, 0, len(exps))
	for _, e := range exps {
		out = append(out, experimentToProto(e))
	}
	return &experimentv1.ListExperimentsResponse{
		Experiments: out,
		// TotalCount is left 0: cursor pagination does not compute an exact total
		// (it can require a full scan), and the proto's PaginationResponse allows
		// omitting it. The next_page_token is the load-bearing field for paging.
		Pagination: &commonv1.PaginationResponse{NextPageToken: nextToken},
	}, nil
}

// UpdateExperiment applies a field-masked edit. owner_id/team/timestamps are
// server-authoritative and cannot be set, so the request carries no such fields.
//
// FIELD-MASK VALIDATION: an empty mask is a no-op edit and almost certainly a
// client bug (the client meant to change something), so we reject it with
// InvalidArgument rather than burning a write that changes nothing. Unknown mask
// names are rejected by the domain (ErrValidation -> InvalidArgument).
func (h *ExperimentHandler) UpdateExperiment(ctx context.Context, req *experimentv1.UpdateExperimentRequest) (*experimentv1.UpdateExperimentResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	actor, claims, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// AUTHORIZE: mutating RPC -> experiments:write.
	if err := requireWriteScope(claims); err != nil {
		return nil, err
	}

	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	if len(req.GetUpdateFields()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "update_fields must list at least one field to update")
	}

	exp, err := h.svc.UpdateExperiment(ctx, actor, domain.UpdateExperimentInput{
		ID:           req.GetId(),
		Name:         req.GetName(),
		Description:  req.GetDescription(),
		Tags:         req.GetTags(),
		UpdateFields: req.GetUpdateFields(),
	})
	if err != nil {
		return nil, toStatusError(err)
	}

	return &experimentv1.UpdateExperimentResponse{
		Experiment: experimentToProto(exp),
	}, nil
}

// ArchiveExperiment SOFT-deletes an experiment (sets archived_at) while keeping
// its runs/metrics for lineage. Idempotent: archiving an already-archived
// experiment returns it unchanged (the domain handles the no-op), so a retried
// archive is safe.
func (h *ExperimentHandler) ArchiveExperiment(ctx context.Context, req *experimentv1.ArchiveExperimentRequest) (*experimentv1.ArchiveExperimentResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	actor, claims, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// AUTHORIZE: mutating RPC -> experiments:write.
	if err := requireWriteScope(claims); err != nil {
		return nil, err
	}

	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	exp, err := h.svc.ArchiveExperiment(ctx, actor, req.GetId())
	if err != nil {
		return nil, toStatusError(err)
	}

	return &experimentv1.ArchiveExperimentResponse{
		Experiment: experimentToProto(exp),
	}, nil
}

// ============================================================================
// RUN LIFECYCLE
// ============================================================================

// StartRun opens a RUNNING run in an experiment (source = API). id/status/source/
// owner/started_at are all server-authoritative; the client supplies only the
// experiment, an optional label, the target model version, and params. The
// optional idempotency_key makes a retried StartRun return the SAME run instead
// of creating a duplicate (the Stripe Idempotency-Key pattern, applied in the
// domain).
func (h *ExperimentHandler) StartRun(ctx context.Context, req *experimentv1.StartRunRequest) (*experimentv1.StartRunResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	actor, claims, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// AUTHORIZE: mutating RPC (writes a new run) -> experiments:write.
	if err := requireWriteScope(claims); err != nil {
		return nil, err
	}

	if req.GetExperimentId() == "" {
		return nil, status.Error(codes.InvalidArgument, "experiment_id is required")
	}

	run, err := h.svc.StartRun(ctx, actor, domain.StartRunInput{
		ExperimentID:   req.GetExperimentId(),
		DisplayName:    req.GetDisplayName(),
		ModelVersionID: req.GetModelVersionId(),
		Params:         paramsFromProto(req.GetParams()),
		IdempotencyKey: req.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, toStatusError(err)
	}

	return &experimentv1.StartRunResponse{
		Run: runToProto(run),
	}, nil
}

// UpdateRunStatus transitions a run to a terminal state. The proto vocabulary is
// "update status"; the domain models it as FinishRun (the only legal status
// change is to a terminal state — see the file header). We convert the proto
// RunStatus to the domain RunStatus and let the domain enforce the state machine:
// an illegal/non-terminal target yields ErrInvalidStatusTransition ->
// FailedPrecondition.
func (h *ExperimentHandler) UpdateRunStatus(ctx context.Context, req *experimentv1.UpdateRunStatusRequest) (*experimentv1.UpdateRunStatusResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	actor, claims, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// AUTHORIZE: mutating RPC (transitions a run) -> experiments:write.
	if err := requireWriteScope(claims); err != nil {
		return nil, err
	}

	if req.GetRunId() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	// Reject the zero/unspecified enum at the boundary: "transition to
	// UNSPECIFIED" is meaningless and almost certainly an unset field. (The domain
	// also rejects it, but a precise InvalidArgument is friendlier than a
	// FailedPrecondition for a missing field.)
	target := runStatusFromProto(req.GetStatus())
	if target == domain.RunStatusUnspecified {
		return nil, status.Error(codes.InvalidArgument, "status is required and must be a terminal state")
	}

	run, err := h.svc.FinishRun(ctx, actor, domain.FinishRunInput{
		RunID:        req.GetRunId(),
		TargetStatus: target,
	})
	if err != nil {
		return nil, toStatusError(err)
	}

	return &experimentv1.UpdateRunStatusResponse{
		Run: runToProto(run),
	}, nil
}

// GetRun fetches a run's metadata, params, headline metrics, and artifacts —
// NOT the full metric series (use GetMetricHistory for the curve).
func (h *ExperimentHandler) GetRun(ctx context.Context, req *experimentv1.GetRunRequest) (*experimentv1.GetRunResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	actor, claims, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// AUTHORIZE: read RPC -> experiments:read (or write, which implies read).
	if err := requireReadScope(claims); err != nil {
		return nil, err
	}

	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	run, err := h.svc.GetRun(ctx, actor, req.GetId())
	if err != nil {
		return nil, toStatusError(err)
	}

	return &experimentv1.GetRunResponse{
		Run: runToProto(run),
	}, nil
}

// ListRuns returns a paginated list of runs within one experiment, optionally
// filtered by status. status_filter == UNSPECIFIED means "no filter" (the domain
// treats RunStatusUnspecified that way), so we do NOT reject the zero enum here.
func (h *ExperimentHandler) ListRuns(ctx context.Context, req *experimentv1.ListRunsRequest) (*experimentv1.ListRunsResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	actor, claims, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// AUTHORIZE: read RPC -> experiments:read (or write, which implies read).
	if err := requireReadScope(claims); err != nil {
		return nil, err
	}

	if req.GetExperimentId() == "" {
		return nil, status.Error(codes.InvalidArgument, "experiment_id is required")
	}

	opts, ok := listOptionsFromProto(req.GetPagination(), defaultPageSize, maxPageSize)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "page_size must not be negative")
	}

	runs, nextToken, err := h.svc.ListRuns(ctx, actor, req.GetExperimentId(), runStatusFromProto(req.GetStatusFilter()), opts)
	if err != nil {
		return nil, toStatusError(err)
	}

	out := make([]*experimentv1.Run, 0, len(runs))
	for _, r := range runs {
		out = append(out, runToProto(r))
	}
	return &experimentv1.ListRunsResponse{
		Runs:       out,
		Pagination: &commonv1.PaginationResponse{NextPageToken: nextToken},
	}, nil
}

// DeleteRun hard-deletes one run + its metrics/params. Idempotent via
// idempotency_key (a retried delete after a network blip must not error just
// because the row is already gone). The lineage guard (no live Registry
// reference) is a cross-service check wired when the Registry client exists; the
// domain enforces ownership/team and the idempotency replay here.
func (h *ExperimentHandler) DeleteRun(ctx context.Context, req *experimentv1.DeleteRunRequest) (*experimentv1.DeleteRunResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	actor, claims, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// AUTHORIZE: mutating RPC (hard-deletes a run) -> experiments:write.
	if err := requireWriteScope(claims); err != nil {
		return nil, err
	}

	if req.GetRunId() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}

	if err := h.svc.DeleteRun(ctx, actor, req.GetRunId(), req.GetIdempotencyKey()); err != nil {
		return nil, toStatusError(err)
	}

	// Empty response: a named empty message leaves room to add audit fields later
	// without a wire-breaking change.
	return &experimentv1.DeleteRunResponse{}, nil
}

// ============================================================================
// INGESTION
// ============================================================================

// LogMetrics is the high-throughput BATCH ingestion RPC. Timestamps are
// server-stamped (the domain overwrites any client value — Timestamp is the
// partition/retention axis and must be server-authoritative); the batch size is
// capped server-side (the domain returns ErrBatchTooLarge -> InvalidArgument).
//
// HANDLER VALIDATION beyond presence: we reject NaN/Inf metric values at the
// boundary. WHY here and not only in the domain: a NaN serializes fine over the
// wire but corrupts every downstream aggregation (min/max/avg become NaN) and
// breaks JSON encoders in the UI. Rejecting at ingest is cheaper than scrubbing
// later. We keep the rest of the batch rules (per-call cap, dedup) in the domain.
func (h *ExperimentHandler) LogMetrics(ctx context.Context, req *experimentv1.LogMetricsRequest) (*experimentv1.LogMetricsResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	actor, claims, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// AUTHORIZE: ingestion is a mutating RPC -> experiments:write.
	if err := requireWriteScope(claims); err != nil {
		return nil, err
	}

	if req.GetRunId() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	if len(req.GetPoints()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "points must contain at least one metric point")
	}
	points := make([]domain.MetricPoint, 0, len(req.GetPoints()))
	for _, p := range req.GetPoints() {
		if p.GetKey() == "" {
			return nil, status.Error(codes.InvalidArgument, "every metric point must have a key")
		}
		if math.IsNaN(p.GetValue()) || math.IsInf(p.GetValue(), 0) {
			// Do NOT echo the run_id or any state; this is a pure input-shape error.
			return nil, status.Error(codes.InvalidArgument, "metric value must be a finite number (no NaN/Inf)")
		}
		// Timestamp is intentionally NOT copied from the client — the domain stamps
		// it server-side. We pass only the client-owned (key, value, step).
		points = append(points, domain.MetricPoint{
			Key:   p.GetKey(),
			Value: p.GetValue(),
			Step:  p.GetStep(),
		})
	}

	res, err := h.svc.LogMetrics(ctx, actor, domain.LogMetricsInput{
		RunID:          req.GetRunId(),
		Points:         points,
		IdempotencyKey: req.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, toStatusError(err)
	}

	// accepted_count may be < len(points) on idempotent replay or intra-batch
	// dedup — the batch API tells the caller exactly what landed so it can
	// reconcile. Guard the int->int32 narrowing (accepted_count can never exceed
	// the batch cap, but be defensive against a misbehaving impl).
	return &experimentv1.LogMetricsResponse{
		AcceptedCount: clampInt32(res.AcceptedCount),
	}, nil
}

// LogParams appends write-once hyperparameters to a RUNNING run. Re-logging an
// identical key/value is an idempotent no-op (counted out of accepted_count); a
// CHANGED value is ErrParamConflict -> FailedPrecondition (a run's configuration
// must not silently change mid-flight).
func (h *ExperimentHandler) LogParams(ctx context.Context, req *experimentv1.LogParamsRequest) (*experimentv1.LogParamsResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	actor, claims, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// AUTHORIZE: ingestion is a mutating RPC -> experiments:write.
	if err := requireWriteScope(claims); err != nil {
		return nil, err
	}

	if req.GetRunId() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	if len(req.GetParams()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "params must contain at least one param")
	}
	for _, p := range req.GetParams() {
		if p.GetKey() == "" {
			return nil, status.Error(codes.InvalidArgument, "every param must have a key")
		}
	}

	res, err := h.svc.LogParams(ctx, actor, domain.LogParamsInput{
		RunID:  req.GetRunId(),
		Params: paramsFromProto(req.GetParams()),
	})
	if err != nil {
		return nil, toStatusError(err)
	}

	return &experimentv1.LogParamsResponse{
		AcceptedCount: clampInt32(res.AcceptedCount),
	}, nil
}

// SetRunArtifacts attaches free-form JSON-shaped artifacts to a run. The proto
// carries a google.protobuf.Struct; the domain works in map[string]any. The
// serialized-size cap is a domain invariant (the handler must not be able to
// bypass it), so we convert the Struct and let the domain reject an over-cap
// payload. The optional idempotency_key replays a retried attach to the same
// effect.
func (h *ExperimentHandler) SetRunArtifacts(ctx context.Context, req *experimentv1.SetRunArtifactsRequest) (*experimentv1.SetRunArtifactsResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	actor, claims, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// AUTHORIZE: attaching artifacts is a mutating RPC -> experiments:write.
	if err := requireWriteScope(claims); err != nil {
		return nil, err
	}

	if req.GetRunId() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	if req.GetArtifacts() == nil {
		return nil, status.Error(codes.InvalidArgument, "artifacts is required")
	}

	run, err := h.svc.SetRunArtifacts(ctx, actor, domain.SetArtifactsInput{
		RunID:          req.GetRunId(),
		Artifacts:      req.GetArtifacts().AsMap(), // structpb.Struct -> map[string]any
		IdempotencyKey: req.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, toStatusError(err)
	}

	return &experimentv1.SetRunArtifactsResponse{
		Run: runToProto(run),
	}, nil
}

// ============================================================================
// ANALYSIS / READ
// ============================================================================

// GetMetricHistory returns ONE run's metric series, paginated, with optional key
// and step-window filters. HasMinStep/HasMaxStep distinguish a real bound of 0
// from "no bound": the proto carries plain int64s where 0 is the zero value, so
// "both 0 = no step bound" is the documented convention. We set the Has* flags
// accordingly. Page size uses the higher metric cap (points are tiny).
func (h *ExperimentHandler) GetMetricHistory(ctx context.Context, req *experimentv1.GetMetricHistoryRequest) (*experimentv1.GetMetricHistoryResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	actor, claims, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// AUTHORIZE: read RPC -> experiments:read (or write, which implies read).
	if err := requireReadScope(claims); err != nil {
		return nil, err
	}

	if req.GetRunId() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}

	minStep, maxStep := req.GetMinStep(), req.GetMaxStep()
	// "Both 0 = no step bound" (proto convention). If either is non-zero we treat
	// the pair as an explicit window [min, max]; reject an inverted window early.
	hasWindow := minStep != 0 || maxStep != 0
	if hasWindow && minStep > maxStep {
		return nil, status.Error(codes.InvalidArgument, "min_step must not exceed max_step")
	}

	opts, ok := listOptionsFromProto(req.GetPagination(), defaultMetricPageSize, maxMetricPageSize)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "page_size must not be negative")
	}

	series, nextToken, err := h.svc.GetMetricHistory(ctx, actor, domain.GetMetricHistoryInput{
		RunID:      req.GetRunId(),
		MetricKeys: req.GetMetricKeys(),
		MinStep:    minStep,
		MaxStep:    maxStep,
		HasMinStep: hasWindow,
		HasMaxStep: hasWindow,
		Pagination: opts,
	})
	if err != nil {
		return nil, toStatusError(err)
	}

	return &experimentv1.GetMetricHistoryResponse{
		Series:     seriesToProto(series),
		Pagination: &commonv1.PaginationResponse{NextPageToken: nextToken},
	}, nil
}

// CompareRuns returns several runs' metric series side-by-side. Run count is
// bounded server-side (maxCompareRuns) — we reject an over-large request early
// with InvalidArgument before any repository fan-out, and the domain re-checks
// (ErrTooManyRuns) as defense in depth. metric_keys narrows the payload (empty =
// all keys).
func (h *ExperimentHandler) CompareRuns(ctx context.Context, req *experimentv1.CompareRunsRequest) (*experimentv1.CompareRunsResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	actor, claims, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// AUTHORIZE: read RPC -> experiments:read (or write, which implies read).
	if err := requireReadScope(claims); err != nil {
		return nil, err
	}

	if len(req.GetRunIds()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "run_ids must contain at least one run")
	}
	if len(req.GetRunIds()) > maxCompareRuns {
		return nil, status.Error(codes.InvalidArgument, "too many runs requested for comparison")
	}

	comparisons, err := h.svc.CompareRuns(ctx, actor, domain.CompareRunsInput{
		RunIDs:     req.GetRunIds(),
		MetricKeys: req.GetMetricKeys(),
	})
	if err != nil {
		return nil, toStatusError(err)
	}

	out := make([]*experimentv1.RunComparison, 0, len(comparisons))
	for _, c := range comparisons {
		out = append(out, &experimentv1.RunComparison{
			Run:    runToProto(c.Run),
			Series: seriesToProto(c.Series),
		})
	}
	return &experimentv1.CompareRunsResponse{
		Comparisons: out,
	}, nil
}

// ============================================================================
// PROTO <-> DOMAIN CONVERTERS (the anti-corruption layer)
// ============================================================================
//
// These are the single place domain types become wire types and vice versa.
// Centralizing them keeps the enum/timestamp/nil-handling rules in one auditable
// spot — a field added to a domain model is surfaced (or deliberately withheld)
// here, not scattered across 15 RPC bodies.

// experimentToProto converts a domain.Experiment to the proto Experiment.
// owner_id/team ARE surfaced (they are not secrets — they tell the client who
// owns the resource); archived_at is nil when the experiment is active.
func experimentToProto(e domain.Experiment) *experimentv1.Experiment {
	out := &experimentv1.Experiment{
		Id:          e.ID,
		Name:        e.Name,
		Description: e.Description,
		Tags:        e.Tags,
		OwnerId:     e.OwnerID,
		Team:        e.Team,
		CreatedAt:   timestamppb.New(e.CreatedAt),
		UpdatedAt:   timestamppb.New(e.UpdatedAt),
	}
	if e.ArchivedAt != nil {
		out.ArchivedAt = timestamppb.New(*e.ArchivedAt)
	}
	return out
}

// runToProto converts a domain.Run to the proto Run. ended_at is nil while the
// run is RUNNING; artifacts (a map[string]any) is converted to a structpb.Struct
// only when present. A conversion failure on artifacts is swallowed to nil rather
// than failing the whole read — artifacts are auxiliary, and a malformed map
// stored by an earlier write should not make a run unreadable. (The write path
// validates artifacts via structpb on the way IN, so this is belt-and-suspenders.)
func runToProto(r domain.Run) *experimentv1.Run {
	out := &experimentv1.Run{
		Id:             r.ID,
		ExperimentId:   r.ExperimentID,
		DisplayName:    r.DisplayName,
		Status:         runStatusToProto(r.Status),
		Source:         runSourceToProto(r.Source),
		ModelVersionId: r.ModelVersionID,
		OwnerId:        r.OwnerID,
		Params:         paramsToProto(r.Params),
		FinalMetrics:   metricsToProto(r.FinalMetrics),
		StartedAt:      timestamppb.New(r.StartedAt),
	}
	if r.EndedAt != nil {
		out.EndedAt = timestamppb.New(*r.EndedAt)
	}
	if r.Artifacts != nil {
		if s, err := structpb.NewStruct(r.Artifacts); err == nil {
			out.Artifacts = s
		}
	}
	return out
}

// paramsFromProto converts proto Params to domain Params. Nil-safe (returns nil
// for an empty input so the domain sees a clean zero value).
func paramsFromProto(in []*experimentv1.Param) []domain.Param {
	if len(in) == 0 {
		return nil
	}
	out := make([]domain.Param, 0, len(in))
	for _, p := range in {
		out = append(out, domain.Param{Key: p.GetKey(), Value: p.GetValue()})
	}
	return out
}

// paramsToProto converts domain Params to proto Params.
func paramsToProto(in []domain.Param) []*experimentv1.Param {
	if len(in) == 0 {
		return nil
	}
	out := make([]*experimentv1.Param, 0, len(in))
	for _, p := range in {
		out = append(out, &experimentv1.Param{Key: p.Key, Value: p.Value})
	}
	return out
}

// metricsToProto converts a flat slice of domain MetricPoints to proto. Used for
// FinalMetrics (the headline snapshot). Timestamp is surfaced (it is
// server-authoritative and meaningful to the client for the final sample).
func metricsToProto(in []domain.MetricPoint) []*experimentv1.MetricPoint {
	if len(in) == 0 {
		return nil
	}
	out := make([]*experimentv1.MetricPoint, 0, len(in))
	for _, p := range in {
		out = append(out, metricPointToProto(p))
	}
	return out
}

// metricPointToProto converts one domain MetricPoint to proto.
func metricPointToProto(p domain.MetricPoint) *experimentv1.MetricPoint {
	return &experimentv1.MetricPoint{
		Key:       p.Key,
		Value:     p.Value,
		Step:      p.Step,
		Timestamp: timestamppb.New(p.Timestamp),
	}
}

// seriesToProto converts domain MetricSeries (the chart-ready columnar view) to
// proto, including each series' points.
func seriesToProto(in []domain.MetricSeries) []*experimentv1.MetricSeries {
	if len(in) == 0 {
		return nil
	}
	out := make([]*experimentv1.MetricSeries, 0, len(in))
	for _, s := range in {
		out = append(out, &experimentv1.MetricSeries{
			Key:    s.Key,
			Points: metricsToProto(s.Points),
		})
	}
	return out
}

// ============================================================================
// ENUM CONVERTERS — proto enum <-> domain typed-string enum
// ============================================================================
//
// The domain uses human-readable typed strings ("RUNNING") so DB rows and logs
// are legible; the proto uses int enums for wire compactness. These two functions
// are the single mapping. An unknown proto value maps to the domain zero
// (Unspecified), which downstream rejects — fail safe, never guess.

// runStatusFromProto maps the proto RunStatus enum to the domain RunStatus.
func runStatusFromProto(s experimentv1.RunStatus) domain.RunStatus {
	switch s {
	case experimentv1.RunStatus_RUN_STATUS_RUNNING:
		return domain.RunStatusRunning
	case experimentv1.RunStatus_RUN_STATUS_FINISHED:
		return domain.RunStatusFinished
	case experimentv1.RunStatus_RUN_STATUS_FAILED:
		return domain.RunStatusFailed
	case experimentv1.RunStatus_RUN_STATUS_KILLED:
		return domain.RunStatusKilled
	default:
		return domain.RunStatusUnspecified
	}
}

// runStatusToProto maps the domain RunStatus to the proto RunStatus enum.
func runStatusToProto(s domain.RunStatus) experimentv1.RunStatus {
	switch s {
	case domain.RunStatusRunning:
		return experimentv1.RunStatus_RUN_STATUS_RUNNING
	case domain.RunStatusFinished:
		return experimentv1.RunStatus_RUN_STATUS_FINISHED
	case domain.RunStatusFailed:
		return experimentv1.RunStatus_RUN_STATUS_FAILED
	case domain.RunStatusKilled:
		return experimentv1.RunStatus_RUN_STATUS_KILLED
	default:
		return experimentv1.RunStatus_RUN_STATUS_UNSPECIFIED
	}
}

// runSourceToProto maps the domain RunSource to the proto RunSource enum. (There
// is no FromProto counterpart: Source is server-authoritative — a client never
// sets it, so it is only ever converted OUT.)
func runSourceToProto(s domain.RunSource) experimentv1.RunSource {
	switch s {
	case domain.RunSourceAPI:
		return experimentv1.RunSource_RUN_SOURCE_API
	case domain.RunSourceEvent:
		return experimentv1.RunSource_RUN_SOURCE_EVENT
	default:
		return experimentv1.RunSource_RUN_SOURCE_UNSPECIFIED
	}
}

// clampInt32 narrows an int (domain accepted-count) to the proto's int32 field
// without overflow. accepted_count is bounded by the per-batch cap (far below
// int32 max), but a defensive clamp avoids a silent wraparound if an impl ever
// returns a bogus value. Negative is impossible (a count), but guarded for
// totality.
func clampInt32(n int) int32 {
	if n < 0 {
		return 0
	}
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(n)
}

// ============================================================================
// ERROR MAPPING — domain sentinels -> gRPC status codes
// ============================================================================

// errServiceNotWired is the canonical response when h.svc is nil (a binary
// running before the domain service is wired). It mirrors what the embedded
// UnimplementedExperimentTrackerServiceServer would return, so a half-wired
// binary behaves exactly like the scaffold instead of panicking on a
// nil-interface call.
var errServiceNotWired = status.Error(codes.Unimplemented, "experiment tracker service not wired")

// toStatusError is the single anti-corruption point between domain errors and
// the gRPC status vocabulary. It is an explicit table — see the Auth service's
// toStatusError for the full rationale; the principle is identical:
//
//	domain sentinel               -> gRPC code           why
//	-------------------------------------------------------------------------
//	ErrValidation                 -> InvalidArgument     client sent bad input
//	ErrBatchTooLarge              -> InvalidArgument     batch over the per-call cap
//	ErrTooManyRuns                -> InvalidArgument     too many runs to compare
//	ErrExperimentNameExists       -> AlreadyExists       unique (team,name) conflict
//	ErrExperimentNotFound         -> NotFound            experiment absent / cross-tenant
//	ErrRunNotFound                -> NotFound            run absent
//	ErrRunNotRunning              -> FailedPrecondition  run is terminal (metrics final)
//	ErrInvalidStatusTransition    -> FailedPrecondition  illegal state-machine edge
//	ErrParamConflict              -> FailedPrecondition  write-once param changed
//	(anything else)               -> Internal (generic)  SANITIZED — no leak
//
// WHY ErrRunNotRunning/ErrInvalidStatusTransition/ErrParamConflict ->
// FailedPrecondition (not InvalidArgument): the client's INPUT was well-formed;
// the RESOURCE is in the wrong STATE for the operation. gRPC's FailedPrecondition
// is exactly "the system is not in a state required for the operation" — the
// client should fix state (e.g. start a new run) before retrying, whereas
// InvalidArgument tells it to fix the request. That distinction drives correct
// client retry logic, so it matters.
//
// WHY the default is Internal with a FIXED message: an unrecognized error might
// wrap SQL text, a connection string, or PII. We log the real error server-side
// (the logging interceptor) and return a constant opaque message. errors.Is is
// used (not ==) so a wrapped sentinel still maps correctly.
func toStatusError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, domain.ErrValidation):
		// Wrapped ErrValidation carries a specific, NON-sensitive message that
		// describes the client's own bad input (never internal state). It is safe
		// and useful to forward — this is the one whitelisted err.Error() path.
		return status.Error(codes.InvalidArgument, err.Error())

	case errors.Is(err, domain.ErrBatchTooLarge):
		return status.Error(codes.InvalidArgument, "metric batch exceeds the per-call cap")

	case errors.Is(err, domain.ErrTooManyRuns):
		return status.Error(codes.InvalidArgument, "too many runs requested for comparison")

	case errors.Is(err, domain.ErrExperimentNameExists):
		return status.Error(codes.AlreadyExists, "an experiment with that name already exists in your team")

	case errors.Is(err, domain.ErrExperimentNotFound):
		return status.Error(codes.NotFound, "experiment not found")

	case errors.Is(err, domain.ErrRunNotFound):
		return status.Error(codes.NotFound, "run not found")

	case errors.Is(err, domain.ErrRunNotRunning):
		return status.Error(codes.FailedPrecondition, "run is not RUNNING; its metrics are final")

	case errors.Is(err, domain.ErrInvalidStatusTransition):
		return status.Error(codes.FailedPrecondition, "invalid run status transition")

	case errors.Is(err, domain.ErrParamConflict):
		return status.Error(codes.FailedPrecondition, "param already set with a different value")

	default:
		// SANITIZE: do not expose err.Error() — it may wrap SQL, secrets, or PII.
		// The real error is logged server-side; the client gets a constant message.
		return status.Error(codes.Internal, "internal error")
	}
}
