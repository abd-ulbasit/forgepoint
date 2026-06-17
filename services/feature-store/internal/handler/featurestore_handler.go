// Package handler implements the gRPC server side of the Feature Store service.
// It is the OUTERMOST ring of Clean Architecture: it speaks proto (wire format)
// and delegates all business logic to the domain.FeatureStoreService interface.
//
// ============================================================================
// HANDLER LAYER RESPONSIBILITIES (the anti-corruption layer)
// ============================================================================
//
// When the per-RPC methods are filled in (the handler phase, after the Postgres
// EventLog + Redis/Postgres view stores exist), each will do exactly three things:
//
//  1. PROTO → DOMAIN: extract + validate request fields, convert the proto
//     google.protobuf.Value feature maps into domain.FeatureValue (the tagged
//     union), and extract the authenticated caller into a domain.Principal from
//     the auth interceptor's TokenClaims — NEVER from request fields (the
//     mass-assignment guard the whole service depends on).
//
//  2. CALL THE DOMAIN SERVICE: invoke svc.DefineFeatureView / WriteFeatures /
//     GetOnlineFeatures / GetHistoricalFeatures / DeleteFeatureView / RebuildViews.
//     The handler holds the INTERFACE — it never knows whether the impl is backed
//     by Postgres+Redis or an in-memory test double.
//
//  3. DOMAIN → PROTO: map the domain result (FeatureView, FeatureVector, …) back
//     to the proto Response, mapping domain.FeatureValueType → the proto enum and
//     domain.FeatureValue → google.protobuf.Value. For RebuildViews (server-
//     streaming), each domain.RebuildProgress frame becomes a stream.Send of a
//     featurestorev1.RebuildViewsResponse.
//
// ERROR MAPPING (also the handler's job): translate the domain sentinels to gRPC
// status codes — ErrValidation/ErrSchemaViolation/ErrAsOfRequired/ErrBatchTooLarge
// → InvalidArgument, ErrViewNotFound → NotFound, ErrViewDeleted/ErrViewNameConflict
// → FailedPrecondition. The domain never imports grpc/codes; this layer is the
// only place that translation happens.
//
// ============================================================================
// EMBEDDING UnimplementedFeatureStoreServiceServer — CORRECT, NOT A STUB
// ============================================================================
//
// protoc-gen-go-grpc generates FeatureStoreServiceServer with a private method
// mustEmbedUnimplementedFeatureStoreServiceServer(), forcing every implementation
// to embed UnimplementedFeatureStoreServiceServer. That base implements every RPC
// (including the server-streaming RebuildViews) to return codes.Unimplemented.
// Embedding it means FeatureStoreHandler:
//
//  1. Satisfies featurestorev1.FeatureStoreServiceServer at compile time RIGHT NOW
//     — the server can be registered and started in this scaffold phase.
//  2. Returns codes.Unimplemented for any RPC not yet overridden — a real, correct
//     gRPC error, never a panic.
//  3. Is forward-compatible: adding a new RPC to the proto doesn't break this
//     service (the embedded base handles it) until we implement it.
//
// This is the IDIOMATIC Go/gRPC scaffold, a production-correct running server —
// not a placeholder. Per-RPC implementations land in the handler phase.
//
// INTERVIEW: "How does grpc-go ensure forward compatibility of service servers?"
//
//	The mustEmbed… private method forces embedding the Unimplemented base; a new
//	RPC compiles against existing servers that embed it (returning Unimplemented)
//	instead of breaking the build. That is gRPC's server-side compatibility story.
package handler

import (
	"context"
	"errors"

	featurestorev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/featurestore/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FeatureStoreHandler is the gRPC server implementation for the Feature Store.
//
// It embeds featurestorev1.UnimplementedFeatureStoreServiceServer to satisfy the
// full FeatureStoreServiceServer interface immediately (all RPCs return
// Unimplemented until the handler phase overrides them).
//
// svc is the domain.FeatureStoreService holding all business logic — the event-
// sourcing engine. In the handler phase each RPC method calls svc.* and converts
// proto↔domain. The svc field is nil during this scaffold phase (main.go passes
// nil); the embedded Unimplemented methods never dereference it, so this is safe.
type FeatureStoreHandler struct {
	featurestorev1.UnimplementedFeatureStoreServiceServer // embedded BY VALUE (grpc-go requirement)
	svc                                                   domain.FeatureStoreService
}

// NewFeatureStoreHandler constructs the handler with the given domain service.
//
// The svc parameter is nil during the scaffold phase because the domain service's
// real ADAPTERS (Postgres EventLog, Redis online store, Postgres offline store,
// the NATS publisher) are a later phase. Once those land, main.go constructs the
// real service and passes it here:
//
//	svc := domain.NewFeatureStoreService(eventLog, onlineStore, offlineStore, clock, idgen)
//	featurestorev1.RegisterFeatureStoreServiceServer(srv.GRPC, handler.NewFeatureStoreHandler(svc))
//
// The nil svc is safe now because the embedded UnimplementedFeatureStoreServiceServer
// answers every RPC without touching svc. When the per-RPC methods are written,
// each will guard a nil service — the guard differs by RPC kind:
//   - UNARY RPCs return (resp, err):
//     if h.svc == nil { return nil, status.Error(codes.Internal, "service not wired") }
//   - the SERVER-STREAMING RebuildViews returns only err (responses flow via
//     stream.Send), so there is no resp to return:
//     if h.svc == nil { return status.Error(codes.Internal, "service not wired") }
func NewFeatureStoreHandler(svc domain.FeatureStoreService) *FeatureStoreHandler {
	return &FeatureStoreHandler{svc: svc}
}

// ============================================================================
// errServiceNotWired — the nil-svc guard message
// ============================================================================
//
// During the scaffold phase main.go constructs the handler with a nil service
// (the Postgres/Redis adapters are the repo phase). A real binary started before
// the service is wired must answer RPCs with codes.Unimplemented, NOT panic on a
// nil dereference. Each method below opens with this guard. We use Unimplemented
// (not Internal) because semantically the method is "not available yet" on this
// build — the exact contract the embedded UnimplementedFeatureStoreServiceServer
// expresses, made explicit now that we override the methods.
const errServiceNotWired = "feature store service not available"

// errPermissionDenied is a HANDLER-LEVEL sentinel for an authorization failure
// (authenticated caller lacking a required scope). The domain has no permission
// sentinel — it expresses scope failures with its generic ErrValidation, which
// would mis-map to InvalidArgument — and this layer owns the error→gRPC-code
// translation, so the handler defines and recognizes this sentinel itself.
// mapDomainError translates it to codes.PermissionDenied; scope-gated RPCs may also
// return that status directly (see RebuildViews). Wrapped with %w where a specific
// message helps, so callers still match errors.Is(err, errPermissionDenied).
var errPermissionDenied = errors.New("featurestore: permission denied")

// rebuildAdminScope is the elevated scope RebuildViews requires. It MUST match the
// domain's adminScope ("features:admin") — a full replay is expensive and
// operationally sensitive, so a routine write token must not reach it. Duplicated
// (not imported) because the constant is unexported in the domain and the handler
// performs the boundary check; the two are pinned to the same string by the
// RebuildViews authorization tests.
const rebuildAdminScope = "features:admin"

// hasScope reports whether the principal's credential carries the named scope. A
// plain linear scan — a token carries a handful of scopes, so a set would cost more
// than it saves. Kept as a named helper so the authorization check reads as prose
// at the call site (if !hasScope(...) { PermissionDenied }).
func hasScope(scopes []string, want string) bool {
	for _, s := range scopes {
		if s == want {
			return true
		}
	}
	return false
}

// ============================================================================
// principalFromContext — the MASS-ASSIGNMENT GUARD (identity from the token only)
// ============================================================================
//
// Every owner/team field the service stamps comes from HERE — the auth
// interceptor's verified Claims — and NEVER from a request field. A client could
// trivially set owner_team="victim-team" on a request body; reading it would let
// them write into / read from another team's namespace. By sourcing identity
// solely from the (cryptographically verified) token claims, that whole class of
// privilege escalation is structurally impossible.
//
// Returns Unauthenticated when no claims are present (the request reached the
// handler without passing the auth interceptor, or as an anonymous call). We map
// to domain.Principal — the domain's transport-agnostic caller type — so the
// service never sees grpcutil.Claims (Clean Architecture: the domain doesn't
// import another package's transport types).
func principalFromContext(ctx context.Context) (domain.Principal, error) {
	claims, ok := grpcutil.ClaimsFromContext(ctx)
	if !ok || claims == nil {
		return domain.Principal{}, status.Error(codes.Unauthenticated, "missing or invalid credentials")
	}
	// UserID + Team are the security-critical fields. An empty team would let a
	// caller fall into a global namespace; we refuse it rather than scope to "".
	if claims.UserID == "" || claims.Team == "" {
		return domain.Principal{}, status.Error(codes.Unauthenticated, "credentials missing required identity")
	}
	return domain.Principal{
		UserID: claims.UserID,
		Team:   claims.Team,
		Scopes: claims.Scopes,
	}, nil
}

// ============================================================================
// mapDomainError — the SINGLE domain-sentinel → gRPC-status translation point
// ============================================================================
//
// The domain speaks sentinel errors (errors.Is-friendly); the wire speaks gRPC
// status codes. This function is the ONLY place that translation happens, so the
// mapping is consistent across all six RPCs and auditable in one read.
//
// SECURITY — SANITIZATION: only KNOWN sentinels get their (curated, safe) message
// forwarded to the client. Any UNRECOGNIZED error (a Postgres driver error, a
// Redis timeout, a wrapped infra failure) collapses to a generic Internal with a
// fixed string — we NEVER echo err.Error() for an unknown error, because that is
// exactly how DSNs, table names, row data / PII, and stack hints leak to callers.
// The real error is still returned to the caller's logs via the server's logging
// interceptor; the CLIENT only ever sees the sanitized form.
//
// CODE CHOICES (why each sentinel maps where):
//   - ErrValidation/ErrSchemaViolation/ErrAsOfRequired/ErrBatchTooLarge →
//     InvalidArgument: the REQUEST is malformed; retrying unchanged won't help.
//   - ErrViewNotFound → NotFound: also used for cross-team ids (the domain
//     deliberately returns NotFound, not PermissionDenied, so a caller can't
//     probe which ids exist in other teams — preserve that here).
//   - ErrViewDeleted/ErrViewNameConflict → FailedPrecondition: the request is
//     well-formed but the resource is in the wrong STATE for it (retrying as-is
//     won't help until the state changes — FailedPrecondition's exact meaning).
//   - errPermissionDenied → PermissionDenied: the caller authenticated but lacks
//     the required scope (an AUTHORIZATION failure, not a malformed request). RPCs
//     gate scope at the handler (see RebuildViews) and return PermissionDenied
//     directly; this case completes the translation table so any permission error
//     that reaches the mapper also gets the right code rather than collapsing to
//     Internal.
func mapDomainError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrValidation),
		errors.Is(err, domain.ErrSchemaViolation),
		errors.Is(err, domain.ErrAsOfRequired),
		errors.Is(err, domain.ErrBatchTooLarge):
		// These sentinels are wrapped with a SPECIFIC, caller-safe message
		// (e.g. "feature %q not in schema") — forwarding err.Error() here is
		// intentional and safe: the domain authors these strings, they contain no
		// infrastructure detail.
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, errPermissionDenied):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, domain.ErrViewNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrViewDeleted),
		errors.Is(err, domain.ErrViewNameConflict):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		// UNKNOWN error — sanitize. Never leak err.Error() (could carry a DSN,
		// SQL, row data, secrets). The server's logging interceptor records the
		// real error for operators; the client gets only this generic message.
		return status.Error(codes.Internal, "internal error")
	}
}

// ============================================================================
// RPC: DefineFeatureView (unary) — define or evolve a view's schema
// ============================================================================
//
// Flow (the same four-step shape every unary method follows):
//  1. nil-svc guard, 2. extract principal (identity), 3. validate + convert the
//     request, 4. call the service and map result/error.
//
// SECURITY: only schema-shaping fields are read off the request. id, owner,
// team, version, timestamps are server-assigned inside the service from the
// Principal + clock + sequence — never copied from the wire (mass-assignment).
func (h *FeatureStoreHandler) DefineFeatureView(ctx context.Context, req *featurestorev1.DefineFeatureViewRequest) (*featurestorev1.DefineFeatureViewResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	p, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}

	// Boundary validation: catch the obviously-malformed request with a clean
	// message before bothering the service. The domain ALSO validates (it owns the
	// authoritative rules — empty schema, dup names, bad types); this is the cheap
	// early gate, not a replacement for it.
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if len(req.GetFeatures()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one feature is required")
	}

	in := domain.DefineFeatureViewInput{
		Name:           req.GetName(),
		Description:    req.GetDescription(),
		Entity:         protoToDomainEntity(req.GetEntity()),
		Features:       protoToDomainFeatureSpecs(req.GetFeatures()),
		IdempotencyKey: req.GetIdempotencyKey(),
	}

	view, err := h.svc.DefineFeatureView(ctx, p, in)
	if err != nil {
		return nil, mapDomainError(err)
	}
	return &featurestorev1.DefineFeatureViewResponse{
		FeatureView: domainToProtoFeatureView(view),
	}, nil
}

// ============================================================================
// RPC: GetFeatureView (unary) — one view by id OR name (a proto oneof handle)
// ============================================================================
//
// The request carries a `oneof handle { feature_view_id; name }`. We dispatch on
// the populated arm to the matching service method. Supplying neither is an
// InvalidArgument (the caller must pick a handle). Team scoping is enforced
// inside the service from the Principal, so a cross-team id returns NotFound.
func (h *FeatureStoreHandler) GetFeatureView(ctx context.Context, req *featurestorev1.GetFeatureViewRequest) (*featurestorev1.GetFeatureViewResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	p, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}

	var view domain.FeatureView
	// Switch on the concrete oneof arm. GetFeatureViewId()/GetName() return "" for
	// the unset arm, so we inspect the wrapper type to know which was actually set
	// (an explicit empty string is still a "set" choice the caller made).
	switch req.GetHandle().(type) {
	case *featurestorev1.GetFeatureViewRequest_FeatureViewId:
		id := req.GetFeatureViewId()
		if id == "" {
			return nil, status.Error(codes.InvalidArgument, "feature_view_id must not be empty")
		}
		view, err = h.svc.GetFeatureViewByID(ctx, p, id)
	case *featurestorev1.GetFeatureViewRequest_Name:
		name := req.GetName()
		if name == "" {
			return nil, status.Error(codes.InvalidArgument, "name must not be empty")
		}
		view, err = h.svc.GetFeatureViewByName(ctx, p, name)
	default:
		return nil, status.Error(codes.InvalidArgument, "a feature_view_id or name handle is required")
	}
	if err != nil {
		return nil, mapDomainError(err)
	}
	return &featurestorev1.GetFeatureViewResponse{
		FeatureView: domainToProtoFeatureView(view),
	}, nil
}

// ============================================================================
// RPC: ListFeatureViews (unary, paginated) — the team's catalog
// ============================================================================
//
// page_size is CLAMPED (not rejected) per the contract: a too-large page is
// harmless to bound silently. We clamp here at the boundary AND the service
// clamps (defense in depth + the constant is the single source of truth). Team
// scoping is server-side; name_filter is the only client-controlled filter.
func (h *FeatureStoreHandler) ListFeatureViews(ctx context.Context, req *featurestorev1.ListFeatureViewsRequest) (*featurestorev1.ListFeatureViewsResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	p, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}

	opts := paginationToListOptions(req.GetPagination())

	views, nextToken, err := h.svc.ListFeatureViews(ctx, p, req.GetNameFilter(), opts)
	if err != nil {
		return nil, mapDomainError(err)
	}

	protoViews := make([]*featurestorev1.FeatureView, 0, len(views))
	for _, v := range views {
		protoViews = append(protoViews, domainToProtoFeatureView(v))
	}
	return &featurestorev1.ListFeatureViewsResponse{
		FeatureViews: protoViews,
		Pagination:   newPaginationResponse(nextToken),
	}, nil
}

// ============================================================================
// RPC: WriteFeatures (unary) — the core EVENT-SOURCING write (append a batch)
// ============================================================================
//
// Validation we do at the boundary: target id present, batch non-empty, batch
// not over the hard cap (rejected loudly — see ErrBatchTooLarge rationale).
// Per-row schema validation (name/type/dimension) remains the SERVICE's job (it
// owns the authoritative check); the handler's job is to convert each wire Value
// into the correct domain FeatureValue KIND, which requires the view's schema.
//
// WHY the handler MUST be schema-driven here (and accepts the extra read):
// google.protobuf.Value is lossy — INT64 rides inside a `number_value` and
// TIMESTAMP rides inside a `string_value`, so a wire number/string is ambiguous
// (DOUBLE-or-INT64 / STRING-or-TIMESTAMP). Only the declared schema disambiguates.
// The domain's validateRow does a STRICT Kind-equality check (it does NOT narrow a
// Double to Int64 or parse a String to a Time), so if the handler guessed wrong
// every INT64/TIMESTAMP write would be rejected. We therefore fetch the view's
// schema first and convert against it (see protoRowsToDomain / protoValueToDomain).
//
// TOCTOU: this adds one read and a small window vs a concurrent DefineFeatureView.
// That is safe because schema evolution is ADDITIVE (features are added / the
// version is bumped; a feature is never re-typed or removed), and the service
// re-validates authoritatively — a racing change at worst yields a clean
// ErrSchemaViolation, never a corrupt or mistyped write.
func (h *FeatureStoreHandler) WriteFeatures(ctx context.Context, req *featurestorev1.WriteFeaturesRequest) (*featurestorev1.WriteFeaturesResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	p, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}

	if req.GetFeatureViewId() == "" {
		return nil, status.Error(codes.InvalidArgument, "feature_view_id is required")
	}
	rows := req.GetFeatures()
	if len(rows) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one feature row is required")
	}
	// Hard cap REJECTED at the boundary (loud failure — never silently truncate
	// feature rows; that would corrupt counts and point-in-time history). The
	// service re-checks (the constant is the contract).
	if len(rows) > domain.MaxBatchSize {
		return nil, status.Errorf(codes.InvalidArgument, "batch of %d rows exceeds maximum of %d", len(rows), domain.MaxBatchSize)
	}

	// SCHEMA FETCH for value disambiguation. Team scoping is enforced inside the
	// service (a cross-team / missing id is ErrViewNotFound; a deleted view is still
	// returned here and refused by the service on the actual write). We map its
	// error through the same translator so e.g. NotFound surfaces correctly rather
	// than being swallowed.
	view, err := h.svc.GetFeatureViewByID(ctx, p, req.GetFeatureViewId())
	if err != nil {
		return nil, mapDomainError(err)
	}

	domainRows, err := protoRowsToDomain(rows, featureSpecTypes(view))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	res, err := h.svc.WriteFeatures(ctx, p, domain.WriteFeaturesInput{
		FeatureViewID:  req.GetFeatureViewId(),
		Rows:           domainRows,
		IdempotencyKey: req.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, mapDomainError(err)
	}
	return &featurestorev1.WriteFeaturesResponse{
		WrittenCount:          int32(res.WrittenCount),
		WrittenThroughVersion: res.WrittenThroughVersion,
	}, nil
}

// ============================================================================
// RPC: GetOnlineFeatures (unary) — the inference HOT PATH (latest values)
// ============================================================================
//
// entity_ids capped at MaxBatchSize (rejected over-cap — bounds hot-path latency
// and payload). Returns found vectors + the explicit missing ids so a caller can
// default/fallback rather than guess.
func (h *FeatureStoreHandler) GetOnlineFeatures(ctx context.Context, req *featurestorev1.GetOnlineFeaturesRequest) (*featurestorev1.GetOnlineFeaturesResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	p, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}

	if req.GetFeatureViewId() == "" {
		return nil, status.Error(codes.InvalidArgument, "feature_view_id is required")
	}
	entityIDs := req.GetEntityIds()
	if len(entityIDs) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one entity_id is required")
	}
	if len(entityIDs) > domain.MaxBatchSize {
		return nil, status.Errorf(codes.InvalidArgument, "entity_ids batch of %d exceeds maximum of %d", len(entityIDs), domain.MaxBatchSize)
	}

	page, err := h.svc.GetOnlineFeatures(ctx, p, domain.GetOnlineFeaturesInput{
		FeatureViewID: req.GetFeatureViewId(),
		EntityIDs:     entityIDs,
		FeatureNames:  req.GetFeatureNames(),
	})
	if err != nil {
		return nil, mapDomainError(err)
	}
	return &featurestorev1.GetOnlineFeaturesResponse{
		Vectors:          domainToProtoFeatureVectors(page.Vectors),
		MissingEntityIds: page.MissingEntityIDs,
	}, nil
}

// ============================================================================
// RPC: GetHistoricalFeatures (unary, paginated) — POINT-IN-TIME read
// ============================================================================
//
// as_of is REQUIRED (a missing cutoff means "now" — non-reproducible). We catch a
// nil/zero as_of at the boundary AND the service enforces ErrAsOfRequired; either
// way the caller gets InvalidArgument. entity_ids capped at MaxBatchSize;
// page_size clamped.
func (h *FeatureStoreHandler) GetHistoricalFeatures(ctx context.Context, req *featurestorev1.GetHistoricalFeaturesRequest) (*featurestorev1.GetHistoricalFeaturesResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	p, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}

	if req.GetFeatureViewId() == "" {
		return nil, status.Error(codes.InvalidArgument, "feature_view_id is required")
	}
	entityIDs := req.GetEntityIds()
	if len(entityIDs) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one entity_id is required")
	}
	if len(entityIDs) > domain.MaxBatchSize {
		return nil, status.Errorf(codes.InvalidArgument, "entity_ids batch of %d exceeds maximum of %d", len(entityIDs), domain.MaxBatchSize)
	}
	asOf := protoToTime(req.GetAsOf())
	if asOf.IsZero() {
		return nil, status.Error(codes.InvalidArgument, "as_of is required for historical reads")
	}

	page, err := h.svc.GetHistoricalFeatures(ctx, p, domain.GetHistoricalFeaturesInput{
		FeatureViewID: req.GetFeatureViewId(),
		EntityIDs:     entityIDs,
		AsOf:          asOf,
		FeatureNames:  req.GetFeatureNames(),
		Pagination:    paginationToListOptions(req.GetPagination()),
	})
	if err != nil {
		return nil, mapDomainError(err)
	}
	return &featurestorev1.GetHistoricalFeaturesResponse{
		Vectors:          domainToProtoFeatureVectors(page.Vectors),
		MissingEntityIds: page.MissingEntityIDs,
		Pagination:       newPaginationResponse(page.NextPageToken),
	}, nil
}

// ============================================================================
// RPC: DeleteFeatureView (unary) — SOFT retire (append terminal event)
// ============================================================================
//
// Only the immutable id is accepted (no name for a destructive op — avoids
// rename-race ambiguity). Idempotent via idempotency_key. Returns the terminal
// view (deleted_at set) so the caller can confirm without a follow-up read.
func (h *FeatureStoreHandler) DeleteFeatureView(ctx context.Context, req *featurestorev1.DeleteFeatureViewRequest) (*featurestorev1.DeleteFeatureViewResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	p, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}

	if req.GetFeatureViewId() == "" {
		return nil, status.Error(codes.InvalidArgument, "feature_view_id is required")
	}

	view, err := h.svc.DeleteFeatureView(ctx, p, req.GetFeatureViewId(), req.GetIdempotencyKey())
	if err != nil {
		return nil, mapDomainError(err)
	}
	return &featurestorev1.DeleteFeatureViewResponse{
		FeatureView: domainToProtoFeatureView(view),
	}, nil
}

// ============================================================================
// RPC: RebuildViews (SERVER-STREAMING) — replay the log to regenerate views
// ============================================================================
//
// This is the only streaming RPC. THREE things make the streaming handler
// different from the unary ones (all interview-critical):
//
//  1. RETURN SHAPE: a streaming handler returns only `error`. There is no resp to
//     return — every response frame flows through stream.Send. So the nil-svc
//     guard returns the status ERROR directly (not nil, err).
//
//  2. THE emit CALLBACK BRIDGES DOMAIN → STREAM: the service is transport-
//     agnostic; it pushes domain.RebuildProgress frames into an emit callback we
//     provide. Our callback converts each frame to a proto RebuildViewsResponse
//     and calls stream.Send. If Send fails (client disconnected, deadline), we
//     return that error from emit, which the service treats as "abort the
//     rebuild" — so a gone client stops the expensive replay promptly instead of
//     wasting work.
//
//  3. CONTEXT CANCELLATION: we honor stream.Context() — if the client cancels or
//     the deadline fires, emit returns the ctx error and the service aborts. We
//     pass stream.Context() into the service so its own loop can also observe
//     cancellation; emit is the second line of defense.
//
// AUTHORIZATION: RebuildViews is ADMIN-gated ("features:admin"). We enforce the
// scope HERE, at the handler, and return codes.PermissionDenied for an under-scoped
// caller — the code the proto and this method's contract promise.
//
// WHY the handler gate (not just the domain's): the domain DOES re-check the scope
// (defense in depth), but it expresses an under-scoped call with its generic
// ErrValidation sentinel, which mapDomainError translates to InvalidArgument — the
// WRONG code for an authorization failure (clients and SLO alerting treat
// "malformed request" very differently from "not authorized"). The domain has no
// dedicated permission sentinel and the handler owns the sentinel→gRPC-code
// translation, so the correct PermissionDenied is produced here, at the boundary
// the authorization protects, BEFORE invoking the service. The domain check remains
// as a second line of defense for any caller that reaches it by another path.
func (h *FeatureStoreHandler) RebuildViews(req *featurestorev1.RebuildViewsRequest, stream featurestorev1.FeatureStoreService_RebuildViewsServer) error {
	// Streaming nil-svc guard: return the status error itself (no resp value).
	if h.svc == nil {
		return status.Error(codes.Unimplemented, errServiceNotWired)
	}
	ctx := stream.Context()
	p, err := principalFromContext(ctx)
	if err != nil {
		return err
	}

	// ADMIN SCOPE GATE → PermissionDenied. An authenticated caller WITHOUT the admin
	// scope is authorized-but-insufficient (PermissionDenied), distinct from an
	// unauthenticated caller (Unauthenticated, handled by principalFromContext above)
	// and from a malformed request (InvalidArgument).
	if !hasScope(p.Scopes, rebuildAdminScope) {
		return status.Errorf(codes.PermissionDenied, "RebuildViews requires the %q scope", rebuildAdminScope)
	}

	in := domain.RebuildViewsInput{
		FeatureViewID: req.GetFeatureViewId(),
		Target:        protoToDomainRebuildTarget(req.GetTarget()),
	}

	// emit is the domain→wire bridge. Each progress frame becomes a stream.Send.
	// Returning an error aborts the rebuild (the service stops replaying).
	emit := func(progress domain.RebuildProgress) error {
		// Cheap, explicit cancellation check before each Send: if the client is
		// gone we surface ctx.Err() so the service stops immediately rather than
		// attempting a Send that would fail anyway.
		if err := ctx.Err(); err != nil {
			return err
		}
		return stream.Send(&featurestorev1.RebuildViewsResponse{
			EventsReplayed:       progress.EventsReplayed,
			TotalEvents:          progress.TotalEvents,
			CurrentFeatureViewId: progress.CurrentFeatureViewID,
			Done:                 progress.Done,
		})
	}

	if err := h.svc.RebuildViews(ctx, p, in, emit); err != nil {
		// A Send/ctx error surfaced through emit is a transport failure, not a
		// domain error — return it as-is so the client sees the real stream status
		// (Canceled/DeadlineExceeded) rather than a misleading Internal. Domain
		// sentinels (e.g. ErrViewNotFound for a scoped rebuild, a permission error)
		// get the standard mapping.
		if errors.Is(err, ctx.Err()) && ctx.Err() != nil {
			return err
		}
		return mapDomainError(err)
	}
	return nil
}
