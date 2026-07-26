// registry_handler_rpc.go — the per-RPC method bodies for RegistryHandler.
//
// ============================================================================
// WHAT THIS FILE IS (and what it deliberately is NOT)
// ============================================================================
//
// registry_handler.go holds the TYPE (RegistryHandler embeds
// UnimplementedRegistryServiceServer and carries the domain.RegistryService).
// THIS file overrides the RPC methods the registry actually implements, turning
// the scaffold into a working server.
//
// The handler's only three jobs (see the package header) realized here, per RPC:
//
//	(1) PROTO → DOMAIN: pull the CLIENT-owned fields from the request into a
//	    domain *Input, and build the SERVER-authoritative domain.Actor from the
//	    auth claims the grpcutil interceptor stamped on the context — never from
//	    the request body. Keeping the Actor out of the Input is the structural
//	    mass-assignment guard.
//	(2) CALL the domain service method (the handler holds the INTERFACE; it has
//	    no idea whether the impl is Postgres+Redis or a test stub).
//	(3) DOMAIN → PROTO + ERROR MAPPING: convert the domain result to a proto
//	    Response and translate domain sentinel errors to precise gRPC codes,
//	    NEVER leaking internal error text to the client.
//
// ============================================================================
// FOUR RPCs ARE INTENTIONALLY LEFT ON THE EMBEDDED Unimplemented BASE
// ============================================================================
//
// The generated RegistryServiceServer interface has 13 RPCs. The four NOT
// overridden here —
//
//	SearchByTag        — a read-projection facet (per-tag Redis set) with no domain
//	                     method yet; it is a pure query the read store will answer.
//	GetUploadURL       — re-issues a presigned PUT URL; belongs to the object-storage
//	                     adapter (MinIO/S3 presign), not the registry domain.
//	GetDownloadURL     — issues a presigned GET URL; same object-storage concern.
//	ConfirmVersionUpload — drives PENDING_UPLOAD → READY, but the authoritative
//	                     artifact digest/size/path MUST be SERVER-MEASURED by the
//	                     same object-storage re-verification adapter. Implementing
//	                     it before that adapter exists would force trusting a CLIENT-
//	                     supplied digest as the content-addressable identity — a
//	                     content-integrity hole. Left on the base until the adapter
//	                     lands. See the detailed rationale on the method comment below.
//
// — are STORAGE/projection concerns whose adapters land in later phases. Because
// RegistryHandler embeds UnimplementedRegistryServiceServer, those four already
// return a correct codes.Unimplemented (not a panic, not a nil-deref). This is
// exactly the forward-compatibility property the embed buys us: we implement the
// subset the domain owns SAFELY today and the base answers the rest safely.
//
// ============================================================================
// THE ERROR-MAPPING TABLE (the single translation point handler→wire)
// ============================================================================
//
// The domain returns BUSINESS sentinels (errors.go) wrapped with %w. The handler
// is the ONLY place that knows gRPC codes; it maps each sentinel with errors.Is:
//
//	domain sentinel                     → gRPC code            (why)
//	──────────────────────────────────────────────────────────────────────────
//	ErrValidation                       → InvalidArgument      malformed input
//	ErrModelNotFound / ErrVersionNotFound → NotFound           absent / not in team
//	ErrModelNameTaken / ErrVersionExists  → AlreadyExists      uniqueness collision
//	ErrModelArchived                    → FailedPrecondition   state forbids op
//	ErrIllegalTransition                → FailedPrecondition   state-machine guard
//	ErrVersionNotReady                  → FailedPrecondition   artifact not READY
//	ErrInvalidStatusTransition          → FailedPrecondition   bad status move
//	(anything else)                     → Internal             SANITIZED message
//
// WHY FailedPrecondition (not Aborted) for the state-machine/archived/ready
// cases: gRPC's guidance is FailedPrecondition = "the system state is not right
// for the operation; the client should NOT retry until it fixes state" (e.g.
// promote a non-READY version → make it READY first). Aborted is for
// concurrency conflicts the client CAN retry (optimistic-lock/transaction
// aborts). Our promotion conflicts are deterministic state rules, not races —
// hence FailedPrecondition. (If PromoteVersionTx ever surfaced a serialization
// failure as a distinct sentinel, THAT would map to Aborted.)
//
// WHY the catch-all is Internal with a FIXED string: an unmapped error is, by
// definition, something we did not anticipate — it may carry a DSN, a row value,
// a stack fragment, PII. Returning err.Error() to the client would leak it. We
// log the real error server-side (for the operator) and return a generic
// "internal error" to the caller. This is the security boundary the task demands.
//
// KEEPING INTERNAL ERRORS OFF THE WIRE: one mapping
// function (toStatus) that only ever emits sentinel-derived messages or a fixed
// "internal error"; the real error is logged, never serialized to the wire.
// ============================================================================
package handler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	registryv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/registry/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/domain"
)

// errServiceNotWired is the FIXED message returned when a handler runs against a
// nil domain service. WHY a guard at all: this binary can be started in the
// scaffold/repo phases with svc==nil (the embedded base normally answers, but
// once we OVERRIDE a method the embedded base no longer shields it — our method
// runs and would nil-deref). Returning codes.Unimplemented (not Internal) is the
// honest signal: "this server is not fully wired to serve this RPC yet", which is
// the same contract the embedded base gives for the un-overridden RPCs.
const errServiceNotWired = "registry service is not wired"

// ============================================================================
// IDENTITY EXTRACTION — the trust boundary
// ============================================================================

// actorFromContext builds the SERVER-AUTHORITATIVE domain.Actor from the auth
// claims the grpcutil auth interceptor placed on the context. This is the ONLY
// source of caller identity the handler trusts — it never reads owner/team from a
// request body. If the claims are missing (an unauthenticated call reached a
// method that requires identity) or carry no user/team, we return Unauthenticated
// rather than guessing.
//
// WHY map Claims→Actor here (not pass Claims into the domain): the domain must not
// import pkg/grpcutil (a transport package). Actor is the domain's own neutral
// identity type; the handler is the seam that converts the transport claims into
// it — the same proto↔domain decoupling discipline applied to identity.
func actorFromContext(ctx context.Context) (domain.Actor, error) {
	claims, ok := grpcutil.ClaimsFromContext(ctx)
	if !ok || claims == nil {
		return domain.Actor{}, status.Error(codes.Unauthenticated, "missing authentication")
	}
	// A token with no subject/team is structurally unusable for a team-scoped,
	// owner-stamping command — reject rather than write a model owned by "".
	if claims.UserID == "" || claims.Team == "" {
		return domain.Actor{}, status.Error(codes.Unauthenticated, "incomplete authentication")
	}
	return domain.Actor{UserID: claims.UserID, Team: claims.Team}, nil
}

// ============================================================================
// ERROR MAPPING — domain sentinel → gRPC status (the single translation point)
// ============================================================================

// toStatus converts a domain error into a gRPC status error with a SANITIZED,
// client-safe message. For mapped sentinels it returns the matching code; for an
// unmapped error it LOGS the real error (operator visibility) and returns a fixed
// "internal error" so nothing internal reaches the wire.
//
// op is the RPC name, used only in the server-side log line (never sent to the
// client). errors.Is is used (not == ) because the domain wraps sentinels with %w
// to attach a human message while preserving identity for matching.
func toStatus(ctx context.Context, op string, err error) error {
	switch {
	case err == nil:
		return nil

	// Validation — the request was malformed. The wrapped message is safe to
	// surface (it describes the client's own bad input, e.g. "model name is
	// required"), so we pass err.Error() through for these client-fault codes.
	case errors.Is(err, domain.ErrValidation):
		return status.Error(codes.InvalidArgument, err.Error())

	// Not found — absent entity (or not in the caller's team; the domain
	// deliberately does not distinguish, to avoid leaking other teams' existence).
	case errors.Is(err, domain.ErrModelNotFound):
		return status.Error(codes.NotFound, "model not found")
	case errors.Is(err, domain.ErrVersionNotFound):
		return status.Error(codes.NotFound, "version not found")

	// Already exists — uniqueness collision the idempotency path didn't absorb.
	case errors.Is(err, domain.ErrModelNameTaken):
		return status.Error(codes.AlreadyExists, "model name already exists in team")
	case errors.Is(err, domain.ErrVersionExists):
		return status.Error(codes.AlreadyExists, "version already exists for model")

	// Failed precondition — the request is well-formed but the entity's state
	// forbids the operation; the client must change state, not blindly retry.
	case errors.Is(err, domain.ErrModelArchived):
		return status.Error(codes.FailedPrecondition, "model is archived")
	case errors.Is(err, domain.ErrIllegalTransition):
		return status.Error(codes.FailedPrecondition, "illegal stage transition")
	case errors.Is(err, domain.ErrVersionNotReady):
		return status.Error(codes.FailedPrecondition, "version artifact is not ready")
	case errors.Is(err, domain.ErrInvalidStatusTransition):
		return status.Error(codes.FailedPrecondition, "invalid version status transition")

	// Anything else is unanticipated: log the truth, return a generic message.
	default:
		slog.ErrorContext(ctx, "registry handler: unmapped internal error",
			slog.String("rpc", op), slog.Any("error", err))
		return status.Error(codes.Internal, "internal error")
	}
}

// ============================================================================
// ENUM MAPPING — proto enum ↔ domain enum, EXPLICITLY (never by int cast)
// ============================================================================
//
// The proto enum and the domain enum happen to share numeric values today, but we
// map them with explicit switches, not `domain.ModelStage(protoEnum)`. WHY: a cast
// silently couples the two — the day someone reorders the proto enum (or the
// domain adds a stage the API hasn't), a cast would mis-map with NO compile error.
// An explicit switch makes the correspondence auditable and lets the two schemas
// evolve independently, which is the whole point of keeping a domain enum separate
// from the wire enum.

// domainStageFromProto maps a wire ModelStage to the domain ModelStage. An
// unrecognized value maps to StageUnspecified (the callers treat that as "invalid"
// or "any" depending on context).
func domainStageFromProto(s registryv1.ModelStage) domain.ModelStage {
	switch s {
	case registryv1.ModelStage_MODEL_STAGE_DEV:
		return domain.StageDev
	case registryv1.ModelStage_MODEL_STAGE_STAGING:
		return domain.StageStaging
	case registryv1.ModelStage_MODEL_STAGE_PRODUCTION:
		return domain.StageProduction
	case registryv1.ModelStage_MODEL_STAGE_ARCHIVED:
		return domain.StageArchived
	default:
		return domain.StageUnspecified
	}
}

// protoStageFromDomain maps a domain ModelStage to the wire ModelStage.
func protoStageFromDomain(s domain.ModelStage) registryv1.ModelStage {
	switch s {
	case domain.StageDev:
		return registryv1.ModelStage_MODEL_STAGE_DEV
	case domain.StageStaging:
		return registryv1.ModelStage_MODEL_STAGE_STAGING
	case domain.StageProduction:
		return registryv1.ModelStage_MODEL_STAGE_PRODUCTION
	case domain.StageArchived:
		return registryv1.ModelStage_MODEL_STAGE_ARCHIVED
	default:
		return registryv1.ModelStage_MODEL_STAGE_UNSPECIFIED
	}
}

// protoStatusFromDomain maps a domain VersionStatus to the wire VersionStatus.
func protoStatusFromDomain(s domain.VersionStatus) registryv1.VersionStatus {
	switch s {
	case domain.StatusPendingUpload:
		return registryv1.VersionStatus_VERSION_STATUS_PENDING_UPLOAD
	case domain.StatusReady:
		return registryv1.VersionStatus_VERSION_STATUS_READY
	case domain.StatusFailed:
		return registryv1.VersionStatus_VERSION_STATUS_FAILED
	default:
		return registryv1.VersionStatus_VERSION_STATUS_UNSPECIFIED
	}
}

// ============================================================================
// DOMAIN → PROTO entity converters
// ============================================================================

// modelToProto maps a domain.Model to its wire form. Timestamps are converted to
// protobuf Timestamps only when set (a zero time.Time → nil, so the JSON/proto is
// clean rather than carrying a 1970 sentinel). Tags are copied as-is (a nil map
// marshals to an empty map, which is the intended "no tags" wire shape).
func modelToProto(m domain.Model) *registryv1.Model {
	pm := &registryv1.Model{
		Id:                m.ID,
		Name:              m.Name,
		Description:       m.Description,
		OwnerId:           m.OwnerID,
		Team:              m.Team,
		Framework:         m.Framework,
		TaskType:          m.TaskType,
		Tags:              m.Tags,
		ProductionVersion: m.ProductionVersion,
		LatestVersion:     m.LatestVersion,
	}
	if !m.CreatedAt.IsZero() {
		pm.CreatedAt = timestamppb.New(m.CreatedAt)
	}
	if !m.UpdatedAt.IsZero() {
		pm.UpdatedAt = timestamppb.New(m.UpdatedAt)
	}
	if !m.ArchivedAt.IsZero() {
		pm.ArchivedAt = timestamppb.New(m.ArchivedAt)
	}
	return pm
}

// versionToProto maps a domain.ModelVersion to its wire form, including the
// explicit stage/status enum mapping and the metrics map → structpb.Struct
// conversion.
func versionToProto(v domain.ModelVersion) *registryv1.ModelVersion {
	pv := &registryv1.ModelVersion{
		Id:             v.ID,
		ModelId:        v.ModelID,
		Version:        v.Version,
		Description:    v.Description,
		ArtifactPath:   v.ArtifactPath,
		ArtifactDigest: v.ArtifactDigest,
		SizeBytes:      v.SizeBytes,
		Metrics:        metricsToStruct(v.Metrics),
		Stage:          protoStageFromDomain(v.Stage),
		Status:         protoStatusFromDomain(v.Status),
		CreatedBy:      v.CreatedBy,
	}
	if !v.CreatedAt.IsZero() {
		pv.CreatedAt = timestamppb.New(v.CreatedAt)
	}
	return pv
}

// metricsToStruct converts the domain's map[string]float64 metrics into the
// proto's google.protobuf.Struct. WHY the domain uses a float map while the wire
// uses Struct: metrics are numeric by nature, so the domain keeps a pure stdlib
// type; the wire uses Struct because the proto chose a schema-free shape. The
// handler is the seam that converts. A nil/empty map → nil Struct (clean wire).
func metricsToStruct(metrics map[string]float64) *structpb.Struct {
	if len(metrics) == 0 {
		return nil
	}
	fields := make(map[string]*structpb.Value, len(metrics))
	for k, v := range metrics {
		fields[k] = structpb.NewNumberValue(v)
	}
	return &structpb.Struct{Fields: fields}
}

// metricsFromStruct converts an incoming proto Struct of metrics into the domain
// map[string]float64. Only NUMBER values are accepted — a non-numeric metric value
// (string/bool/nested) is a client error (InvalidArgument), because the domain's
// metrics are strictly numeric. Returns nil for a nil/empty struct.
func metricsFromStruct(s *structpb.Struct) (map[string]float64, error) {
	if s == nil || len(s.GetFields()) == 0 {
		return nil, nil
	}
	out := make(map[string]float64, len(s.GetFields()))
	for k, v := range s.GetFields() {
		// structpb.Value is a oneof; we require the number arm. GetNumberValue on a
		// non-number returns 0, so we must check the kind explicitly to reject it.
		if _, ok := v.GetKind().(*structpb.Value_NumberValue); !ok {
			return nil, fmt.Errorf("%w: metric %q must be numeric", domain.ErrValidation, k)
		}
		out[k] = v.GetNumberValue()
	}
	return out, nil
}

// ============================================================================
// PAGINATION helpers (clamp request size, build response cursor)
// ============================================================================

// pageSizeFromProto reads the requested page size from a commonv1.PaginationRequest
// and clamps it into [DefaultPageSize, MaxPageSize] semantics: 0/unset → 0 (the
// domain service then applies DefaultPageSize), a value over MaxPageSize is
// rejected as InvalidArgument rather than silently clamped.
//
// WHY reject an over-cap size instead of clamping it: silently returning fewer
// items than asked makes a client's pagination math wrong (it thinks it asked for
// 1000, got 100, and may conclude the set is exhausted). An explicit
// InvalidArgument tells the client its request was out of contract. (Negative is
// also rejected.) The domain ALSO clamps defensively — defense in depth — but the
// handler gives the clean client-facing error.
func pageSizeFromProto(p *commonv1.PaginationRequest) (int, error) {
	size := int(p.GetPageSize())
	if size < 0 {
		return 0, status.Error(codes.InvalidArgument, "page_size must not be negative")
	}
	if size > domain.MaxPageSize {
		return 0, status.Errorf(codes.InvalidArgument, "page_size must not exceed %d", domain.MaxPageSize)
	}
	return size, nil
}

// paginationResponse builds the wire PaginationResponse from a next-page cursor plus
// the total count of the full result set.
//
// total is the count of the WHOLE filtered set (not just this page), surfaced as the
// proto's total_count. The BFF dashboard reads exactly this field (it calls ListModels
// with page_size=1 to cheaply learn "how many models?"). Leaving it 0 — as an earlier
// version did — made the dashboard render "0 models" while the list showed the real
// rows. Callers that genuinely cannot/needn't compute a total pass 0 (the proto's
// documented "total unknown"); ListModels now passes the real count from the read
// model, while ListVersions still passes 0 (no consumer needs a version total yet).
func paginationResponse(nextToken string, total int) *commonv1.PaginationResponse {
	return &commonv1.PaginationResponse{
		NextPageToken: nextToken,
		TotalCount:    int32(total),
	}
}

// ============================================================================
// COMMANDS
// ============================================================================

// RegisterModel creates a new model. The owner/team are taken from the auth claims
// (via actorFromContext), NEVER from the request — the request struct doesn't even
// carry them. The handler validates the one required client field (name) for a
// fast, transport-level rejection, then defers the authoritative validation +
// uniqueness + idempotency to the domain service.
func (h *RegistryHandler) RegisterModel(ctx context.Context, req *registryv1.RegisterModelRequest) (*registryv1.RegisterModelResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// Cheap transport-level guard. The domain re-validates authoritatively; this
	// just turns the most common client mistake into a clean InvalidArgument
	// without a round trip into the service.
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	in := domain.RegisterModelInput{
		Name:           req.GetName(),
		Description:    req.GetDescription(),
		Framework:      req.GetFramework(),
		TaskType:       req.GetTaskType(),
		Tags:           req.GetTags(),
		IdempotencyKey: req.GetIdempotencyKey(),
	}
	model, err := h.svc.RegisterModel(ctx, actor, in)
	if err != nil {
		return nil, toStatus(ctx, "RegisterModel", err)
	}
	return &registryv1.RegisterModelResponse{Model: modelToProto(model)}, nil
}

// UpdateModel patches the small mutable surface (description/tags) using the
// explicit apply flags. The flags are carried straight through to the domain Input
// so an omitted field never silently clears stored data (the proto3-presence fix).
func (h *RegistryHandler) UpdateModel(ctx context.Context, req *registryv1.UpdateModelRequest) (*registryv1.UpdateModelResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	in := domain.UpdateModelInput{
		ModelID:           req.GetId(),
		Description:       req.GetDescription(),
		UpdateDescription: req.GetUpdateDescription(),
		Tags:              req.GetTags(),
		ReplaceTags:       req.GetReplaceTags(),
		IdempotencyKey:    req.GetIdempotencyKey(),
	}
	model, err := h.svc.UpdateModel(ctx, actor, in)
	if err != nil {
		return nil, toStatus(ctx, "UpdateModel", err)
	}
	return &registryv1.UpdateModelResponse{Model: modelToProto(model)}, nil
}

// CreateVersion cuts a new version of an existing model. The artifact bytes never
// flow through here — only metadata. The service returns the version in
// PENDING_UPLOAD; the presigned upload URL is the object-storage adapter's job (a
// later phase), so upload_url / upload_url_expires_at are left unset on the
// response for now. We DO surface the version itself.
func (h *RegistryHandler) CreateVersion(ctx context.Context, req *registryv1.CreateVersionRequest) (*registryv1.CreateVersionResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetModelId() == "" {
		return nil, status.Error(codes.InvalidArgument, "model_id is required")
	}
	// Convert + validate metrics at the boundary (reject non-numeric metric values
	// before they ever reach the domain).
	metrics, err := metricsFromStruct(req.GetMetrics())
	if err != nil {
		return nil, toStatus(ctx, "CreateVersion", err)
	}

	in := domain.CreateVersionInput{
		ModelID:        req.GetModelId(),
		Version:        req.GetVersion(), // "" → service auto-assigns the next label
		Description:    req.GetDescription(),
		Metrics:        metrics,
		IdempotencyKey: req.GetIdempotencyKey(),
	}
	version, err := h.svc.CreateVersion(ctx, actor, in)
	if err != nil {
		return nil, toStatus(ctx, "CreateVersion", err)
	}
	return &registryv1.CreateVersionResponse{Version: versionToProto(version)}, nil
}

// ConfirmVersionUpload is INTENTIONALLY NOT OVERRIDDEN here — it is left on the
// embedded UnimplementedRegistryServiceServer base, which returns
// codes.Unimplemented. This is a deliberate SECURITY decision, not an oversight.
//
// ----------------------------------------------------------------------------
// WHY: content-integrity / mass-assignment — the digest must be SERVER-MEASURED
// ----------------------------------------------------------------------------
// ConfirmVersionUpload drives PENDING_UPLOAD → READY. The whole point of a
// version's ArtifactDigest is that it is a CONTENT-ADDRESSABLE identity: a hash
// the SERVER computed by re-reading the uploaded object from object storage. The
// READY edge also EMITS ModelVersionReady (→ fp.models.version.ready), which
// serving/billing/orchestrator trust to pull and run that exact artifact.
//
// A previous draft of this method passed Success=true and copied the client's
// req.ExpectedDigest into domain.MarkVersionReadyInput.ArtifactDigest, which the
// domain (MarkVersionReady) unconditionally persists as v.ArtifactDigest before
// flipping Status=READY. That made the "expected digest" — a CLIENT ASSERTION —
// the AUTHORITATIVE digest with no re-verification anywhere in the path. Net
// effect: any authenticated team member could confirm a PENDING_UPLOAD version
// with an ARBITRARY digest and the version became READY with that forged,
// attacker-chosen content identity, then fanned out to downstream consumers.
// That defeats the exact guarantee the digest exists to provide.
//
// ----------------------------------------------------------------------------
// WHY UNIMPLEMENTED RATHER THAN A "PARTIAL" IMPLEMENTATION
// ----------------------------------------------------------------------------
// The authoritative ArtifactDigest/ArtifactPath/SizeBytes can ONLY come from a
// server-side re-read of the object — the object-storage re-verification adapter
// (MinIO/S3), which lands in a later phase alongside GetUploadURL/GetDownloadURL
// (the other storage-concern RPCs left on the base above). Until that adapter
// exists there is NO trustworthy way to reach a READY state, so the only honest
// behavior is to make the transition UNREACHABLE: returning codes.Unimplemented
// is truthful ("this server cannot confirm uploads yet"), whereas accepting the
// call and flipping to READY on client input would be a lie that ships a
// security hole. We do NOT expose a client-driven path that writes the measured
// fields, because there is no request field that may legitimately land there.
//
// When the storage adapter arrives, the correct shape is: the adapter re-reads
// the object, computes the digest/size/path itself, cross-checks the optional
// client ExpectedDigest as a NON-authoritative assertion (mismatch → reject),
// and ONLY THEN calls MarkVersionReady with SERVER-measured values. That work
// also touches the domain (a separate, non-authoritative ExpectedDigest field
// the domain only cross-checks, never copies into v.ArtifactDigest) and is out
// of scope for this handler-only change.
//
// WHY Unimplemented AND NOT BEST-EFFORT: because the
// READY edge is a trust boundary that fans out a content-addressable identity to
// other services. A best-effort confirm that trusts client input isn't a smaller
// feature, it's a content-integrity vulnerability. Unimplemented keeps the
// dangerous edge unreachable until the server can actually measure the artifact.

// PromoteVersion advances a version through the stage state machine. target_stage
// is the ONLY place a client influences stage, and even here it is a REQUEST the
// domain validates against the state machine. We validate that the wire stage maps
// to a REAL stage (not UNSPECIFIED) at the boundary for a clean InvalidArgument.
func (h *RegistryHandler) PromoteVersion(ctx context.Context, req *registryv1.PromoteVersionRequest) (*registryv1.PromoteVersionResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetVersionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "version_id is required")
	}
	target := domainStageFromProto(req.GetTargetStage())
	if !target.IsValid() {
		// UNSPECIFIED or an unknown wire value — the client must name a real stage.
		return nil, status.Error(codes.InvalidArgument, "target_stage is invalid")
	}

	in := domain.PromoteVersionInput{
		VersionID:      req.GetVersionId(),
		TargetStage:    target,
		IdempotencyKey: req.GetIdempotencyKey(),
	}
	result, err := h.svc.PromoteVersion(ctx, actor, in)
	if err != nil {
		return nil, toStatus(ctx, "PromoteVersion", err)
	}

	resp := &registryv1.PromoteVersionResponse{
		Version: versionToProto(result.Promoted),
	}
	// The demoted version is only present when the single-production invariant swap
	// actually demoted a prior prod version (Demoted.ID == "" means none). We only
	// populate the wire field when there was a real demotion, so a client can rely
	// on its presence meaning "a swap happened".
	if result.Demoted.ID != "" {
		resp.DemotedVersion = versionToProto(result.Demoted)
	}
	return resp, nil
}

// DeleteModel soft-deletes (archives) a model and all its versions. It maps to the
// domain ArchiveModel. The response is empty (DeleteModelResponse has no fields) —
// the archive is a fire-and-confirm; the caller can GetModel to observe ArchivedAt.
func (h *RegistryHandler) DeleteModel(ctx context.Context, req *registryv1.DeleteModelRequest) (*registryv1.DeleteModelResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	if _, err := h.svc.ArchiveModel(ctx, actor, req.GetId(), req.GetIdempotencyKey()); err != nil {
		return nil, toStatus(ctx, "DeleteModel", err)
	}
	return &registryv1.DeleteModelResponse{}, nil
}

// ============================================================================
// QUERIES (read the eventually-consistent projection)
// ============================================================================

// GetModel returns a single model by id (preferred) or name, team-scoped to the
// actor. Exactly one of id/name must be provided; the boundary rejects "neither"
// with InvalidArgument (the domain treats id as taking precedence when both set).
func (h *RegistryHandler) GetModel(ctx context.Context, req *registryv1.GetModelRequest) (*registryv1.GetModelResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetId() == "" && req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "provide a model id or name")
	}

	model, err := h.svc.GetModel(ctx, actor, req.GetId(), req.GetName())
	if err != nil {
		return nil, toStatus(ctx, "GetModel", err)
	}
	return &registryv1.GetModelResponse{Model: modelToProto(model)}, nil
}

// ListModels returns a team-scoped, newest-first page from the projection. Team
// scoping is applied by the domain from the Actor — the request filter can only
// NARROW within the caller's team, never widen to another's.
func (h *RegistryHandler) ListModels(ctx context.Context, req *registryv1.ListModelsRequest) (*registryv1.ListModelsResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	size, err := pageSizeFromProto(req.GetPagination())
	if err != nil {
		return nil, err
	}

	in := domain.ListModelsInput{
		Filter: domain.ListModelsFilter{
			TaskType:        req.GetTaskTypeFilter(),
			Framework:       req.GetFrameworkFilter(),
			IncludeArchived: req.GetIncludeArchived(),
		},
		PageSize:  size,
		PageToken: req.GetPagination().GetPageToken(),
	}
	page, err := h.svc.ListModels(ctx, actor, in)
	if err != nil {
		return nil, toStatus(ctx, "ListModels", err)
	}

	models := make([]*registryv1.Model, 0, len(page.Items))
	for _, m := range page.Items {
		models = append(models, modelToProto(m))
	}
	return &registryv1.ListModelsResponse{
		Models: models,
		// Carry the read-model's accurate total so the dashboard tile ("N models")
		// matches the list. See paginationResponse.
		Pagination: paginationResponse(page.NextToken, page.Total),
	}, nil
}

// GetVersion returns a single version by id (preferred) or by (model_id, version)
// label. The boundary requires either an id OR a complete (model_id+version) pair.
func (h *RegistryHandler) GetVersion(ctx context.Context, req *registryv1.GetVersionRequest) (*registryv1.GetVersionResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// Accept id alone, or the (model_id, version) pair. Reject everything else so
	// the client gets a precise contract error rather than a confusing NotFound.
	if req.GetId() == "" && (req.GetModelId() == "" || req.GetVersion() == "") {
		return nil, status.Error(codes.InvalidArgument, "provide a version id or (model_id and version)")
	}

	version, err := h.svc.GetVersion(ctx, actor, req.GetId(), req.GetModelId(), req.GetVersion())
	if err != nil {
		return nil, toStatus(ctx, "GetVersion", err)
	}
	return &registryv1.GetVersionResponse{Version: versionToProto(version)}, nil
}

// ListVersions returns a model's versions newest-first, optionally filtered to one
// stage (UNSPECIFIED = any). model_id is required; page size is clamped.
func (h *RegistryHandler) ListVersions(ctx context.Context, req *registryv1.ListVersionsRequest) (*registryv1.ListVersionsResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetModelId() == "" {
		return nil, status.Error(codes.InvalidArgument, "model_id is required")
	}
	size, err := pageSizeFromProto(req.GetPagination())
	if err != nil {
		return nil, err
	}
	// StageUnspecified is a LEGAL filter value here ("any stage"), so we do NOT
	// reject it — unlike PromoteVersion where a stage must be a real target.
	stageFilter := domainStageFromProto(req.GetStageFilter())

	page, err := h.svc.ListVersions(ctx, actor, req.GetModelId(), stageFilter, size, req.GetPagination().GetPageToken())
	if err != nil {
		return nil, toStatus(ctx, "ListVersions", err)
	}

	versions := make([]*registryv1.ModelVersion, 0, len(page.Items))
	for _, v := range page.Items {
		versions = append(versions, versionToProto(v))
	}
	return &registryv1.ListVersionsResponse{
		Versions: versions,
		// total 0: no consumer needs a version total yet (the proto's "total unknown").
		Pagination: paginationResponse(page.NextToken, page.Total),
	}, nil
}
