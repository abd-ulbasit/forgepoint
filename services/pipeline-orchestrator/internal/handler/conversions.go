// conversions.go — the proto ⇄ domain mapping + error translation that keeps the
// handler methods thin.
//
// ============================================================================
// WHY THESE LIVE IN THE HANDLER LAYER (and nowhere else)
// ============================================================================
//
// The dependency rule (Clean Architecture) says the domain must not import the
// generated proto types. So SOMEONE has to translate between
// pipelinev1.* (wire) and domain.* (business). That someone is the handler — the
// outermost adapter. These functions are that boundary, factored out of the RPC
// methods so each method reads as "validate → convert → call → convert" without
// the mechanical field-copying inline.
//
// Two directions:
//   - *ToDomain   : inbound. Validates enum ranges + shape, returns
//     codes.InvalidArgument for garbage. NEVER reads identity
//     fields (id/created_by/team) from the request — those are
//     server-authoritative.
//   - *ToProto    : outbound. Pure field copying of a trusted domain value.
//
// The enum mappers use an EXPLICIT switch rather than a raw int cast. The proto
// and domain enum integer values are aligned on purpose (see models.go), so a
// cast WOULD work today — but an explicit switch (a) rejects out-of-range client
// ints precisely (anti-garbage), (b) is auditable line-by-line,
// and (c) does not silently break if the two enums ever diverge. The price is a
// few lines of boilerplate; the safety is worth it at a trust boundary.
package handler

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	pipelinev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/pipeline/v1"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/domain"
)

// errServiceNotWired is the canonical nil-svc guard error shared by every unary
// RPC. codes.Unimplemented (not Internal) is the honest answer: the binary is
// running but this capability was not provided to the handler (the scaffold/early
// boot window before the repo phase wires the engine). The message is sanitized —
// it reveals nothing about internals.
var errServiceNotWired = status.Error(codes.Unimplemented, "pipeline service is not wired")

// ============================================================================
// ENUM MAPPERS (inbound, validated)
// ============================================================================

// pipelineTypeToDomain maps a REQUIRED PipelineType (Create). UNSPECIFIED is
// rejected — a pipeline must declare its execution strategy.
func pipelineTypeToDomain(t pipelinev1.PipelineType) (domain.PipelineType, error) {
	switch t {
	case pipelinev1.PipelineType_PIPELINE_TYPE_DEPLOYMENT_SAGA:
		return domain.PipelineTypeDeploymentSaga, nil
	case pipelinev1.PipelineType_PIPELINE_TYPE_TRAINING_DAG:
		return domain.PipelineTypeTrainingDAG, nil
	case pipelinev1.PipelineType_PIPELINE_TYPE_BATCH_INFERENCE:
		return domain.PipelineTypeBatchInference, nil
	default:
		// Covers UNSPECIFIED (0) and any out-of-range int a client invented.
		return domain.PipelineTypeUnspecified, status.Error(codes.InvalidArgument, "type must be a valid pipeline type")
	}
}

// pipelineTypeFilterToDomain maps an OPTIONAL PipelineType filter (List).
// UNSPECIFIED is allowed (means "any type"); only an out-of-range int is rejected.
func pipelineTypeFilterToDomain(t pipelinev1.PipelineType) (domain.PipelineType, error) {
	switch t {
	case pipelinev1.PipelineType_PIPELINE_TYPE_UNSPECIFIED:
		return domain.PipelineTypeUnspecified, nil
	case pipelinev1.PipelineType_PIPELINE_TYPE_DEPLOYMENT_SAGA:
		return domain.PipelineTypeDeploymentSaga, nil
	case pipelinev1.PipelineType_PIPELINE_TYPE_TRAINING_DAG:
		return domain.PipelineTypeTrainingDAG, nil
	case pipelinev1.PipelineType_PIPELINE_TYPE_BATCH_INFERENCE:
		return domain.PipelineTypeBatchInference, nil
	default:
		return domain.PipelineTypeUnspecified, status.Error(codes.InvalidArgument, "type_filter must be a valid pipeline type")
	}
}

// stepTypeToDomain maps a REQUIRED StepType. UNSPECIFIED / out-of-range is
// rejected at the edge. The domain ALSO rejects unknown types (ErrUnknownStepType)
// — this edge check gives a clean per-step InvalidArgument before the domain runs.
func stepTypeToDomain(t pipelinev1.StepType) (domain.StepType, error) {
	if t < pipelinev1.StepType_STEP_TYPE_VALIDATE || t > pipelinev1.StepType_STEP_TYPE_CUSTOM {
		return domain.StepTypeUnspecified, status.Error(codes.InvalidArgument, "step type must be a known step type")
	}
	// Values are aligned 1:1 with the domain enum (validated above to be in range).
	return domain.StepType(t), nil
}

// executionStatusFilterToDomain maps an OPTIONAL ExecutionStatus filter (List).
// UNSPECIFIED means "any status"; only an out-of-range int is rejected.
func executionStatusFilterToDomain(s pipelinev1.ExecutionStatus) (domain.ExecutionStatus, error) {
	if s < pipelinev1.ExecutionStatus_EXECUTION_STATUS_UNSPECIFIED || s > pipelinev1.ExecutionStatus_EXECUTION_STATUS_CANCELLED {
		return domain.ExecutionStatusUnspecified, status.Error(codes.InvalidArgument, "status_filter must be a valid execution status")
	}
	return domain.ExecutionStatus(s), nil
}

// ============================================================================
// ENUM MAPPERS (outbound, total) — domain → proto
// ============================================================================
//
// These are TOTAL functions over the domain enum: a domain value the handler
// doesn't recognize maps to UNSPECIFIED rather than panicking. The domain values
// are trusted (the service produced them), so we don't return errors here.

func pipelineTypeToProto(t domain.PipelineType) pipelinev1.PipelineType {
	switch t {
	case domain.PipelineTypeDeploymentSaga:
		return pipelinev1.PipelineType_PIPELINE_TYPE_DEPLOYMENT_SAGA
	case domain.PipelineTypeTrainingDAG:
		return pipelinev1.PipelineType_PIPELINE_TYPE_TRAINING_DAG
	case domain.PipelineTypeBatchInference:
		return pipelinev1.PipelineType_PIPELINE_TYPE_BATCH_INFERENCE
	default:
		return pipelinev1.PipelineType_PIPELINE_TYPE_UNSPECIFIED
	}
}

func stepTypeToProto(t domain.StepType) pipelinev1.StepType {
	if !t.IsKnown() {
		return pipelinev1.StepType_STEP_TYPE_UNSPECIFIED
	}
	return pipelinev1.StepType(t)
}

func executionStatusToProto(s domain.ExecutionStatus) pipelinev1.ExecutionStatus {
	switch s {
	case domain.ExecutionStatusPending:
		return pipelinev1.ExecutionStatus_EXECUTION_STATUS_PENDING
	case domain.ExecutionStatusRunning:
		return pipelinev1.ExecutionStatus_EXECUTION_STATUS_RUNNING
	case domain.ExecutionStatusCompensating:
		return pipelinev1.ExecutionStatus_EXECUTION_STATUS_COMPENSATING
	case domain.ExecutionStatusCompleted:
		return pipelinev1.ExecutionStatus_EXECUTION_STATUS_COMPLETED
	case domain.ExecutionStatusFailed:
		return pipelinev1.ExecutionStatus_EXECUTION_STATUS_FAILED
	case domain.ExecutionStatusCancelled:
		return pipelinev1.ExecutionStatus_EXECUTION_STATUS_CANCELLED
	default:
		return pipelinev1.ExecutionStatus_EXECUTION_STATUS_UNSPECIFIED
	}
}

func stepStatusToProto(s domain.StepStatus) pipelinev1.StepStatus {
	switch s {
	case domain.StepStatusPending:
		return pipelinev1.StepStatus_STEP_STATUS_PENDING
	case domain.StepStatusRunning:
		return pipelinev1.StepStatus_STEP_STATUS_RUNNING
	case domain.StepStatusCompleted:
		return pipelinev1.StepStatus_STEP_STATUS_COMPLETED
	case domain.StepStatusFailed:
		return pipelinev1.StepStatus_STEP_STATUS_FAILED
	case domain.StepStatusSkipped:
		return pipelinev1.StepStatus_STEP_STATUS_SKIPPED
	case domain.StepStatusCompensating:
		return pipelinev1.StepStatus_STEP_STATUS_COMPENSATING
	case domain.StepStatusCompensated:
		return pipelinev1.StepStatus_STEP_STATUS_COMPENSATED
	case domain.StepStatusCompensationFailed:
		return pipelinev1.StepStatus_STEP_STATUS_COMPENSATION_FAILED
	default:
		return pipelinev1.StepStatus_STEP_STATUS_UNSPECIFIED
	}
}

// ============================================================================
// STEP / PIPELINE / EXECUTION MAPPERS
// ============================================================================

// stepsToDomain converts the inbound step graph. It validates per-step shape
// (non-empty id, known type) at the edge; the deep graph validation (cycles,
// dangling edges, duplicate ids, caps) is the domain's job. We do NOT enforce the
// 256-step cap here — the domain owns MaxStepsPerPipeline so the rule holds for
// every adapter (a future CLI / event consumer), not just this handler.
func stepsToDomain(in []*pipelinev1.StepDefinition) ([]domain.StepDefinition, error) {
	if len(in) == 0 {
		// An empty graph is rejected by the domain (ErrEmptyPipeline), but catching
		// it here gives a clean InvalidArgument without entering the service.
		return nil, status.Error(codes.InvalidArgument, "at least one step is required")
	}
	out := make([]domain.StepDefinition, 0, len(in))
	for _, s := range in {
		if s == nil {
			return nil, status.Error(codes.InvalidArgument, "step must not be nil")
		}
		if s.GetId() == "" {
			return nil, status.Error(codes.InvalidArgument, "every step must have a non-empty id")
		}
		st, err := stepTypeToDomain(s.GetType())
		if err != nil {
			return nil, err
		}
		out = append(out, domain.StepDefinition{
			ID:                 s.GetId(),
			Name:               s.GetName(),
			Type:               st,
			DependsOn:          append([]string(nil), s.GetDependsOn()...),
			CompensationStepID: s.GetCompensationStepId(),
			Config:             structToMap(s.GetConfig()),
			Timeout:            s.GetTimeout().AsDuration(), // nil → 0 (engine default)
			MaxRetries:         int(s.GetMaxRetries()),
		})
	}
	return out, nil
}

func stepDefToProto(s domain.StepDefinition) *pipelinev1.StepDefinition {
	out := &pipelinev1.StepDefinition{
		Id:                 s.ID,
		Name:               s.Name,
		Type:               stepTypeToProto(s.Type),
		DependsOn:          append([]string(nil), s.DependsOn...),
		CompensationStepId: s.CompensationStepID,
		Config:             mapToStruct(s.Config),
		MaxRetries:         int32(s.MaxRetries),
	}
	// Only emit a duration when one is set — a zero Timeout means "engine default",
	// which is best represented as an absent field (nil) rather than 0s.
	if s.Timeout > 0 {
		out.Timeout = durationpb.New(s.Timeout)
	}
	return out
}

func pipelineToProto(p domain.PipelineDefinition) *pipelinev1.PipelineDefinition {
	steps := make([]*pipelinev1.StepDefinition, 0, len(p.Steps))
	for i := range p.Steps {
		steps = append(steps, stepDefToProto(p.Steps[i]))
	}
	out := &pipelinev1.PipelineDefinition{
		Id:        p.ID,
		Name:      p.Name,
		Type:      pipelineTypeToProto(p.Type),
		Steps:     steps,
		CreatedBy: p.CreatedBy,
		Team:      p.Team,
	}
	if !p.CreatedAt.IsZero() {
		out.CreatedAt = timestamppb.New(p.CreatedAt)
	}
	return out
}

func stepExecutionToProto(s domain.StepExecution) *pipelinev1.StepExecution {
	out := &pipelinev1.StepExecution{
		Id:          s.ID,
		ExecutionId: s.ExecutionID,
		StepId:      s.StepID,
		Status:      stepStatusToProto(s.Status),
		Output:      mapToStruct(s.Output),
		Error:       s.Error,
		Attempt:     int32(s.Attempt),
	}
	if s.StartedAt != nil {
		out.StartedAt = timestamppb.New(*s.StartedAt)
	}
	if s.CompletedAt != nil {
		out.CompletedAt = timestamppb.New(*s.CompletedAt)
	}
	return out
}

func executionToProto(e domain.Execution) *pipelinev1.Execution {
	steps := make([]*pipelinev1.StepExecution, 0, len(e.Steps))
	for i := range e.Steps {
		steps = append(steps, stepExecutionToProto(e.Steps[i]))
	}
	out := &pipelinev1.Execution{
		Id:             e.ID,
		PipelineId:     e.PipelineID,
		Status:         executionStatusToProto(e.Status),
		CurrentStep:    e.CurrentStep,
		StepExecutions: steps,
		TriggeredBy:    e.TriggeredBy,
		Input:          mapToStruct(e.Input),
		Error:          e.Error,
	}
	if !e.StartedAt.IsZero() {
		out.StartedAt = timestamppb.New(e.StartedAt)
	}
	if e.CompletedAt != nil {
		out.CompletedAt = timestamppb.New(*e.CompletedAt)
	}
	return out
}

// ============================================================================
// STRUCT (config / input / output) MAPPERS
// ============================================================================
//
// google.protobuf.Struct ⇄ map[string]any. We convert via structpb's own
// AsMap/NewStruct so JSON-ish shapes (nested maps, lists, numbers) round-trip
// correctly. A malformed struct that NewStruct can't represent (e.g. a NaN) is
// dropped to nil rather than erroring the response — the domain already treated
// the value as opaque untrusted config, and a non-representable echo isn't worth
// failing a read over.

func structToMap(s *structpb.Struct) map[string]any {
	if s == nil {
		return nil
	}
	return s.AsMap()
}

func mapToStruct(m map[string]any) *structpb.Struct {
	if len(m) == 0 {
		return nil
	}
	s, err := structpb.NewStruct(m)
	if err != nil {
		// Non-representable value → omit rather than fail the whole response.
		return nil
	}
	return s
}

// ============================================================================
// PAGINATION MAPPERS
// ============================================================================

// pageSizeToDomain validates the wire page_size and returns the value to hand
// the domain (which re-clamps to default 20 / max 100). We reject only a NEGATIVE
// page_size as malformed; 0 means "use default" and a too-large value is clamped
// by the domain (not an error). The domain owns the cap so the rule holds for
// every adapter, not just this handler.
func pageSizeToDomain(p *commonv1.PaginationRequest) (int, error) {
	if p == nil {
		return 0, nil // → domain default
	}
	ps := p.GetPageSize()
	if ps < 0 {
		return 0, status.Error(codes.InvalidArgument, "page_size must not be negative")
	}
	return int(ps), nil
}

func pageTokenFromProto(p *commonv1.PaginationRequest) string {
	if p == nil {
		return ""
	}
	return p.GetPageToken()
}

// paginationResponse builds the outbound pagination metadata. total_count is left
// at its zero value: the domain List ports return only the next cursor (a count
// would require a second COUNT query the cursor model deliberately avoids), so we
// do not fabricate one.
func paginationResponse(nextToken string) *commonv1.PaginationResponse {
	return &commonv1.PaginationResponse{
		NextPageToken: nextToken,
	}
}

// ============================================================================
// ERROR MAPPING — domain sentinel → gRPC status code (SANITIZED)
// ============================================================================
//
// This is THE security-critical function of the handler: it decides what the
// client learns about a failure. The rules:
//
//   - VALIDATION (ErrValidation + its specific wrappers: cycle, dangling edge,
//     unknown step type, duplicate id, too many steps, empty) → InvalidArgument.
//     The domain's validation messages are SAFE to surface — they describe the
//     client's own malformed input ("step 'deploy' depends on unknown step
//     'buidl'"), contain no internals/PII/secrets, and are exactly what helps the
//     caller fix the request. We pass them through.
//
//   - NOT FOUND (ErrPipelineNotFound / ErrExecutionNotFound) → NotFound. These
//     are tenancy-scoped in the domain: an out-of-scope id reads as not-found
//     (anti-IDOR — we never confirm/deny existence of another team's resource).
//
//   - PRECONDITION (ErrPipelineArchived / ErrExecutionNotCancellable) →
//     FailedPrecondition. The resource exists but is in the wrong state for the
//     operation (archived template can't be triggered; terminal run can't be
//     cancelled). The sentinel's own message is safe and actionable.
//
//   - EVERYTHING ELSE (engine internals: ErrCompensationFailed, ErrNoExecutor,
//     ErrTerminalPersist, ErrStepPersist, a wrapped DB error, an unexpected
//     panic-recovery error, ...) → Internal with a FLAT, SANITIZED message. We
//     deliberately do NOT echo err.Error(): an internal error can carry a
//     connection string, a SQL fragment, a hostname, a stack detail — none of
//     which a client may see. The real error is logged server-side by the logging
//     interceptor; the client gets "internal error".
//
// WHY errors.Is (not ==): the domain wraps validation causes with %w
// (ErrCycleDetected wraps ErrValidation), and the service may wrap a sentinel
// with extra context. errors.Is walks the chain, so a wrapped ErrPipelineNotFound
// still maps to NotFound. We check the MOST SPECIFIC sentinels first, then the
// umbrella ErrValidation, then fall through to Internal.
func mapDomainError(err error) error {
	if err == nil {
		return nil
	}

	// If a layer already produced a gRPC status (e.g. the handler's own
	// actorFromContext, or a future status-aware adapter), preserve it verbatim.
	if _, ok := status.FromError(err); ok && status.Code(err) != codes.Unknown {
		return err
	}

	switch {
	// --- NotFound ---------------------------------------------------------
	case errors.Is(err, domain.ErrPipelineNotFound),
		errors.Is(err, domain.ErrExecutionNotFound):
		return status.Error(codes.NotFound, err.Error())

	// --- FailedPrecondition (exists, wrong state) -------------------------
	case errors.Is(err, domain.ErrPipelineArchived),
		errors.Is(err, domain.ErrExecutionNotCancellable):
		return status.Error(codes.FailedPrecondition, err.Error())

	// --- InvalidArgument (validation; messages are client-safe) -----------
	// ErrValidation is the umbrella every specific validation sentinel wraps, so
	// one case catches cycle / dangling / unknown-type / duplicate / too-many /
	// empty as well as a bare ErrValidation.
	case errors.Is(err, domain.ErrValidation):
		return status.Error(codes.InvalidArgument, err.Error())

	// --- Internal (engine internals — SANITIZED, never echo err) ----------
	// ErrCompensationFailed / ErrNoExecutor / ErrTerminalPersist / ErrStepPersist
	// and any unrecognized error fall here. The client gets a flat message; the
	// detail is logged server-side.
	default:
		return status.Error(codes.Internal, "internal error")
	}
}
