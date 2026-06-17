// Package handler implements the gRPC server side of the Model Serving service.
// It is the outermost layer in Clean Architecture: it speaks proto (wire format)
// and delegates all business logic to the domain.ServingService interface.
//
// ============================================================================
// HANDLER LAYER RESPONSIBILITIES (the anti-corruption layer)
// ============================================================================
//
// The handler has exactly three jobs, and NO business logic:
//
//  1. PROTO → DOMAIN: extract + validate fields from the incoming proto Request
//     and build domain input types (e.g. *servingv1.PredictRequest →
//     domain.PredictInput, including the proto TensorData → domain.Tensor
//     conversion). The domain never sees a proto type.
//
//  2. CALL THE DOMAIN SERVICE: invoke the matching ServingService method. The
//     handler holds a domain.ServingService INTERFACE — it never knows whether
//     the implementation is the real registry (backed by the ONNX runtime + MinIO
//     fetcher) or an in-memory test stub.
//
//  3. DOMAIN → PROTO: map the domain result back to a proto Response, and map
//     domain SENTINEL ERRORS to gRPC status codes (ErrModelNotReady →
//     FailedPrecondition, ErrModelNotFound → NotFound, ErrArtifactURINotAllowed
//     → PermissionDenied, ErrValidation → InvalidArgument, etc.).
//
// ============================================================================
// AUTHORITY MODEL — WHY THIS HANDLER PULLS NO IDENTITY FROM CLAIMS (read this)
// ============================================================================
//
// Most handlers on this platform inject SERVER-AUTHORITATIVE fields (owner id,
// principal, team) from the auth interceptor's Claims (grpcutil.ClaimsFromContext)
// and NEVER trust a client-supplied owner/id — that is the anti-mass-assignment
// rule. The Model Serving service is the DELIBERATE exception, and the absence of
// claim-injection here is a designed property, not an omission:
//
//   - A serving pod is AUTH-AGNOSTIC by design (the Sidecar pattern). The
//     Inference Gateway owns identity, rate-limiting, billing, and traffic-split;
//     the pod "just does math." See serving.proto's Sidecar rationale.
//   - Consequently NONE of the proto requests carry an owner/principal/billing
//     field, and NONE of the domain input types (PredictInput, LoadModelInput,
//     ListOptions) have an identity field. There is therefore no
//     server-authoritative field to populate from Claims — there is nothing a
//     client could set to attribute, charge, or authorize a call.
//   - Server-authoritative DATA still exists, but it is derived by the DOMAIN
//     from the loaded artifact (state, digest, schema, timestamps,
//     model_version, latency, from_cache), never from the request body. The
//     handler simply never copies those out of the request.
//
// AUTHORIZATION (who may LoadModel vs Predict) is enforced by the gRPC
// interceptor chain (pkg/grpcutil) BEFORE the handler runs, by method name —
// the gateway's identity may Predict; only the operator's identity may
// LoadModel/UnloadModel. The handler does not re-check authority because the
// interceptor already did, and the pod grants no authority from any request
// field. If a future RPC ever needed per-call attribution, THIS is where it
// would read grpcutil.ClaimsFromContext(ctx) and pass the claim — not the
// request — into the domain input.
//
// ============================================================================
// ERROR MAPPING — DOMAIN SENTINELS → gRPC STATUS CODES (the translation table)
// ============================================================================
//
// The single mapStatusErr helper below is the ONE translation point. It uses
// errors.Is against the domain sentinels (errors.go) so it is robust to %w
// wrapping (the domain wraps sentinels with a human detail). The mapping:
//
//	ErrValidation            → InvalidArgument    (malformed request the domain rejected)
//	ErrModelNotFound         → NotFound           (no such model resident on this pod)
//	ErrModelAlreadyExists    → AlreadyExists      (different artifact under same ref)
//	ErrArtifactURINotAllowed → PermissionDenied   (SSRF allow-list reject)
//	ErrModelNotReady         → FailedPrecondition (right model, wrong STATE — gateway can reroute)
//	ErrModelRequestMismatch  → FailedPrecondition (routed to the wrong pod — gateway can reroute)
//	ErrDigestMismatch        → FailedPrecondition (integrity precondition failed)
//	ErrInferenceFailed(+bad input) → InvalidArgument  (client tensor the engine rejected)
//	ErrInferenceFailed(other)      → Internal          (server-side runtime fault)
//	anything else            → Internal (SANITIZED — the raw error is never leaked)
//
// SECURITY: for codes.Internal we return a FIXED, generic message
// ("internal serving error") and never the underlying error text. The raw error
// may contain a storage path, an ONNX op name, a stack-ish detail, or other
// internal state that must not reach an external caller. For the client-error
// codes the message is a clean, caller-actionable string we author here — never
// the domain's wrapped detail verbatim (which could also carry internals).
//
// INTERVIEW: "How do you stop internal errors leaking to clients?" One
// translation function, errors.Is on typed sentinels, a sanitized default of
// codes.Internal with a constant message, and author-controlled messages for the
// known client-error codes. The wrapped detail stays in the server logs.
// ============================================================================
//
// ============================================================================
// EMBEDDING UnimplementedModelServingServiceServer — WHY THIS IS CORRECT
// ============================================================================
//
// protoc-gen-go-grpc generates ModelServingServiceServer with a private method
// mustEmbedUnimplementedModelServingServiceServer(), forcing every
// implementation to embed UnimplementedModelServingServiceServer. That embedded
// base implements EVERY RPC to return codes.Unimplemented. Embedding it makes
// ServingHandler:
//
//  1. satisfy the ModelServingServiceServer interface AT COMPILE TIME — the
//     server can be registered and started RIGHT NOW, and
//  2. stay forward-compatible: a new RPC added to the proto is handled by the
//     embedded base (Unimplemented) until we implement it.
//
// The per-RPC methods below OVERRIDE the embedded base. They keep the nil-svc
// guard so a not-yet-fully-wired binary (main.go currently passes nil until the
// runtime/storage adapters land in the repo phase) is SAFE: it returns a real
// codes.Unimplemented instead of panicking on a nil dereference.
//
// ============================================================================
package handler

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	servingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/serving/v1"
	"github.com/abd-ulbasit/forgepoint/services/model-serving/internal/domain"
)

// ServingHandler is the gRPC server implementation for the Model Serving service.
//
// It embeds servingv1.UnimplementedModelServingServiceServer to satisfy the full
// servingv1.ModelServingServiceServer interface; the per-RPC methods below
// override it. svc is the domain.ServingService holding the model-runtime-registry
// business logic. svc may be nil before the runtime/storage adapters are wired
// (main.go passes nil in the scaffold/repo-pending phase) — every method guards
// that case and returns codes.Unimplemented rather than dereferencing nil.
type ServingHandler struct {
	servingv1.UnimplementedModelServingServiceServer // embedded by value (grpc-go requirement)
	svc                                              domain.ServingService
}

// NewServingHandler creates a ServingHandler with the given domain service.
//
// svc may be nil until the runtime (ONNX) and storage (MinIO fetcher) ADAPTERS
// exist to inject as the domain service's ports. Once those land, main.go
// constructs the real service and passes it here. Until then, the nil-svc guard
// in each method keeps the registered server safe.
func NewServingHandler(svc domain.ServingService) *ServingHandler {
	return &ServingHandler{svc: svc}
}

// errServiceNotWired is returned (as codes.Unimplemented) when an RPC is invoked
// on a handler whose domain service is not yet wired. WHY Unimplemented and not
// Internal: from the CLIENT's perspective the method genuinely is not yet
// available on this binary — Unimplemented is the honest, retry-elsewhere code,
// and it matches what the embedded base would return for a truly-missing RPC.
// It carries no internal detail.
const errServiceNotWired = "model serving service is not wired on this binary"

// sanitizedInternal is the FIXED message returned for every codes.Internal. The
// underlying error is intentionally discarded from the wire response (it is the
// caller's job to log it) so no internal path/op/state leaks to the client.
const sanitizedInternal = "internal serving error"

// ============================================================================
// DATA PLANE
// ============================================================================

// Predict runs one synchronous inference. The hot path.
//
// VALIDATION done HERE (proto-shape, before the domain): at least one input
// tensor present, and every input tensor is well-formed at the wire level (has a
// concrete, explicit dtype). The DEEP guards (per-pod max-tensor-count,
// max-bytes, overflow-safe layout check) live in the domain service so they are
// authoritative and unit-tested there; the handler does not duplicate them, it
// converts and lets the domain enforce them (mapped back via ErrValidation).
func (h *ServingHandler) Predict(ctx context.Context, req *servingv1.PredictRequest) (*servingv1.PredictResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	// A predict with no inputs is meaningless; reject early with a clean message.
	if len(req.GetInputs()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one input tensor is required")
	}

	// Convert the proto input tensors → domain tensors. A nil map entry or an
	// UNSPECIFIED dtype is a wire-level malformation we reject here (the domain's
	// layout check would also reject UNSPECIFIED, but failing at the boundary
	// gives the cleanest message and avoids building a half-formed domain input).
	inputs, err := protoTensorsToDomain(req.GetInputs())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	in := domain.PredictInput{
		// RequestedRef is the client's stated target. It grants NO authority — the
		// domain re-validates it against the resident model (defense in depth) and
		// returns ErrModelRequestMismatch on a mismatch. Empty = "this pod's model".
		RequestedRef:   domain.ModelRef{Name: req.GetModelName(), Version: req.GetVersion()},
		Inputs:         inputs,
		IdempotencyKey: req.GetIdempotencyKey(),
		CorrelationID:  req.GetCorrelationId(),
	}

	res, err := h.svc.Predict(ctx, in)
	if err != nil {
		return nil, mapStatusErr(err)
	}

	return &servingv1.PredictResponse{
		Outputs:          domainTensorsToProto(res.Outputs),
		ModelVersion:     res.ModelVersion,
		InferenceLatency: durationpb.New(res.InferenceLatency),
		CorrelationId:    res.CorrelationID,
		FromCache:        res.FromCache,
	}, nil
}

// StreamPredict is a bidirectional stream for batch/online scoring.
//
// STREAMING CONTRACT (the interview-critical mechanics):
//   - This is a Recv→process→Send LOOP. For each request message we run one
//     inference and send back one response paired by idempotency_key (responses
//     are NOT 1:1-ordered with requests in general; the key pairs them).
//   - ctx cancellation is honored at the TOP of every iteration AND propagates
//     into svc.Predict via stream.Context(). When the client cancels or the
//     deadline fires, Recv returns an error and we exit; we also explicitly check
//     ctx.Err() so a cancellation mid-loop returns the right status instead of
//     attempting another inference.
//   - A PER-MESSAGE validation/inference error is mapped to a status and
//     RETURNED, which terminates the stream. WHY terminate rather than send an
//     error-in-band: the proto has no per-message error field, so the gRPC stream
//     status is the only error channel. A malformed message is a client bug worth
//     surfacing loudly, not swallowing.
//   - The nil-svc guard returns ONLY the status error (no nil response) because a
//     streaming handler's return is a single error; responses flow via Send.
func (h *ServingHandler) StreamPredict(stream servingv1.ModelServingService_StreamPredictServer) error {
	if h.svc == nil {
		return status.Error(codes.Unimplemented, errServiceNotWired)
	}

	ctx := stream.Context()
	for {
		// Honor cancellation/deadline before blocking on the next Recv. WHY check
		// here too (Recv also observes ctx): it makes the cancellation status
		// deterministic — we return codes.Canceled/DeadlineExceeded rather than
		// whatever transport error Recv happens to surface on a torn-down stream.
		if err := ctx.Err(); err != nil {
			return ctxStatusErr(err)
		}

		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			// Clean client half-close: the client is done sending. Return nil to
			// close our side gracefully.
			return nil
		}
		if err != nil {
			// Transport/cancellation error on Recv. If it was a context error,
			// map it precisely; otherwise it is already a status (or close to it)
			// — return as-is so the client sees the real transport status.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxStatusErr(ctxErr)
			}
			return err
		}

		if len(req.GetInputs()) == 0 {
			return status.Error(codes.InvalidArgument, "at least one input tensor is required")
		}
		inputs, convErr := protoTensorsToDomain(req.GetInputs())
		if convErr != nil {
			return status.Error(codes.InvalidArgument, convErr.Error())
		}

		res, predErr := h.svc.Predict(ctx, domain.PredictInput{
			// Stream messages carry no model_name/version (the stream is bound to
			// the pod's model); empty RequestedRef means "this pod's model".
			Inputs:         inputs,
			IdempotencyKey: req.GetIdempotencyKey(),
			CorrelationID:  req.GetCorrelationId(),
		})
		if predErr != nil {
			return mapStatusErr(predErr)
		}

		if sendErr := stream.Send(&servingv1.StreamPredictResponse{
			Outputs:          domainTensorsToProto(res.Outputs),
			ModelVersion:     res.ModelVersion,
			InferenceLatency: durationpb.New(res.InferenceLatency),
			// Echo the request's key so the client pairs this response with it.
			IdempotencyKey: req.GetIdempotencyKey(),
			CorrelationId:  res.CorrelationID,
		}); sendErr != nil {
			// Send failed (client gone / stream broken). Surface it; do not retry.
			return sendErr
		}
	}
}

// ============================================================================
// INTROSPECTION
// ============================================================================

// GetModelInfo returns the served model's identity + I/O schema.
func (h *ServingHandler) GetModelInfo(ctx context.Context, req *servingv1.GetModelInfoRequest) (*servingv1.GetModelInfoResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}

	// Empty name/version is legal: it means "this pod's model". No validation
	// beyond that — the domain returns ErrModelNotFound if nothing is resident.
	ref := domain.ModelRef{Name: req.GetModelName(), Version: req.GetVersion()}

	info, err := h.svc.GetModelInfo(ctx, ref)
	if err != nil {
		return nil, mapStatusErr(err)
	}

	return &servingv1.GetModelInfoResponse{
		ModelInfo: loadedModelToProtoInfo(info),
	}, nil
}

// ListLoadedModels returns the resident models' live status, paginated.
//
// PAGE-SIZE CAP: the handler passes the requested page size through to the
// domain, which clamps it to [1, maxPageSize] (the contract cap). The handler
// also rejects an explicitly NEGATIVE page size at the wire boundary (a negative
// is never meaningful; 0 means "use default" and is left to the domain to clamp).
func (h *ServingHandler) ListLoadedModels(ctx context.Context, req *servingv1.ListLoadedModelsRequest) (*servingv1.ListLoadedModelsResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}

	pageSize := int(req.GetPagination().GetPageSize())
	if pageSize < 0 {
		return nil, status.Error(codes.InvalidArgument, "page_size must not be negative")
	}

	// state_filter must be a VALID enum value (or UNSPECIFIED = no filter). A
	// garbage enum int is rejected here rather than silently treated as "no
	// filter", which would mask a client bug.
	stateFilter, err := protoStateToDomain(req.GetStateFilter())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	models, nextToken, err := h.svc.ListLoadedModels(ctx, domain.ListOptions{
		PageSize:    pageSize,
		PageToken:   req.GetPagination().GetPageToken(),
		StateFilter: stateFilter,
	})
	if err != nil {
		return nil, mapStatusErr(err)
	}

	protoModels := make([]*servingv1.ModelStatus, 0, len(models))
	for _, m := range models {
		protoModels = append(protoModels, statusToProto(m))
	}

	return &servingv1.ListLoadedModelsResponse{
		Models: protoModels,
		Pagination: &commonv1.PaginationResponse{
			NextPageToken: nextToken,
			// total_count is the resident model count returned this page. The
			// domain does not return a separate grand total (a single-model pod has
			// 0..1), so we report the page length — accurate for this service.
			TotalCount: int32(len(models)),
		},
	}, nil
}

// ============================================================================
// LIFECYCLE
// ============================================================================

// LoadModel pulls an artifact from object storage and loads it into the engine.
//
// VALIDATION done HERE (proto-shape): model_name, version, and artifact_uri are
// required. The SSRF allow-list check is the DOMAIN's job (it owns the allow-list
// config and runs the check before any fetch); a rejected URI maps back to
// ErrArtifactURINotAllowed → PermissionDenied.
func (h *ServingHandler) LoadModel(ctx context.Context, req *servingv1.LoadModelRequest) (*servingv1.LoadModelResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	if req.GetModelName() == "" {
		return nil, status.Error(codes.InvalidArgument, "model_name is required")
	}
	if req.GetVersion() == "" {
		return nil, status.Error(codes.InvalidArgument, "version is required")
	}
	if req.GetArtifactUri() == "" {
		return nil, status.Error(codes.InvalidArgument, "artifact_uri is required")
	}

	st, err := h.svc.LoadModel(ctx, domain.LoadModelInput{
		Ref:            domain.ModelRef{Name: req.GetModelName(), Version: req.GetVersion()},
		ArtifactURI:    req.GetArtifactUri(),
		ExpectedDigest: req.GetExpectedDigest(),
		IdempotencyKey: req.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, mapStatusErr(err)
	}

	return &servingv1.LoadModelResponse{Status: statusToProto(st)}, nil
}

// UnloadModel frees a model from memory. Idempotent in the domain (unloading an
// already-unloaded/unknown model is a no-op success), so the handler needs no
// special-casing — it just converts and maps.
func (h *ServingHandler) UnloadModel(ctx context.Context, req *servingv1.UnloadModelRequest) (*servingv1.UnloadModelResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}

	// Empty name/version = the pod's single model. `reason` is free-form audit
	// text — no validation beyond passing it through.
	ref := domain.ModelRef{Name: req.GetModelName(), Version: req.GetVersion()}

	st, err := h.svc.UnloadModel(ctx, ref, req.GetReason())
	if err != nil {
		return nil, mapStatusErr(err)
	}

	return &servingv1.UnloadModelResponse{Status: statusToProto(st)}, nil
}

// GetModelStatus returns the live lifecycle status of a resident model.
func (h *ServingHandler) GetModelStatus(ctx context.Context, req *servingv1.GetModelStatusRequest) (*servingv1.GetModelStatusResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}

	ref := domain.ModelRef{Name: req.GetModelName(), Version: req.GetVersion()}

	st, err := h.svc.GetModelStatus(ctx, ref)
	if err != nil {
		return nil, mapStatusErr(err)
	}

	return &servingv1.GetModelStatusResponse{Status: statusToProto(st)}, nil
}

// ============================================================================
// AUTOSCALING + PROBES
// ============================================================================

// GetServingMetrics returns the point-in-time metrics snapshot (the HPA signal).
// It never errors at the domain level (the snapshot is always available), so the
// only failure mode is the nil-svc guard.
func (h *ServingHandler) GetServingMetrics(ctx context.Context, req *servingv1.GetServingMetricsRequest) (*servingv1.GetServingMetricsResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}

	m := h.svc.GetServingMetrics(ctx)

	return &servingv1.GetServingMetricsResponse{
		Metrics: &servingv1.ServingMetrics{
			InflightRequests: m.InflightRequests,
			TotalRequests:    m.TotalRequests,
			FailedRequests:   m.FailedRequests,
			P50Latency:       durationpb.New(m.P50Latency),
			P99Latency:       durationpb.New(m.P99Latency),
			ModelMemoryBytes: m.ModelMemoryBytes,
		},
	}, nil
}

// HealthCheck reports serving readiness + the latency signal behind liveness.
//
// WHY this RPC does not return an error for "not serving": a not-ready model is a
// NORMAL, expected verdict (HEALTH_STATUS_NOT_SERVING), not an RPC failure. The
// K8s readinessProbe keys on the verdict field, so the RPC succeeds and the
// verdict carries the meaning. The domain HealthCheck never errors; the only
// failure mode is the nil-svc guard.
func (h *ServingHandler) HealthCheck(ctx context.Context, req *servingv1.HealthCheckRequest) (*servingv1.HealthCheckResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, errServiceNotWired)
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}

	verdict, state, lastLatency := h.svc.HealthCheck(ctx, req.GetModelName())

	return &servingv1.HealthCheckResponse{
		Status:               verdictToProto(verdict),
		ModelState:           stateToProto(state),
		LastInferenceLatency: durationpb.New(lastLatency),
	}, nil
}

// ============================================================================
// ERROR MAPPING — the single translation point
// ============================================================================

// mapStatusErr converts a domain error into a gRPC status error with a precise
// code and a SANITIZED message. It is the only place the handler turns a domain
// error into a wire status, so the mapping (and the no-leak guarantee) lives in
// one auditable function. See the package-doc translation table.
//
// ORDER MATTERS for the ErrInferenceFailed branch: a failed inference caused by
// MALFORMED CLIENT INPUT (ErrEngineBadInput, wrapped inside ErrInferenceFailed by
// the domain) is a CLIENT error → InvalidArgument; any other inference failure is
// a SERVER fault → Internal. We check the more-specific ErrEngineBadInput first.
func mapStatusErr(err error) error {
	switch {
	case err == nil:
		return nil

	case errors.Is(err, domain.ErrValidation):
		// The domain wraps ErrValidation with a human detail that it authored for
		// THIS purpose (no PII/secrets — it is a shape message like "too many input
		// tensors"). Safe to surface as the InvalidArgument message.
		return status.Error(codes.InvalidArgument, err.Error())

	case errors.Is(err, domain.ErrModelNotFound):
		return status.Error(codes.NotFound, "model not found on this pod")

	case errors.Is(err, domain.ErrModelAlreadyExists):
		return status.Error(codes.AlreadyExists, "model version already loaded with a different artifact")

	case errors.Is(err, domain.ErrArtifactURINotAllowed):
		// PermissionDenied (not InvalidArgument): the URI is well-formed but the
		// caller is not permitted to load from it (SSRF allow-list). We do NOT echo
		// the URI back — that would confirm allow-list contents to a prober.
		return status.Error(codes.PermissionDenied, "artifact URI is not permitted")

	case errors.Is(err, domain.ErrModelNotReady):
		// FailedPrecondition: the right model on the right pod, just not in a
		// serveable STATE. Lets the gateway reroute instead of treating it as fatal.
		return status.Error(codes.FailedPrecondition, "model is not ready to serve predictions")

	case errors.Is(err, domain.ErrModelRequestMismatch):
		// FailedPrecondition: the pod hosts a model, the request asked for a
		// different one (a routing bug). Reroutable, like ErrModelNotReady.
		return status.Error(codes.FailedPrecondition, "request targets a model this pod does not serve")

	case errors.Is(err, domain.ErrDigestMismatch):
		return status.Error(codes.FailedPrecondition, "artifact digest does not match expected digest")

	case errors.Is(err, domain.ErrInferenceFailed):
		// Distinguish a client-input fault from a server fault. ErrEngineBadInput is
		// wrapped inside ErrInferenceFailed when the engine rejected the tensors as
		// malformed for this model → that is the CALLER's error → InvalidArgument
		// (with a clean, fixed message — never the raw engine detail).
		if errors.Is(err, domain.ErrEngineBadInput) {
			return status.Error(codes.InvalidArgument, "inference inputs were rejected by the model")
		}
		// Any other inference failure is a server-side runtime fault — sanitize.
		return status.Error(codes.Internal, sanitizedInternal)

	default:
		// UNKNOWN error: never leak. Fixed generic message; the raw error is the
		// server's to log, not the client's to read.
		return status.Error(codes.Internal, sanitizedInternal)
	}
}

// ctxStatusErr maps a context error (cancellation/deadline) to the canonical gRPC
// status. WHY map explicitly instead of returning ctx.Err(): grpc-go would turn a
// bare context.Canceled into codes.Canceled anyway, but doing it here makes the
// streaming control flow's intent explicit and the status deterministic.
func ctxStatusErr(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled by client")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
	default:
		return status.Error(codes.Internal, sanitizedInternal)
	}
}
