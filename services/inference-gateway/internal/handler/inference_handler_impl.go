// inference_handler_impl.go — the per-RPC method bodies for InferenceHandler.
//
// ============================================================================
// WHAT THIS FILE IS (and why it's split from inference_handler.go)
// ============================================================================
//
// inference_handler.go holds the TYPE + constructor + the long teaching block on
// why we embed Unimplemented. THIS file holds the actual RPC implementations:
// the proto↔domain conversion, the validation, the call into the domain service,
// and the sentinel→status mapping. Splitting keeps the "what is this handler"
// doc separate from the "how each RPC works" mechanics — both compile into the
// same *InferenceHandler.
//
// Implementing a method here OVERRIDES the embedded
// UnimplementedInferenceGatewayServiceServer's version of it (Go method
// promotion: the outer type's method wins). Any RPC NOT implemented here still
// falls through to the embedded Unimplemented base — forward-compatible by
// construction.
//
// ============================================================================
// THE FOUR JOBS, IN ORDER, FOR EVERY RPC
// ============================================================================
//
//  1. NIL-SVC GUARD. main.go wires nil during the scaffold phase. A unary RPC
//     returns codes.Unimplemented; a SERVER-STREAMING RPC returns the SAME
//     status as an error (never a nil response — there is no response value, the
//     results flow via stream.Send). We use Unimplemented (not Internal) so a
//     not-yet-wired binary advertises "this RPC isn't available here" exactly
//     like the embedded base would — honest and probe-friendly.
//
//  2. IDENTITY + AUTHZ FROM CLAIMS (the trust boundary). The billed/tenant
//     principal is taken from the VERIFIED claims the auth interceptor put in the
//     context (grpcutil.ClaimsFromContext), NEVER from a request field. No
//     *Request here even CARRIES an api_key/team/owner — a client-supplied
//     principal would be account-takeover-for-billing (the inference event meters
//     against api_key_id). version_override and the whole control plane require
//     an ELEVATED scope; an ordinary caller setting version_override is rejected
//     (PermissionDenied), not silently honored.
//
//  3. VALIDATE + CONVERT proto → domain. Cheap edge checks (required fields,
//     dtype enum validity, batch-size caps, page-size caps) → InvalidArgument
//     with a CLEAN message (no internal detail). Deep domain invariants (tensor
//     byte-length vs shape, weight sums) are validated by the domain and surface
//     as sentinels we map below — we do not duplicate that math here.
//
//  4. CALL the domain service, then map the result/sentinel back. The
//     sentinel→gRPC-status mapping (statusFromDomainErr) is the service's failure
//     CONTRACT and the single translation point; everything unrecognized becomes
//     a SANITIZED codes.Internal (we never echo raw error text — it can carry
//     endpoints, query fragments, or PII).
//
// INTERVIEW: the streaming auth/cancellation mechanics to study are (a) a
// server-streaming RPC's nil-guard returns the status as the error, and (b) the
// stream loop checks stream.Context().Err() each iteration so a client
// disconnect / deadline stops work promptly instead of scoring a dead batch.
// ============================================================================
package handler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	inferencev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/inference/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/domain"
)

// ============================================================================
// LIMITS + SCOPES (the edge contract constants)
// ============================================================================

const (
	// maxBatchItems is the HARD cap on BatchPredict.items — part of the API
	// contract (proto BatchPredictRequest doc), not a tunable. A unary batch
	// buffers the whole request and the whole response in memory, so an unbounded
	// list is a memory-DoS; 256 bounds one call. Bigger jobs use StreamPredict.
	maxBatchItems = 256

	// maxStreamItems is the cap on StreamPredict.items. Responses stream (bounded
	// response memory), but the REQUEST list is still received and held in full,
	// so it is bounded too — 10x the unary cap. Above it → InvalidArgument.
	maxStreamItems = 10000

	// maxPageSize caps page_size for the paginated list RPCs (mirrors common.proto
	// "default 20, max 100"). We cap rather than reject so a client asking for
	// 1000 simply gets 100 — friendlier than an error and still bounds the row
	// scan / response size.
	defaultPageSize = 20
	maxPageSize     = 100
)

const (
	// scopeInferencePredict is the data-plane scope. Every Predict/Batch/Stream/
	// GetModelInfo caller must hold it. (The auth interceptor authenticates; the
	// per-RPC scope check authorizes — authn vs authz are distinct.)
	scopeInferencePredict = "inference:predict"

	// scopeInferenceVersionOverride is the ELEVATED scope that lets a caller pin
	// version_override (bypassing canary splitting). Held by the canary executor
	// and debuggers; ordinary traffic does not have it and must leave the field
	// empty. WHY a dedicated scope and not "admin": overriding the split is a
	// narrow, auditable capability separate from reshaping routes.
	scopeInferenceVersionOverride = "inference:override"

	// scopeInferenceAdmin gates the entire ROUTING CONTROL PLANE (Upsert /
	// SetTrafficSplit / DeleteRoute) and the operator reads (GetRoute /
	// ListRoutes / circuit observability) that expose backend endpoints &
	// breaker internals. Only operators and the deploy/canary executor hold it.
	scopeInferenceAdmin = "inference:admin"
)

// ============================================================================
// IDENTITY + AUTHZ HELPERS (the trust boundary, in one place)
// ============================================================================

// principalFromContext extracts the SERVER-AUTHORITATIVE caller identity from the
// verified claims the auth interceptor injected. This is the security linchpin of
// the whole service: the Principal (api_key_id billed, team for quota) comes ONLY
// from here, never from a request field.
//
// Missing claims → Unauthenticated. In production the auth interceptor rejects
// unauthenticated RPCs before the handler runs, so this is a defense-in-depth
// backstop (and the honest answer when a test/dev path forgot to authenticate):
// we refuse to act on an anonymous request rather than bill a zero-value
// principal. We map claims.UserID → APIKeyID: for this gateway the authenticated
// principal IS the calling API key's id (the meter handle); the field is named
// UserID generically in the shared Claims type.
func principalFromContext(ctx context.Context) (domain.Principal, *grpcutil.Claims, error) {
	claims, ok := grpcutil.ClaimsFromContext(ctx)
	if !ok || claims == nil || claims.UserID == "" {
		return domain.Principal{}, nil, status.Error(codes.Unauthenticated, "missing or invalid authentication")
	}
	return domain.Principal{APIKeyID: claims.UserID, Team: claims.Team}, claims, nil
}

// hasScope reports whether the claims carry the named scope. A nil claims set has
// no scopes (caller is treated as unprivileged) — fail-closed for authorization,
// the opposite of the rate limiter's fail-open (there the risk is lost traffic;
// here the risk is privilege escalation, so we deny by default).
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

// requireScope returns a PermissionDenied status if the caller lacks the scope.
// The message names the missing capability (not the caller's actual scopes — we
// don't echo the token's contents back) so a client can self-diagnose without
// leaking what else the principal can or cannot do.
func requireScope(claims *grpcutil.Claims, scope string) error {
	if !hasScope(claims, scope) {
		return status.Errorf(codes.PermissionDenied, "caller lacks required scope %q", scope)
	}
	return nil
}

// ============================================================================
// SENTINEL → gRPC STATUS MAPPING (the service's failure contract, one place)
// ============================================================================

// statusFromDomainErr maps a domain sentinel error to the precise gRPC status the
// API contract promises. This is THE single translation point (interview-critical
// — be ready to recite it):
//
//	ErrNoRoute        → NotFound            (model not routable)
//	ErrInvalidInput   → InvalidArgument     (bad tensors/shape — data plane)
//	ErrRouteValidation→ InvalidArgument     (bad route write — control plane)
//	ErrRateLimited    → ResourceExhausted   (token bucket empty → HTTP 429)
//	ErrQuotaExceeded  → ResourceExhausted   (team over billing quota)
//	ErrCircuitOpen    → Unavailable         (breaker tripped — fail fast)
//	ErrUpstream       → Unavailable         (backend ran and failed)
//	ErrTimeout        → DeadlineExceeded    (backend too slow)
//	(anything else)   → Internal, SANITIZED (never leak raw error text/PII)
//
// WHY errors.Is (not == ): the domain wraps sentinels with %w plus a specific
// message (e.g. "inference: invalid input: tensor \"x\" is 8 bytes, want 16").
// errors.Is unwraps to match the CLASS; the wrapped message is a clean,
// PII-free, human-readable detail authored by the domain, so it is safe to
// surface for the client-fault classes (NotFound / InvalidArgument). For the
// resilience-reject classes we use a FIXED message — a 429/503 client only needs
// the class, and a fixed string can't accidentally leak a backend endpoint.
//
// WHY map Internal to a constant string and DISCARD err.Error(): an unrecognized
// error is, by definition, one we didn't vet for safety — it could embed a Redis
// DSN, a serving endpoint, or a stack-ish detail. Returning a generic
// "internal error" is the secure default; the real error is logged server-side
// (by the logging interceptor), where operators can see it and clients cannot.
func statusFromDomainErr(err error) error {
	switch {
	case err == nil:
		return nil

	// ---- client-fault classes: safe to surface the domain's clean message ----
	case errors.Is(err, domain.ErrNoRoute):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrInvalidInput):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrRouteValidation):
		return status.Error(codes.InvalidArgument, err.Error())

	// ---- resilience rejects: FIXED message, the class is what matters ----
	case errors.Is(err, domain.ErrRateLimited):
		return status.Error(codes.ResourceExhausted, "rate limited")
	case errors.Is(err, domain.ErrQuotaExceeded):
		return status.Error(codes.ResourceExhausted, "team quota exceeded")
	case errors.Is(err, domain.ErrCircuitOpen):
		return status.Error(codes.Unavailable, "backend temporarily unavailable")
	case errors.Is(err, domain.ErrUpstream):
		return status.Error(codes.Unavailable, "upstream backend error")
	case errors.Is(err, domain.ErrTimeout):
		return status.Error(codes.DeadlineExceeded, "upstream timeout")

	// ---- anything unrecognized: SANITIZED Internal (never echo err.Error()) ----
	default:
		return status.Error(codes.Internal, "internal error")
	}
}

// failureReasonToProto maps the domain's resilience taxonomy to the canonical
// events.v1 enum used on the sync surface (BatchPredictResult.failure_reason).
// The two are deliberately 1:1 (the domain mirrors the event enum to stay free of
// gen/go imports); this is the trivial, TOTAL mapping at the boundary.
func failureReasonToProto(r domain.FailureReason) eventsv1.InferenceFailureReason {
	switch r {
	case domain.FailureReasonNoRoute:
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_NO_ROUTE
	case domain.FailureReasonRateLimited:
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_RATE_LIMITED
	case domain.FailureReasonBulkheadFull:
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_BULKHEAD_FULL
	case domain.FailureReasonCircuitOpen:
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_CIRCUIT_OPEN
	case domain.FailureReasonUpstreamError:
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_UPSTREAM_ERROR
	case domain.FailureReasonTimeout:
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_TIMEOUT
	case domain.FailureReasonInvalidInput:
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_INVALID_INPUT
	case domain.FailureReasonQuotaExceeded:
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_QUOTA_EXCEEDED
	default:
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_UNSPECIFIED
	}
}

// failureReasonFromDomainErr classifies a Predict error into the resilience
// taxonomy for a per-item batch result. It mirrors the domain's own
// classification (impl.fail) so a streamed/batched item carries the SAME reason
// the async event would. Unrecognized → UPSTREAM_ERROR (a server-side fault the
// client should treat as a backend problem, not an input problem).
func failureReasonFromDomainErr(err error) eventsv1.InferenceFailureReason {
	switch {
	case errors.Is(err, domain.ErrNoRoute):
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_NO_ROUTE
	case errors.Is(err, domain.ErrRateLimited):
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_RATE_LIMITED
	case errors.Is(err, domain.ErrQuotaExceeded):
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_QUOTA_EXCEEDED
	case errors.Is(err, domain.ErrCircuitOpen):
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_CIRCUIT_OPEN
	case errors.Is(err, domain.ErrTimeout):
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_TIMEOUT
	case errors.Is(err, domain.ErrInvalidInput):
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_INVALID_INPUT
	default:
		return eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_UPSTREAM_ERROR
	}
}

// ============================================================================
// TENSOR + ENUM CONVERTERS (proto ↔ domain, the anti-corruption layer)
// ============================================================================

// tensorsToDomain converts a proto inputs map to the domain's transport-free
// tensor map, validating the dtype enum at the edge. A nil/empty map returns nil
// (the domain's validatePredict rejects "no inputs" with a clean message). WHY
// validate dtype HERE: an UNSPECIFIED dtype is a malformed REQUEST (the client
// didn't set a required enum) — a cheap edge reject with InvalidArgument is far
// clearer than letting an unspecified type reach length validation. We carry the
// dtype's STRING name into the domain (Tensor.DType is an opaque label) so the
// domain stays free of the generated enum type.
func tensorsToDomain(in map[string]*inferencev1.TensorData) (map[string]domain.Tensor, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make(map[string]domain.Tensor, len(in))
	for name, td := range in {
		if td == nil {
			return nil, status.Errorf(codes.InvalidArgument, "input tensor %q is nil", name)
		}
		if td.GetDtype() == inferencev1.DataType_DATA_TYPE_UNSPECIFIED {
			return nil, status.Errorf(codes.InvalidArgument, "input tensor %q has unspecified dtype", name)
		}
		out[name] = domain.Tensor{
			Shape: td.GetShape(),
			DType: td.GetDtype().String(),
			Data:  td.GetData(),
		}
	}
	return out, nil
}

// dtypeFromName maps the domain's opaque dtype label back to the proto enum for
// responses. The domain forwards the SAME label it received (it never invents a
// dtype), so this round-trips a value tensorsToDomain produced. Unknown →
// UNSPECIFIED (defensive; should not happen for gateway-forwarded outputs).
func dtypeFromName(name string) inferencev1.DataType {
	if v, ok := inferencev1.DataType_value[name]; ok {
		return inferencev1.DataType(v)
	}
	return inferencev1.DataType_DATA_TYPE_UNSPECIFIED
}

// tensorsToProto converts a domain output tensor map back to proto for a
// response. nil in → nil out (preserves "no outputs" exactly).
func tensorsToProto(in map[string]domain.Tensor) map[string]*inferencev1.TensorData {
	if in == nil {
		return nil
	}
	out := make(map[string]*inferencev1.TensorData, len(in))
	for name, t := range in {
		out[name] = &inferencev1.TensorData{
			Shape: t.Shape,
			Dtype: dtypeFromName(t.DType),
			Data:  t.Data,
		}
	}
	return out
}

// targetStatusToProto maps the domain lifecycle enum to its proto twin (same
// ordinal values by construction — the domain enum "mirrors the proto enum").
func targetStatusToProto(s domain.TargetStatus) inferencev1.TargetStatus {
	switch s {
	case domain.TargetStatusActive:
		return inferencev1.TargetStatus_TARGET_STATUS_ACTIVE
	case domain.TargetStatusDraining:
		return inferencev1.TargetStatus_TARGET_STATUS_DRAINING
	case domain.TargetStatusUnhealthy:
		return inferencev1.TargetStatus_TARGET_STATUS_UNHEALTHY
	default:
		return inferencev1.TargetStatus_TARGET_STATUS_UNSPECIFIED
	}
}

// circuitStateToProto maps the domain breaker label to the proto enum.
func circuitStateToProto(s domain.CircuitState) inferencev1.CircuitBreakerState {
	switch s {
	case domain.CircuitClosed:
		return inferencev1.CircuitBreakerState_CIRCUIT_BREAKER_STATE_CLOSED
	case domain.CircuitOpen:
		return inferencev1.CircuitBreakerState_CIRCUIT_BREAKER_STATE_OPEN
	case domain.CircuitHalfOpen:
		return inferencev1.CircuitBreakerState_CIRCUIT_BREAKER_STATE_HALF_OPEN
	default:
		return inferencev1.CircuitBreakerState_CIRCUIT_BREAKER_STATE_UNSPECIFIED
	}
}

// routeToProto converts a domain Route to its proto wire form, INCLUDING the
// server-resolved endpoint and status (this is the OPERATOR view — GetRoute /
// UpsertRoute / SetTrafficSplit responses — which is allowed to see endpoints,
// unlike the caller-facing ModelInfo). The timestamp is the well-known type;
// a zero time becomes nil (no spurious 1970 timestamp on the wire).
func routeToProto(r domain.Route) *inferencev1.Route {
	targets := make([]*inferencev1.RouteTarget, 0, len(r.Targets))
	for _, t := range r.Targets {
		targets = append(targets, &inferencev1.RouteTarget{
			Version:   t.Version,
			Endpoint:  t.Endpoint,
			WeightBps: int32(t.WeightBps),
			Status:    targetStatusToProto(t.Status),
		})
	}
	return &inferencev1.Route{
		ModelName: r.ModelName,
		Targets:   targets,
		UpdatedAt: protoTimestamp(r.UpdatedAt),
	}
}

// circuitSnapshotToProto converts an observability snapshot to proto.
func circuitSnapshotToProto(s domain.CircuitSnapshot) *inferencev1.CircuitState {
	return &inferencev1.CircuitState{
		ModelName:           s.ModelName,
		Version:             s.Version,
		State:               circuitStateToProto(s.State),
		ConsecutiveFailures: int32(s.ConsecutiveFailures),
		LastTransitionAt:    protoTimestamp(s.LastTransitionAt),
	}
}

// protoTimestamp returns a proto Timestamp, or nil for the zero time. WHY nil and
// not the epoch: a zero domain time means "never set"; emitting it as
// 1970-01-01 would be a lie a client could mis-read as a real (very old) update.
func protoTimestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// ============================================================================
// DATA PLANE
// ============================================================================

// Predict implements the hot-path single-inference RPC.
func (h *InferenceHandler) Predict(ctx context.Context, req *inferencev1.PredictRequest) (*inferencev1.PredictResponse, error) {
	// 1. NIL-SVC GUARD (unary → return the status as an error value).
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, "inference service not wired")
	}
	// 2. IDENTITY + AUTHZ from verified claims (never a request field).
	p, claims, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := requireScope(claims, scopeInferencePredict); err != nil {
		return nil, err
	}
	// version_override is a PRIVILEGED escape hatch: only an elevated caller may
	// set it. An ordinary caller that supplies it is REJECTED (not silently
	// honored, not silently stripped) — explicit is safer and audit-clear.
	if req.GetVersionOverride() != "" {
		if err := requireScope(claims, scopeInferenceVersionOverride); err != nil {
			return nil, err
		}
	}
	// 3. VALIDATE + CONVERT. Presence of model_name is a cheap edge check; the
	// domain re-validates (and validates tensor byte-lengths) and owns the
	// authoritative message.
	if req.GetModelName() == "" {
		return nil, status.Error(codes.InvalidArgument, "model_name is required")
	}
	inputs, err := tensorsToDomain(req.GetInputs())
	if err != nil {
		return nil, err
	}
	in := domain.PredictInput{
		ModelName:       req.GetModelName(),
		Inputs:          inputs,
		VersionOverride: req.GetVersionOverride(),
		IdempotencyKey:  req.GetIdempotencyKey(),
	}
	// 4. CALL the domain + map result/sentinel.
	out, err := h.svc.Predict(ctx, p, in)
	if err != nil {
		return nil, statusFromDomainErr(err)
	}
	return &inferencev1.PredictResponse{
		Outputs:       tensorsToProto(out.Outputs),
		ServedVersion: out.ServedVersion,
		Latency:       durationpb.New(out.Latency),
		RequestId:     out.RequestID,
	}, nil
}

// BatchPredict implements the bounded unary batch RPC with PARTIAL-SUCCESS
// semantics: a bad item produces a per-item error, it does NOT fail the batch.
//
// WHY one request id minted here and reused per item: the proto's
// BatchPredictResponse.request_id is the call-level handle; each item's outcome
// is correlated by its client item_id. (The domain mints its OWN per-Predict id
// internally for the event/Billing dedupe; the batch envelope id is separate and
// call-scoped.)
func (h *InferenceHandler) BatchPredict(ctx context.Context, req *inferencev1.BatchPredictRequest) (*inferencev1.BatchPredictResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, "inference service not wired")
	}
	p, claims, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := requireScope(claims, scopeInferencePredict); err != nil {
		return nil, err
	}
	if req.GetVersionOverride() != "" {
		if err := requireScope(claims, scopeInferenceVersionOverride); err != nil {
			return nil, err
		}
	}
	if req.GetModelName() == "" {
		return nil, status.Error(codes.InvalidArgument, "model_name is required")
	}
	items := req.GetItems()
	if len(items) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one item is required")
	}
	// HARD CAP enforced at the edge (contract, not a hint) → InvalidArgument.
	if len(items) > maxBatchItems {
		return nil, status.Errorf(codes.InvalidArgument, "batch exceeds the %d-item limit; use StreamPredict for larger batches", maxBatchItems)
	}

	results := make([]*inferencev1.BatchPredictResult, 0, len(items))
	for idx, item := range items {
		// A nil item or empty id is a malformed REQUEST element → surface it as a
		// per-item error (partial success), not a whole-batch failure.
		//
		// IDEMPOTENCY (billing-correctness): the proto's BatchPredictRequest has a
		// CALL-LEVEL idempotency_key whose doc promises "safe retry without
		// double-billing", but BatchPredictItem has no key of its own. We must NOT
		// drop that field on the bulk path: if we did, a retried batch re-runs every
		// item as brand-new work, each item emits a fresh InferenceCompleted, and
		// Billing meters the retry as a second charge. We derive a per-item key from
		// the call key + the item's position so each item is INDEPENDENTLY dedupable
		// on retry (a retry sends the same items in the same order → same keys → the
		// domain's idempotency store collapses the duplicate). See scoreItem for the
		// derivation rationale (why index, not item_id).
		results = append(results, h.scoreItem(ctx, p, req.GetModelName(), req.GetVersionOverride(), req.GetIdempotencyKey(), idx, item))
	}
	return &inferencev1.BatchPredictResponse{
		Results:   results,
		RequestId: newBatchRequestID(),
	}, nil
}

// StreamPredict implements the SERVER-STREAMING large-batch RPC: one request,
// N responses streamed AS each item completes.
//
// STREAMING MECHANICS to study (interview):
//   - the NIL-SVC GUARD returns the status as the error (no response value).
//   - the identity/authz come from stream.Context() — the auth interceptor wraps
//     the ServerStream so its Context() carries the verified Claims (the stream
//     interceptor's grpc.ServerStream wrapper is what makes ClaimsFromContext
//     work inside a streaming handler).
//   - the loop checks stream.Context().Err() each iteration so a client
//     disconnect or deadline STOPS the batch promptly — we never keep scoring a
//     batch nobody is listening to (honoring ctx cancellation, as required).
func (h *InferenceHandler) StreamPredict(req *inferencev1.StreamPredictRequest, stream inferencev1.InferenceGatewayService_StreamPredictServer) error {
	if h.svc == nil {
		// SERVER-STREAMING: return the status as an error (there is NO response
		// value to return; results would otherwise flow via stream.Send).
		return status.Error(codes.Unimplemented, "inference service not wired")
	}
	ctx := stream.Context()
	p, claims, err := principalFromContext(ctx)
	if err != nil {
		return err
	}
	if err := requireScope(claims, scopeInferencePredict); err != nil {
		return err
	}
	if req.GetVersionOverride() != "" {
		if err := requireScope(claims, scopeInferenceVersionOverride); err != nil {
			return err
		}
	}
	if req.GetModelName() == "" {
		return status.Error(codes.InvalidArgument, "model_name is required")
	}
	items := req.GetItems()
	if len(items) == 0 {
		return status.Error(codes.InvalidArgument, "at least one item is required")
	}
	if len(items) > maxStreamItems {
		return status.Errorf(codes.InvalidArgument, "stream exceeds the %d-item limit", maxStreamItems)
	}

	requestID := newBatchRequestID()
	for idx, item := range items {
		// HONOR CANCELLATION: bail the moment the client goes away / deadline
		// fires, returning the ctx error as the stream's terminal status. This is
		// the difference between a gateway that wastes work on a dead stream and
		// one that frees the slot immediately.
		if cerr := ctx.Err(); cerr != nil {
			return status.FromContextError(cerr).Err()
		}
		// IDEMPOTENCY: same billing-correctness contract as BatchPredict — thread
		// the call-level idempotency_key into a per-item key so a re-streamed batch
		// dedupes item-by-item instead of double-billing the whole job. (See
		// scoreItem for why the key is derived from the item INDEX.)
		result := h.scoreItem(ctx, p, req.GetModelName(), req.GetVersionOverride(), req.GetIdempotencyKey(), idx, item)
		if serr := stream.Send(&inferencev1.StreamPredictResponse{
			Result:    result,
			RequestId: requestID,
		}); serr != nil {
			// A Send error means the transport/stream is broken (client gone).
			// Stop and return it; grpc-go turns it into the terminal status.
			return serr
		}
	}
	return nil
}

// scoreItem runs ONE batch/stream item through the domain Predict and packs the
// outcome into a BatchPredictResult with PARTIAL-SUCCESS semantics — success
// carries outputs+served_version; failure carries a structured ErrorDetail AND a
// machine-readable failure_reason so a bulk client can decide per-item retry
// policy (retry TIMEOUT, never INVALID_INPUT). It NEVER returns a Go error: a
// single bad item must not abort the batch/stream.
//
// IDEMPOTENCY DERIVATION (billing-correctness, the whole reason callIdemKey/idx
// are threaded here):
//   - The proto exposes ONE idempotency_key per CALL (BatchPredictRequest /
//     StreamPredictRequest field 4) and none per item. But each item runs its own
//     domain Predict and emits its own InferenceCompleted that Billing meters, so
//     dedupe must be PER ITEM, not per call. We therefore expand the single call
//     key into N per-item keys.
//   - WHY the item INDEX and not item_id: item_id is client-supplied and "opaque
//     to the gateway" (proto) — it may be empty, or duplicated across items. A key
//     built from item_id would be unstable (empty → all items share one key →
//     false dedupe collisions that DROP valid predictions) or non-unique. The
//     index is server-derived, always present, and unique within a call; and
//     because a correct retry resends the SAME items in the SAME order, item k on
//     the retry gets the SAME derived key as on the original — exactly the
//     property the domain's idempotency store needs to collapse the duplicate.
//   - WHY guard empty callIdemKey: if the client supplied NO call key, we leave
//     the per-item key empty too. Synthesizing "<empty>:3" would fabricate a
//     dedupe guarantee the client never asked for and could wrongly suppress a
//     legitimately distinct call. Empty in → empty out, mirroring unary Predict.
func (h *InferenceHandler) scoreItem(
	ctx context.Context,
	p domain.Principal,
	modelName, versionOverride string,
	callIdemKey string,
	idx int,
	item *inferencev1.BatchPredictItem,
) *inferencev1.BatchPredictResult {
	if item == nil {
		return &inferencev1.BatchPredictResult{
			Error: &commonv1.ErrorDetail{
				Code:    codes.InvalidArgument.String(),
				Message: "item is nil",
			},
			FailureReason: eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_INVALID_INPUT,
		}
	}
	// Convert this item's tensors; a dtype/nil-tensor fault is a per-item input
	// error (still partial success — other items proceed).
	inputs, convErr := tensorsToDomain(item.GetInputs())
	if convErr != nil {
		return &inferencev1.BatchPredictResult{
			ItemId: item.GetItemId(),
			Error: &commonv1.ErrorDetail{
				Code:    codes.InvalidArgument.String(),
				Message: sanitizedMessage(convErr),
			},
			FailureReason: eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_INVALID_INPUT,
		}
	}
	out, err := h.svc.Predict(ctx, p, domain.PredictInput{
		ModelName:       modelName,
		Inputs:          inputs,
		VersionOverride: versionOverride,
		IdempotencyKey:  perItemIdempotencyKey(callIdemKey, idx),
	})
	if err != nil {
		st := status.Convert(statusFromDomainErr(err))
		return &inferencev1.BatchPredictResult{
			ItemId: item.GetItemId(),
			Error: &commonv1.ErrorDetail{
				Code:    st.Code().String(),
				Message: st.Message(),
			},
			FailureReason: failureReasonFromDomainErr(err),
		}
	}
	return &inferencev1.BatchPredictResult{
		ItemId:        item.GetItemId(),
		Outputs:       tensorsToProto(out.Outputs),
		ServedVersion: out.ServedVersion,
		FailureReason: eventsv1.InferenceFailureReason_INFERENCE_FAILURE_REASON_UNSPECIFIED,
	}
}

// perItemIdempotencyKey expands a single CALL-level idempotency key into a
// stable, per-item key for the bulk paths (BatchPredict / StreamPredict).
//
// The format is "<callKey>:<index>" — the index anchors the key to the item's
// POSITION in the call, which is the only retry-stable, always-present, unique-
// within-the-call identifier the gateway controls (item_id is client-supplied
// and may be empty/duplicated; see scoreItem for the full rationale).
//
// Empty callKey → empty result: a client that opted OUT of idempotency on the
// call must not be silently opted IN per item, and an empty base must never
// produce a real-looking key like ":0" that could collide across distinct calls.
func perItemIdempotencyKey(callKey string, idx int) string {
	if callKey == "" {
		return ""
	}
	return callKey + ":" + strconv.Itoa(idx)
}

// sanitizedMessage extracts a client-safe message from an error: if it's already
// a gRPC status (our edge errors are), use its message (authored clean); else a
// generic string. Guards against echoing an unvetted Go error into a response.
func sanitizedMessage(err error) string {
	if st, ok := status.FromError(err); ok {
		return st.Message()
	}
	return "invalid input"
}

// ============================================================================
// MODEL METADATA (read) — the CALLER view
// ============================================================================

// GetModelInfo returns the caller-facing model contract (schema + routable
// versions + serving status), deliberately OMITTING server-internal fields
// (backend endpoints, breaker internals). It is built from the operator Route via
// GetRoute on the domain, then PROJECTED down to the caller-safe shape — the
// projection is the whole point of this RPC vs GetRoute.
//
// NOTE on schema (inputs/outputs TensorSpecs): the tensor signature is learned
// from the serving backend's signature, which the domain service does not expose
// through its current interface (no schema port yet). We return the routing-
// derived fields the domain DOES know (is_serving, versions, updated_at) and
// leave the schema slices empty rather than fabricate one — honest partial info
// the HTTP edge can still use for version/serving display. (When a schema port
// lands, this fills inputs/outputs without changing the RPC contract.)
func (h *InferenceHandler) GetModelInfo(ctx context.Context, req *inferencev1.GetModelInfoRequest) (*inferencev1.GetModelInfoResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, "inference service not wired")
	}
	p, claims, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := requireScope(claims, scopeInferencePredict); err != nil {
		return nil, err
	}
	if req.GetModelName() == "" {
		return nil, status.Error(codes.InvalidArgument, "model_name is required")
	}
	// TENANCY: scope the lookup to the caller's own team (from verified claims) so a
	// caller can only see its OWN model's info; another team's identically-named
	// model resolves to NotFound (no cross-tenant read, no existence oracle).
	route, err := h.svc.GetRoute(ctx, p.Team, req.GetModelName())
	if err != nil {
		return nil, statusFromDomainErr(err)
	}
	// PROJECT the operator route down to the caller-safe VersionInfo list:
	// version + weight + is_stable ONLY — no endpoints, no status internals.
	versions := make([]*inferencev1.VersionInfo, 0, len(route.Targets))
	for _, t := range route.Targets {
		versions = append(versions, &inferencev1.VersionInfo{
			Version:   t.Version,
			WeightBps: int32(t.WeightBps),
			IsStable:  t.IsStable,
		})
	}
	return &inferencev1.GetModelInfoResponse{
		ModelInfo: &inferencev1.ModelInfo{
			ModelName: route.ModelName,
			IsServing: route.IsServing(),
			Versions:  versions,
			UpdatedAt: protoTimestamp(route.UpdatedAt),
		},
	}, nil
}

// ============================================================================
// ROUTING CONTROL PLANE (elevated scope) — break-glass manual control
// ============================================================================

// GetRoute returns the OPERATOR view of a route (includes endpoints), so it
// requires the elevated admin scope.
func (h *InferenceHandler) GetRoute(ctx context.Context, req *inferencev1.GetRouteRequest) (*inferencev1.GetRouteResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, "inference service not wired")
	}
	p, claims, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := requireScope(claims, scopeInferenceAdmin); err != nil {
		return nil, err
	}
	if req.GetModelName() == "" {
		return nil, status.Error(codes.InvalidArgument, "model_name is required")
	}
	// TENANCY: even an admin-scoped caller is scoped to its OWN team's routes — the
	// admin scope gates the operator VIEW (endpoints/status), not cross-tenant
	// reach. A route owned by another team is NotFound (no oracle).
	route, err := h.svc.GetRoute(ctx, p.Team, req.GetModelName())
	if err != nil {
		return nil, statusFromDomainErr(err)
	}
	return &inferencev1.GetRouteResponse{Route: routeToProto(route)}, nil
}

// ListRoutes pages the whole routing table (operator dashboard / CLI). It caps
// page_size at maxPageSize HERE (the store trusts the handler to have capped it,
// per RouteStore.List's contract) and defaults an unset size.
func (h *InferenceHandler) ListRoutes(ctx context.Context, req *inferencev1.ListRoutesRequest) (*inferencev1.ListRoutesResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, "inference service not wired")
	}
	p, claims, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := requireScope(claims, scopeInferenceAdmin); err != nil {
		return nil, err
	}
	opts := paginationToOptions(req.GetPagination())
	// TENANCY: list only the caller's own team's routes (the domain filters the page
	// by OwnerTeam) — a team never enumerates another team's models or endpoints.
	routes, nextToken, err := h.svc.ListRoutes(ctx, p.Team, opts)
	if err != nil {
		return nil, statusFromDomainErr(err)
	}
	protoRoutes := make([]*inferencev1.Route, 0, len(routes))
	for _, r := range routes {
		protoRoutes = append(protoRoutes, routeToProto(r))
	}
	return &inferencev1.ListRoutesResponse{
		Routes: protoRoutes,
		Pagination: &commonv1.PaginationResponse{
			NextPageToken: nextToken,
		},
	}, nil
}

// UpsertRoute is the break-glass create/replace. SECURITY (anti mass-assignment):
// only version + weight_bps are read from each proposed RouteTarget — endpoint
// and status are IGNORED here (the domain server-resolves the endpoint from the
// existing deploy record; accepting a client endpoint would be SSRF). The domain
// validates the weight sum and surfaces ErrRouteValidation, which we map to
// InvalidArgument.
func (h *InferenceHandler) UpsertRoute(ctx context.Context, req *inferencev1.UpsertRouteRequest) (*inferencev1.UpsertRouteResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, "inference service not wired")
	}
	p, claims, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := requireScope(claims, scopeInferenceAdmin); err != nil {
		return nil, err
	}
	if req.GetModelName() == "" {
		return nil, status.Error(codes.InvalidArgument, "model_name is required")
	}
	if len(req.GetTargets()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one target is required")
	}
	// Build the NARROW domain input: deliberately drop endpoint/status from each
	// proto RouteTarget — the dedicated ProposedTarget type closes the mass-
	// assignment hole by construction (it has no endpoint/status field).
	proposed := make([]domain.ProposedTarget, 0, len(req.GetTargets()))
	for _, t := range req.GetTargets() {
		proposed = append(proposed, domain.ProposedTarget{
			Version:   t.GetVersion(),
			WeightBps: int(t.GetWeightBps()),
		})
	}
	// TENANCY: the write is scoped to the caller's own team — it creates/replaces a
	// route UNDER p.Team and resolves endpoints only from that team's existing
	// route, so a caller can neither overwrite nor SSRF-resolve against another
	// team's route of the same name.
	route, err := h.svc.UpsertRoute(ctx, p.Team, req.GetModelName(), proposed)
	if err != nil {
		return nil, statusFromDomainErr(err)
	}
	return &inferencev1.UpsertRouteResponse{Route: routeToProto(route)}, nil
}

// SetTrafficSplit is the canary dial — adjusts ONLY weights of existing versions.
// Like UpsertRoute it accepts a minimal (version, weight) payload so a caller
// cannot smuggle in an endpoint/status change. The domain rejects unknown
// versions / bad sums with ErrRouteValidation and a missing route with
// ErrNoRoute → NotFound.
func (h *InferenceHandler) SetTrafficSplit(ctx context.Context, req *inferencev1.SetTrafficSplitRequest) (*inferencev1.SetTrafficSplitResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, "inference service not wired")
	}
	p, claims, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := requireScope(claims, scopeInferenceAdmin); err != nil {
		return nil, err
	}
	if req.GetModelName() == "" {
		return nil, status.Error(codes.InvalidArgument, "model_name is required")
	}
	if len(req.GetWeights()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one weight is required")
	}
	weights := make([]domain.TrafficWeight, 0, len(req.GetWeights()))
	for _, w := range req.GetWeights() {
		weights = append(weights, domain.TrafficWeight{
			Version:   w.GetVersion(),
			WeightBps: int(w.GetWeightBps()),
		})
	}
	// TENANCY: reweight only within the caller's own team namespace; another team's
	// model name resolves to NotFound (ErrNoRoute).
	route, err := h.svc.SetTrafficSplit(ctx, p.Team, req.GetModelName(), weights)
	if err != nil {
		return nil, statusFromDomainErr(err)
	}
	return &inferencev1.SetTrafficSplitResponse{Route: routeToProto(route)}, nil
}

// DeleteRoute removes a model from routing. Idempotent in the domain (deleting an
// absent route is a no-op), so a repeated delete is OK and returns the empty
// response — exactly the idempotent-consumer contract.
func (h *InferenceHandler) DeleteRoute(ctx context.Context, req *inferencev1.DeleteRouteRequest) (*inferencev1.DeleteRouteResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, "inference service not wired")
	}
	p, claims, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := requireScope(claims, scopeInferenceAdmin); err != nil {
		return nil, err
	}
	if req.GetModelName() == "" {
		return nil, status.Error(codes.InvalidArgument, "model_name is required")
	}
	// TENANCY: delete only within the caller's own team namespace — a name another
	// team owns is untouched (the delete is a no-op against this team's absent
	// route), so a caller can never destroy another team's route.
	if err := h.svc.DeleteRoute(ctx, p.Team, req.GetModelName()); err != nil {
		return nil, statusFromDomainErr(err)
	}
	return &inferencev1.DeleteRouteResponse{}, nil
}

// ============================================================================
// RESILIENCE OBSERVABILITY (elevated scope — exposes breaker internals)
// ============================================================================

// GetCircuitState reports one backend's (model, version) breaker. Breakers are
// PER backend, so model+version are both required. A breaker not present in the
// snapshot → NotFound (we never invent a CLOSED state for a backend we've never
// seen — that would mask "this version isn't routed").
func (h *InferenceHandler) GetCircuitState(ctx context.Context, req *inferencev1.GetCircuitStateRequest) (*inferencev1.GetCircuitStateResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, "inference service not wired")
	}
	_, claims, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := requireScope(claims, scopeInferenceAdmin); err != nil {
		return nil, err
	}
	if req.GetModelName() == "" || req.GetVersion() == "" {
		return nil, status.Error(codes.InvalidArgument, "model_name and version are required")
	}
	// CircuitStates is a pure read over the in-memory registry (no ctx needed by
	// the domain). Filter by model, then find the exact version.
	for _, snap := range h.svc.CircuitStates(req.GetModelName()) {
		if snap.Version == req.GetVersion() {
			return &inferencev1.GetCircuitStateResponse{
				CircuitState: circuitSnapshotToProto(snap),
			}, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "no circuit breaker for model %q version %q", req.GetModelName(), req.GetVersion())
}

// ListCircuitStates lists breaker states (optional model filter), paginated. The
// domain registry returns an unpaginated snapshot, so the handler applies the
// page window here (cap + offset cursor) — keeping the registry a simple in-memory
// read and the pagination policy at the edge.
func (h *InferenceHandler) ListCircuitStates(ctx context.Context, req *inferencev1.ListCircuitStatesRequest) (*inferencev1.ListCircuitStatesResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Unimplemented, "inference service not wired")
	}
	_, claims, err := principalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := requireScope(claims, scopeInferenceAdmin); err != nil {
		return nil, err
	}
	snaps := h.svc.CircuitStates(req.GetModelNameFilter())
	states := make([]*inferencev1.CircuitState, 0, len(snaps))
	for _, snap := range snaps {
		states = append(states, circuitSnapshotToProto(snap))
	}
	return &inferencev1.ListCircuitStatesResponse{
		CircuitStates: states,
		Pagination:    &commonv1.PaginationResponse{TotalCount: int32(len(states))},
	}, nil
}

// ============================================================================
// SHARED SMALL HELPERS
// ============================================================================

// paginationToOptions converts a proto PaginationRequest to domain ListOptions,
// applying the server-side page-size policy: unset → default, over-max → capped.
// We CAP rather than reject so a generous client request degrades gracefully.
func paginationToOptions(p *commonv1.PaginationRequest) domain.ListOptions {
	size := int(p.GetPageSize())
	switch {
	case size <= 0:
		size = defaultPageSize
	case size > maxPageSize:
		size = maxPageSize
	}
	return domain.ListOptions{
		PageSize:  size,
		PageToken: p.GetPageToken(),
	}
}

// newBatchRequestID mints a call-level id for a batch/stream envelope: an RFC
// 4122 v4 UUID from crypto/rand. WHY the handler owns this rather than exporting
// the domain's per-predict id minter: the batch envelope id is a TRANSPORT-level
// correlation handle (one per call), distinct from the domain's per-prediction
// billing/dedupe id (one per item, minted inside Predict). Keeping it here keeps
// the layering clean — the domain's id is its concern, the envelope id is the
// handler's. 122 bits of entropy → globally unique with no coordination.
func newBatchRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand only fails on a broken entropy source — effectively
		// unreachable. A batch id is best-effort correlation (not load-bearing
		// for correctness), so degrade to an empty id rather than fail the call.
		return ""
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
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
