// ============================================================================
// Forgepoint Inference Gateway Service Proto Definitions
// ============================================================================
//
// WHY: The Inference Gateway is the SINGLE FRONT DOOR for all online prediction
// traffic. External clients never talk to a model-serving pod directly; they
// talk to this gateway, which decides WHICH model version handles the request,
// PROTECTS the backends from overload, and RECORDS what happened. It is the
// "API Gateway" microservices pattern specialized for ML inference.
//
// WHAT'S HERE:
//   - Tensor I/O types (TensorData) — the wire shape of model inputs/outputs
//   - Predict / BatchPredict (unary) + StreamPredict (server-streaming batch)
//   - Routing & traffic-split admin RPCs (the canary control plane)
//   - Circuit-breaker observability RPCs
//   - InferenceCompletedEvent / InferenceFailedEvent — typed NATS payloads
//
// PATTERN — API GATEWAY + RESILIENCE STACK:
//   This service composes FOUR classic resilience patterns into one request
//   path. Each is interview-namable and each is realized by a part of this API:
//
//     1. TRAFFIC SPLITTING (canary / A-B): a model has N candidate versions,
//        each with a weight. The gateway picks one per request by weighted
//        random selection. Realized by Route + RouteTarget.weight_bps and the
//        SetTrafficSplit RPC. The CLIENT does not choose the version (that
//        would defeat canarying); the SERVER resolves it and reports it back
//        in PredictResponse.served_version.
//
//     2. CIRCUIT BREAKER (3-state: closed → open → half-open): per backend
//        endpoint. When a version's serving pods start failing, its breaker
//        trips OPEN and the gateway stops sending it traffic (fail fast)
//        instead of piling requests onto a sick backend. Observable via
//        GetCircuitState / ListCircuitStates.
//
//     3. RATE LIMITING (token bucket, per API key): caps request rate so one
//        tenant can't exhaust shared capacity. Enforced in the interceptor /
//        handler, not modeled as an RPC — but the rejection surfaces as a
//        gRPC RESOURCE_EXHAUSTED status (HTTP 429 at the edge).
//
//     4. BULKHEAD (per-model concurrency semaphore): isolates models so a slow
//        model can't consume every worker and starve the others. Like rate
//        limiting it is enforced inline; over the limit → RESOURCE_EXHAUSTED.
//
// WHY THE GATEWAY DOES NOT IMPORT serving.proto:
//   Tempting: the gateway forwards to model-serving, so why not reuse its
//   PredictRequest? Because services are DECOUPLED — proto-level coupling
//   would mean a change to serving's contract forces a gateway redeploy, and
//   buf-breaking on serving.proto would fail the gateway's lint. Instead the
//   gateway owns its OWN external contract (this file) and maps to/from the
//   serving client at the repository boundary. The tensor types here are a
//   deliberate, stable mirror — the public API the gateway promises clients,
//   independent of how serving happens to model tensors internally. This is
//   the standard gateway discipline (Kong/Envoy don't share upstream protos).
//
// REAL-WORLD COMPARISON:
//   - Envoy / Istio: weighted clusters + outlier detection (circuit breaking)
//     + local rate limiting — exactly this stack, generic for HTTP/gRPC.
//   - AWS SageMaker endpoints: "production variants" with traffic weights for
//     A/B and canary — RouteTarget.weight_bps is our production-variant weight.
//   - Seldon / KServe: InferenceService with canary traffic percentage; the
//     "transformer/router" component is this gateway's job.
//   - Stripe API: per-key rate limits returning 429 + Retry-After.
//
// CALL FLOW (the hot path):
//   Client ──HTTP/gRPC──► Inference Gateway.Predict
//        │ 1. auth interceptor (ValidateToken via Auth service)
//        │ 2. rate limit (token bucket, per API key, Redis-backed)
//        │ 3. route lookup (RouteTable: model_name → [versions+weights])
//        │ 4. traffic split (weighted random → chosen version)
//        │ 5. circuit breaker check (is this version's breaker closed?)
//        │ 6. bulkhead acquire (per-model concurrency slot)
//        │ 7. forward to model-serving (gRPC, with retry+backoff)
//        │ 8. record outcome → publish InferenceCompleted/Failed (async)
//        ▼
//   PredictResponse (outputs + served_version + latency_ms + request_id)
//
// EVENTS PUBLISHED (subject hierarchy from the platform design):
//   fp.inference.completed → InferenceCompletedEvent
//   fp.inference.failed    → InferenceFailedEvent
//   Consumed by: Billing (meter usage), Experiment Tracker (record),
//                Model Monitor (drift windows over features + predictions).
//
// VERSIONING: Package path includes v1 following Buf/Google convention.
// Breaking changes require a new forgepoint.inference.v2 package.
// ============================================================================

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.6.2
// - protoc             (unknown)
// source: forgepoint/inference/v1/inference.proto

package inferencev1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	InferenceGatewayService_Predict_FullMethodName           = "/forgepoint.inference.v1.InferenceGatewayService/Predict"
	InferenceGatewayService_BatchPredict_FullMethodName      = "/forgepoint.inference.v1.InferenceGatewayService/BatchPredict"
	InferenceGatewayService_StreamPredict_FullMethodName     = "/forgepoint.inference.v1.InferenceGatewayService/StreamPredict"
	InferenceGatewayService_GetRoute_FullMethodName          = "/forgepoint.inference.v1.InferenceGatewayService/GetRoute"
	InferenceGatewayService_ListRoutes_FullMethodName        = "/forgepoint.inference.v1.InferenceGatewayService/ListRoutes"
	InferenceGatewayService_UpsertRoute_FullMethodName       = "/forgepoint.inference.v1.InferenceGatewayService/UpsertRoute"
	InferenceGatewayService_SetTrafficSplit_FullMethodName   = "/forgepoint.inference.v1.InferenceGatewayService/SetTrafficSplit"
	InferenceGatewayService_DeleteRoute_FullMethodName       = "/forgepoint.inference.v1.InferenceGatewayService/DeleteRoute"
	InferenceGatewayService_GetCircuitState_FullMethodName   = "/forgepoint.inference.v1.InferenceGatewayService/GetCircuitState"
	InferenceGatewayService_ListCircuitStates_FullMethodName = "/forgepoint.inference.v1.InferenceGatewayService/ListCircuitStates"
)

// InferenceGatewayServiceClient is the client API for InferenceGatewayService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// ============================================================================
// INFERENCE GATEWAY SERVICE
// ============================================================================
//
// WHY one service with both a hot DATA plane (Predict*) and a CONTROL plane
// (route/breaker admin): they're cohesive — the control plane configures
// exactly what the data plane enforces, and both live in the same process that
// holds the in-memory routing table and breaker state. Splitting them would
// force the data plane to fetch routing config over the network on the hot
// path (latency) or duplicate it. Envoy follows the same shape: one proxy
// process, an xDS control surface plus the data path.
//
// RPC CATEGORIES:
//
//	DATA PLANE (hot path):   Predict, BatchPredict, StreamPredict
//	ROUTING CONTROL PLANE:   GetRoute, ListRoutes, UpsertRoute,
//	                         SetTrafficSplit, DeleteRoute
//	RESILIENCE OBSERVABILITY: GetCircuitState, ListCircuitStates
//
// NOTE ON THE EXTERNAL HTTP API: the platform also exposes
// POST /v1/models/{model}/predict via an HTTP handler (JSON in/out). That
// handler maps onto Predict here — gRPC is the canonical internal contract;
// the HTTP edge is a thin JSON adapter over it (TensorData bytes ↔ JSON arrays).
//
// AUTH: every RPC runs behind the shared auth interceptor (ValidateToken +
// CheckPermission). Data-plane calls require an inference scope; control-plane
// RPCs require an elevated deploy/admin scope (only operators and the canary
// executor reshape routes). version_override on Predict requires elevation too.
// ============================================================================
type InferenceGatewayServiceClient interface {
	// Predict routes a single inference request to a model-serving backend via
	// the full resilience stack (rate limit → route → traffic split → circuit
	// breaker → bulkhead → forward with retry) and returns the outputs plus the
	// version that served and the latency observed. Emits InferenceCompleted on
	// success / InferenceFailed on failure (async, off the response path).
	Predict(ctx context.Context, in *PredictRequest, opts ...grpc.CallOption) (*PredictResponse, error)
	// BatchPredict scores many inputs for one model in a single request,
	// returning one result per item with PARTIAL-SUCCESS semantics (a bad item
	// doesn't fail the batch). Use for BOUNDED batches (capped at 256 items
	// server-side); for large/unbounded batches use StreamPredict.
	BatchPredict(ctx context.Context, in *BatchPredictRequest, opts ...grpc.CallOption) (*BatchPredictResponse, error)
	// StreamPredict scores a (potentially large) batch and SERVER-STREAMS one
	// result per item as each completes — incremental delivery with bounded
	// gateway memory. The single request carries all items; results flow back
	// until the stream ends. Choose this over BatchPredict when the batch is
	// large or you want to start consuming results before the batch finishes.
	StreamPredict(ctx context.Context, in *StreamPredictRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[StreamPredictResponse], error)
	// GetRoute returns the current routing-table entry (versions + weights) for
	// one model. Read-only; used by operators, the UI, and the canary executor
	// to inspect current splits.
	GetRoute(ctx context.Context, in *GetRouteRequest, opts ...grpc.CallOption) (*GetRouteResponse, error)
	// ListRoutes returns the whole routing table, paginated (page_size capped at
	// 100 server-side). For the operator dashboard / `fp` CLI.
	ListRoutes(ctx context.Context, in *ListRoutesRequest, opts ...grpc.CallOption) (*ListRoutesResponse, error)
	// UpsertRoute creates or replaces a model's route (break-glass manual
	// control; routes are normally driven by ModelDeployed events). Endpoints
	// are server-resolved (never accepted from the client) and weights must sum
	// to 10000. Requires elevated permission. Idempotent via idempotency_key.
	UpsertRoute(ctx context.Context, in *UpsertRouteRequest, opts ...grpc.CallOption) (*UpsertRouteResponse, error)
	// SetTrafficSplit adjusts ONLY the version weights of an existing route —
	// the canary dial (e.g., 90/10 → 50/50 → 0/100). The most common rollout
	// operation; narrow and audit-friendly by design. Requires elevated
	// permission. Idempotent.
	SetTrafficSplit(ctx context.Context, in *SetTrafficSplitRequest, opts ...grpc.CallOption) (*SetTrafficSplitResponse, error)
	// DeleteRoute removes a model from routing entirely (subsequent predicts get
	// NOT_FOUND). Normally driven by ModelUndeployed; this is the override.
	// Requires elevated permission. Idempotent (deleting twice is a no-op).
	DeleteRoute(ctx context.Context, in *DeleteRouteRequest, opts ...grpc.CallOption) (*DeleteRouteResponse, error)
	// GetCircuitState reports the breaker state for one backend (model+version):
	// CLOSED / OPEN / HALF_OPEN plus failure count and last transition. Turns
	// an invisible failure mode into an observable one for dashboards/operators.
	GetCircuitState(ctx context.Context, in *GetCircuitStateRequest, opts ...grpc.CallOption) (*GetCircuitStateResponse, error)
	// ListCircuitStates lists breaker states across backends (optional model
	// filter), paginated — the at-a-glance health view of all routed backends.
	ListCircuitStates(ctx context.Context, in *ListCircuitStatesRequest, opts ...grpc.CallOption) (*ListCircuitStatesResponse, error)
}

type inferenceGatewayServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewInferenceGatewayServiceClient(cc grpc.ClientConnInterface) InferenceGatewayServiceClient {
	return &inferenceGatewayServiceClient{cc}
}

func (c *inferenceGatewayServiceClient) Predict(ctx context.Context, in *PredictRequest, opts ...grpc.CallOption) (*PredictResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(PredictResponse)
	err := c.cc.Invoke(ctx, InferenceGatewayService_Predict_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *inferenceGatewayServiceClient) BatchPredict(ctx context.Context, in *BatchPredictRequest, opts ...grpc.CallOption) (*BatchPredictResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(BatchPredictResponse)
	err := c.cc.Invoke(ctx, InferenceGatewayService_BatchPredict_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *inferenceGatewayServiceClient) StreamPredict(ctx context.Context, in *StreamPredictRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[StreamPredictResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &InferenceGatewayService_ServiceDesc.Streams[0], InferenceGatewayService_StreamPredict_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[StreamPredictRequest, StreamPredictResponse]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type InferenceGatewayService_StreamPredictClient = grpc.ServerStreamingClient[StreamPredictResponse]

func (c *inferenceGatewayServiceClient) GetRoute(ctx context.Context, in *GetRouteRequest, opts ...grpc.CallOption) (*GetRouteResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetRouteResponse)
	err := c.cc.Invoke(ctx, InferenceGatewayService_GetRoute_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *inferenceGatewayServiceClient) ListRoutes(ctx context.Context, in *ListRoutesRequest, opts ...grpc.CallOption) (*ListRoutesResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ListRoutesResponse)
	err := c.cc.Invoke(ctx, InferenceGatewayService_ListRoutes_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *inferenceGatewayServiceClient) UpsertRoute(ctx context.Context, in *UpsertRouteRequest, opts ...grpc.CallOption) (*UpsertRouteResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(UpsertRouteResponse)
	err := c.cc.Invoke(ctx, InferenceGatewayService_UpsertRoute_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *inferenceGatewayServiceClient) SetTrafficSplit(ctx context.Context, in *SetTrafficSplitRequest, opts ...grpc.CallOption) (*SetTrafficSplitResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(SetTrafficSplitResponse)
	err := c.cc.Invoke(ctx, InferenceGatewayService_SetTrafficSplit_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *inferenceGatewayServiceClient) DeleteRoute(ctx context.Context, in *DeleteRouteRequest, opts ...grpc.CallOption) (*DeleteRouteResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(DeleteRouteResponse)
	err := c.cc.Invoke(ctx, InferenceGatewayService_DeleteRoute_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *inferenceGatewayServiceClient) GetCircuitState(ctx context.Context, in *GetCircuitStateRequest, opts ...grpc.CallOption) (*GetCircuitStateResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetCircuitStateResponse)
	err := c.cc.Invoke(ctx, InferenceGatewayService_GetCircuitState_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *inferenceGatewayServiceClient) ListCircuitStates(ctx context.Context, in *ListCircuitStatesRequest, opts ...grpc.CallOption) (*ListCircuitStatesResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ListCircuitStatesResponse)
	err := c.cc.Invoke(ctx, InferenceGatewayService_ListCircuitStates_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// InferenceGatewayServiceServer is the server API for InferenceGatewayService service.
// All implementations must embed UnimplementedInferenceGatewayServiceServer
// for forward compatibility.
//
// ============================================================================
// INFERENCE GATEWAY SERVICE
// ============================================================================
//
// WHY one service with both a hot DATA plane (Predict*) and a CONTROL plane
// (route/breaker admin): they're cohesive — the control plane configures
// exactly what the data plane enforces, and both live in the same process that
// holds the in-memory routing table and breaker state. Splitting them would
// force the data plane to fetch routing config over the network on the hot
// path (latency) or duplicate it. Envoy follows the same shape: one proxy
// process, an xDS control surface plus the data path.
//
// RPC CATEGORIES:
//
//	DATA PLANE (hot path):   Predict, BatchPredict, StreamPredict
//	ROUTING CONTROL PLANE:   GetRoute, ListRoutes, UpsertRoute,
//	                         SetTrafficSplit, DeleteRoute
//	RESILIENCE OBSERVABILITY: GetCircuitState, ListCircuitStates
//
// NOTE ON THE EXTERNAL HTTP API: the platform also exposes
// POST /v1/models/{model}/predict via an HTTP handler (JSON in/out). That
// handler maps onto Predict here — gRPC is the canonical internal contract;
// the HTTP edge is a thin JSON adapter over it (TensorData bytes ↔ JSON arrays).
//
// AUTH: every RPC runs behind the shared auth interceptor (ValidateToken +
// CheckPermission). Data-plane calls require an inference scope; control-plane
// RPCs require an elevated deploy/admin scope (only operators and the canary
// executor reshape routes). version_override on Predict requires elevation too.
// ============================================================================
type InferenceGatewayServiceServer interface {
	// Predict routes a single inference request to a model-serving backend via
	// the full resilience stack (rate limit → route → traffic split → circuit
	// breaker → bulkhead → forward with retry) and returns the outputs plus the
	// version that served and the latency observed. Emits InferenceCompleted on
	// success / InferenceFailed on failure (async, off the response path).
	Predict(context.Context, *PredictRequest) (*PredictResponse, error)
	// BatchPredict scores many inputs for one model in a single request,
	// returning one result per item with PARTIAL-SUCCESS semantics (a bad item
	// doesn't fail the batch). Use for BOUNDED batches (capped at 256 items
	// server-side); for large/unbounded batches use StreamPredict.
	BatchPredict(context.Context, *BatchPredictRequest) (*BatchPredictResponse, error)
	// StreamPredict scores a (potentially large) batch and SERVER-STREAMS one
	// result per item as each completes — incremental delivery with bounded
	// gateway memory. The single request carries all items; results flow back
	// until the stream ends. Choose this over BatchPredict when the batch is
	// large or you want to start consuming results before the batch finishes.
	StreamPredict(*StreamPredictRequest, grpc.ServerStreamingServer[StreamPredictResponse]) error
	// GetRoute returns the current routing-table entry (versions + weights) for
	// one model. Read-only; used by operators, the UI, and the canary executor
	// to inspect current splits.
	GetRoute(context.Context, *GetRouteRequest) (*GetRouteResponse, error)
	// ListRoutes returns the whole routing table, paginated (page_size capped at
	// 100 server-side). For the operator dashboard / `fp` CLI.
	ListRoutes(context.Context, *ListRoutesRequest) (*ListRoutesResponse, error)
	// UpsertRoute creates or replaces a model's route (break-glass manual
	// control; routes are normally driven by ModelDeployed events). Endpoints
	// are server-resolved (never accepted from the client) and weights must sum
	// to 10000. Requires elevated permission. Idempotent via idempotency_key.
	UpsertRoute(context.Context, *UpsertRouteRequest) (*UpsertRouteResponse, error)
	// SetTrafficSplit adjusts ONLY the version weights of an existing route —
	// the canary dial (e.g., 90/10 → 50/50 → 0/100). The most common rollout
	// operation; narrow and audit-friendly by design. Requires elevated
	// permission. Idempotent.
	SetTrafficSplit(context.Context, *SetTrafficSplitRequest) (*SetTrafficSplitResponse, error)
	// DeleteRoute removes a model from routing entirely (subsequent predicts get
	// NOT_FOUND). Normally driven by ModelUndeployed; this is the override.
	// Requires elevated permission. Idempotent (deleting twice is a no-op).
	DeleteRoute(context.Context, *DeleteRouteRequest) (*DeleteRouteResponse, error)
	// GetCircuitState reports the breaker state for one backend (model+version):
	// CLOSED / OPEN / HALF_OPEN plus failure count and last transition. Turns
	// an invisible failure mode into an observable one for dashboards/operators.
	GetCircuitState(context.Context, *GetCircuitStateRequest) (*GetCircuitStateResponse, error)
	// ListCircuitStates lists breaker states across backends (optional model
	// filter), paginated — the at-a-glance health view of all routed backends.
	ListCircuitStates(context.Context, *ListCircuitStatesRequest) (*ListCircuitStatesResponse, error)
	mustEmbedUnimplementedInferenceGatewayServiceServer()
}

// UnimplementedInferenceGatewayServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedInferenceGatewayServiceServer struct{}

func (UnimplementedInferenceGatewayServiceServer) Predict(context.Context, *PredictRequest) (*PredictResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method Predict not implemented")
}
func (UnimplementedInferenceGatewayServiceServer) BatchPredict(context.Context, *BatchPredictRequest) (*BatchPredictResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method BatchPredict not implemented")
}
func (UnimplementedInferenceGatewayServiceServer) StreamPredict(*StreamPredictRequest, grpc.ServerStreamingServer[StreamPredictResponse]) error {
	return status.Error(codes.Unimplemented, "method StreamPredict not implemented")
}
func (UnimplementedInferenceGatewayServiceServer) GetRoute(context.Context, *GetRouteRequest) (*GetRouteResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetRoute not implemented")
}
func (UnimplementedInferenceGatewayServiceServer) ListRoutes(context.Context, *ListRoutesRequest) (*ListRoutesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ListRoutes not implemented")
}
func (UnimplementedInferenceGatewayServiceServer) UpsertRoute(context.Context, *UpsertRouteRequest) (*UpsertRouteResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method UpsertRoute not implemented")
}
func (UnimplementedInferenceGatewayServiceServer) SetTrafficSplit(context.Context, *SetTrafficSplitRequest) (*SetTrafficSplitResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method SetTrafficSplit not implemented")
}
func (UnimplementedInferenceGatewayServiceServer) DeleteRoute(context.Context, *DeleteRouteRequest) (*DeleteRouteResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method DeleteRoute not implemented")
}
func (UnimplementedInferenceGatewayServiceServer) GetCircuitState(context.Context, *GetCircuitStateRequest) (*GetCircuitStateResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetCircuitState not implemented")
}
func (UnimplementedInferenceGatewayServiceServer) ListCircuitStates(context.Context, *ListCircuitStatesRequest) (*ListCircuitStatesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ListCircuitStates not implemented")
}
func (UnimplementedInferenceGatewayServiceServer) mustEmbedUnimplementedInferenceGatewayServiceServer() {
}
func (UnimplementedInferenceGatewayServiceServer) testEmbeddedByValue() {}

// UnsafeInferenceGatewayServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to InferenceGatewayServiceServer will
// result in compilation errors.
type UnsafeInferenceGatewayServiceServer interface {
	mustEmbedUnimplementedInferenceGatewayServiceServer()
}

func RegisterInferenceGatewayServiceServer(s grpc.ServiceRegistrar, srv InferenceGatewayServiceServer) {
	// If the following call panics, it indicates UnimplementedInferenceGatewayServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&InferenceGatewayService_ServiceDesc, srv)
}

func _InferenceGatewayService_Predict_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PredictRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InferenceGatewayServiceServer).Predict(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: InferenceGatewayService_Predict_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InferenceGatewayServiceServer).Predict(ctx, req.(*PredictRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _InferenceGatewayService_BatchPredict_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BatchPredictRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InferenceGatewayServiceServer).BatchPredict(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: InferenceGatewayService_BatchPredict_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InferenceGatewayServiceServer).BatchPredict(ctx, req.(*BatchPredictRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _InferenceGatewayService_StreamPredict_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(StreamPredictRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(InferenceGatewayServiceServer).StreamPredict(m, &grpc.GenericServerStream[StreamPredictRequest, StreamPredictResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type InferenceGatewayService_StreamPredictServer = grpc.ServerStreamingServer[StreamPredictResponse]

func _InferenceGatewayService_GetRoute_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRouteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InferenceGatewayServiceServer).GetRoute(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: InferenceGatewayService_GetRoute_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InferenceGatewayServiceServer).GetRoute(ctx, req.(*GetRouteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _InferenceGatewayService_ListRoutes_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListRoutesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InferenceGatewayServiceServer).ListRoutes(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: InferenceGatewayService_ListRoutes_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InferenceGatewayServiceServer).ListRoutes(ctx, req.(*ListRoutesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _InferenceGatewayService_UpsertRoute_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpsertRouteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InferenceGatewayServiceServer).UpsertRoute(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: InferenceGatewayService_UpsertRoute_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InferenceGatewayServiceServer).UpsertRoute(ctx, req.(*UpsertRouteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _InferenceGatewayService_SetTrafficSplit_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SetTrafficSplitRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InferenceGatewayServiceServer).SetTrafficSplit(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: InferenceGatewayService_SetTrafficSplit_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InferenceGatewayServiceServer).SetTrafficSplit(ctx, req.(*SetTrafficSplitRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _InferenceGatewayService_DeleteRoute_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteRouteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InferenceGatewayServiceServer).DeleteRoute(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: InferenceGatewayService_DeleteRoute_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InferenceGatewayServiceServer).DeleteRoute(ctx, req.(*DeleteRouteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _InferenceGatewayService_GetCircuitState_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetCircuitStateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InferenceGatewayServiceServer).GetCircuitState(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: InferenceGatewayService_GetCircuitState_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InferenceGatewayServiceServer).GetCircuitState(ctx, req.(*GetCircuitStateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _InferenceGatewayService_ListCircuitStates_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListCircuitStatesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InferenceGatewayServiceServer).ListCircuitStates(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: InferenceGatewayService_ListCircuitStates_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InferenceGatewayServiceServer).ListCircuitStates(ctx, req.(*ListCircuitStatesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// InferenceGatewayService_ServiceDesc is the grpc.ServiceDesc for InferenceGatewayService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var InferenceGatewayService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "forgepoint.inference.v1.InferenceGatewayService",
	HandlerType: (*InferenceGatewayServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Predict",
			Handler:    _InferenceGatewayService_Predict_Handler,
		},
		{
			MethodName: "BatchPredict",
			Handler:    _InferenceGatewayService_BatchPredict_Handler,
		},
		{
			MethodName: "GetRoute",
			Handler:    _InferenceGatewayService_GetRoute_Handler,
		},
		{
			MethodName: "ListRoutes",
			Handler:    _InferenceGatewayService_ListRoutes_Handler,
		},
		{
			MethodName: "UpsertRoute",
			Handler:    _InferenceGatewayService_UpsertRoute_Handler,
		},
		{
			MethodName: "SetTrafficSplit",
			Handler:    _InferenceGatewayService_SetTrafficSplit_Handler,
		},
		{
			MethodName: "DeleteRoute",
			Handler:    _InferenceGatewayService_DeleteRoute_Handler,
		},
		{
			MethodName: "GetCircuitState",
			Handler:    _InferenceGatewayService_GetCircuitState_Handler,
		},
		{
			MethodName: "ListCircuitStates",
			Handler:    _InferenceGatewayService_ListCircuitStates_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamPredict",
			Handler:       _InferenceGatewayService_StreamPredict_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "forgepoint/inference/v1/inference.proto",
}
