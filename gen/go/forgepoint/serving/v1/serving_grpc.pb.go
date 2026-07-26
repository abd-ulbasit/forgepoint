// ============================================================================
// Forgepoint Model Serving Service Proto Definitions
// ============================================================================
//
// WHY: The Model Serving service is the "muscle" of the platform — it loads a
// trained ML model (ONNX, CPU-only, tiny) into memory and answers prediction
// requests. Everything upstream (registry, pipelines, gateway) exists so that a
// model artifact lands here and gets served with low latency.
//
// WHAT'S HERE:
//   - Tensor primitives: TensorData, DataType (the wire format for ML I/O)
//   - Domain messages: ModelInfo, TensorSpec, ServingMetrics, ModelState
//   - RPCs: Predict, StreamPredict (data plane) + LoadModel, UnloadModel,
//     GetModelStatus, GetModelInfo, ListLoadedModels, GetServingMetrics,
//     HealthCheck (control plane)
//   - NO event-payload messages. Serving is a PURE EVENT CONSUMER (see the
//     EVENTING block below) — it publishes nothing on the bus.
//
// EVENTING ROLE — PURE CONSUMER, ZERO PRODUCED EVENTS (read this):
//   An earlier draft of this proto defined ModelLoadedEvent,
//   PredictionCompletedEvent, and ModelUnloadedEvent. ALL THREE ARE DELETED.
//   The platform's canonical event contract (proto/forgepoint/events/v1/
//   events.proto) makes serving a pure consumer:
//     - The ONE inference/metering event is the gateway's
//       events.InferenceCompleted (fp.inference.completed) — the gateway alone
//       knows latency, the served version post traffic-split, the billed
//       principal, and canary status. A serving pod must NOT emit a competing
//       metering event (it would double-count and disagree on the served
//       version). So PredictionCompletedEvent is gone (conflict #1).
//     - The AUTHORITATIVE deploy lifecycle is the orchestrator saga's
//       events.ModelDeployed / events.ModelUndeployed (fp.pipelines.model.*),
//       NOT a serving-pod ModelLoaded/ModelUnloaded side note. A pod loading a
//       model is an IMPLEMENTATION DETAIL of a deploy the saga owns; emitting a
//       second "loaded" fact would race the saga's authoritative one and create
//       two sources of truth for "is version X live?" (conflict #2). So
//       ModelLoadedEvent / ModelUnloadedEvent are gone.
//   WHAT SERVING CONSUMES (all defined in events.proto, unmarshalled in Go at
//   the consume boundary — this proto declares none of them, so it does NOT
//   import events.proto; redeclaring or importing-without-use would violate the
//   decoupling rule and Buf's import hygiene):
//     - fp.models.version.ready    (events.ModelVersionReady)  → a version is
//          now loadable; pre-warm or LoadModel it.
//     - fp.pipelines.model.deployed (events.ModelDeployed)     → ensure this
//          version is LOADED (the controller reconciles the pod to READY).
//     - fp.pipelines.model.undeployed (events.ModelUndeployed) → UNLOAD the
//          version to free memory.
//     - fp.models.promoted         (events.ModelPromoted)      → the prod
//          pointer moved; (re)load/route to the new prod weights.
//     - fp.models.archived         (events.ModelArchived)      → tear down the
//          running instance(s) of the archived model.
//   These reactions are realized by the LoadModel / UnloadModel control-plane
//   RPCs below, driven by the per-version-Deployment controller that owns the
//   subscription. The data-plane Predict path emits nothing.
//
// PATTERN — Sidecar + Horizontal Autoscaling (HPA on custom metrics):
//
//   SIDECAR shape of THIS service: each model version runs as its OWN
//   Deployment (one Deployment per model version in the fp-models namespace).
//   The pod is "the model" — a thin gRPC server wrapping the ONNX runtime with
//   the model weights loaded in memory. The Inference Gateway is the only thing
//   allowed to call it (NetworkPolicy). This is the sidecar/per-pod-pattern
//   philosophy: keep the serving runtime dumb, single-purpose, and replaceable;
//   push routing, auth, rate-limiting, and circuit-breaking UP into the gateway.
//
//   HOW THE API SHAPE REALIZES THE PATTERN:
//     1. Predict is deliberately MINIMAL and STATELESS — no auth fields, no
//        billing fields, no routing fields. Those are the gateway's job. A
//        serving pod just does math. This keeps the pod horizontally scalable:
//        any replica can answer any request (no sticky state).
//     2. GetServingMetrics exposes inflight_requests and latency so a custom
//        metrics adapter (Prometheus Adapter / KEDA) can drive the HPA. The
//        scaling signal is part of the CONTRACT, not an implementation detail.
//     3. HealthCheck distinguishes liveness (process alive) from readiness
//        (model actually loaded into memory) — the two K8s probes a per-model
//        Deployment needs. Readiness = model loaded; liveness = recent
//        inference latency under threshold.
//
//   WHY NOT one big multi-model server (à la NVIDIA Triton / TF Serving)?
//     - A multi-model server is more resource-efficient but couples model
//       lifecycles: redeploying model A risks model B; one model's memory leak
//       OOM-kills the shared pod; you can't scale model A independently of B.
//     - One-Deployment-per-version gives independent scaling, independent
//       rollout/rollback, blast-radius isolation, and per-model HPA — which is
//       exactly what the gateway's traffic-splitting (canary) needs.
//     - Tradeoff: more pods, higher baseline cost, slower cold start. Here
//       isolation is worth more than bin-packing efficiency: sharing a pod
//       makes every rollout a multi-model risk, which is the failure this
//       design exists to avoid. KEDA scale-to-zero (M6) recovers idle cost.
//
//   HOW THE HPA KNOWS TO SCALE: the pod
//   publishes inflight_requests via GetServingMetrics + a /metrics Prometheus
//   endpoint; Prometheus Adapter surfaces it as a custom metric; HPA targets an
//   average inflight per pod. CPU alone is a poor signal for inference (latency
//   is dominated by request queuing, not raw CPU), so a custom queue-depth
//   metric is the correct scaling dimension. This is the whole point of the
//   "HPA on custom metrics" pattern.
//
// REAL-WORLD COMPARISON:
//   - KServe / Seldon Core: one InferenceService per model, scales per model
//     (same isolation philosophy as ours).
//   - NVIDIA Triton / TF Serving: multi-model single server (the alternative
//     we rejected for isolation reasons).
//   - SageMaker Endpoints: managed per-endpoint scaling — conceptually our
//     "one Deployment per version".
//
// CALL FLOW (data plane):
//   External Client ──HTTP──► Inference Gateway
//        (gateway does: auth, rate-limit, circuit-breaker, traffic-split)
//                          └──gRPC──► ModelServing.Predict()  [this service]
//                                        └─ ONNX runtime in-memory inference
//        ◄──PredictResponse── (returns to the gateway; the POD emits NO event)
//                          └──the GATEWAY publishes──► fp.inference.completed
//                                          (events.InferenceCompleted —
//                                           billing + monitor consume it).
//   The serving pod is intentionally outside the event path: it does math and
//   returns; the gateway, which owns latency/served-version/principal/canary,
//   is the single producer of the inference fact. See the EVENTING block above.
//
// VERSIONING: Package path includes v1 (Buf/Google convention). Breaking
// changes require a new forgepoint.serving.v2 package.
// ============================================================================

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.6.2
// - protoc             (unknown)
// source: forgepoint/serving/v1/serving.proto

package servingv1

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
	ModelServingService_Predict_FullMethodName           = "/forgepoint.serving.v1.ModelServingService/Predict"
	ModelServingService_StreamPredict_FullMethodName     = "/forgepoint.serving.v1.ModelServingService/StreamPredict"
	ModelServingService_GetModelInfo_FullMethodName      = "/forgepoint.serving.v1.ModelServingService/GetModelInfo"
	ModelServingService_ListLoadedModels_FullMethodName  = "/forgepoint.serving.v1.ModelServingService/ListLoadedModels"
	ModelServingService_LoadModel_FullMethodName         = "/forgepoint.serving.v1.ModelServingService/LoadModel"
	ModelServingService_UnloadModel_FullMethodName       = "/forgepoint.serving.v1.ModelServingService/UnloadModel"
	ModelServingService_GetModelStatus_FullMethodName    = "/forgepoint.serving.v1.ModelServingService/GetModelStatus"
	ModelServingService_GetServingMetrics_FullMethodName = "/forgepoint.serving.v1.ModelServingService/GetServingMetrics"
	ModelServingService_HealthCheck_FullMethodName       = "/forgepoint.serving.v1.ModelServingService/HealthCheck"
)

// ModelServingServiceClient is the client API for ModelServingService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// ============================================================================
// NO EVENT-PAYLOAD MESSAGES HERE — BY DESIGN
// ============================================================================
//
// A serving pod publishes NOTHING on the NATS bus. The three former event
// messages (ModelLoadedEvent, PredictionCompletedEvent, ModelUnloadedEvent)
// were DELETED when the platform adopted the canonical event contract
// (proto/forgepoint/events/v1/events.proto). See the EVENTING block at the top
// of this file for the full rationale. In short:
//   - The single inference/metering event is the gateway's
//     events.InferenceCompleted (the gateway owns latency/served-version/
//     principal/canary) — a serving pod must not emit a competing one.
//   - The authoritative deploy lifecycle is the orchestrator saga's
//     events.ModelDeployed / events.ModelUndeployed — a serving pod loading or
//     unloading a model is an implementation detail of that saga, not a second
//     source of truth.
//
// Serving CONSUMES events.{ModelVersionReady, ModelDeployed, ModelUndeployed,
// ModelPromoted, ModelArchived} (unmarshalled in Go at the consume boundary by
// the per-version-Deployment controller) and reacts via the LoadModel /
// UnloadModel control-plane RPCs below. Because this proto declares and
// references no event type, it does NOT import events.proto — importing without
// use would violate the decoupling rule and Buf's import hygiene.
//
// ============================================================================
// MODEL SERVING SERVICE
// ============================================================================
//
// WHY one ModelServingService split into a data plane and a control plane:
//
//   - DATA PLANE (Predict, StreamPredict): the hot path, latency-critical,
//     called only by the Inference Gateway, scaled by the HPA. Minimal,
//     stateless, auth-agnostic — see the Sidecar rationale at the top.
//
//   - CONTROL PLANE (LoadModel, UnloadModel, GetModelStatus, GetModelInfo,
//     GetServingMetrics, HealthCheck): low-frequency lifecycle/observability
//     calls made by the operator/controller, probes, and metric adapters.
//
//     They share one service (not two) because they target the SAME pod and the
//     same in-memory model; a separate AdminService would just add a second
//     listener for no isolation benefit on a single-model pod. Authorization is
//     enforced by the interceptor chain (the gateway's identity may Predict;
//     only the operator's identity may LoadModel/UnloadModel).
//
// RPC CATEGORIES:
//
//	DATA PLANE:    Predict, StreamPredict
//	INTROSPECTION: GetModelInfo, ListLoadedModels
//	LIFECYCLE:     LoadModel, UnloadModel, GetModelStatus
//	AUTOSCALING:   GetServingMetrics
//	PROBES:        HealthCheck
//
// NONE of these RPCs publish an event — the data plane returns inline and the
// control plane mutates pod-local state; the platform's record of what served
// (events.InferenceCompleted) and what is deployed (events.ModelDeployed/
// Undeployed) is produced by the gateway and orchestrator respectively.
//
// ASCII — where each RPC sits:
//
//	┌───────────────── Inference Gateway ─────────────────┐
//	│  Predict / StreamPredict  ──────►  (data plane, HPA-scaled) │
//	│  GetServingMetrics  ──────►  least-inflight routing         │
//	└────────────────────────────────────────────────────┘
//	┌──────────── Operator / Controller ─────────────────┐
//	│  LoadModel / UnloadModel / GetModelStatus / ListLoadedModels ►│
//	│  (driven by consumed ModelDeployed/Undeployed/Promoted/...)  │
//	└────────────────────────────────────────────────────┘
//	┌──────────────────── Kubelet ───────────────────────┐
//	│  HealthCheck (readiness=READY, liveness=latency<thr)────────►│
//	└────────────────────────────────────────────────────┘
//
// ============================================================================
type ModelServingServiceClient interface {
	// Predict runs a single synchronous inference. The hot path; called by the
	// Inference Gateway after it has authed, rate-limited, and chosen a version.
	// NO side effect on the bus: the pod returns the result and the GATEWAY
	// publishes events.InferenceCompleted (billing + monitor consume that single
	// canonical event). Idempotent on idempotency_key purely to make gateway
	// retries cheap (result cache, not a billing concern).
	Predict(ctx context.Context, in *PredictRequest, opts ...grpc.CallOption) (*PredictResponse, error)
	// StreamPredict is a bidirectional stream for high-throughput batch scoring
	// or long-lived online feeds. Opt-in (unary Predict is the primary path).
	// Each request message carries its own idempotency_key; responses are not
	// 1:1-ordered with requests, so clients pair them via that key.
	StreamPredict(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[StreamPredictRequest, StreamPredictResponse], error)
	// GetModelInfo returns the served model's identity + I/O schema so clients
	// can build valid Predict requests. All fields server-authoritative.
	GetModelInfo(ctx context.Context, in *GetModelInfoRequest, opts ...grpc.CallOption) (*GetModelInfoResponse, error)
	// ListLoadedModels returns the live status of every model resident on the pod
	// (0..1 today; forward-compatible with multi-model). Paginated (common.v1;
	// page_size server-clamped to [1,100]). The reconcile loop uses it to compare
	// desired (consumed ModelDeployed events) vs actual (resident) models.
	ListLoadedModels(ctx context.Context, in *ListLoadedModelsRequest, opts ...grpc.CallOption) (*ListLoadedModelsResponse, error)
	// LoadModel pulls an artifact from object storage and loads it into the ONNX
	// runtime. Admin/controller-only — typically driven by a consumed
	// events.ModelDeployed / events.ModelVersionReady. Load is async — poll
	// GetModelStatus for READY. Idempotent: re-loading an already-READY identical
	// (name,version,digest) is a no-op returning current status. SSRF-guarded:
	// artifact_uri is validated against the pod's allow-listed bucket/prefix.
	LoadModel(ctx context.Context, in *LoadModelRequest, opts ...grpc.CallOption) (*LoadModelResponse, error)
	// UnloadModel frees the model from memory. Admin/controller-only — typically
	// driven by a consumed events.ModelUndeployed / events.ModelArchived. After
	// unload Predict returns FAILED_PRECONDITION. Publishes NO event (the
	// authoritative teardown fact is the orchestrator's events.ModelUndeployed).
	UnloadModel(ctx context.Context, in *UnloadModelRequest, opts ...grpc.CallOption) (*UnloadModelResponse, error)
	// GetModelStatus returns the live lifecycle state (DOWNLOADING/LOADING/READY/
	// FAILED/UNLOADED). Used by the operator's reconcile loop and admin tooling.
	GetModelStatus(ctx context.Context, in *GetModelStatusRequest, opts ...grpc.CallOption) (*GetModelStatusResponse, error)
	// GetServingMetrics returns the inflight/latency snapshot that drives the HPA
	// (custom-metrics autoscaling) and the gateway's least-inflight routing.
	GetServingMetrics(ctx context.Context, in *GetServingMetricsRequest, opts ...grpc.CallOption) (*GetServingMetricsResponse, error)
	// HealthCheck reports serving readiness (model loaded) + the latency signal
	// behind liveness. Wired to K8s readiness/liveness probes for this per-model
	// Deployment.
	HealthCheck(ctx context.Context, in *HealthCheckRequest, opts ...grpc.CallOption) (*HealthCheckResponse, error)
}

type modelServingServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewModelServingServiceClient(cc grpc.ClientConnInterface) ModelServingServiceClient {
	return &modelServingServiceClient{cc}
}

func (c *modelServingServiceClient) Predict(ctx context.Context, in *PredictRequest, opts ...grpc.CallOption) (*PredictResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(PredictResponse)
	err := c.cc.Invoke(ctx, ModelServingService_Predict_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *modelServingServiceClient) StreamPredict(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[StreamPredictRequest, StreamPredictResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &ModelServingService_ServiceDesc.Streams[0], ModelServingService_StreamPredict_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[StreamPredictRequest, StreamPredictResponse]{ClientStream: stream}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type ModelServingService_StreamPredictClient = grpc.BidiStreamingClient[StreamPredictRequest, StreamPredictResponse]

func (c *modelServingServiceClient) GetModelInfo(ctx context.Context, in *GetModelInfoRequest, opts ...grpc.CallOption) (*GetModelInfoResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetModelInfoResponse)
	err := c.cc.Invoke(ctx, ModelServingService_GetModelInfo_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *modelServingServiceClient) ListLoadedModels(ctx context.Context, in *ListLoadedModelsRequest, opts ...grpc.CallOption) (*ListLoadedModelsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ListLoadedModelsResponse)
	err := c.cc.Invoke(ctx, ModelServingService_ListLoadedModels_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *modelServingServiceClient) LoadModel(ctx context.Context, in *LoadModelRequest, opts ...grpc.CallOption) (*LoadModelResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(LoadModelResponse)
	err := c.cc.Invoke(ctx, ModelServingService_LoadModel_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *modelServingServiceClient) UnloadModel(ctx context.Context, in *UnloadModelRequest, opts ...grpc.CallOption) (*UnloadModelResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(UnloadModelResponse)
	err := c.cc.Invoke(ctx, ModelServingService_UnloadModel_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *modelServingServiceClient) GetModelStatus(ctx context.Context, in *GetModelStatusRequest, opts ...grpc.CallOption) (*GetModelStatusResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetModelStatusResponse)
	err := c.cc.Invoke(ctx, ModelServingService_GetModelStatus_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *modelServingServiceClient) GetServingMetrics(ctx context.Context, in *GetServingMetricsRequest, opts ...grpc.CallOption) (*GetServingMetricsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetServingMetricsResponse)
	err := c.cc.Invoke(ctx, ModelServingService_GetServingMetrics_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *modelServingServiceClient) HealthCheck(ctx context.Context, in *HealthCheckRequest, opts ...grpc.CallOption) (*HealthCheckResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(HealthCheckResponse)
	err := c.cc.Invoke(ctx, ModelServingService_HealthCheck_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ModelServingServiceServer is the server API for ModelServingService service.
// All implementations must embed UnimplementedModelServingServiceServer
// for forward compatibility.
//
// ============================================================================
// NO EVENT-PAYLOAD MESSAGES HERE — BY DESIGN
// ============================================================================
//
// A serving pod publishes NOTHING on the NATS bus. The three former event
// messages (ModelLoadedEvent, PredictionCompletedEvent, ModelUnloadedEvent)
// were DELETED when the platform adopted the canonical event contract
// (proto/forgepoint/events/v1/events.proto). See the EVENTING block at the top
// of this file for the full rationale. In short:
//   - The single inference/metering event is the gateway's
//     events.InferenceCompleted (the gateway owns latency/served-version/
//     principal/canary) — a serving pod must not emit a competing one.
//   - The authoritative deploy lifecycle is the orchestrator saga's
//     events.ModelDeployed / events.ModelUndeployed — a serving pod loading or
//     unloading a model is an implementation detail of that saga, not a second
//     source of truth.
//
// Serving CONSUMES events.{ModelVersionReady, ModelDeployed, ModelUndeployed,
// ModelPromoted, ModelArchived} (unmarshalled in Go at the consume boundary by
// the per-version-Deployment controller) and reacts via the LoadModel /
// UnloadModel control-plane RPCs below. Because this proto declares and
// references no event type, it does NOT import events.proto — importing without
// use would violate the decoupling rule and Buf's import hygiene.
//
// ============================================================================
// MODEL SERVING SERVICE
// ============================================================================
//
// WHY one ModelServingService split into a data plane and a control plane:
//
//   - DATA PLANE (Predict, StreamPredict): the hot path, latency-critical,
//     called only by the Inference Gateway, scaled by the HPA. Minimal,
//     stateless, auth-agnostic — see the Sidecar rationale at the top.
//
//   - CONTROL PLANE (LoadModel, UnloadModel, GetModelStatus, GetModelInfo,
//     GetServingMetrics, HealthCheck): low-frequency lifecycle/observability
//     calls made by the operator/controller, probes, and metric adapters.
//
//     They share one service (not two) because they target the SAME pod and the
//     same in-memory model; a separate AdminService would just add a second
//     listener for no isolation benefit on a single-model pod. Authorization is
//     enforced by the interceptor chain (the gateway's identity may Predict;
//     only the operator's identity may LoadModel/UnloadModel).
//
// RPC CATEGORIES:
//
//	DATA PLANE:    Predict, StreamPredict
//	INTROSPECTION: GetModelInfo, ListLoadedModels
//	LIFECYCLE:     LoadModel, UnloadModel, GetModelStatus
//	AUTOSCALING:   GetServingMetrics
//	PROBES:        HealthCheck
//
// NONE of these RPCs publish an event — the data plane returns inline and the
// control plane mutates pod-local state; the platform's record of what served
// (events.InferenceCompleted) and what is deployed (events.ModelDeployed/
// Undeployed) is produced by the gateway and orchestrator respectively.
//
// ASCII — where each RPC sits:
//
//	┌───────────────── Inference Gateway ─────────────────┐
//	│  Predict / StreamPredict  ──────►  (data plane, HPA-scaled) │
//	│  GetServingMetrics  ──────►  least-inflight routing         │
//	└────────────────────────────────────────────────────┘
//	┌──────────── Operator / Controller ─────────────────┐
//	│  LoadModel / UnloadModel / GetModelStatus / ListLoadedModels ►│
//	│  (driven by consumed ModelDeployed/Undeployed/Promoted/...)  │
//	└────────────────────────────────────────────────────┘
//	┌──────────────────── Kubelet ───────────────────────┐
//	│  HealthCheck (readiness=READY, liveness=latency<thr)────────►│
//	└────────────────────────────────────────────────────┘
//
// ============================================================================
type ModelServingServiceServer interface {
	// Predict runs a single synchronous inference. The hot path; called by the
	// Inference Gateway after it has authed, rate-limited, and chosen a version.
	// NO side effect on the bus: the pod returns the result and the GATEWAY
	// publishes events.InferenceCompleted (billing + monitor consume that single
	// canonical event). Idempotent on idempotency_key purely to make gateway
	// retries cheap (result cache, not a billing concern).
	Predict(context.Context, *PredictRequest) (*PredictResponse, error)
	// StreamPredict is a bidirectional stream for high-throughput batch scoring
	// or long-lived online feeds. Opt-in (unary Predict is the primary path).
	// Each request message carries its own idempotency_key; responses are not
	// 1:1-ordered with requests, so clients pair them via that key.
	StreamPredict(grpc.BidiStreamingServer[StreamPredictRequest, StreamPredictResponse]) error
	// GetModelInfo returns the served model's identity + I/O schema so clients
	// can build valid Predict requests. All fields server-authoritative.
	GetModelInfo(context.Context, *GetModelInfoRequest) (*GetModelInfoResponse, error)
	// ListLoadedModels returns the live status of every model resident on the pod
	// (0..1 today; forward-compatible with multi-model). Paginated (common.v1;
	// page_size server-clamped to [1,100]). The reconcile loop uses it to compare
	// desired (consumed ModelDeployed events) vs actual (resident) models.
	ListLoadedModels(context.Context, *ListLoadedModelsRequest) (*ListLoadedModelsResponse, error)
	// LoadModel pulls an artifact from object storage and loads it into the ONNX
	// runtime. Admin/controller-only — typically driven by a consumed
	// events.ModelDeployed / events.ModelVersionReady. Load is async — poll
	// GetModelStatus for READY. Idempotent: re-loading an already-READY identical
	// (name,version,digest) is a no-op returning current status. SSRF-guarded:
	// artifact_uri is validated against the pod's allow-listed bucket/prefix.
	LoadModel(context.Context, *LoadModelRequest) (*LoadModelResponse, error)
	// UnloadModel frees the model from memory. Admin/controller-only — typically
	// driven by a consumed events.ModelUndeployed / events.ModelArchived. After
	// unload Predict returns FAILED_PRECONDITION. Publishes NO event (the
	// authoritative teardown fact is the orchestrator's events.ModelUndeployed).
	UnloadModel(context.Context, *UnloadModelRequest) (*UnloadModelResponse, error)
	// GetModelStatus returns the live lifecycle state (DOWNLOADING/LOADING/READY/
	// FAILED/UNLOADED). Used by the operator's reconcile loop and admin tooling.
	GetModelStatus(context.Context, *GetModelStatusRequest) (*GetModelStatusResponse, error)
	// GetServingMetrics returns the inflight/latency snapshot that drives the HPA
	// (custom-metrics autoscaling) and the gateway's least-inflight routing.
	GetServingMetrics(context.Context, *GetServingMetricsRequest) (*GetServingMetricsResponse, error)
	// HealthCheck reports serving readiness (model loaded) + the latency signal
	// behind liveness. Wired to K8s readiness/liveness probes for this per-model
	// Deployment.
	HealthCheck(context.Context, *HealthCheckRequest) (*HealthCheckResponse, error)
	mustEmbedUnimplementedModelServingServiceServer()
}

// UnimplementedModelServingServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedModelServingServiceServer struct{}

func (UnimplementedModelServingServiceServer) Predict(context.Context, *PredictRequest) (*PredictResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method Predict not implemented")
}
func (UnimplementedModelServingServiceServer) StreamPredict(grpc.BidiStreamingServer[StreamPredictRequest, StreamPredictResponse]) error {
	return status.Error(codes.Unimplemented, "method StreamPredict not implemented")
}
func (UnimplementedModelServingServiceServer) GetModelInfo(context.Context, *GetModelInfoRequest) (*GetModelInfoResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetModelInfo not implemented")
}
func (UnimplementedModelServingServiceServer) ListLoadedModels(context.Context, *ListLoadedModelsRequest) (*ListLoadedModelsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ListLoadedModels not implemented")
}
func (UnimplementedModelServingServiceServer) LoadModel(context.Context, *LoadModelRequest) (*LoadModelResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method LoadModel not implemented")
}
func (UnimplementedModelServingServiceServer) UnloadModel(context.Context, *UnloadModelRequest) (*UnloadModelResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method UnloadModel not implemented")
}
func (UnimplementedModelServingServiceServer) GetModelStatus(context.Context, *GetModelStatusRequest) (*GetModelStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetModelStatus not implemented")
}
func (UnimplementedModelServingServiceServer) GetServingMetrics(context.Context, *GetServingMetricsRequest) (*GetServingMetricsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetServingMetrics not implemented")
}
func (UnimplementedModelServingServiceServer) HealthCheck(context.Context, *HealthCheckRequest) (*HealthCheckResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method HealthCheck not implemented")
}
func (UnimplementedModelServingServiceServer) mustEmbedUnimplementedModelServingServiceServer() {}
func (UnimplementedModelServingServiceServer) testEmbeddedByValue()                             {}

// UnsafeModelServingServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ModelServingServiceServer will
// result in compilation errors.
type UnsafeModelServingServiceServer interface {
	mustEmbedUnimplementedModelServingServiceServer()
}

func RegisterModelServingServiceServer(s grpc.ServiceRegistrar, srv ModelServingServiceServer) {
	// If the following call panics, it indicates UnimplementedModelServingServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&ModelServingService_ServiceDesc, srv)
}

func _ModelServingService_Predict_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PredictRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ModelServingServiceServer).Predict(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ModelServingService_Predict_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ModelServingServiceServer).Predict(ctx, req.(*PredictRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ModelServingService_StreamPredict_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(ModelServingServiceServer).StreamPredict(&grpc.GenericServerStream[StreamPredictRequest, StreamPredictResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type ModelServingService_StreamPredictServer = grpc.BidiStreamingServer[StreamPredictRequest, StreamPredictResponse]

func _ModelServingService_GetModelInfo_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetModelInfoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ModelServingServiceServer).GetModelInfo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ModelServingService_GetModelInfo_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ModelServingServiceServer).GetModelInfo(ctx, req.(*GetModelInfoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ModelServingService_ListLoadedModels_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListLoadedModelsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ModelServingServiceServer).ListLoadedModels(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ModelServingService_ListLoadedModels_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ModelServingServiceServer).ListLoadedModels(ctx, req.(*ListLoadedModelsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ModelServingService_LoadModel_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LoadModelRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ModelServingServiceServer).LoadModel(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ModelServingService_LoadModel_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ModelServingServiceServer).LoadModel(ctx, req.(*LoadModelRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ModelServingService_UnloadModel_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UnloadModelRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ModelServingServiceServer).UnloadModel(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ModelServingService_UnloadModel_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ModelServingServiceServer).UnloadModel(ctx, req.(*UnloadModelRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ModelServingService_GetModelStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetModelStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ModelServingServiceServer).GetModelStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ModelServingService_GetModelStatus_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ModelServingServiceServer).GetModelStatus(ctx, req.(*GetModelStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ModelServingService_GetServingMetrics_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetServingMetricsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ModelServingServiceServer).GetServingMetrics(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ModelServingService_GetServingMetrics_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ModelServingServiceServer).GetServingMetrics(ctx, req.(*GetServingMetricsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ModelServingService_HealthCheck_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HealthCheckRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ModelServingServiceServer).HealthCheck(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ModelServingService_HealthCheck_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ModelServingServiceServer).HealthCheck(ctx, req.(*HealthCheckRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ModelServingService_ServiceDesc is the grpc.ServiceDesc for ModelServingService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ModelServingService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "forgepoint.serving.v1.ModelServingService",
	HandlerType: (*ModelServingServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Predict",
			Handler:    _ModelServingService_Predict_Handler,
		},
		{
			MethodName: "GetModelInfo",
			Handler:    _ModelServingService_GetModelInfo_Handler,
		},
		{
			MethodName: "ListLoadedModels",
			Handler:    _ModelServingService_ListLoadedModels_Handler,
		},
		{
			MethodName: "LoadModel",
			Handler:    _ModelServingService_LoadModel_Handler,
		},
		{
			MethodName: "UnloadModel",
			Handler:    _ModelServingService_UnloadModel_Handler,
		},
		{
			MethodName: "GetModelStatus",
			Handler:    _ModelServingService_GetModelStatus_Handler,
		},
		{
			MethodName: "GetServingMetrics",
			Handler:    _ModelServingService_GetServingMetrics_Handler,
		},
		{
			MethodName: "HealthCheck",
			Handler:    _ModelServingService_HealthCheck_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamPredict",
			Handler:       _ModelServingService_StreamPredict_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "forgepoint/serving/v1/serving.proto",
}
