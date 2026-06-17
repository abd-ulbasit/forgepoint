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
//          now loadable; pre-warm / be ready to LoadModel it.
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
//     - Tradeoff: more pods, higher baseline cost, slower cold start. For a
//       portfolio platform demonstrating K8s autoscaling patterns, the
//       isolation story is worth more than the bin-packing efficiency. KEDA
//       scale-to-zero (M6) recovers idle cost.
//
//   INTERVIEW NOTE: Expect "how does the HPA know to scale?" Answer: the pod
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

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.11
// 	protoc        (unknown)
// source: forgepoint/serving/v1/serving.proto

package servingv1

import (
	v1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// DataType is the element type of a tensor's raw byte buffer.
// WHY an enum (not a free string like "float32"): an enum is validated at the
// proto layer, gives generated constants, and prevents typos like "flaot32".
// Suffix/prefix conventions satisfy Buf STANDARD (ENUM_ZERO_VALUE_SUFFIX +
// ENUM_VALUE_PREFIX): zero value is *_UNSPECIFIED, every value is DATA_TYPE_*.
type DataType int32

const (
	// Unset/unknown. A request with this dtype is rejected (INVALID_ARGUMENT) —
	// forces clients to be explicit about element type.
	DataType_DATA_TYPE_UNSPECIFIED DataType = 0
	// 32-bit IEEE-754 float. The default for most CPU ONNX models (features and
	// logits). 4 bytes/element.
	DataType_DATA_TYPE_FLOAT32 DataType = 1
	// 64-bit IEEE-754 float. Some sklearn-exported models use doubles. 8 bytes.
	DataType_DATA_TYPE_FLOAT64 DataType = 2
	// 32-bit signed integer. Categorical/index inputs. 4 bytes.
	DataType_DATA_TYPE_INT32 DataType = 3
	// 64-bit signed integer. Label outputs, token IDs. 8 bytes.
	DataType_DATA_TYPE_INT64 DataType = 4
	// 8-bit boolean (0/1). Mask inputs. 1 byte/element.
	DataType_DATA_TYPE_BOOL DataType = 5
	// UTF-8 string elements. WHY length-prefixed, not fixed-width: strings are
	// variable length, so for STRING tensors `data` holds, per element, a
	// 4-byte little-endian length followed by that many UTF-8 bytes. Used by
	// NLP models that take raw text. Kept last so adding it didn't renumber.
	DataType_DATA_TYPE_STRING DataType = 6
)

// Enum value maps for DataType.
var (
	DataType_name = map[int32]string{
		0: "DATA_TYPE_UNSPECIFIED",
		1: "DATA_TYPE_FLOAT32",
		2: "DATA_TYPE_FLOAT64",
		3: "DATA_TYPE_INT32",
		4: "DATA_TYPE_INT64",
		5: "DATA_TYPE_BOOL",
		6: "DATA_TYPE_STRING",
	}
	DataType_value = map[string]int32{
		"DATA_TYPE_UNSPECIFIED": 0,
		"DATA_TYPE_FLOAT32":     1,
		"DATA_TYPE_FLOAT64":     2,
		"DATA_TYPE_INT32":       3,
		"DATA_TYPE_INT64":       4,
		"DATA_TYPE_BOOL":        5,
		"DATA_TYPE_STRING":      6,
	}
)

func (x DataType) Enum() *DataType {
	p := new(DataType)
	*p = x
	return p
}

func (x DataType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (DataType) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_serving_v1_serving_proto_enumTypes[0].Descriptor()
}

func (DataType) Type() protoreflect.EnumType {
	return &file_forgepoint_serving_v1_serving_proto_enumTypes[0]
}

func (x DataType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use DataType.Descriptor instead.
func (DataType) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{0}
}

// ============================================================================
// ModelState
// ============================================================================
//
// WHY an enum, not a bool "loaded": a model in a serving pod moves through a
// lifecycle (downloading the artifact from MinIO → loading into ONNX → ready),
// and can fail or be unloaded. A single bool can't express "currently
// downloading" vs "load failed". The gateway's readiness routing and the
// admin UI need the distinction.
//
// STATE MACHINE:
//
//	UNSPECIFIED
//	     │ LoadModel / startup
//	     ▼
//	DOWNLOADING ──(MinIO fetch ok)──► LOADING ──(ONNX init ok)──► READY
//	     │                               │                          │
//	     │ (fetch err)                   │ (init err)               │ UnloadModel
//	     ▼                               ▼                          ▼
//	   FAILED ◄───────────────────────FAILED                    UNLOADED
//
// READY is the only state where Predict succeeds; all others return
// FAILED_PRECONDITION so the gateway can route elsewhere.
// ============================================================================
type ModelState int32

const (
	// Unknown/unset.
	ModelState_MODEL_STATE_UNSPECIFIED ModelState = 0
	// Pulling the model artifact from object storage (MinIO/S3) to local disk.
	ModelState_MODEL_STATE_DOWNLOADING ModelState = 1
	// Artifact on disk; initializing the ONNX runtime session (allocating the
	// in-memory inference graph). Brief but non-zero — this is "cold start".
	ModelState_MODEL_STATE_LOADING ModelState = 2
	// Model is loaded in memory and serving predictions. Readiness probe passes.
	ModelState_MODEL_STATE_READY ModelState = 3
	// Load failed (bad artifact, OOM, unsupported op). See ModelStatus.message
	// for the reason. Pod stays unready; K8s/operator decides on restart.
	ModelState_MODEL_STATE_FAILED ModelState = 4
	// Model was deliberately unloaded (admin UnloadModel) to free memory.
	// Distinct from FAILED so operators don't alert on an intentional unload.
	ModelState_MODEL_STATE_UNLOADED ModelState = 5
)

// Enum value maps for ModelState.
var (
	ModelState_name = map[int32]string{
		0: "MODEL_STATE_UNSPECIFIED",
		1: "MODEL_STATE_DOWNLOADING",
		2: "MODEL_STATE_LOADING",
		3: "MODEL_STATE_READY",
		4: "MODEL_STATE_FAILED",
		5: "MODEL_STATE_UNLOADED",
	}
	ModelState_value = map[string]int32{
		"MODEL_STATE_UNSPECIFIED": 0,
		"MODEL_STATE_DOWNLOADING": 1,
		"MODEL_STATE_LOADING":     2,
		"MODEL_STATE_READY":       3,
		"MODEL_STATE_FAILED":      4,
		"MODEL_STATE_UNLOADED":    5,
	}
)

func (x ModelState) Enum() *ModelState {
	p := new(ModelState)
	*p = x
	return p
}

func (x ModelState) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ModelState) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_serving_v1_serving_proto_enumTypes[1].Descriptor()
}

func (ModelState) Type() protoreflect.EnumType {
	return &file_forgepoint_serving_v1_serving_proto_enumTypes[1]
}

func (x ModelState) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ModelState.Descriptor instead.
func (ModelState) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{1}
}

// HealthStatus expresses the serving-readiness verdict.
type HealthStatus int32

const (
	// Unknown/unset.
	HealthStatus_HEALTH_STATUS_UNSPECIFIED HealthStatus = 0
	// Model loaded and inference latency within threshold — accept traffic.
	HealthStatus_HEALTH_STATUS_SERVING HealthStatus = 1
	// Process alive but not ready (model still loading, or latency degraded).
	// K8s should keep the pod but route traffic away (readiness fail).
	HealthStatus_HEALTH_STATUS_NOT_SERVING HealthStatus = 2
)

// Enum value maps for HealthStatus.
var (
	HealthStatus_name = map[int32]string{
		0: "HEALTH_STATUS_UNSPECIFIED",
		1: "HEALTH_STATUS_SERVING",
		2: "HEALTH_STATUS_NOT_SERVING",
	}
	HealthStatus_value = map[string]int32{
		"HEALTH_STATUS_UNSPECIFIED": 0,
		"HEALTH_STATUS_SERVING":     1,
		"HEALTH_STATUS_NOT_SERVING": 2,
	}
)

func (x HealthStatus) Enum() *HealthStatus {
	p := new(HealthStatus)
	*p = x
	return p
}

func (x HealthStatus) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (HealthStatus) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_serving_v1_serving_proto_enumTypes[2].Descriptor()
}

func (HealthStatus) Type() protoreflect.EnumType {
	return &file_forgepoint_serving_v1_serving_proto_enumTypes[2]
}

func (x HealthStatus) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use HealthStatus.Descriptor instead.
func (HealthStatus) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{2}
}

// TensorData is a single named tensor on the wire (input feature or output).
// See the TENSOR PRIMITIVES block above for the full byte-layout contract.
type TensorData struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Dimensions, row-major. e.g. [1, 4] = batch of 1, four features.
	// A leading batch dimension is conventional but not required by this service.
	Shape []int64 `protobuf:"varint,1,rep,packed,name=shape,proto3" json:"shape,omitempty"`
	// Raw little-endian, row-major element bytes. Length MUST equal
	// product(shape) * sizeof(dtype) (or the length-prefixed encoding for STRING).
	// The server rejects mismatches with INVALID_ARGUMENT.
	Data []byte `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
	// How to interpret `data`. Must match the model's expected input dtype;
	// the server validates against the loaded model's schema.
	Dtype         DataType `protobuf:"varint,3,opt,name=dtype,proto3,enum=forgepoint.serving.v1.DataType" json:"dtype,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *TensorData) Reset() {
	*x = TensorData{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TensorData) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TensorData) ProtoMessage() {}

func (x *TensorData) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TensorData.ProtoReflect.Descriptor instead.
func (*TensorData) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{0}
}

func (x *TensorData) GetShape() []int64 {
	if x != nil {
		return x.Shape
	}
	return nil
}

func (x *TensorData) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *TensorData) GetDtype() DataType {
	if x != nil {
		return x.Dtype
	}
	return DataType_DATA_TYPE_UNSPECIFIED
}

// ============================================================================
// TensorSpec
// ============================================================================
//
// WHY: A model's input/output schema is part of its public contract. Clients
// (and the gateway) need to know "this model wants a tensor named 'input' of
// shape [-1, 4] float32" to build valid requests and validate before sending.
// This is the served-side mirror of the registry's model schema.
//
// DYNAMIC DIMENSIONS: a shape entry of -1 means "dynamic" (typically the batch
// dimension). e.g. [-1, 4] = any number of 4-feature rows. This mirrors ONNX's
// own dynamic-axis convention, so SDKs can map it 1:1.
// ============================================================================
type TensorSpec struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The tensor's name as the ONNX graph expects it (e.g. "input", "float_input").
	// Predict requests key their `inputs` map by this name.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// Expected dimensions; -1 marks a dynamic axis (usually batch). Informational
	// contract for clients; the runtime enforces the concrete shape at inference.
	Shape []int64 `protobuf:"varint,2,rep,packed,name=shape,proto3" json:"shape,omitempty"`
	// Expected element type for this tensor.
	Dtype         DataType `protobuf:"varint,3,opt,name=dtype,proto3,enum=forgepoint.serving.v1.DataType" json:"dtype,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *TensorSpec) Reset() {
	*x = TensorSpec{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TensorSpec) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TensorSpec) ProtoMessage() {}

func (x *TensorSpec) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TensorSpec.ProtoReflect.Descriptor instead.
func (*TensorSpec) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{1}
}

func (x *TensorSpec) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *TensorSpec) GetShape() []int64 {
	if x != nil {
		return x.Shape
	}
	return nil
}

func (x *TensorSpec) GetDtype() DataType {
	if x != nil {
		return x.Dtype
	}
	return DataType_DATA_TYPE_UNSPECIFIED
}

// ============================================================================
// ModelInfo
// ============================================================================
//
// WHY: The static description of the model this pod serves — its identity and
// I/O schema. Returned by GetModelInfo so clients/SDKs can introspect and
// build correct requests without out-of-band documentation.
//
// SECURITY/AUTHORITY NOTE: every field here is SERVER-authoritative. It is
// derived from the loaded artifact's metadata (set by the registry/training
// pipeline), never accepted from a client. There is no "create model" RPC that
// takes a ModelInfo — a serving pod serves exactly the model it was deployed
// with. This prevents a caller from spoofing model identity.
// ============================================================================
type ModelInfo struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Logical model name (matches the registry), e.g. "fraud-detector".
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// The exact version this pod serves, e.g. "v3" or a semver/sha. Because it's
	// one-Deployment-per-version, a given pod's version is fixed for its lifetime.
	Version string `protobuf:"bytes,2,opt,name=version,proto3" json:"version,omitempty"`
	// Input tensors the model expects. Clients key Predict.inputs by these names.
	InputSchema []*TensorSpec `protobuf:"bytes,3,rep,name=input_schema,json=inputSchema,proto3" json:"input_schema,omitempty"`
	// Output tensors the model produces. Predict.outputs is keyed by these names.
	OutputSchema []*TensorSpec `protobuf:"bytes,4,rep,name=output_schema,json=outputSchema,proto3" json:"output_schema,omitempty"`
	// When this model finished loading into memory on this pod (READY transition).
	// Server-set; useful for cold-start observability and cache-warmth checks.
	LoadedAt *timestamppb.Timestamp `protobuf:"bytes,5,opt,name=loaded_at,json=loadedAt,proto3" json:"loaded_at,omitempty"`
	// Opaque digest (e.g. sha256) of the artifact bytes pulled from MinIO. Lets
	// the gateway/registry confirm the pod is serving the exact artifact it
	// expects (supply-chain integrity), without exposing the artifact itself.
	ArtifactDigest string `protobuf:"bytes,6,opt,name=artifact_digest,json=artifactDigest,proto3" json:"artifact_digest,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *ModelInfo) Reset() {
	*x = ModelInfo{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ModelInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ModelInfo) ProtoMessage() {}

func (x *ModelInfo) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ModelInfo.ProtoReflect.Descriptor instead.
func (*ModelInfo) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{2}
}

func (x *ModelInfo) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *ModelInfo) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *ModelInfo) GetInputSchema() []*TensorSpec {
	if x != nil {
		return x.InputSchema
	}
	return nil
}

func (x *ModelInfo) GetOutputSchema() []*TensorSpec {
	if x != nil {
		return x.OutputSchema
	}
	return nil
}

func (x *ModelInfo) GetLoadedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.LoadedAt
	}
	return nil
}

func (x *ModelInfo) GetArtifactDigest() string {
	if x != nil {
		return x.ArtifactDigest
	}
	return ""
}

// ============================================================================
// ServingMetrics
// ============================================================================
//
// WHY this is a first-class RPC response and not just /metrics scraping:
//
//	The HPA-driving signal (inflight_requests) is part of the SERVICE CONTRACT
//	in this pattern. Exposing it via gRPC lets the gateway make fast
//	load-aware routing decisions (least-inflight) AND lets a metrics adapter
//	read a stable, documented shape. The Prometheus /metrics endpoint is the
//	transport for the HPA; this message is the canonical schema.
//
// INTERVIEW NOTE: "Why inflight requests and not CPU for autoscaling?"
//
//	Inference latency is dominated by request queuing once the CPU is busy;
//	inflight (concurrency/queue depth) crosses the danger threshold BEFORE CPU
//	saturates, giving the HPA earlier, more stable scaling signals. This is the
//	crux of "HPA on custom metrics".
//
// ============================================================================
type ServingMetrics struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Requests currently being processed (the primary custom HPA metric). HPA
	// targets an average inflight-per-pod; when actual exceeds target, scale out.
	InflightRequests int32 `protobuf:"varint,1,opt,name=inflight_requests,json=inflightRequests,proto3" json:"inflight_requests,omitempty"`
	// Total predictions served since process start. Monotonic counter; used to
	// derive predictions/sec (RED "Rate") at the scrape layer.
	TotalRequests int64 `protobuf:"varint,2,opt,name=total_requests,json=totalRequests,proto3" json:"total_requests,omitempty"`
	// Total failed predictions since start (RED "Errors"). Combined with
	// total_requests gives an error rate for alerting.
	FailedRequests int64 `protobuf:"varint,3,opt,name=failed_requests,json=failedRequests,proto3" json:"failed_requests,omitempty"`
	// Rolling p50 inference latency. WHY Duration not ms-int: Duration is the
	// proto well-known type for elapsed time (nanosecond precision, self-
	// describing) and matches our other time fields' style.
	P50Latency *durationpb.Duration `protobuf:"bytes,4,opt,name=p50_latency,json=p50Latency,proto3" json:"p50_latency,omitempty"`
	// Rolling p99 inference latency (RED "Duration", tail). Also the liveness
	// signal: if p99 exceeds a configured threshold the liveness probe fails and
	// K8s restarts the pod.
	P99Latency *durationpb.Duration `protobuf:"bytes,5,opt,name=p99_latency,json=p99Latency,proto3" json:"p99_latency,omitempty"`
	// Resident model memory estimate in bytes. Capacity-planning signal for the
	// operator (how many model versions fit per node). Server-computed.
	ModelMemoryBytes int64 `protobuf:"varint,6,opt,name=model_memory_bytes,json=modelMemoryBytes,proto3" json:"model_memory_bytes,omitempty"`
	unknownFields    protoimpl.UnknownFields
	sizeCache        protoimpl.SizeCache
}

func (x *ServingMetrics) Reset() {
	*x = ServingMetrics{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ServingMetrics) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ServingMetrics) ProtoMessage() {}

func (x *ServingMetrics) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ServingMetrics.ProtoReflect.Descriptor instead.
func (*ServingMetrics) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{3}
}

func (x *ServingMetrics) GetInflightRequests() int32 {
	if x != nil {
		return x.InflightRequests
	}
	return 0
}

func (x *ServingMetrics) GetTotalRequests() int64 {
	if x != nil {
		return x.TotalRequests
	}
	return 0
}

func (x *ServingMetrics) GetFailedRequests() int64 {
	if x != nil {
		return x.FailedRequests
	}
	return 0
}

func (x *ServingMetrics) GetP50Latency() *durationpb.Duration {
	if x != nil {
		return x.P50Latency
	}
	return nil
}

func (x *ServingMetrics) GetP99Latency() *durationpb.Duration {
	if x != nil {
		return x.P99Latency
	}
	return nil
}

func (x *ServingMetrics) GetModelMemoryBytes() int64 {
	if x != nil {
		return x.ModelMemoryBytes
	}
	return 0
}

// PredictRequest carries the input tensors for one inference call.
//
// WHY model_name + version here even though it's one-model-per-pod:
//
//	Defense in depth. The gateway routes to the right pod, but the pod
//	re-validates that the request targets the model it actually serves —
//	if they mismatch it returns FAILED_PRECONDITION. This catches gateway
//	routing bugs and stale connections instead of silently serving the wrong
//	model. version may be empty to mean "whatever this pod serves".
//
// SECURITY (mass-assignment + spoofing): the request body carries NO
// auth/owner/principal/billing/api-key fields, and there is nothing here a
// caller could set to attribute, charge, or authorize the call. Identity is
// established by the gateway's auth interceptor and propagated as gRPC context
// metadata, NEVER trusted from the request body — the serving pod is
// intentionally auth-agnostic (see the Sidecar rationale up top). Because the
// pod emits NO event, there is also no billed-principal field to spoof here
// (metering happens entirely off the gateway's events.InferenceCompleted).
//
// INPUT-SIZE BOUND (DoS guard, in the contract not just prose): a Predict call
// may carry AT MOST 64 named input tensors (map entries), and the server
// rejects any single TensorData whose `data` exceeds the configured max tensor
// bytes (default 16 MiB) with INVALID_ARGUMENT. These caps are server-enforced
// (a map cardinality cap cannot be expressed in proto3), preventing a hostile
// or buggy client from OOM-ing a pod with a giant/fan-out request.
type PredictRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Target model logical name. Validated against the pod's loaded model.
	ModelName string `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// Target version. Empty = "this pod's version". If set and it doesn't match,
	// the pod rejects rather than mis-serving.
	Version string `protobuf:"bytes,2,opt,name=version,proto3" json:"version,omitempty"`
	// Named input tensors, keyed by TensorSpec.name from the model's input_schema.
	// WHY a map (not repeated): models name their inputs; a map makes the binding
	// explicit and order-independent, matching ONNX's named-input API exactly.
	// SERVER-CAPPED at 64 entries (see INPUT-SIZE BOUND above).
	Inputs map[string]*TensorData `protobuf:"bytes,3,rep,name=inputs,proto3" json:"inputs,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	// Client-supplied idempotency key. WHY it still matters even though Predict
	// emits NO event now:
	//
	//	Predict is effectively pure (in→out math), but a NETWORK RETRY after a
	//	successful inference whose response was lost still pays the full compute
	//	cost again. The pod keeps a short-lived result cache keyed by this id so a
	//	retry returns the prior result without re-running the ONNX session — pure
	//	latency/compute savings, not a correctness/billing concern (billing is the
	//	gateway's, off events.InferenceCompleted). Optional: empty = recompute
	//	every time. Recommended for any client that retries.
	IdempotencyKey string `protobuf:"bytes,4,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	// Optional correlation id propagated from the gateway for distributed tracing.
	// Echoed back in PredictResponse so the gateway can stitch this pod's span to
	// the request it will later report on events.InferenceCompleted. Purely a
	// trace/correlation handle — carries no authority.
	CorrelationId string `protobuf:"bytes,5,opt,name=correlation_id,json=correlationId,proto3" json:"correlation_id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *PredictRequest) Reset() {
	*x = PredictRequest{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PredictRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PredictRequest) ProtoMessage() {}

func (x *PredictRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PredictRequest.ProtoReflect.Descriptor instead.
func (*PredictRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{4}
}

func (x *PredictRequest) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *PredictRequest) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *PredictRequest) GetInputs() map[string]*TensorData {
	if x != nil {
		return x.Inputs
	}
	return nil
}

func (x *PredictRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

func (x *PredictRequest) GetCorrelationId() string {
	if x != nil {
		return x.CorrelationId
	}
	return ""
}

// PredictResponse returns the model's output tensors plus serving metadata.
type PredictResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Named output tensors, keyed by TensorSpec.name from output_schema.
	Outputs map[string]*TensorData `protobuf:"bytes,1,rep,name=outputs,proto3" json:"outputs,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	// The model version that actually produced this result. Echoed so the caller
	// (and the gateway, for canary attribution) records which version answered —
	// critical when traffic is split across versions.
	ModelVersion string `protobuf:"bytes,2,opt,name=model_version,json=modelVersion,proto3" json:"model_version,omitempty"`
	// Server-measured inference wall time for THIS call (not including network).
	// Lets the gateway record per-call latency and the client display it.
	InferenceLatency *durationpb.Duration `protobuf:"bytes,3,opt,name=inference_latency,json=inferenceLatency,proto3" json:"inference_latency,omitempty"`
	// Echo of the request's correlation_id for distributed tracing. The gateway
	// uses it to link this pod's inference span to the request it will report on
	// events.InferenceCompleted.
	CorrelationId string `protobuf:"bytes,4,opt,name=correlation_id,json=correlationId,proto3" json:"correlation_id,omitempty"`
	// True if this response was served from the pod's result cache (an idempotent
	// retry) rather than freshly computed. Purely an observability/latency signal
	// for the caller (no event is emitted either way — metering is the gateway's).
	FromCache     bool `protobuf:"varint,5,opt,name=from_cache,json=fromCache,proto3" json:"from_cache,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *PredictResponse) Reset() {
	*x = PredictResponse{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PredictResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PredictResponse) ProtoMessage() {}

func (x *PredictResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PredictResponse.ProtoReflect.Descriptor instead.
func (*PredictResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{5}
}

func (x *PredictResponse) GetOutputs() map[string]*TensorData {
	if x != nil {
		return x.Outputs
	}
	return nil
}

func (x *PredictResponse) GetModelVersion() string {
	if x != nil {
		return x.ModelVersion
	}
	return ""
}

func (x *PredictResponse) GetInferenceLatency() *durationpb.Duration {
	if x != nil {
		return x.InferenceLatency
	}
	return nil
}

func (x *PredictResponse) GetCorrelationId() string {
	if x != nil {
		return x.CorrelationId
	}
	return ""
}

func (x *PredictResponse) GetFromCache() bool {
	if x != nil {
		return x.FromCache
	}
	return false
}

// ============================================================================
// StreamPredict (optional streaming data plane)
// ============================================================================
//
// WHY a bidirectional stream in addition to unary Predict:
//
//	Two real use cases:
//	  1. HIGH-THROUGHPUT batch scoring: a client pushes a long sequence of rows
//	     and reads results as they complete, amortizing TCP/gRPC handshake and
//	     HTTP/2 framing over many predictions instead of one RPC per row.
//	  2. ONLINE/low-latency feeds: a long-lived connection (e.g. a feature
//	     stream) sends inputs continuously and receives predictions, avoiding
//	     per-request connection setup.
//
// WHY BIDIRECTIONAL (stream→stream) not server-streaming:
//
//	The client decides when to send the next input (it may be reacting to prior
//	outputs or rate-limiting itself), so inputs must be a stream too. Responses
//	are NOT guaranteed 1:1-ordered with requests in general, which is why each
//	carries its own idempotency_key/correlation_id to pair them.
//
// TRADEOFF: streaming complicates the HPA inflight accounting (a stream holds a
// connection but may be idle) and load balancing (L7 gRPC LBs balance streams,
// not messages). For that reason unary Predict remains the PRIMARY path the
// gateway uses; StreamPredict is opt-in for batch/online clients. Documented so
// an interviewer sees we know streaming isn't free.
//
// Buf note: streaming RPCs still require dedicated Request/Response messages
// (RPC_REQUEST_RESPONSE_UNIQUE). We reuse the same tensor shapes but in
// stream-specific wrappers so the unary and streaming contracts can evolve
// independently.
type StreamPredictRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// One inference's inputs, same semantics (and the same server-side 64-entry /
	// max-tensor-bytes caps) as PredictRequest.inputs. The server additionally
	// bounds total in-flight stream messages per connection (default 256) to keep
	// HPA inflight accounting and per-pod memory bounded — a slow consumer cannot
	// make the pod buffer unboundedly.
	Inputs map[string]*TensorData `protobuf:"bytes,1,rep,name=inputs,proto3" json:"inputs,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	// Per-message idempotency key. It serves TWO purposes on a stream:
	//
	//	(1) it PAIRS each response to its request (responses are not 1:1-ordered
	//	    with requests), and
	//	(2) it makes a retried/duplicated stream message a cache hit instead of a
	//	    recompute (same pure latency/compute saving as unary — NOT a billing
	//	    concern; the pod emits no event).
	//
	// Recommended on every message; if empty, that message is always recomputed
	// and the client must rely on send order to pair its response.
	IdempotencyKey string `protobuf:"bytes,2,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	// Optional per-message correlation id for tracing individual predictions
	// within the stream.
	CorrelationId string `protobuf:"bytes,3,opt,name=correlation_id,json=correlationId,proto3" json:"correlation_id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *StreamPredictRequest) Reset() {
	*x = StreamPredictRequest{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *StreamPredictRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StreamPredictRequest) ProtoMessage() {}

func (x *StreamPredictRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StreamPredictRequest.ProtoReflect.Descriptor instead.
func (*StreamPredictRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{6}
}

func (x *StreamPredictRequest) GetInputs() map[string]*TensorData {
	if x != nil {
		return x.Inputs
	}
	return nil
}

func (x *StreamPredictRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

func (x *StreamPredictRequest) GetCorrelationId() string {
	if x != nil {
		return x.CorrelationId
	}
	return ""
}

type StreamPredictResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The outputs for one inference.
	Outputs map[string]*TensorData `protobuf:"bytes,1,rep,name=outputs,proto3" json:"outputs,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	// The version that produced this result.
	ModelVersion string `protobuf:"bytes,2,opt,name=model_version,json=modelVersion,proto3" json:"model_version,omitempty"`
	// Inference wall time for this single prediction.
	InferenceLatency *durationpb.Duration `protobuf:"bytes,3,opt,name=inference_latency,json=inferenceLatency,proto3" json:"inference_latency,omitempty"`
	// Echoes the request message's idempotency_key so the client can pair this
	// response with the request it answers (responses are not order-guaranteed).
	IdempotencyKey string `protobuf:"bytes,4,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	// Echoes the request message's correlation id for tracing.
	CorrelationId string `protobuf:"bytes,5,opt,name=correlation_id,json=correlationId,proto3" json:"correlation_id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *StreamPredictResponse) Reset() {
	*x = StreamPredictResponse{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *StreamPredictResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StreamPredictResponse) ProtoMessage() {}

func (x *StreamPredictResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StreamPredictResponse.ProtoReflect.Descriptor instead.
func (*StreamPredictResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{7}
}

func (x *StreamPredictResponse) GetOutputs() map[string]*TensorData {
	if x != nil {
		return x.Outputs
	}
	return nil
}

func (x *StreamPredictResponse) GetModelVersion() string {
	if x != nil {
		return x.ModelVersion
	}
	return ""
}

func (x *StreamPredictResponse) GetInferenceLatency() *durationpb.Duration {
	if x != nil {
		return x.InferenceLatency
	}
	return nil
}

func (x *StreamPredictResponse) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

func (x *StreamPredictResponse) GetCorrelationId() string {
	if x != nil {
		return x.CorrelationId
	}
	return ""
}

// GetModelInfoRequest asks for the static schema/identity of the served model.
// WHY it still takes a name/version even though the pod serves one model:
//
//	Symmetry with the gateway's interface and forward-compat — keeps the
//	request shape stable if a future multi-model variant ever appears. Empty
//	fields mean "the model this pod serves".
type GetModelInfoRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Optional logical name filter; empty = this pod's model.
	ModelName string `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// Optional version filter; empty = this pod's version.
	Version       string `protobuf:"bytes,2,opt,name=version,proto3" json:"version,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetModelInfoRequest) Reset() {
	*x = GetModelInfoRequest{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[8]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetModelInfoRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetModelInfoRequest) ProtoMessage() {}

func (x *GetModelInfoRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[8]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetModelInfoRequest.ProtoReflect.Descriptor instead.
func (*GetModelInfoRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{8}
}

func (x *GetModelInfoRequest) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *GetModelInfoRequest) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

// GetModelInfoResponse wraps ModelInfo.
// WHY wrap (not return ModelInfo directly): Buf STANDARD RPC_RESPONSE_STANDARD_NAME
// requires a <Rpc>Response message, and wrapping lets us add fields later (e.g.
// warmup status) without mutating the shared ModelInfo domain type.
type GetModelInfoResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The static description of the served model.
	ModelInfo     *ModelInfo `protobuf:"bytes,1,opt,name=model_info,json=modelInfo,proto3" json:"model_info,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetModelInfoResponse) Reset() {
	*x = GetModelInfoResponse{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[9]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetModelInfoResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetModelInfoResponse) ProtoMessage() {}

func (x *GetModelInfoResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[9]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetModelInfoResponse.ProtoReflect.Descriptor instead.
func (*GetModelInfoResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{9}
}

func (x *GetModelInfoResponse) GetModelInfo() *ModelInfo {
	if x != nil {
		return x.ModelInfo
	}
	return nil
}

// ============================================================================
// ListLoadedModels (control plane — fleet/pod introspection)
// ============================================================================
//
// WHY this RPC exists even though it's one-model-per-pod TODAY:
//
//	The per-version-Deployment controller and admin/`fp` CLI need a uniform way
//	to ask a pod "what do you currently have resident, and in what state?" —
//	without N separate GetModelStatus calls and without assuming the pod holds
//	exactly one model. On today's single-model pod this returns 0 or 1 entry;
//	it is also the forward-compatible seam if a future multi-model pod variant
//	(the rejected Triton-style server) ever appears. The reconcile loop reads
//	this to detect drift between desired (the Deployment spec / consumed
//	ModelDeployed events) and actual (resident) models.
//
// PAGINATION (reuse common.v1, mandated for every list): even though a serving
// pod holds a tiny number of models, the contract requires pagination on ALL
// list RPCs for uniformity and so the same SDK list helpers work everywhere.
// PAGE-SIZE CAP IS PART OF THE CONTRACT: server clamps page_size to [1, 100]
// (default 20) — a request for more returns at most 100. This is the same cap
// common.PaginationRequest documents; stated here so it is a contract promise,
// not an implementation accident.
type ListLoadedModelsRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Cursor + page-size envelope (common.v1). page_size is server-clamped to
	// [1,100], default 20; page_token is the opaque cursor from a prior response.
	Pagination *v1.PaginationRequest `protobuf:"bytes,1,opt,name=pagination,proto3" json:"pagination,omitempty"`
	// Optional state filter: when set, only models in this lifecycle state are
	// returned (e.g. list only READY models the pod can actually serve). Empty/
	// UNSPECIFIED = all states. A FILTER, not an authority field.
	StateFilter   ModelState `protobuf:"varint,2,opt,name=state_filter,json=stateFilter,proto3,enum=forgepoint.serving.v1.ModelState" json:"state_filter,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListLoadedModelsRequest) Reset() {
	*x = ListLoadedModelsRequest{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[10]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListLoadedModelsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListLoadedModelsRequest) ProtoMessage() {}

func (x *ListLoadedModelsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[10]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListLoadedModelsRequest.ProtoReflect.Descriptor instead.
func (*ListLoadedModelsRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{10}
}

func (x *ListLoadedModelsRequest) GetPagination() *v1.PaginationRequest {
	if x != nil {
		return x.Pagination
	}
	return nil
}

func (x *ListLoadedModelsRequest) GetStateFilter() ModelState {
	if x != nil {
		return x.StateFilter
	}
	return ModelState_MODEL_STATE_UNSPECIFIED
}

// ListLoadedModelsResponse returns the resident models' live status plus the
// pagination cursor. Returns ModelStatus (the live view), not ModelInfo, because
// the fleet view cares about state/health, not full I/O schema (fetch that per
// model via GetModelInfo when needed).
type ListLoadedModelsResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The resident models' live lifecycle status (0..page_size entries).
	Models []*ModelStatus `protobuf:"bytes,1,rep,name=models,proto3" json:"models,omitempty"`
	// next_page_token (empty = last page) + total_count (resident model count).
	Pagination    *v1.PaginationResponse `protobuf:"bytes,2,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListLoadedModelsResponse) Reset() {
	*x = ListLoadedModelsResponse{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[11]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListLoadedModelsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListLoadedModelsResponse) ProtoMessage() {}

func (x *ListLoadedModelsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[11]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListLoadedModelsResponse.ProtoReflect.Descriptor instead.
func (*ListLoadedModelsResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{11}
}

func (x *ListLoadedModelsResponse) GetModels() []*ModelStatus {
	if x != nil {
		return x.Models
	}
	return nil
}

func (x *ListLoadedModelsResponse) GetPagination() *v1.PaginationResponse {
	if x != nil {
		return x.Pagination
	}
	return nil
}

// LoadModelRequest tells the pod to fetch an artifact from object storage and
// load it into the ONNX runtime. Used by the operator/controller that manages
// per-version Deployments (and at startup the pod self-loads from env vars).
//
// WHY load-by-reference (URI/digest), not by uploading bytes here:
//
//	The artifact lives in MinIO/S3 (the registry put it there). Streaming
//	megabytes of weights through a control RPC would be wasteful and couple the
//	control plane to artifact size. We pass a pointer; the pod pulls it. This
//	mirrors KServe's storageUri model.
//
// SECURITY: the caller specifies WHERE to load from but the pod only accepts
// URIs within its configured allowed bucket/prefix (validated server-side) —
// a caller can't point a serving pod at an arbitrary attacker-controlled URL
// (SSRF / supply-chain guard). expected_digest lets the pod verify integrity.
type LoadModelRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Logical model name to register this artifact under on the pod.
	ModelName string `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// Version label for the artifact (must match the Deployment's intended ver).
	Version string `protobuf:"bytes,2,opt,name=version,proto3" json:"version,omitempty"`
	// Object-storage URI of the ONNX artifact, e.g. "s3://fp-models/fraud/v3.onnx".
	// Validated against the pod's allow-listed bucket/prefix before fetching.
	ArtifactUri string `protobuf:"bytes,3,opt,name=artifact_uri,json=artifactUri,proto3" json:"artifact_uri,omitempty"`
	// Optional expected sha256 digest. If set, the pod aborts the load (FAILED)
	// when the fetched bytes don't match — integrity/supply-chain protection.
	ExpectedDigest string `protobuf:"bytes,4,opt,name=expected_digest,json=expectedDigest,proto3" json:"expected_digest,omitempty"`
	// Idempotency key so a retried LoadModel (e.g. controller re-reconcile) does
	// not re-download/re-init an already-loaded identical model. If the requested
	// (name, version, digest) is already READY, the pod returns the current
	// status without reloading.
	IdempotencyKey string `protobuf:"bytes,5,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *LoadModelRequest) Reset() {
	*x = LoadModelRequest{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[12]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *LoadModelRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LoadModelRequest) ProtoMessage() {}

func (x *LoadModelRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[12]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LoadModelRequest.ProtoReflect.Descriptor instead.
func (*LoadModelRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{12}
}

func (x *LoadModelRequest) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *LoadModelRequest) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *LoadModelRequest) GetArtifactUri() string {
	if x != nil {
		return x.ArtifactUri
	}
	return ""
}

func (x *LoadModelRequest) GetExpectedDigest() string {
	if x != nil {
		return x.ExpectedDigest
	}
	return ""
}

func (x *LoadModelRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// LoadModelResponse returns the resulting model status (which may still be
// DOWNLOADING/LOADING — load is asynchronous; poll GetModelStatus for READY).
// WHY return status, not empty: the caller wants to know whether the load was
// accepted and the current state, without an immediate second RPC.
type LoadModelResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Current lifecycle status after accepting the load request.
	Status        *ModelStatus `protobuf:"bytes,1,opt,name=status,proto3" json:"status,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *LoadModelResponse) Reset() {
	*x = LoadModelResponse{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[13]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *LoadModelResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LoadModelResponse) ProtoMessage() {}

func (x *LoadModelResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[13]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LoadModelResponse.ProtoReflect.Descriptor instead.
func (*LoadModelResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{13}
}

func (x *LoadModelResponse) GetStatus() *ModelStatus {
	if x != nil {
		return x.Status
	}
	return nil
}

// UnloadModelRequest frees a model from memory (e.g. before shutdown or to
// reclaim RAM). After unload the pod reports MODEL_STATE_UNLOADED and Predict
// returns FAILED_PRECONDITION until reloaded. Admin/controller-only — the
// caller's authority comes from the auth interceptor, never from this body.
type UnloadModelRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Model to unload. Empty fields = the pod's single loaded model.
	ModelName string `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// Version to unload; empty = the pod's current version.
	Version string `protobuf:"bytes,2,opt,name=version,proto3" json:"version,omitempty"`
	// Optional reason for audit/observability ("undeployed", "shutdown",
	// "evicted"). Free-form but small; lets operators distinguish an intentional
	// unload from an eviction in logs. NOT published as an event (serving emits
	// none) — the authoritative teardown fact is the orchestrator's
	// events.ModelUndeployed.reason that TRIGGERED this call.
	Reason string `protobuf:"bytes,3,opt,name=reason,proto3" json:"reason,omitempty"`
	// Idempotency key so a retried UnloadModel (controller re-reconcile after the
	// model is already UNLOADED) is a no-op returning the current status rather
	// than an error. Optional.
	IdempotencyKey string `protobuf:"bytes,4,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *UnloadModelRequest) Reset() {
	*x = UnloadModelRequest{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[14]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UnloadModelRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UnloadModelRequest) ProtoMessage() {}

func (x *UnloadModelRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[14]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UnloadModelRequest.ProtoReflect.Descriptor instead.
func (*UnloadModelRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{14}
}

func (x *UnloadModelRequest) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *UnloadModelRequest) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *UnloadModelRequest) GetReason() string {
	if x != nil {
		return x.Reason
	}
	return ""
}

func (x *UnloadModelRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// UnloadModelResponse is intentionally minimal but named (Buf RPC_RESPONSE_
// STANDARD_NAME) and forward-compatible.
type UnloadModelResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Status after unload (expected MODEL_STATE_UNLOADED).
	Status        *ModelStatus `protobuf:"bytes,1,opt,name=status,proto3" json:"status,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UnloadModelResponse) Reset() {
	*x = UnloadModelResponse{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[15]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UnloadModelResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UnloadModelResponse) ProtoMessage() {}

func (x *UnloadModelResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[15]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UnloadModelResponse.ProtoReflect.Descriptor instead.
func (*UnloadModelResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{15}
}

func (x *UnloadModelResponse) GetStatus() *ModelStatus {
	if x != nil {
		return x.Status
	}
	return nil
}

// ModelStatus is the live lifecycle view of the served model — used by probes,
// the operator's reconcile loop, and admin tooling.
type ModelStatus struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The model's name (echoed for callers managing several pods).
	ModelName string `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// The model's version.
	Version string `protobuf:"bytes,2,opt,name=version,proto3" json:"version,omitempty"`
	// Current lifecycle state (the state machine in ModelState's docs).
	State ModelState `protobuf:"varint,3,opt,name=state,proto3,enum=forgepoint.serving.v1.ModelState" json:"state,omitempty"`
	// Human-readable detail — especially the reason on FAILED ("digest mismatch",
	// "unsupported ONNX op: X", "OOM loading 2.1GB model"). Empty when healthy.
	Message string `protobuf:"bytes,4,opt,name=message,proto3" json:"message,omitempty"`
	// When the pod last transitioned state. Lets the operator detect a stuck
	// DOWNLOADING/LOADING (cold-start watchdog).
	UpdatedAt     *timestamppb.Timestamp `protobuf:"bytes,5,opt,name=updated_at,json=updatedAt,proto3" json:"updated_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ModelStatus) Reset() {
	*x = ModelStatus{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[16]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ModelStatus) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ModelStatus) ProtoMessage() {}

func (x *ModelStatus) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[16]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ModelStatus.ProtoReflect.Descriptor instead.
func (*ModelStatus) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{16}
}

func (x *ModelStatus) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *ModelStatus) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *ModelStatus) GetState() ModelState {
	if x != nil {
		return x.State
	}
	return ModelState_MODEL_STATE_UNSPECIFIED
}

func (x *ModelStatus) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

func (x *ModelStatus) GetUpdatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.UpdatedAt
	}
	return nil
}

// GetModelStatusRequest asks for the current lifecycle status.
type GetModelStatusRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Model name; empty = this pod's model.
	ModelName string `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// Version; empty = this pod's version.
	Version       string `protobuf:"bytes,2,opt,name=version,proto3" json:"version,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetModelStatusRequest) Reset() {
	*x = GetModelStatusRequest{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[17]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetModelStatusRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetModelStatusRequest) ProtoMessage() {}

func (x *GetModelStatusRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[17]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetModelStatusRequest.ProtoReflect.Descriptor instead.
func (*GetModelStatusRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{17}
}

func (x *GetModelStatusRequest) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *GetModelStatusRequest) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

// GetModelStatusResponse wraps ModelStatus (Buf naming + forward-compat).
type GetModelStatusResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Current lifecycle status.
	Status        *ModelStatus `protobuf:"bytes,1,opt,name=status,proto3" json:"status,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetModelStatusResponse) Reset() {
	*x = GetModelStatusResponse{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[18]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetModelStatusResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetModelStatusResponse) ProtoMessage() {}

func (x *GetModelStatusResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[18]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetModelStatusResponse.ProtoReflect.Descriptor instead.
func (*GetModelStatusResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{18}
}

func (x *GetModelStatusResponse) GetStatus() *ModelStatus {
	if x != nil {
		return x.Status
	}
	return nil
}

// GetServingMetricsRequest takes no selectors today (one model per pod) but is
// a named message so the metric surface can grow (e.g. per-window stats).
type GetServingMetricsRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetServingMetricsRequest) Reset() {
	*x = GetServingMetricsRequest{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[19]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetServingMetricsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetServingMetricsRequest) ProtoMessage() {}

func (x *GetServingMetricsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[19]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetServingMetricsRequest.ProtoReflect.Descriptor instead.
func (*GetServingMetricsRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{19}
}

// GetServingMetricsResponse wraps the metrics snapshot used for autoscaling and
// load-aware routing. WHY a snapshot RPC in addition to /metrics scraping: see
// ServingMetrics docs — the gateway reads this for least-inflight routing.
type GetServingMetricsResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Point-in-time serving metrics.
	Metrics       *ServingMetrics `protobuf:"bytes,1,opt,name=metrics,proto3" json:"metrics,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetServingMetricsResponse) Reset() {
	*x = GetServingMetricsResponse{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[20]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetServingMetricsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetServingMetricsResponse) ProtoMessage() {}

func (x *GetServingMetricsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[20]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetServingMetricsResponse.ProtoReflect.Descriptor instead.
func (*GetServingMetricsResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{20}
}

func (x *GetServingMetricsResponse) GetMetrics() *ServingMetrics {
	if x != nil {
		return x.Metrics
	}
	return nil
}

// HealthCheckRequest optionally scopes the check to a model (future multi-model).
type HealthCheckRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Empty = check this pod's model. Mirrors gRPC's standard health protocol
	// "service" selector so existing health tooling maps cleanly.
	ModelName     string `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *HealthCheckRequest) Reset() {
	*x = HealthCheckRequest{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[21]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *HealthCheckRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HealthCheckRequest) ProtoMessage() {}

func (x *HealthCheckRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[21]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HealthCheckRequest.ProtoReflect.Descriptor instead.
func (*HealthCheckRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{21}
}

func (x *HealthCheckRequest) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

// HealthCheckResponse reports serving status + the signals behind the verdict.
type HealthCheckResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The readiness verdict K8s readinessProbe keys on.
	Status HealthStatus `protobuf:"varint,1,opt,name=status,proto3,enum=forgepoint.serving.v1.HealthStatus" json:"status,omitempty"`
	// Current model lifecycle state (richer context behind `status`).
	ModelState ModelState `protobuf:"varint,2,opt,name=model_state,json=modelState,proto3,enum=forgepoint.serving.v1.ModelState" json:"model_state,omitempty"`
	// Most recent inference latency observed; the livenessProbe compares this to
	// its configured threshold to decide alive vs wedged.
	LastInferenceLatency *durationpb.Duration `protobuf:"bytes,3,opt,name=last_inference_latency,json=lastInferenceLatency,proto3" json:"last_inference_latency,omitempty"`
	unknownFields        protoimpl.UnknownFields
	sizeCache            protoimpl.SizeCache
}

func (x *HealthCheckResponse) Reset() {
	*x = HealthCheckResponse{}
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[22]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *HealthCheckResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HealthCheckResponse) ProtoMessage() {}

func (x *HealthCheckResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_serving_v1_serving_proto_msgTypes[22]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HealthCheckResponse.ProtoReflect.Descriptor instead.
func (*HealthCheckResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_serving_v1_serving_proto_rawDescGZIP(), []int{22}
}

func (x *HealthCheckResponse) GetStatus() HealthStatus {
	if x != nil {
		return x.Status
	}
	return HealthStatus_HEALTH_STATUS_UNSPECIFIED
}

func (x *HealthCheckResponse) GetModelState() ModelState {
	if x != nil {
		return x.ModelState
	}
	return ModelState_MODEL_STATE_UNSPECIFIED
}

func (x *HealthCheckResponse) GetLastInferenceLatency() *durationpb.Duration {
	if x != nil {
		return x.LastInferenceLatency
	}
	return nil
}

var File_forgepoint_serving_v1_serving_proto protoreflect.FileDescriptor

const file_forgepoint_serving_v1_serving_proto_rawDesc = "" +
	"\n" +
	"#forgepoint/serving/v1/serving.proto\x12\x15forgepoint.serving.v1\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1egoogle/protobuf/duration.proto\x1a!forgepoint/common/v1/common.proto\"m\n" +
	"\n" +
	"TensorData\x12\x14\n" +
	"\x05shape\x18\x01 \x03(\x03R\x05shape\x12\x12\n" +
	"\x04data\x18\x02 \x01(\fR\x04data\x125\n" +
	"\x05dtype\x18\x03 \x01(\x0e2\x1f.forgepoint.serving.v1.DataTypeR\x05dtype\"m\n" +
	"\n" +
	"TensorSpec\x12\x12\n" +
	"\x04name\x18\x01 \x01(\tR\x04name\x12\x14\n" +
	"\x05shape\x18\x02 \x03(\x03R\x05shape\x125\n" +
	"\x05dtype\x18\x03 \x01(\x0e2\x1f.forgepoint.serving.v1.DataTypeR\x05dtype\"\xa9\x02\n" +
	"\tModelInfo\x12\x12\n" +
	"\x04name\x18\x01 \x01(\tR\x04name\x12\x18\n" +
	"\aversion\x18\x02 \x01(\tR\aversion\x12D\n" +
	"\finput_schema\x18\x03 \x03(\v2!.forgepoint.serving.v1.TensorSpecR\vinputSchema\x12F\n" +
	"\routput_schema\x18\x04 \x03(\v2!.forgepoint.serving.v1.TensorSpecR\foutputSchema\x127\n" +
	"\tloaded_at\x18\x05 \x01(\v2\x1a.google.protobuf.TimestampR\bloadedAt\x12'\n" +
	"\x0fartifact_digest\x18\x06 \x01(\tR\x0eartifactDigest\"\xb3\x02\n" +
	"\x0eServingMetrics\x12+\n" +
	"\x11inflight_requests\x18\x01 \x01(\x05R\x10inflightRequests\x12%\n" +
	"\x0etotal_requests\x18\x02 \x01(\x03R\rtotalRequests\x12'\n" +
	"\x0ffailed_requests\x18\x03 \x01(\x03R\x0efailedRequests\x12:\n" +
	"\vp50_latency\x18\x04 \x01(\v2\x19.google.protobuf.DurationR\n" +
	"p50Latency\x12:\n" +
	"\vp99_latency\x18\x05 \x01(\v2\x19.google.protobuf.DurationR\n" +
	"p99Latency\x12,\n" +
	"\x12model_memory_bytes\x18\x06 \x01(\x03R\x10modelMemoryBytes\"\xc2\x02\n" +
	"\x0ePredictRequest\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\x12\x18\n" +
	"\aversion\x18\x02 \x01(\tR\aversion\x12I\n" +
	"\x06inputs\x18\x03 \x03(\v21.forgepoint.serving.v1.PredictRequest.InputsEntryR\x06inputs\x12'\n" +
	"\x0fidempotency_key\x18\x04 \x01(\tR\x0eidempotencyKey\x12%\n" +
	"\x0ecorrelation_id\x18\x05 \x01(\tR\rcorrelationId\x1a\\\n" +
	"\vInputsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x127\n" +
	"\x05value\x18\x02 \x01(\v2!.forgepoint.serving.v1.TensorDataR\x05value:\x028\x01\"\xf2\x02\n" +
	"\x0fPredictResponse\x12M\n" +
	"\aoutputs\x18\x01 \x03(\v23.forgepoint.serving.v1.PredictResponse.OutputsEntryR\aoutputs\x12#\n" +
	"\rmodel_version\x18\x02 \x01(\tR\fmodelVersion\x12F\n" +
	"\x11inference_latency\x18\x03 \x01(\v2\x19.google.protobuf.DurationR\x10inferenceLatency\x12%\n" +
	"\x0ecorrelation_id\x18\x04 \x01(\tR\rcorrelationId\x12\x1d\n" +
	"\n" +
	"from_cache\x18\x05 \x01(\bR\tfromCache\x1a]\n" +
	"\fOutputsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x127\n" +
	"\x05value\x18\x02 \x01(\v2!.forgepoint.serving.v1.TensorDataR\x05value:\x028\x01\"\x95\x02\n" +
	"\x14StreamPredictRequest\x12O\n" +
	"\x06inputs\x18\x01 \x03(\v27.forgepoint.serving.v1.StreamPredictRequest.InputsEntryR\x06inputs\x12'\n" +
	"\x0fidempotency_key\x18\x02 \x01(\tR\x0eidempotencyKey\x12%\n" +
	"\x0ecorrelation_id\x18\x03 \x01(\tR\rcorrelationId\x1a\\\n" +
	"\vInputsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x127\n" +
	"\x05value\x18\x02 \x01(\v2!.forgepoint.serving.v1.TensorDataR\x05value:\x028\x01\"\x88\x03\n" +
	"\x15StreamPredictResponse\x12S\n" +
	"\aoutputs\x18\x01 \x03(\v29.forgepoint.serving.v1.StreamPredictResponse.OutputsEntryR\aoutputs\x12#\n" +
	"\rmodel_version\x18\x02 \x01(\tR\fmodelVersion\x12F\n" +
	"\x11inference_latency\x18\x03 \x01(\v2\x19.google.protobuf.DurationR\x10inferenceLatency\x12'\n" +
	"\x0fidempotency_key\x18\x04 \x01(\tR\x0eidempotencyKey\x12%\n" +
	"\x0ecorrelation_id\x18\x05 \x01(\tR\rcorrelationId\x1a]\n" +
	"\fOutputsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x127\n" +
	"\x05value\x18\x02 \x01(\v2!.forgepoint.serving.v1.TensorDataR\x05value:\x028\x01\"N\n" +
	"\x13GetModelInfoRequest\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\x12\x18\n" +
	"\aversion\x18\x02 \x01(\tR\aversion\"W\n" +
	"\x14GetModelInfoResponse\x12?\n" +
	"\n" +
	"model_info\x18\x01 \x01(\v2 .forgepoint.serving.v1.ModelInfoR\tmodelInfo\"\xa8\x01\n" +
	"\x17ListLoadedModelsRequest\x12G\n" +
	"\n" +
	"pagination\x18\x01 \x01(\v2'.forgepoint.common.v1.PaginationRequestR\n" +
	"pagination\x12D\n" +
	"\fstate_filter\x18\x02 \x01(\x0e2!.forgepoint.serving.v1.ModelStateR\vstateFilter\"\xa0\x01\n" +
	"\x18ListLoadedModelsResponse\x12:\n" +
	"\x06models\x18\x01 \x03(\v2\".forgepoint.serving.v1.ModelStatusR\x06models\x12H\n" +
	"\n" +
	"pagination\x18\x02 \x01(\v2(.forgepoint.common.v1.PaginationResponseR\n" +
	"pagination\"\xc0\x01\n" +
	"\x10LoadModelRequest\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\x12\x18\n" +
	"\aversion\x18\x02 \x01(\tR\aversion\x12!\n" +
	"\fartifact_uri\x18\x03 \x01(\tR\vartifactUri\x12'\n" +
	"\x0fexpected_digest\x18\x04 \x01(\tR\x0eexpectedDigest\x12'\n" +
	"\x0fidempotency_key\x18\x05 \x01(\tR\x0eidempotencyKey\"O\n" +
	"\x11LoadModelResponse\x12:\n" +
	"\x06status\x18\x01 \x01(\v2\".forgepoint.serving.v1.ModelStatusR\x06status\"\x8e\x01\n" +
	"\x12UnloadModelRequest\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\x12\x18\n" +
	"\aversion\x18\x02 \x01(\tR\aversion\x12\x16\n" +
	"\x06reason\x18\x03 \x01(\tR\x06reason\x12'\n" +
	"\x0fidempotency_key\x18\x04 \x01(\tR\x0eidempotencyKey\"Q\n" +
	"\x13UnloadModelResponse\x12:\n" +
	"\x06status\x18\x01 \x01(\v2\".forgepoint.serving.v1.ModelStatusR\x06status\"\xd4\x01\n" +
	"\vModelStatus\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\x12\x18\n" +
	"\aversion\x18\x02 \x01(\tR\aversion\x127\n" +
	"\x05state\x18\x03 \x01(\x0e2!.forgepoint.serving.v1.ModelStateR\x05state\x12\x18\n" +
	"\amessage\x18\x04 \x01(\tR\amessage\x129\n" +
	"\n" +
	"updated_at\x18\x05 \x01(\v2\x1a.google.protobuf.TimestampR\tupdatedAt\"P\n" +
	"\x15GetModelStatusRequest\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\x12\x18\n" +
	"\aversion\x18\x02 \x01(\tR\aversion\"T\n" +
	"\x16GetModelStatusResponse\x12:\n" +
	"\x06status\x18\x01 \x01(\v2\".forgepoint.serving.v1.ModelStatusR\x06status\"\x1a\n" +
	"\x18GetServingMetricsRequest\"\\\n" +
	"\x19GetServingMetricsResponse\x12?\n" +
	"\ametrics\x18\x01 \x01(\v2%.forgepoint.serving.v1.ServingMetricsR\ametrics\"3\n" +
	"\x12HealthCheckRequest\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\"\xe7\x01\n" +
	"\x13HealthCheckResponse\x12;\n" +
	"\x06status\x18\x01 \x01(\x0e2#.forgepoint.serving.v1.HealthStatusR\x06status\x12B\n" +
	"\vmodel_state\x18\x02 \x01(\x0e2!.forgepoint.serving.v1.ModelStateR\n" +
	"modelState\x12O\n" +
	"\x16last_inference_latency\x18\x03 \x01(\v2\x19.google.protobuf.DurationR\x14lastInferenceLatency*\xa7\x01\n" +
	"\bDataType\x12\x19\n" +
	"\x15DATA_TYPE_UNSPECIFIED\x10\x00\x12\x15\n" +
	"\x11DATA_TYPE_FLOAT32\x10\x01\x12\x15\n" +
	"\x11DATA_TYPE_FLOAT64\x10\x02\x12\x13\n" +
	"\x0fDATA_TYPE_INT32\x10\x03\x12\x13\n" +
	"\x0fDATA_TYPE_INT64\x10\x04\x12\x12\n" +
	"\x0eDATA_TYPE_BOOL\x10\x05\x12\x14\n" +
	"\x10DATA_TYPE_STRING\x10\x06*\xa8\x01\n" +
	"\n" +
	"ModelState\x12\x1b\n" +
	"\x17MODEL_STATE_UNSPECIFIED\x10\x00\x12\x1b\n" +
	"\x17MODEL_STATE_DOWNLOADING\x10\x01\x12\x17\n" +
	"\x13MODEL_STATE_LOADING\x10\x02\x12\x15\n" +
	"\x11MODEL_STATE_READY\x10\x03\x12\x16\n" +
	"\x12MODEL_STATE_FAILED\x10\x04\x12\x18\n" +
	"\x14MODEL_STATE_UNLOADED\x10\x05*g\n" +
	"\fHealthStatus\x12\x1d\n" +
	"\x19HEALTH_STATUS_UNSPECIFIED\x10\x00\x12\x19\n" +
	"\x15HEALTH_STATUS_SERVING\x10\x01\x12\x1d\n" +
	"\x19HEALTH_STATUS_NOT_SERVING\x10\x022\xd0\a\n" +
	"\x13ModelServingService\x12X\n" +
	"\aPredict\x12%.forgepoint.serving.v1.PredictRequest\x1a&.forgepoint.serving.v1.PredictResponse\x12n\n" +
	"\rStreamPredict\x12+.forgepoint.serving.v1.StreamPredictRequest\x1a,.forgepoint.serving.v1.StreamPredictResponse(\x010\x01\x12g\n" +
	"\fGetModelInfo\x12*.forgepoint.serving.v1.GetModelInfoRequest\x1a+.forgepoint.serving.v1.GetModelInfoResponse\x12s\n" +
	"\x10ListLoadedModels\x12..forgepoint.serving.v1.ListLoadedModelsRequest\x1a/.forgepoint.serving.v1.ListLoadedModelsResponse\x12^\n" +
	"\tLoadModel\x12'.forgepoint.serving.v1.LoadModelRequest\x1a(.forgepoint.serving.v1.LoadModelResponse\x12d\n" +
	"\vUnloadModel\x12).forgepoint.serving.v1.UnloadModelRequest\x1a*.forgepoint.serving.v1.UnloadModelResponse\x12m\n" +
	"\x0eGetModelStatus\x12,.forgepoint.serving.v1.GetModelStatusRequest\x1a-.forgepoint.serving.v1.GetModelStatusResponse\x12v\n" +
	"\x11GetServingMetrics\x12/.forgepoint.serving.v1.GetServingMetricsRequest\x1a0.forgepoint.serving.v1.GetServingMetricsResponse\x12d\n" +
	"\vHealthCheck\x12).forgepoint.serving.v1.HealthCheckRequest\x1a*.forgepoint.serving.v1.HealthCheckResponseBJZHgithub.com/abd-ulbasit/forgepoint/gen/go/forgepoint/serving/v1;servingv1b\x06proto3"

var (
	file_forgepoint_serving_v1_serving_proto_rawDescOnce sync.Once
	file_forgepoint_serving_v1_serving_proto_rawDescData []byte
)

func file_forgepoint_serving_v1_serving_proto_rawDescGZIP() []byte {
	file_forgepoint_serving_v1_serving_proto_rawDescOnce.Do(func() {
		file_forgepoint_serving_v1_serving_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_forgepoint_serving_v1_serving_proto_rawDesc), len(file_forgepoint_serving_v1_serving_proto_rawDesc)))
	})
	return file_forgepoint_serving_v1_serving_proto_rawDescData
}

var file_forgepoint_serving_v1_serving_proto_enumTypes = make([]protoimpl.EnumInfo, 3)
var file_forgepoint_serving_v1_serving_proto_msgTypes = make([]protoimpl.MessageInfo, 27)
var file_forgepoint_serving_v1_serving_proto_goTypes = []any{
	(DataType)(0),                     // 0: forgepoint.serving.v1.DataType
	(ModelState)(0),                   // 1: forgepoint.serving.v1.ModelState
	(HealthStatus)(0),                 // 2: forgepoint.serving.v1.HealthStatus
	(*TensorData)(nil),                // 3: forgepoint.serving.v1.TensorData
	(*TensorSpec)(nil),                // 4: forgepoint.serving.v1.TensorSpec
	(*ModelInfo)(nil),                 // 5: forgepoint.serving.v1.ModelInfo
	(*ServingMetrics)(nil),            // 6: forgepoint.serving.v1.ServingMetrics
	(*PredictRequest)(nil),            // 7: forgepoint.serving.v1.PredictRequest
	(*PredictResponse)(nil),           // 8: forgepoint.serving.v1.PredictResponse
	(*StreamPredictRequest)(nil),      // 9: forgepoint.serving.v1.StreamPredictRequest
	(*StreamPredictResponse)(nil),     // 10: forgepoint.serving.v1.StreamPredictResponse
	(*GetModelInfoRequest)(nil),       // 11: forgepoint.serving.v1.GetModelInfoRequest
	(*GetModelInfoResponse)(nil),      // 12: forgepoint.serving.v1.GetModelInfoResponse
	(*ListLoadedModelsRequest)(nil),   // 13: forgepoint.serving.v1.ListLoadedModelsRequest
	(*ListLoadedModelsResponse)(nil),  // 14: forgepoint.serving.v1.ListLoadedModelsResponse
	(*LoadModelRequest)(nil),          // 15: forgepoint.serving.v1.LoadModelRequest
	(*LoadModelResponse)(nil),         // 16: forgepoint.serving.v1.LoadModelResponse
	(*UnloadModelRequest)(nil),        // 17: forgepoint.serving.v1.UnloadModelRequest
	(*UnloadModelResponse)(nil),       // 18: forgepoint.serving.v1.UnloadModelResponse
	(*ModelStatus)(nil),               // 19: forgepoint.serving.v1.ModelStatus
	(*GetModelStatusRequest)(nil),     // 20: forgepoint.serving.v1.GetModelStatusRequest
	(*GetModelStatusResponse)(nil),    // 21: forgepoint.serving.v1.GetModelStatusResponse
	(*GetServingMetricsRequest)(nil),  // 22: forgepoint.serving.v1.GetServingMetricsRequest
	(*GetServingMetricsResponse)(nil), // 23: forgepoint.serving.v1.GetServingMetricsResponse
	(*HealthCheckRequest)(nil),        // 24: forgepoint.serving.v1.HealthCheckRequest
	(*HealthCheckResponse)(nil),       // 25: forgepoint.serving.v1.HealthCheckResponse
	nil,                               // 26: forgepoint.serving.v1.PredictRequest.InputsEntry
	nil,                               // 27: forgepoint.serving.v1.PredictResponse.OutputsEntry
	nil,                               // 28: forgepoint.serving.v1.StreamPredictRequest.InputsEntry
	nil,                               // 29: forgepoint.serving.v1.StreamPredictResponse.OutputsEntry
	(*timestamppb.Timestamp)(nil),     // 30: google.protobuf.Timestamp
	(*durationpb.Duration)(nil),       // 31: google.protobuf.Duration
	(*v1.PaginationRequest)(nil),      // 32: forgepoint.common.v1.PaginationRequest
	(*v1.PaginationResponse)(nil),     // 33: forgepoint.common.v1.PaginationResponse
}
var file_forgepoint_serving_v1_serving_proto_depIdxs = []int32{
	0,  // 0: forgepoint.serving.v1.TensorData.dtype:type_name -> forgepoint.serving.v1.DataType
	0,  // 1: forgepoint.serving.v1.TensorSpec.dtype:type_name -> forgepoint.serving.v1.DataType
	4,  // 2: forgepoint.serving.v1.ModelInfo.input_schema:type_name -> forgepoint.serving.v1.TensorSpec
	4,  // 3: forgepoint.serving.v1.ModelInfo.output_schema:type_name -> forgepoint.serving.v1.TensorSpec
	30, // 4: forgepoint.serving.v1.ModelInfo.loaded_at:type_name -> google.protobuf.Timestamp
	31, // 5: forgepoint.serving.v1.ServingMetrics.p50_latency:type_name -> google.protobuf.Duration
	31, // 6: forgepoint.serving.v1.ServingMetrics.p99_latency:type_name -> google.protobuf.Duration
	26, // 7: forgepoint.serving.v1.PredictRequest.inputs:type_name -> forgepoint.serving.v1.PredictRequest.InputsEntry
	27, // 8: forgepoint.serving.v1.PredictResponse.outputs:type_name -> forgepoint.serving.v1.PredictResponse.OutputsEntry
	31, // 9: forgepoint.serving.v1.PredictResponse.inference_latency:type_name -> google.protobuf.Duration
	28, // 10: forgepoint.serving.v1.StreamPredictRequest.inputs:type_name -> forgepoint.serving.v1.StreamPredictRequest.InputsEntry
	29, // 11: forgepoint.serving.v1.StreamPredictResponse.outputs:type_name -> forgepoint.serving.v1.StreamPredictResponse.OutputsEntry
	31, // 12: forgepoint.serving.v1.StreamPredictResponse.inference_latency:type_name -> google.protobuf.Duration
	5,  // 13: forgepoint.serving.v1.GetModelInfoResponse.model_info:type_name -> forgepoint.serving.v1.ModelInfo
	32, // 14: forgepoint.serving.v1.ListLoadedModelsRequest.pagination:type_name -> forgepoint.common.v1.PaginationRequest
	1,  // 15: forgepoint.serving.v1.ListLoadedModelsRequest.state_filter:type_name -> forgepoint.serving.v1.ModelState
	19, // 16: forgepoint.serving.v1.ListLoadedModelsResponse.models:type_name -> forgepoint.serving.v1.ModelStatus
	33, // 17: forgepoint.serving.v1.ListLoadedModelsResponse.pagination:type_name -> forgepoint.common.v1.PaginationResponse
	19, // 18: forgepoint.serving.v1.LoadModelResponse.status:type_name -> forgepoint.serving.v1.ModelStatus
	19, // 19: forgepoint.serving.v1.UnloadModelResponse.status:type_name -> forgepoint.serving.v1.ModelStatus
	1,  // 20: forgepoint.serving.v1.ModelStatus.state:type_name -> forgepoint.serving.v1.ModelState
	30, // 21: forgepoint.serving.v1.ModelStatus.updated_at:type_name -> google.protobuf.Timestamp
	19, // 22: forgepoint.serving.v1.GetModelStatusResponse.status:type_name -> forgepoint.serving.v1.ModelStatus
	6,  // 23: forgepoint.serving.v1.GetServingMetricsResponse.metrics:type_name -> forgepoint.serving.v1.ServingMetrics
	2,  // 24: forgepoint.serving.v1.HealthCheckResponse.status:type_name -> forgepoint.serving.v1.HealthStatus
	1,  // 25: forgepoint.serving.v1.HealthCheckResponse.model_state:type_name -> forgepoint.serving.v1.ModelState
	31, // 26: forgepoint.serving.v1.HealthCheckResponse.last_inference_latency:type_name -> google.protobuf.Duration
	3,  // 27: forgepoint.serving.v1.PredictRequest.InputsEntry.value:type_name -> forgepoint.serving.v1.TensorData
	3,  // 28: forgepoint.serving.v1.PredictResponse.OutputsEntry.value:type_name -> forgepoint.serving.v1.TensorData
	3,  // 29: forgepoint.serving.v1.StreamPredictRequest.InputsEntry.value:type_name -> forgepoint.serving.v1.TensorData
	3,  // 30: forgepoint.serving.v1.StreamPredictResponse.OutputsEntry.value:type_name -> forgepoint.serving.v1.TensorData
	7,  // 31: forgepoint.serving.v1.ModelServingService.Predict:input_type -> forgepoint.serving.v1.PredictRequest
	9,  // 32: forgepoint.serving.v1.ModelServingService.StreamPredict:input_type -> forgepoint.serving.v1.StreamPredictRequest
	11, // 33: forgepoint.serving.v1.ModelServingService.GetModelInfo:input_type -> forgepoint.serving.v1.GetModelInfoRequest
	13, // 34: forgepoint.serving.v1.ModelServingService.ListLoadedModels:input_type -> forgepoint.serving.v1.ListLoadedModelsRequest
	15, // 35: forgepoint.serving.v1.ModelServingService.LoadModel:input_type -> forgepoint.serving.v1.LoadModelRequest
	17, // 36: forgepoint.serving.v1.ModelServingService.UnloadModel:input_type -> forgepoint.serving.v1.UnloadModelRequest
	20, // 37: forgepoint.serving.v1.ModelServingService.GetModelStatus:input_type -> forgepoint.serving.v1.GetModelStatusRequest
	22, // 38: forgepoint.serving.v1.ModelServingService.GetServingMetrics:input_type -> forgepoint.serving.v1.GetServingMetricsRequest
	24, // 39: forgepoint.serving.v1.ModelServingService.HealthCheck:input_type -> forgepoint.serving.v1.HealthCheckRequest
	8,  // 40: forgepoint.serving.v1.ModelServingService.Predict:output_type -> forgepoint.serving.v1.PredictResponse
	10, // 41: forgepoint.serving.v1.ModelServingService.StreamPredict:output_type -> forgepoint.serving.v1.StreamPredictResponse
	12, // 42: forgepoint.serving.v1.ModelServingService.GetModelInfo:output_type -> forgepoint.serving.v1.GetModelInfoResponse
	14, // 43: forgepoint.serving.v1.ModelServingService.ListLoadedModels:output_type -> forgepoint.serving.v1.ListLoadedModelsResponse
	16, // 44: forgepoint.serving.v1.ModelServingService.LoadModel:output_type -> forgepoint.serving.v1.LoadModelResponse
	18, // 45: forgepoint.serving.v1.ModelServingService.UnloadModel:output_type -> forgepoint.serving.v1.UnloadModelResponse
	21, // 46: forgepoint.serving.v1.ModelServingService.GetModelStatus:output_type -> forgepoint.serving.v1.GetModelStatusResponse
	23, // 47: forgepoint.serving.v1.ModelServingService.GetServingMetrics:output_type -> forgepoint.serving.v1.GetServingMetricsResponse
	25, // 48: forgepoint.serving.v1.ModelServingService.HealthCheck:output_type -> forgepoint.serving.v1.HealthCheckResponse
	40, // [40:49] is the sub-list for method output_type
	31, // [31:40] is the sub-list for method input_type
	31, // [31:31] is the sub-list for extension type_name
	31, // [31:31] is the sub-list for extension extendee
	0,  // [0:31] is the sub-list for field type_name
}

func init() { file_forgepoint_serving_v1_serving_proto_init() }
func file_forgepoint_serving_v1_serving_proto_init() {
	if File_forgepoint_serving_v1_serving_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_forgepoint_serving_v1_serving_proto_rawDesc), len(file_forgepoint_serving_v1_serving_proto_rawDesc)),
			NumEnums:      3,
			NumMessages:   27,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_forgepoint_serving_v1_serving_proto_goTypes,
		DependencyIndexes: file_forgepoint_serving_v1_serving_proto_depIdxs,
		EnumInfos:         file_forgepoint_serving_v1_serving_proto_enumTypes,
		MessageInfos:      file_forgepoint_serving_v1_serving_proto_msgTypes,
	}.Build()
	File_forgepoint_serving_v1_serving_proto = out.File
	file_forgepoint_serving_v1_serving_proto_goTypes = nil
	file_forgepoint_serving_v1_serving_proto_depIdxs = nil
}
