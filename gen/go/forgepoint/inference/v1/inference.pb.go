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
//   - GetModelInfo (the HTTP GET /v1/models/{model}/info backing RPC)
//
// WHAT IS DELIBERATELY NOT HERE (event payloads live in forgepoint.events.v1):
//   This file used to declare its OWN InferenceCompletedEvent / InferenceFailedEvent
//   NATS payloads. They are GONE. An adversarial cross-service review found the
//   event schemas were authored in parallel and DISAGREED — most damningly, this
//   gateway AND model-serving both claimed the "an inference happened, bill it"
//   event (inference.InferenceCompletedEvent vs serving.PredictionCompletedEvent),
//   so Billing could double-meter. The platform now has ONE canonical event
//   contract: forgepoint/events/v1/events.proto. The gateway publishes
//   events.InferenceCompleted / events.InferenceFailed from there and DELETES its
//   local copies. This proto keeps only its RPC request/response/domain types —
//   the SYNC contract — and depends on the events package for the ASYNC contract.
//   WHY the split: the event schema is a separately-versioned PUBLISHED contract
//   (schema-registry thinking) that must not be coupled to any one service's API
//   types; see the long DESIGN block at the top of events.proto.
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
// EVENTS PRODUCED (canonical — payloads defined in forgepoint.events.v1):
//   fp.inference.completed → events.InferenceCompleted  (THE single canonical
//        inference event — the gateway alone knows latency, the version that
//        ACTUALLY served post-split, the billed principal api_key_id, is_canary,
//        and token_count. serving emits NO competing event.)
//   fp.inference.failed    → events.InferenceFailed
//   Consumed by: Billing (meter requests + tokens, dedupe on request_id),
//                Experiment Tracker (A/B outcomes per served version),
//                Model Monitor (drift windows; request_id is the ground-truth
//                join key for delayed labels).
//
// EVENTS CONSUMED (all canonical events.v1 payloads; gateway is a pure reactor
// on the control plane — the routing table is driven by events, not API writes):
//   fp.pipelines.model.deployed   → events.ModelDeployed   → ADD a route target
//        (the deploy SAGA owns this; the gateway uses its server-resolved
//        endpoint + initial weight_bps — never a client-supplied endpoint).
//   fp.pipelines.model.undeployed → events.ModelUndeployed → REMOVE a route target
//   fp.models.promoted            → events.ModelPromoted   → repoint traffic to
//        the new PRODUCTION version (and tear down the auto-demoted old one).
//   fp.models.archived            → events.ModelArchived   → DROP the route.
//   fp.billing.quota.exceeded     → events.QuotaExceeded   → flip the team's
//        Redis quota cache to "blocked" so subsequent predicts pre-flight-reject
//        with FAILURE_REASON_QUOTA_EXCEEDED (eventual consistency is fine here).
//
// VERSIONING: Package path includes v1 following Buf/Google convention.
// Breaking changes require a new forgepoint.inference.v2 package.
// ============================================================================

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.11
// 	protoc        (unknown)
// source: forgepoint/inference/v1/inference.proto

package inferencev1

import (
	v1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	v11 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
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

// ============================================================================
// DataType
// ============================================================================
//
// WHY an explicit dtype enum: a model input is just bytes on the wire; the
// receiver must know how to interpret those bytes (4-byte float vs 8-byte
// float vs int). Carrying the dtype with the tensor makes the payload
// self-describing — the gateway can validate against the model's declared
// input schema before forwarding, catching client mistakes (sending int32
// where float32 is expected) at the edge with a clear INVALID_ARGUMENT
// instead of a cryptic failure deep in the ONNX runtime.
//
// WHY only a small numeric set: Forgepoint serves tiny CPU ONNX models
// (tabular/iris-class). float32 covers ~all feature vectors; we add a few
// common types for completeness. STRING is included for categorical/text
// inputs that some models embed. Adding more later is backward compatible
// (appending enum values never breaks the wire format).
// ============================================================================
type DataType int32

const (
	// Required zero value (Buf ENUM_ZERO_VALUE_SUFFIX). Treat as "unset" —
	// the gateway rejects tensors with an unspecified dtype.
	DataType_DATA_TYPE_UNSPECIFIED DataType = 0
	// IEEE-754 32-bit float. The default for ML feature vectors.
	DataType_DATA_TYPE_FLOAT32 DataType = 1
	// IEEE-754 64-bit float. For models that need double precision.
	DataType_DATA_TYPE_FLOAT64 DataType = 2
	// Signed 32-bit integer (e.g., categorical indices, token IDs).
	DataType_DATA_TYPE_INT32 DataType = 3
	// Signed 64-bit integer.
	DataType_DATA_TYPE_INT64 DataType = 4
	// UTF-8 strings (categorical/text features). Encoded length-prefixed in
	// the bytes payload; the runtime decodes per the model's string handling.
	DataType_DATA_TYPE_STRING DataType = 5
	// Boolean (1 byte per element). For binary flags.
	DataType_DATA_TYPE_BOOL DataType = 6
)

// Enum value maps for DataType.
var (
	DataType_name = map[int32]string{
		0: "DATA_TYPE_UNSPECIFIED",
		1: "DATA_TYPE_FLOAT32",
		2: "DATA_TYPE_FLOAT64",
		3: "DATA_TYPE_INT32",
		4: "DATA_TYPE_INT64",
		5: "DATA_TYPE_STRING",
		6: "DATA_TYPE_BOOL",
	}
	DataType_value = map[string]int32{
		"DATA_TYPE_UNSPECIFIED": 0,
		"DATA_TYPE_FLOAT32":     1,
		"DATA_TYPE_FLOAT64":     2,
		"DATA_TYPE_INT32":       3,
		"DATA_TYPE_INT64":       4,
		"DATA_TYPE_STRING":      5,
		"DATA_TYPE_BOOL":        6,
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
	return file_forgepoint_inference_v1_inference_proto_enumTypes[0].Descriptor()
}

func (DataType) Type() protoreflect.EnumType {
	return &file_forgepoint_inference_v1_inference_proto_enumTypes[0]
}

func (x DataType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use DataType.Descriptor instead.
func (DataType) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{0}
}

// ============================================================================
// TargetStatus
// ============================================================================
//
// WHY a status enum vs a bare bool "active": a canary rollout has more than
// two states. DRAINING (let in-flight finish, send no new traffic) is what
// makes a graceful rollback possible — flipping straight to removed would
// kill in-flight predictions. This mirrors Envoy/K8s endpoint draining.
// ============================================================================
type TargetStatus int32

const (
	// Required zero value.
	TargetStatus_TARGET_STATUS_UNSPECIFIED TargetStatus = 0
	// Receiving traffic per its weight_bps.
	TargetStatus_TARGET_STATUS_ACTIVE TargetStatus = 1
	// No new traffic; in-flight requests allowed to complete. Used during
	// rollback/promotion before full removal. Effective weight is treated as 0.
	TargetStatus_TARGET_STATUS_DRAINING TargetStatus = 2
	// Quarantined by the circuit breaker (backend unhealthy). The gateway sets
	// this automatically; it is not a client-settable state.
	TargetStatus_TARGET_STATUS_UNHEALTHY TargetStatus = 3
)

// Enum value maps for TargetStatus.
var (
	TargetStatus_name = map[int32]string{
		0: "TARGET_STATUS_UNSPECIFIED",
		1: "TARGET_STATUS_ACTIVE",
		2: "TARGET_STATUS_DRAINING",
		3: "TARGET_STATUS_UNHEALTHY",
	}
	TargetStatus_value = map[string]int32{
		"TARGET_STATUS_UNSPECIFIED": 0,
		"TARGET_STATUS_ACTIVE":      1,
		"TARGET_STATUS_DRAINING":    2,
		"TARGET_STATUS_UNHEALTHY":   3,
	}
)

func (x TargetStatus) Enum() *TargetStatus {
	p := new(TargetStatus)
	*p = x
	return p
}

func (x TargetStatus) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (TargetStatus) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_inference_v1_inference_proto_enumTypes[1].Descriptor()
}

func (TargetStatus) Type() protoreflect.EnumType {
	return &file_forgepoint_inference_v1_inference_proto_enumTypes[1]
}

func (x TargetStatus) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use TargetStatus.Descriptor instead.
func (TargetStatus) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{1}
}

// ============================================================================
// CircuitBreakerState
// ============================================================================
type CircuitBreakerState int32

const (
	// Required zero value.
	CircuitBreakerState_CIRCUIT_BREAKER_STATE_UNSPECIFIED CircuitBreakerState = 0
	// Normal operation: requests pass through, failures are counted.
	CircuitBreakerState_CIRCUIT_BREAKER_STATE_CLOSED CircuitBreakerState = 1
	// Tripped: requests fail fast without hitting the backend.
	CircuitBreakerState_CIRCUIT_BREAKER_STATE_OPEN CircuitBreakerState = 2
	// Probing: a limited number of trial requests determine recovery.
	CircuitBreakerState_CIRCUIT_BREAKER_STATE_HALF_OPEN CircuitBreakerState = 3
)

// Enum value maps for CircuitBreakerState.
var (
	CircuitBreakerState_name = map[int32]string{
		0: "CIRCUIT_BREAKER_STATE_UNSPECIFIED",
		1: "CIRCUIT_BREAKER_STATE_CLOSED",
		2: "CIRCUIT_BREAKER_STATE_OPEN",
		3: "CIRCUIT_BREAKER_STATE_HALF_OPEN",
	}
	CircuitBreakerState_value = map[string]int32{
		"CIRCUIT_BREAKER_STATE_UNSPECIFIED": 0,
		"CIRCUIT_BREAKER_STATE_CLOSED":      1,
		"CIRCUIT_BREAKER_STATE_OPEN":        2,
		"CIRCUIT_BREAKER_STATE_HALF_OPEN":   3,
	}
)

func (x CircuitBreakerState) Enum() *CircuitBreakerState {
	p := new(CircuitBreakerState)
	*p = x
	return p
}

func (x CircuitBreakerState) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (CircuitBreakerState) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_inference_v1_inference_proto_enumTypes[2].Descriptor()
}

func (CircuitBreakerState) Type() protoreflect.EnumType {
	return &file_forgepoint_inference_v1_inference_proto_enumTypes[2]
}

func (x CircuitBreakerState) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use CircuitBreakerState.Descriptor instead.
func (CircuitBreakerState) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{2}
}

// ============================================================================
// TensorData
// ============================================================================
//
// WHY raw bytes + shape + dtype instead of repeated float / repeated double:
//   - PERFORMANCE: a 1000-feature vector as `repeated float` is 1000 protobuf
//     varint-tagged fields; as packed bytes it's a single length-delimited
//     blob the runtime can memcpy straight into a tensor. This is how every
//     serious inference protocol (KServe v2, TF Serving, Triton) does it.
//   - ZERO-COPY: the gateway forwards the bytes to model-serving without
//     deserializing the numbers it doesn't need to inspect — it only reads
//     shape/dtype for validation. Cheaper gateway = lower added latency.
//   - DTYPE FLEXIBILITY: one message type carries float/int/string tensors;
//     no separate FloatTensor / IntTensor messages.
//
// TRADEOFF: bytes are opaque to JSON/grpcurl — you can't eyeball the numbers.
// For the external HTTP API the gateway accepts/returns JSON arrays and
// converts; this binary form is the efficient internal/gRPC representation.
//
// INTERVIEW NOTE: be ready to explain "why not repeated float?" — answer:
// wire size + zero-copy forwarding. shape carries dimensions (e.g., [1, 4]
// for one iris sample of 4 features); data length must equal product(shape) *
// sizeof(dtype), which the gateway validates.
// ============================================================================
type TensorData struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Tensor dimensions, outermost first. E.g., [1, 4] = batch of 1, 4 features.
	// An empty shape denotes a scalar. The gateway validates that
	// len(data) == product(shape) * elem_size(dtype).
	Shape []int64 `protobuf:"varint,1,rep,packed,name=shape,proto3" json:"shape,omitempty"`
	// Element type of the bytes in `data`. Required (non-UNSPECIFIED).
	Dtype DataType `protobuf:"varint,2,opt,name=dtype,proto3,enum=forgepoint.inference.v1.DataType" json:"dtype,omitempty"`
	// The raw tensor contents, densely packed in row-major (C) order for the
	// given dtype. Opaque to the gateway except for length validation.
	Data          []byte `protobuf:"bytes,3,opt,name=data,proto3" json:"data,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *TensorData) Reset() {
	*x = TensorData{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TensorData) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TensorData) ProtoMessage() {}

func (x *TensorData) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[0]
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
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{0}
}

func (x *TensorData) GetShape() []int64 {
	if x != nil {
		return x.Shape
	}
	return nil
}

func (x *TensorData) GetDtype() DataType {
	if x != nil {
		return x.Dtype
	}
	return DataType_DATA_TYPE_UNSPECIFIED
}

func (x *TensorData) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

// ============================================================================
// RouteTarget
// ============================================================================
//
// WHY weights in BASIS POINTS (per-10,000) not percent: integer basis points
// let us express fine-grained canaries (e.g., 0.5% = 50 bps) exactly, with no
// float rounding when the weights must sum to a fixed total. This is the same
// reason finance uses bps. The set of a model's RouteTargets must have
// weight_bps summing to 10000 (100%); the gateway enforces this on write.
//
// WHY status here: a target can exist but be temporarily DRAINING (taking no
// new traffic while in-flight requests finish) during a rollback. Status is
// SERVER-authoritative — clients propose weights, the gateway owns lifecycle.
// ============================================================================
type RouteTarget struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The model version this target points to (e.g., "v3", "2024-06-01-a").
	// Combined with the parent Route's model_name, this identifies a backend.
	Version string `protobuf:"bytes,1,opt,name=version,proto3" json:"version,omitempty"`
	// The model-serving endpoint for this version (e.g., a K8s Service DNS name
	// like "iris-v3.fp-models.svc:9090"). SERVER-authoritative: it is resolved
	// from the ModelDeployed event the gateway consumed, NOT supplied by API
	// clients — accepting an arbitrary endpoint from a client would be an SSRF
	// hole (the gateway would forward to anywhere). Read-only in responses.
	Endpoint string `protobuf:"bytes,2,opt,name=endpoint,proto3" json:"endpoint,omitempty"`
	// Traffic share for this version in basis points (0–10000). All targets of
	// a model must sum to 10000. E.g., stable=9000 (90%), canary=1000 (10%).
	WeightBps int32 `protobuf:"varint,3,opt,name=weight_bps,json=weightBps,proto3" json:"weight_bps,omitempty"`
	// Lifecycle status of this target. SERVER-authoritative.
	Status        TargetStatus `protobuf:"varint,4,opt,name=status,proto3,enum=forgepoint.inference.v1.TargetStatus" json:"status,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *RouteTarget) Reset() {
	*x = RouteTarget{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RouteTarget) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RouteTarget) ProtoMessage() {}

func (x *RouteTarget) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RouteTarget.ProtoReflect.Descriptor instead.
func (*RouteTarget) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{1}
}

func (x *RouteTarget) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *RouteTarget) GetEndpoint() string {
	if x != nil {
		return x.Endpoint
	}
	return ""
}

func (x *RouteTarget) GetWeightBps() int32 {
	if x != nil {
		return x.WeightBps
	}
	return 0
}

func (x *RouteTarget) GetStatus() TargetStatus {
	if x != nil {
		return x.Status
	}
	return TargetStatus_TARGET_STATUS_UNSPECIFIED
}

// ============================================================================
// Route
// ============================================================================
//
// WHY a Route per model (not per version): clients call by MODEL NAME (e.g.,
// "fraud-detector"); the gateway hides which versions exist behind it. A Route
// is the routing-table row for one model — the list of candidate versions and
// how traffic splits among them. This indirection is the whole point of a
// gateway: the client's contract ("predict with fraud-detector") is stable
// while we shift traffic between v1/v2/v3 underneath.
//
// HOW IT'S POPULATED: the routing table is driven by EVENTS, not API writes in
// the common case. The gateway consumes fp.pipelines.model.deployed (the saga
// promotes a version) and adds/updates targets. The admin RPCs (UpsertRoute /
// SetTrafficSplit) are the manual override / break-glass control plane for
// operators and the canary executor.
// ============================================================================
type Route struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The public model name clients call (e.g., "fraud-detector"). Unique key
	// of the routing table.
	ModelName string `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// The candidate versions and their traffic weights. weight_bps across all
	// ACTIVE targets must sum to 10000.
	Targets []*RouteTarget `protobuf:"bytes,2,rep,name=targets,proto3" json:"targets,omitempty"`
	// When this route was last changed (deploy event or admin write).
	// SERVER-authoritative timestamp — never accepted on write.
	UpdatedAt     *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=updated_at,json=updatedAt,proto3" json:"updated_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Route) Reset() {
	*x = Route{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Route) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Route) ProtoMessage() {}

func (x *Route) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Route.ProtoReflect.Descriptor instead.
func (*Route) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{2}
}

func (x *Route) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *Route) GetTargets() []*RouteTarget {
	if x != nil {
		return x.Targets
	}
	return nil
}

func (x *Route) GetUpdatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.UpdatedAt
	}
	return nil
}

// ============================================================================
// CircuitState
// ============================================================================
//
// WHY expose circuit state at all: the breaker is internal machinery, but
// OPERATORS and dashboards need to see it ("why is v3 getting no traffic?
// because its breaker is OPEN"). Exposing it read-only turns an invisible
// failure mode into an observable one. The metric
// fp_gateway_circuit_breaker_state mirrors this for Prometheus.
//
// THE 3-STATE MACHINE (interview-critical):
//
//	┌─────────┐  failures ≥ threshold   ┌──────┐
//	│ CLOSED  │ ──────────────────────► │ OPEN │
//	│ (normal)│                          │(fail │
//	└─────────┘ ◄──── successes ≥ ─────┐ │ fast)│
//	     ▲       success_threshold     │ └──────┘
//	     │                             │     │ after open_timeout
//	     │                        ┌──────────┐│
//	     └────── any failure ─────│ HALF_OPEN│◄ (send 1 probe)
//	                              └──────────┘
//
//	CLOSED: requests flow; count failures. Too many → OPEN.
//	OPEN: reject immediately (fail fast) for open_timeout; don't hammer a
//	      sick backend. After the timeout → HALF_OPEN.
//	HALF_OPEN: allow a few probe requests. If they succeed → CLOSED (recovered);
//	      if any fails → back to OPEN (still sick).
//
// ============================================================================
type CircuitState struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The model + version (backend) this breaker guards.
	ModelName string `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	Version   string `protobuf:"bytes,2,opt,name=version,proto3" json:"version,omitempty"`
	// Current breaker state.
	State CircuitBreakerState `protobuf:"varint,3,opt,name=state,proto3,enum=forgepoint.inference.v1.CircuitBreakerState" json:"state,omitempty"`
	// Consecutive failures observed in the current window (drives the trip
	// decision). SERVER-authoritative observability field.
	ConsecutiveFailures int32 `protobuf:"varint,4,opt,name=consecutive_failures,json=consecutiveFailures,proto3" json:"consecutive_failures,omitempty"`
	// When the breaker last changed state. With state=OPEN this anchors the
	// open_timeout countdown to HALF_OPEN. SERVER-authoritative.
	LastTransitionAt *timestamppb.Timestamp `protobuf:"bytes,5,opt,name=last_transition_at,json=lastTransitionAt,proto3" json:"last_transition_at,omitempty"`
	unknownFields    protoimpl.UnknownFields
	sizeCache        protoimpl.SizeCache
}

func (x *CircuitState) Reset() {
	*x = CircuitState{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CircuitState) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CircuitState) ProtoMessage() {}

func (x *CircuitState) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CircuitState.ProtoReflect.Descriptor instead.
func (*CircuitState) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{3}
}

func (x *CircuitState) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *CircuitState) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *CircuitState) GetState() CircuitBreakerState {
	if x != nil {
		return x.State
	}
	return CircuitBreakerState_CIRCUIT_BREAKER_STATE_UNSPECIFIED
}

func (x *CircuitState) GetConsecutiveFailures() int32 {
	if x != nil {
		return x.ConsecutiveFailures
	}
	return 0
}

func (x *CircuitState) GetLastTransitionAt() *timestamppb.Timestamp {
	if x != nil {
		return x.LastTransitionAt
	}
	return nil
}

// PredictRequest is the hot-path message: one inference call.
//
// WHY the client may NOT pick the served version freely: if a client could
// pin every request to "v-stable", canary versions would never receive the
// traffic they need to be evaluated — defeating the whole traffic-split
// pattern. So we model TWO knobs with different trust levels:
//   - model_name (required): WHICH model. Client-chosen, always honored.
//   - version_override (optional): pin a specific version. This is a
//     PRIVILEGED escape hatch (debugging a specific version, the canary
//     executor probing the new version). The gateway authorizes it; ordinary
//     traffic leaves it empty and is split by weight. Documented as override
//     so it's obvious it bypasses canary splitting.
type PredictRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The public model name to predict with (e.g., "fraud-detector"). Required.
	ModelName string `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// The named input tensors, keyed by the model's input names
	// (e.g., {"features": TensorData{...}}). The gateway validates names/shapes
	// against the model's declared input schema before forwarding.
	Inputs map[string]*TensorData `protobuf:"bytes,2,rep,name=inputs,proto3" json:"inputs,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	// OPTIONAL privileged override: force a specific version, bypassing weighted
	// traffic splitting. Empty = let the gateway split by weight (the norm).
	// Requires elevated permission; used by the canary executor and for debug.
	VersionOverride string `protobuf:"bytes,3,opt,name=version_override,json=versionOverride,proto3" json:"version_override,omitempty"`
	// Idempotency key for safe retries. WHY on a PREDICT (a read-ish op)?
	// Predictions can have side effects downstream — every call emits an
	// InferenceCompleted event that Billing meters. If a client retries after a
	// timeout, a duplicate key lets the gateway recognize the retry and avoid
	// DOUBLE-BILLING / double-emitting. The gateway caches recent keys (short
	// TTL in Redis). Empty = no idempotency guarantee (gateway treats as unique).
	IdempotencyKey string `protobuf:"bytes,4,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *PredictRequest) Reset() {
	*x = PredictRequest{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PredictRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PredictRequest) ProtoMessage() {}

func (x *PredictRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[4]
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
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{4}
}

func (x *PredictRequest) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *PredictRequest) GetInputs() map[string]*TensorData {
	if x != nil {
		return x.Inputs
	}
	return nil
}

func (x *PredictRequest) GetVersionOverride() string {
	if x != nil {
		return x.VersionOverride
	}
	return ""
}

func (x *PredictRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// PredictResponse returns the model outputs plus the metadata that makes the
// gateway's decisions OBSERVABLE to the caller.
//
// WHY echo served_version: the client asked for a model, not a version, but it
// MUST be told which version actually answered — for A/B analysis ("which
// variant produced this?"), for debugging, and so the client can correlate
// with the InferenceCompleted event. This is SERVER-authoritative: it reflects
// what the gateway chose, never what the client requested.
type PredictResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The named output tensors, keyed by the model's output names
	// (e.g., {"probabilities": TensorData{...}}).
	Outputs map[string]*TensorData `protobuf:"bytes,1,rep,name=outputs,proto3" json:"outputs,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	// Which model version actually served this request. SERVER-authoritative —
	// the result of weighted traffic splitting (or version_override).
	ServedVersion string `protobuf:"bytes,2,opt,name=served_version,json=servedVersion,proto3" json:"served_version,omitempty"`
	// End-to-end latency the gateway observed for the backend call. WHY Duration
	// not int64 ms: google.protobuf.Duration is the typed well-known unit, has
	// sub-ms precision, and avoids "is this ms or µs?" ambiguity. The event's
	// latency_ms is a denormalized convenience for metering/dashboards.
	Latency *durationpb.Duration `protobuf:"bytes,3,opt,name=latency,proto3" json:"latency,omitempty"`
	// Unique ID the gateway assigned to this request (UUID). Returned to the
	// client AND carried on the InferenceCompleted event, so the client can
	// join its logs to platform events/traces. SERVER-authoritative.
	RequestId     string `protobuf:"bytes,4,opt,name=request_id,json=requestId,proto3" json:"request_id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *PredictResponse) Reset() {
	*x = PredictResponse{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PredictResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PredictResponse) ProtoMessage() {}

func (x *PredictResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[5]
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
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{5}
}

func (x *PredictResponse) GetOutputs() map[string]*TensorData {
	if x != nil {
		return x.Outputs
	}
	return nil
}

func (x *PredictResponse) GetServedVersion() string {
	if x != nil {
		return x.ServedVersion
	}
	return ""
}

func (x *PredictResponse) GetLatency() *durationpb.Duration {
	if x != nil {
		return x.Latency
	}
	return nil
}

func (x *PredictResponse) GetRequestId() string {
	if x != nil {
		return x.RequestId
	}
	return ""
}

// BatchPredictRequest carries MANY independent inputs in one call.
//
// WHY a unary batch RPC in addition to streaming: for a bounded, modest batch
// (a few hundred rows) a single request/response is simpler for clients than
// managing a stream, and lets the gateway fan out to the backend efficiently.
// For UNBOUNDED or very large batches, use StreamPredict (server-streaming)
// so results flow back incrementally without buffering everything in memory.
// We offer BOTH and document when to reach for which (see service comments).
type BatchPredictRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The public model name. Required. One model per batch (a batch is N rows
	// for the SAME model, not a mix).
	ModelName string `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// The batch items. Each is one independent prediction's named inputs.
	// HARD CAP: the gateway rejects more than 256 items server-side with
	// INVALID_ARGUMENT, to bound memory/latency of a single unary call (this is a
	// MAX, not a tunable default — it is part of the contract, not a hint). For
	// larger jobs use StreamPredict (cap 10000) so results stream incrementally.
	Items []*BatchPredictItem `protobuf:"bytes,2,rep,name=items,proto3" json:"items,omitempty"`
	// OPTIONAL privileged version override applied to the WHOLE batch (debug /
	// canary probing). Empty = weighted split, decided once per item.
	VersionOverride string `protobuf:"bytes,3,opt,name=version_override,json=versionOverride,proto3" json:"version_override,omitempty"`
	// Idempotency key for the whole batch (safe retry without double-billing).
	IdempotencyKey string `protobuf:"bytes,4,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *BatchPredictRequest) Reset() {
	*x = BatchPredictRequest{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *BatchPredictRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchPredictRequest) ProtoMessage() {}

func (x *BatchPredictRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchPredictRequest.ProtoReflect.Descriptor instead.
func (*BatchPredictRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{6}
}

func (x *BatchPredictRequest) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *BatchPredictRequest) GetItems() []*BatchPredictItem {
	if x != nil {
		return x.Items
	}
	return nil
}

func (x *BatchPredictRequest) GetVersionOverride() string {
	if x != nil {
		return x.VersionOverride
	}
	return ""
}

func (x *BatchPredictRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// BatchPredictItem is one row in a batch — its inputs plus a client-supplied
// id so the client can correlate each result back to its request (order is
// preserved, but an explicit id is robust to partial failures / reordering).
type BatchPredictItem struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Client-chosen correlation id for this item (e.g., a row key). Echoed back
	// in the matching BatchPredictResult. Opaque to the gateway.
	ItemId string `protobuf:"bytes,1,opt,name=item_id,json=itemId,proto3" json:"item_id,omitempty"`
	// Named input tensors for this single prediction (same shape rules as
	// PredictRequest.inputs).
	Inputs        map[string]*TensorData `protobuf:"bytes,2,rep,name=inputs,proto3" json:"inputs,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *BatchPredictItem) Reset() {
	*x = BatchPredictItem{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *BatchPredictItem) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchPredictItem) ProtoMessage() {}

func (x *BatchPredictItem) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchPredictItem.ProtoReflect.Descriptor instead.
func (*BatchPredictItem) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{7}
}

func (x *BatchPredictItem) GetItemId() string {
	if x != nil {
		return x.ItemId
	}
	return ""
}

func (x *BatchPredictItem) GetInputs() map[string]*TensorData {
	if x != nil {
		return x.Inputs
	}
	return nil
}

// BatchPredictResponse returns one result per input item.
//
// WHY per-item status instead of failing the whole batch: in a batch, item 7
// being malformed shouldn't discard the other 255 valid predictions. Each
// result carries its own success/error (PARTIAL SUCCESS semantics), which is
// far more useful for bulk scoring jobs than all-or-nothing.
type BatchPredictResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// One result per request item, correlated by item_id. Same length/order as
	// the request items (barring explicit reordering, item_id is authoritative).
	Results []*BatchPredictResult `protobuf:"bytes,1,rep,name=results,proto3" json:"results,omitempty"`
	// Unique ID for the whole batch call. SERVER-authoritative.
	RequestId     string `protobuf:"bytes,2,opt,name=request_id,json=requestId,proto3" json:"request_id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *BatchPredictResponse) Reset() {
	*x = BatchPredictResponse{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[8]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *BatchPredictResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchPredictResponse) ProtoMessage() {}

func (x *BatchPredictResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[8]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchPredictResponse.ProtoReflect.Descriptor instead.
func (*BatchPredictResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{8}
}

func (x *BatchPredictResponse) GetResults() []*BatchPredictResult {
	if x != nil {
		return x.Results
	}
	return nil
}

func (x *BatchPredictResponse) GetRequestId() string {
	if x != nil {
		return x.RequestId
	}
	return ""
}

// BatchPredictResult is the outcome for a single batch item — either outputs
// or a structured error, never silently dropped.
type BatchPredictResult struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The client's item_id this result corresponds to.
	ItemId string `protobuf:"bytes,1,opt,name=item_id,json=itemId,proto3" json:"item_id,omitempty"`
	// The output tensors for this item. Empty if `error` is set.
	Outputs map[string]*TensorData `protobuf:"bytes,2,rep,name=outputs,proto3" json:"outputs,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	// Which version served THIS item. Items in one batch can be served by
	// different versions (weighted split is per-item). SERVER-authoritative.
	ServedVersion string `protobuf:"bytes,3,opt,name=served_version,json=servedVersion,proto3" json:"served_version,omitempty"`
	// Per-item error if this prediction failed. Nil/unset on success. Reuses
	// the common structured error type so clients handle it uniformly with
	// single-predict errors. PARTIAL-SUCCESS marker for this item.
	Error *v1.ErrorDetail `protobuf:"bytes,4,opt,name=error,proto3" json:"error,omitempty"`
	// The gateway's resilience classification for THIS item's failure (which
	// pattern fired). INFERENCE_FAILURE_REASON_UNSPECIFIED on success. WHY surface
	// it on a batch item and not on unary Predict: a unary failure rides a gRPC
	// status code (RESOURCE_EXHAUSTED, UNAVAILABLE, ...) that already carries the
	// class; a batch is PARTIAL-SUCCESS over a single OK envelope, so each item
	// must carry its own machine-readable cause here for a bulk-scoring client to
	// decide per-item retry policy (retry a TIMEOUT, never an INVALID_INPUT).
	// WHY the events enum (not a private mirror): this is the SAME failure
	// taxonomy the gateway publishes on events.InferenceFailed, so reusing
	// events.InferenceFailureReason makes the sync result and the async event
	// speak one vocabulary with no divergent-copy / mapping risk.
	FailureReason v11.InferenceFailureReason `protobuf:"varint,5,opt,name=failure_reason,json=failureReason,proto3,enum=forgepoint.events.v1.InferenceFailureReason" json:"failure_reason,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *BatchPredictResult) Reset() {
	*x = BatchPredictResult{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[9]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *BatchPredictResult) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchPredictResult) ProtoMessage() {}

func (x *BatchPredictResult) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[9]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchPredictResult.ProtoReflect.Descriptor instead.
func (*BatchPredictResult) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{9}
}

func (x *BatchPredictResult) GetItemId() string {
	if x != nil {
		return x.ItemId
	}
	return ""
}

func (x *BatchPredictResult) GetOutputs() map[string]*TensorData {
	if x != nil {
		return x.Outputs
	}
	return nil
}

func (x *BatchPredictResult) GetServedVersion() string {
	if x != nil {
		return x.ServedVersion
	}
	return ""
}

func (x *BatchPredictResult) GetError() *v1.ErrorDetail {
	if x != nil {
		return x.Error
	}
	return nil
}

func (x *BatchPredictResult) GetFailureReason() v11.InferenceFailureReason {
	if x != nil {
		return x.FailureReason
	}
	return v11.InferenceFailureReason(0)
}

// StreamPredictRequest is a single request that yields a STREAM of results.
//
// WHY SERVER-STREAMING (1 request → N responses) and not bidi here: the
// client knows all its inputs up front (it's a batch job), so it sends them
// once; the gateway streams results back AS each prediction completes. This
// gives incremental delivery (client starts processing result 1 while result
// 500 is still computing) and bounded gateway memory (no need to hold the
// whole response set). Use this for LARGE batches; use unary BatchPredict for
// small ones.
//
// WHEN BIDI WOULD BE RIGHT (and why we DON'T use it): bidirectional streaming
// fits an OPEN-ENDED feed where the client keeps sending inputs over a long-
// lived connection (e.g., a sensor stream). Forgepoint's online predict is
// request/response and its batch is "submit N, get N back" — neither needs
// the client to keep pushing after the initial request. We deliberately avoid
// bidi's added complexity (flow control on both directions, harder retries).
type StreamPredictRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The public model name. Required.
	ModelName string `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// All batch items to score. The gateway streams a StreamPredictResponse per
	// item as each completes. WHY there is STILL a cap even though responses
	// stream: the REQUEST is unary — the whole `items` list must be received and
	// held before the first result streams back, so an unbounded list is a memory
	// DoS on the request side. The gateway caps this at 10000 items server-side
	// (10x the unary BatchPredict cap; see service docs) and rejects above it with
	// INVALID_ARGUMENT. The bulkhead independently bounds in-flight concurrency to
	// the backend. For truly unbounded feeds, submit multiple stream calls.
	Items []*BatchPredictItem `protobuf:"bytes,2,rep,name=items,proto3" json:"items,omitempty"`
	// OPTIONAL privileged version override for the whole stream.
	VersionOverride string `protobuf:"bytes,3,opt,name=version_override,json=versionOverride,proto3" json:"version_override,omitempty"`
	// Idempotency key for the streamed batch.
	IdempotencyKey string `protobuf:"bytes,4,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *StreamPredictRequest) Reset() {
	*x = StreamPredictRequest{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[10]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *StreamPredictRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StreamPredictRequest) ProtoMessage() {}

func (x *StreamPredictRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[10]
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
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{10}
}

func (x *StreamPredictRequest) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *StreamPredictRequest) GetItems() []*BatchPredictItem {
	if x != nil {
		return x.Items
	}
	return nil
}

func (x *StreamPredictRequest) GetVersionOverride() string {
	if x != nil {
		return x.VersionOverride
	}
	return ""
}

func (x *StreamPredictRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// StreamPredictResponse is ONE message in the response stream — the result for
// a single item, delivered as soon as it's ready.
//
// RPC_RESPONSE_STANDARD_NAME note: a server-streaming RPC's response type must
// still be named "<Rpc>Response"; the streaming is expressed by `stream` on
// the RPC, not by the type name. Each streamed message is one of these.
type StreamPredictResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The result for one item (outputs or per-item error), correlated by
	// item_id. Reuses BatchPredictResult so streamed and batched results share
	// one shape — clients write one result handler for both paths.
	Result *BatchPredictResult `protobuf:"bytes,1,opt,name=result,proto3" json:"result,omitempty"`
	// Unique ID of the overall stream call, repeated on each message so a client
	// can attribute every streamed result to the same call. SERVER-authoritative.
	RequestId     string `protobuf:"bytes,2,opt,name=request_id,json=requestId,proto3" json:"request_id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *StreamPredictResponse) Reset() {
	*x = StreamPredictResponse{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[11]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *StreamPredictResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StreamPredictResponse) ProtoMessage() {}

func (x *StreamPredictResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[11]
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
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{11}
}

func (x *StreamPredictResponse) GetResult() *BatchPredictResult {
	if x != nil {
		return x.Result
	}
	return nil
}

func (x *StreamPredictResponse) GetRequestId() string {
	if x != nil {
		return x.RequestId
	}
	return ""
}

// GetRouteRequest fetches the current routing-table entry for one model.
type GetRouteRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The model whose route to fetch.
	ModelName     string `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetRouteRequest) Reset() {
	*x = GetRouteRequest{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[12]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetRouteRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetRouteRequest) ProtoMessage() {}

func (x *GetRouteRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[12]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetRouteRequest.ProtoReflect.Descriptor instead.
func (*GetRouteRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{12}
}

func (x *GetRouteRequest) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

// GetRouteResponse wraps the Route. WHY wrap (not return Route directly): Buf
// RPC_RESPONSE_STANDARD_NAME, plus forward-compat (we can later add e.g. a
// resolved_at or source field without touching the shared Route domain type).
type GetRouteResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The current route, including all targets and their weights.
	Route         *Route `protobuf:"bytes,1,opt,name=route,proto3" json:"route,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetRouteResponse) Reset() {
	*x = GetRouteResponse{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[13]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetRouteResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetRouteResponse) ProtoMessage() {}

func (x *GetRouteResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[13]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetRouteResponse.ProtoReflect.Descriptor instead.
func (*GetRouteResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{13}
}

func (x *GetRouteResponse) GetRoute() *Route {
	if x != nil {
		return x.Route
	}
	return nil
}

// ListRoutesRequest lists all routing-table entries (the whole routing table),
// paginated. Reuses common pagination for table-wide consistency.
type ListRoutesRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Cursor-based pagination. page_size defaults to 20, capped at 100
	// server-side (see common.proto). Prevents a client from pulling the entire
	// routing table in one unbounded response.
	Pagination    *v1.PaginationRequest `protobuf:"bytes,1,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListRoutesRequest) Reset() {
	*x = ListRoutesRequest{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[14]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListRoutesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListRoutesRequest) ProtoMessage() {}

func (x *ListRoutesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[14]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListRoutesRequest.ProtoReflect.Descriptor instead.
func (*ListRoutesRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{14}
}

func (x *ListRoutesRequest) GetPagination() *v1.PaginationRequest {
	if x != nil {
		return x.Pagination
	}
	return nil
}

type ListRoutesResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The page of routes.
	Routes []*Route `protobuf:"bytes,1,rep,name=routes,proto3" json:"routes,omitempty"`
	// Pagination metadata: next_page_token + total_count.
	Pagination    *v1.PaginationResponse `protobuf:"bytes,2,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListRoutesResponse) Reset() {
	*x = ListRoutesResponse{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[15]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListRoutesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListRoutesResponse) ProtoMessage() {}

func (x *ListRoutesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[15]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListRoutesResponse.ProtoReflect.Descriptor instead.
func (*ListRoutesResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{15}
}

func (x *ListRoutesResponse) GetRoutes() []*Route {
	if x != nil {
		return x.Routes
	}
	return nil
}

func (x *ListRoutesResponse) GetPagination() *v1.PaginationResponse {
	if x != nil {
		return x.Pagination
	}
	return nil
}

// UpsertRouteRequest is the BREAK-GLASS manual control to create/replace a
// route. In normal operation routes are driven by ModelDeployed events; this
// RPC is for operators and the canary executor to override.
//
// SECURITY — what is and isn't accepted (anti mass-assignment):
//   - The caller supplies model_name and the desired targets' version +
//     weight_bps ONLY.
//   - endpoint is NOT honored from the request — the gateway resolves each
//     version's endpoint from its deploy record. Accepting a client endpoint
//     would let a caller make the gateway forward to an arbitrary host (SSRF).
//   - status and updated_at are SERVER-authoritative and ignored on input.
//   - The gateway validates that ACTIVE targets' weight_bps sum to 10000.
type UpsertRouteRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The model whose route to create or replace.
	ModelName string `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// Desired targets. Only `version` and `weight_bps` are read from each;
	// `endpoint`, `status` are server-resolved/ignored (see message doc).
	Targets []*RouteTarget `protobuf:"bytes,2,rep,name=targets,proto3" json:"targets,omitempty"`
	// Idempotency key — UpsertRoute is a mutation; a retried create/replace with
	// the same key must not produce a divergent table or duplicate audit events.
	IdempotencyKey string `protobuf:"bytes,3,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *UpsertRouteRequest) Reset() {
	*x = UpsertRouteRequest{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[16]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpsertRouteRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpsertRouteRequest) ProtoMessage() {}

func (x *UpsertRouteRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[16]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpsertRouteRequest.ProtoReflect.Descriptor instead.
func (*UpsertRouteRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{16}
}

func (x *UpsertRouteRequest) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *UpsertRouteRequest) GetTargets() []*RouteTarget {
	if x != nil {
		return x.Targets
	}
	return nil
}

func (x *UpsertRouteRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// UpsertRouteResponse returns the route AS STORED (with server-resolved
// endpoints, status, and updated_at), so the caller sees the authoritative
// result of its write, not just an echo of its input.
type UpsertRouteResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Route         *Route                 `protobuf:"bytes,1,opt,name=route,proto3" json:"route,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UpsertRouteResponse) Reset() {
	*x = UpsertRouteResponse{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[17]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpsertRouteResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpsertRouteResponse) ProtoMessage() {}

func (x *UpsertRouteResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[17]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpsertRouteResponse.ProtoReflect.Descriptor instead.
func (*UpsertRouteResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{17}
}

func (x *UpsertRouteResponse) GetRoute() *Route {
	if x != nil {
		return x.Route
	}
	return nil
}

// SetTrafficSplitRequest adjusts ONLY the weights of an existing route — the
// canary "dial". WHY a dedicated RPC separate from UpsertRoute: shifting
// traffic (90/10 → 50/50 → 0/100) is the single most common, highest-stakes
// operation in a rollout, and it's done repeatedly by the canary executor as
// metrics pass thresholds. A narrow, purpose-built RPC is safer (you can't
// accidentally drop a target) and clearer in audit logs ("traffic shifted")
// than a full route replace.
type SetTrafficSplitRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The model whose traffic to re-weight.
	ModelName string `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// New weights per version. Each entry must reference a version already
	// present in the route. weight_bps across all referenced ACTIVE targets
	// must sum to 10000. The gateway rejects unknown versions and bad sums.
	Weights []*TrafficWeight `protobuf:"bytes,2,rep,name=weights,proto3" json:"weights,omitempty"`
	// Idempotency key — re-applying the same split (retry) is a no-op, not a
	// double shift.
	IdempotencyKey string `protobuf:"bytes,3,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *SetTrafficSplitRequest) Reset() {
	*x = SetTrafficSplitRequest{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[18]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SetTrafficSplitRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SetTrafficSplitRequest) ProtoMessage() {}

func (x *SetTrafficSplitRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[18]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SetTrafficSplitRequest.ProtoReflect.Descriptor instead.
func (*SetTrafficSplitRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{18}
}

func (x *SetTrafficSplitRequest) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *SetTrafficSplitRequest) GetWeights() []*TrafficWeight {
	if x != nil {
		return x.Weights
	}
	return nil
}

func (x *SetTrafficSplitRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// TrafficWeight is a (version, weight) pair — the minimal payload for the
// traffic dial. Deliberately NOT RouteTarget: SetTrafficSplit must not let a
// caller smuggle in endpoint/status changes, so it accepts only the two
// fields it is allowed to change.
type TrafficWeight struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// An existing version in the route.
	Version string `protobuf:"bytes,1,opt,name=version,proto3" json:"version,omitempty"`
	// New traffic share in basis points (0–10000). Setting 0 effectively drains
	// a version of new traffic without removing it from the route.
	WeightBps     int32 `protobuf:"varint,2,opt,name=weight_bps,json=weightBps,proto3" json:"weight_bps,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *TrafficWeight) Reset() {
	*x = TrafficWeight{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[19]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TrafficWeight) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TrafficWeight) ProtoMessage() {}

func (x *TrafficWeight) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[19]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TrafficWeight.ProtoReflect.Descriptor instead.
func (*TrafficWeight) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{19}
}

func (x *TrafficWeight) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *TrafficWeight) GetWeightBps() int32 {
	if x != nil {
		return x.WeightBps
	}
	return 0
}

// SetTrafficSplitResponse returns the updated route so the caller confirms the
// new weights took effect exactly as intended.
type SetTrafficSplitResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Route         *Route                 `protobuf:"bytes,1,opt,name=route,proto3" json:"route,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SetTrafficSplitResponse) Reset() {
	*x = SetTrafficSplitResponse{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[20]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SetTrafficSplitResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SetTrafficSplitResponse) ProtoMessage() {}

func (x *SetTrafficSplitResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[20]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SetTrafficSplitResponse.ProtoReflect.Descriptor instead.
func (*SetTrafficSplitResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{20}
}

func (x *SetTrafficSplitResponse) GetRoute() *Route {
	if x != nil {
		return x.Route
	}
	return nil
}

// DeleteRouteRequest removes a model from the routing table entirely. After
// this, predict calls for the model fail with NOT_FOUND. Normally driven by a
// ModelUndeployed event; the RPC is the manual override.
type DeleteRouteRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The model to remove from routing.
	ModelName string `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// Idempotency key — deleting an already-deleted route is a safe no-op.
	IdempotencyKey string `protobuf:"bytes,2,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *DeleteRouteRequest) Reset() {
	*x = DeleteRouteRequest{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[21]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DeleteRouteRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteRouteRequest) ProtoMessage() {}

func (x *DeleteRouteRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[21]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteRouteRequest.ProtoReflect.Descriptor instead.
func (*DeleteRouteRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{21}
}

func (x *DeleteRouteRequest) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *DeleteRouteRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// DeleteRouteResponse is intentionally empty. WHY a named empty message vs
// google.protobuf.Empty: Buf RPC_RESPONSE_STANDARD_NAME requires "<Rpc>Response"
// naming, and a named type is forward-compatible (we could later add e.g.
// drained_request_count without changing the RPC signature).
type DeleteRouteResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *DeleteRouteResponse) Reset() {
	*x = DeleteRouteResponse{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[22]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DeleteRouteResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteRouteResponse) ProtoMessage() {}

func (x *DeleteRouteResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[22]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteRouteResponse.ProtoReflect.Descriptor instead.
func (*DeleteRouteResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{22}
}

// GetCircuitStateRequest inspects one backend's breaker. WHY model+version:
// breakers are PER BACKEND (per version), not per model — v2 can be tripped
// while v1 stays healthy, which is exactly what protects you during a bad
// canary.
type GetCircuitStateRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	ModelName     string                 `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	Version       string                 `protobuf:"bytes,2,opt,name=version,proto3" json:"version,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetCircuitStateRequest) Reset() {
	*x = GetCircuitStateRequest{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[23]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetCircuitStateRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetCircuitStateRequest) ProtoMessage() {}

func (x *GetCircuitStateRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[23]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetCircuitStateRequest.ProtoReflect.Descriptor instead.
func (*GetCircuitStateRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{23}
}

func (x *GetCircuitStateRequest) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *GetCircuitStateRequest) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

type GetCircuitStateResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The current breaker state for the requested backend.
	CircuitState  *CircuitState `protobuf:"bytes,1,opt,name=circuit_state,json=circuitState,proto3" json:"circuit_state,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetCircuitStateResponse) Reset() {
	*x = GetCircuitStateResponse{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[24]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetCircuitStateResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetCircuitStateResponse) ProtoMessage() {}

func (x *GetCircuitStateResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[24]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetCircuitStateResponse.ProtoReflect.Descriptor instead.
func (*GetCircuitStateResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{24}
}

func (x *GetCircuitStateResponse) GetCircuitState() *CircuitState {
	if x != nil {
		return x.CircuitState
	}
	return nil
}

// ListCircuitStatesRequest lists breaker states across backends, for
// dashboards/operators. Optional model filter; paginated.
type ListCircuitStatesRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Optional: only breakers for this model. Empty = all backends.
	ModelNameFilter string `protobuf:"bytes,1,opt,name=model_name_filter,json=modelNameFilter,proto3" json:"model_name_filter,omitempty"`
	// Cursor-based pagination (page_size default 20, max 100 server-side).
	Pagination    *v1.PaginationRequest `protobuf:"bytes,2,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListCircuitStatesRequest) Reset() {
	*x = ListCircuitStatesRequest{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[25]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListCircuitStatesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListCircuitStatesRequest) ProtoMessage() {}

func (x *ListCircuitStatesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[25]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListCircuitStatesRequest.ProtoReflect.Descriptor instead.
func (*ListCircuitStatesRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{25}
}

func (x *ListCircuitStatesRequest) GetModelNameFilter() string {
	if x != nil {
		return x.ModelNameFilter
	}
	return ""
}

func (x *ListCircuitStatesRequest) GetPagination() *v1.PaginationRequest {
	if x != nil {
		return x.Pagination
	}
	return nil
}

type ListCircuitStatesResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The page of breaker states.
	CircuitStates []*CircuitState `protobuf:"bytes,1,rep,name=circuit_states,json=circuitStates,proto3" json:"circuit_states,omitempty"`
	// Pagination metadata.
	Pagination    *v1.PaginationResponse `protobuf:"bytes,2,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListCircuitStatesResponse) Reset() {
	*x = ListCircuitStatesResponse{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[26]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListCircuitStatesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListCircuitStatesResponse) ProtoMessage() {}

func (x *ListCircuitStatesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[26]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListCircuitStatesResponse.ProtoReflect.Descriptor instead.
func (*ListCircuitStatesResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{26}
}

func (x *ListCircuitStatesResponse) GetCircuitStates() []*CircuitState {
	if x != nil {
		return x.CircuitStates
	}
	return nil
}

func (x *ListCircuitStatesResponse) GetPagination() *v1.PaginationResponse {
	if x != nil {
		return x.Pagination
	}
	return nil
}

// ============================================================================
// TensorSpec
// ============================================================================
//
// WHY a SPEC (no bytes) distinct from TensorData (carries bytes): GetModelInfo
// publishes the SHAPE of the contract — "input 'features' is FLOAT32 [1,4]" —
// so a client (or the HTTP edge) can validate a request locally before sending
// it. A -1 in a dimension means "dynamic" (e.g., a variable batch axis), the
// usual convention in ONNX/Triton model signatures.
// ============================================================================
type TensorSpec struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The tensor's name in the model signature (e.g., "features"). The key a
	// client uses in PredictRequest.inputs / reads from PredictResponse.outputs.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// Element type the model expects/produces for this tensor.
	Dtype DataType `protobuf:"varint,2,opt,name=dtype,proto3,enum=forgepoint.inference.v1.DataType" json:"dtype,omitempty"`
	// Declared dimensions, outermost first; -1 marks a dynamic axis (e.g., a
	// variable batch size). Empty = scalar.
	Shape         []int64 `protobuf:"varint,3,rep,packed,name=shape,proto3" json:"shape,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *TensorSpec) Reset() {
	*x = TensorSpec{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[27]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TensorSpec) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TensorSpec) ProtoMessage() {}

func (x *TensorSpec) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[27]
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
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{27}
}

func (x *TensorSpec) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *TensorSpec) GetDtype() DataType {
	if x != nil {
		return x.Dtype
	}
	return DataType_DATA_TYPE_UNSPECIFIED
}

func (x *TensorSpec) GetShape() []int64 {
	if x != nil {
		return x.Shape
	}
	return nil
}

// ============================================================================
// VersionInfo
// ============================================================================
//
// A caller-safe projection of one routable version: its label and current
// traffic share. Deliberately OMITS the backend endpoint and breaker internals
// (those are operator-only, exposed via GetRoute/GetCircuitState) — a public
// caller has no business seeing the serving pod's address.
// ============================================================================
type VersionInfo struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The version label (e.g., "v3"). What shows up as served_version.
	Version string `protobuf:"bytes,1,opt,name=version,proto3" json:"version,omitempty"`
	// This version's current traffic share in basis points (0–10000). Lets a UI
	// show "v3 is taking 10% canary traffic" without operator scope.
	WeightBps int32 `protobuf:"varint,2,opt,name=weight_bps,json=weightBps,proto3" json:"weight_bps,omitempty"`
	// True if this version is the current stable (non-canary) target. Lets a
	// caller/UI distinguish the canary from the stable variant.
	IsStable      bool `protobuf:"varint,3,opt,name=is_stable,json=isStable,proto3" json:"is_stable,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *VersionInfo) Reset() {
	*x = VersionInfo{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[28]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *VersionInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*VersionInfo) ProtoMessage() {}

func (x *VersionInfo) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[28]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use VersionInfo.ProtoReflect.Descriptor instead.
func (*VersionInfo) Descriptor() ([]byte, []int) {
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{28}
}

func (x *VersionInfo) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *VersionInfo) GetWeightBps() int32 {
	if x != nil {
		return x.WeightBps
	}
	return 0
}

func (x *VersionInfo) GetIsStable() bool {
	if x != nil {
		return x.IsStable
	}
	return false
}

// GetModelInfoRequest asks for one model's public contract.
type GetModelInfoRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The public model name to describe. Required. (No version/api_key/team here:
	// the version set is what the gateway routes; the principal is from the token.)
	ModelName     string `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetModelInfoRequest) Reset() {
	*x = GetModelInfoRequest{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[29]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetModelInfoRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetModelInfoRequest) ProtoMessage() {}

func (x *GetModelInfoRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[29]
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
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{29}
}

func (x *GetModelInfoRequest) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

// GetModelInfoResponse is the caller-facing description of a model.
type GetModelInfoResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The model contract (schema + routable versions + serving status).
	ModelInfo     *ModelInfo `protobuf:"bytes,1,opt,name=model_info,json=modelInfo,proto3" json:"model_info,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetModelInfoResponse) Reset() {
	*x = GetModelInfoResponse{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[30]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetModelInfoResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetModelInfoResponse) ProtoMessage() {}

func (x *GetModelInfoResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[30]
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
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{30}
}

func (x *GetModelInfoResponse) GetModelInfo() *ModelInfo {
	if x != nil {
		return x.ModelInfo
	}
	return nil
}

// ============================================================================
// ModelInfo
// ============================================================================
//
// The CALLER VIEW of a model: enough to build a valid request and understand
// what will answer it, and NOTHING server-internal. Contrast with Route (the
// operator view) which carries endpoints/status. Populated by the gateway from
// the routing table it built off ModelDeployed/ModelPromoted events plus the
// input/output schema it learned from the serving backend's signature.
// ============================================================================
type ModelInfo struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The public model name clients call.
	ModelName string `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// Whether the model is currently routable (has at least one ACTIVE target).
	// False = predicts will fail FAILURE_REASON_NO_ROUTE. Lets a UI grey it out.
	IsServing bool `protobuf:"varint,2,opt,name=is_serving,json=isServing,proto3" json:"is_serving,omitempty"`
	// The model's input tensor signature (names/dtypes/shapes) for client-side
	// request validation at the edge.
	Inputs []*TensorSpec `protobuf:"bytes,3,rep,name=inputs,proto3" json:"inputs,omitempty"`
	// The model's output tensor signature, so a client knows what keys/shapes to
	// expect back in PredictResponse.outputs.
	Outputs []*TensorSpec `protobuf:"bytes,4,rep,name=outputs,proto3" json:"outputs,omitempty"`
	// The currently routable versions and their traffic weights (caller-safe
	// projection — no endpoints). Mirrors the live split a Predict would use.
	Versions []*VersionInfo `protobuf:"bytes,5,rep,name=versions,proto3" json:"versions,omitempty"`
	// When the routing entry backing this info last changed (deploy/promote event
	// or admin write). SERVER-authoritative; lets a client reason about staleness.
	UpdatedAt     *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=updated_at,json=updatedAt,proto3" json:"updated_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ModelInfo) Reset() {
	*x = ModelInfo{}
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[31]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ModelInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ModelInfo) ProtoMessage() {}

func (x *ModelInfo) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_inference_v1_inference_proto_msgTypes[31]
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
	return file_forgepoint_inference_v1_inference_proto_rawDescGZIP(), []int{31}
}

func (x *ModelInfo) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *ModelInfo) GetIsServing() bool {
	if x != nil {
		return x.IsServing
	}
	return false
}

func (x *ModelInfo) GetInputs() []*TensorSpec {
	if x != nil {
		return x.Inputs
	}
	return nil
}

func (x *ModelInfo) GetOutputs() []*TensorSpec {
	if x != nil {
		return x.Outputs
	}
	return nil
}

func (x *ModelInfo) GetVersions() []*VersionInfo {
	if x != nil {
		return x.Versions
	}
	return nil
}

func (x *ModelInfo) GetUpdatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.UpdatedAt
	}
	return nil
}

var File_forgepoint_inference_v1_inference_proto protoreflect.FileDescriptor

const file_forgepoint_inference_v1_inference_proto_rawDesc = "" +
	"\n" +
	"'forgepoint/inference/v1/inference.proto\x12\x17forgepoint.inference.v1\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1egoogle/protobuf/duration.proto\x1a!forgepoint/common/v1/common.proto\x1a!forgepoint/events/v1/events.proto\"o\n" +
	"\n" +
	"TensorData\x12\x14\n" +
	"\x05shape\x18\x01 \x03(\x03R\x05shape\x127\n" +
	"\x05dtype\x18\x02 \x01(\x0e2!.forgepoint.inference.v1.DataTypeR\x05dtype\x12\x12\n" +
	"\x04data\x18\x03 \x01(\fR\x04data\"\xa1\x01\n" +
	"\vRouteTarget\x12\x18\n" +
	"\aversion\x18\x01 \x01(\tR\aversion\x12\x1a\n" +
	"\bendpoint\x18\x02 \x01(\tR\bendpoint\x12\x1d\n" +
	"\n" +
	"weight_bps\x18\x03 \x01(\x05R\tweightBps\x12=\n" +
	"\x06status\x18\x04 \x01(\x0e2%.forgepoint.inference.v1.TargetStatusR\x06status\"\xa1\x01\n" +
	"\x05Route\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\x12>\n" +
	"\atargets\x18\x02 \x03(\v2$.forgepoint.inference.v1.RouteTargetR\atargets\x129\n" +
	"\n" +
	"updated_at\x18\x03 \x01(\v2\x1a.google.protobuf.TimestampR\tupdatedAt\"\x88\x02\n" +
	"\fCircuitState\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\x12\x18\n" +
	"\aversion\x18\x02 \x01(\tR\aversion\x12B\n" +
	"\x05state\x18\x03 \x01(\x0e2,.forgepoint.inference.v1.CircuitBreakerStateR\x05state\x121\n" +
	"\x14consecutive_failures\x18\x04 \x01(\x05R\x13consecutiveFailures\x12H\n" +
	"\x12last_transition_at\x18\x05 \x01(\v2\x1a.google.protobuf.TimestampR\x10lastTransitionAt\"\xb0\x02\n" +
	"\x0ePredictRequest\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\x12K\n" +
	"\x06inputs\x18\x02 \x03(\v23.forgepoint.inference.v1.PredictRequest.InputsEntryR\x06inputs\x12)\n" +
	"\x10version_override\x18\x03 \x01(\tR\x0fversionOverride\x12'\n" +
	"\x0fidempotency_key\x18\x04 \x01(\tR\x0eidempotencyKey\x1a^\n" +
	"\vInputsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x129\n" +
	"\x05value\x18\x02 \x01(\v2#.forgepoint.inference.v1.TensorDataR\x05value:\x028\x01\"\xbe\x02\n" +
	"\x0fPredictResponse\x12O\n" +
	"\aoutputs\x18\x01 \x03(\v25.forgepoint.inference.v1.PredictResponse.OutputsEntryR\aoutputs\x12%\n" +
	"\x0eserved_version\x18\x02 \x01(\tR\rservedVersion\x123\n" +
	"\alatency\x18\x03 \x01(\v2\x19.google.protobuf.DurationR\alatency\x12\x1d\n" +
	"\n" +
	"request_id\x18\x04 \x01(\tR\trequestId\x1a_\n" +
	"\fOutputsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x129\n" +
	"\x05value\x18\x02 \x01(\v2#.forgepoint.inference.v1.TensorDataR\x05value:\x028\x01\"\xc9\x01\n" +
	"\x13BatchPredictRequest\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\x12?\n" +
	"\x05items\x18\x02 \x03(\v2).forgepoint.inference.v1.BatchPredictItemR\x05items\x12)\n" +
	"\x10version_override\x18\x03 \x01(\tR\x0fversionOverride\x12'\n" +
	"\x0fidempotency_key\x18\x04 \x01(\tR\x0eidempotencyKey\"\xda\x01\n" +
	"\x10BatchPredictItem\x12\x17\n" +
	"\aitem_id\x18\x01 \x01(\tR\x06itemId\x12M\n" +
	"\x06inputs\x18\x02 \x03(\v25.forgepoint.inference.v1.BatchPredictItem.InputsEntryR\x06inputs\x1a^\n" +
	"\vInputsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x129\n" +
	"\x05value\x18\x02 \x01(\v2#.forgepoint.inference.v1.TensorDataR\x05value:\x028\x01\"|\n" +
	"\x14BatchPredictResponse\x12E\n" +
	"\aresults\x18\x01 \x03(\v2+.forgepoint.inference.v1.BatchPredictResultR\aresults\x12\x1d\n" +
	"\n" +
	"request_id\x18\x02 \x01(\tR\trequestId\"\x97\x03\n" +
	"\x12BatchPredictResult\x12\x17\n" +
	"\aitem_id\x18\x01 \x01(\tR\x06itemId\x12R\n" +
	"\aoutputs\x18\x02 \x03(\v28.forgepoint.inference.v1.BatchPredictResult.OutputsEntryR\aoutputs\x12%\n" +
	"\x0eserved_version\x18\x03 \x01(\tR\rservedVersion\x127\n" +
	"\x05error\x18\x04 \x01(\v2!.forgepoint.common.v1.ErrorDetailR\x05error\x12S\n" +
	"\x0efailure_reason\x18\x05 \x01(\x0e2,.forgepoint.events.v1.InferenceFailureReasonR\rfailureReason\x1a_\n" +
	"\fOutputsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x129\n" +
	"\x05value\x18\x02 \x01(\v2#.forgepoint.inference.v1.TensorDataR\x05value:\x028\x01\"\xca\x01\n" +
	"\x14StreamPredictRequest\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\x12?\n" +
	"\x05items\x18\x02 \x03(\v2).forgepoint.inference.v1.BatchPredictItemR\x05items\x12)\n" +
	"\x10version_override\x18\x03 \x01(\tR\x0fversionOverride\x12'\n" +
	"\x0fidempotency_key\x18\x04 \x01(\tR\x0eidempotencyKey\"{\n" +
	"\x15StreamPredictResponse\x12C\n" +
	"\x06result\x18\x01 \x01(\v2+.forgepoint.inference.v1.BatchPredictResultR\x06result\x12\x1d\n" +
	"\n" +
	"request_id\x18\x02 \x01(\tR\trequestId\"0\n" +
	"\x0fGetRouteRequest\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\"H\n" +
	"\x10GetRouteResponse\x124\n" +
	"\x05route\x18\x01 \x01(\v2\x1e.forgepoint.inference.v1.RouteR\x05route\"\\\n" +
	"\x11ListRoutesRequest\x12G\n" +
	"\n" +
	"pagination\x18\x01 \x01(\v2'.forgepoint.common.v1.PaginationRequestR\n" +
	"pagination\"\x96\x01\n" +
	"\x12ListRoutesResponse\x126\n" +
	"\x06routes\x18\x01 \x03(\v2\x1e.forgepoint.inference.v1.RouteR\x06routes\x12H\n" +
	"\n" +
	"pagination\x18\x02 \x01(\v2(.forgepoint.common.v1.PaginationResponseR\n" +
	"pagination\"\x9c\x01\n" +
	"\x12UpsertRouteRequest\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\x12>\n" +
	"\atargets\x18\x02 \x03(\v2$.forgepoint.inference.v1.RouteTargetR\atargets\x12'\n" +
	"\x0fidempotency_key\x18\x03 \x01(\tR\x0eidempotencyKey\"K\n" +
	"\x13UpsertRouteResponse\x124\n" +
	"\x05route\x18\x01 \x01(\v2\x1e.forgepoint.inference.v1.RouteR\x05route\"\xa2\x01\n" +
	"\x16SetTrafficSplitRequest\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\x12@\n" +
	"\aweights\x18\x02 \x03(\v2&.forgepoint.inference.v1.TrafficWeightR\aweights\x12'\n" +
	"\x0fidempotency_key\x18\x03 \x01(\tR\x0eidempotencyKey\"H\n" +
	"\rTrafficWeight\x12\x18\n" +
	"\aversion\x18\x01 \x01(\tR\aversion\x12\x1d\n" +
	"\n" +
	"weight_bps\x18\x02 \x01(\x05R\tweightBps\"O\n" +
	"\x17SetTrafficSplitResponse\x124\n" +
	"\x05route\x18\x01 \x01(\v2\x1e.forgepoint.inference.v1.RouteR\x05route\"\\\n" +
	"\x12DeleteRouteRequest\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\x12'\n" +
	"\x0fidempotency_key\x18\x02 \x01(\tR\x0eidempotencyKey\"\x15\n" +
	"\x13DeleteRouteResponse\"Q\n" +
	"\x16GetCircuitStateRequest\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\x12\x18\n" +
	"\aversion\x18\x02 \x01(\tR\aversion\"e\n" +
	"\x17GetCircuitStateResponse\x12J\n" +
	"\rcircuit_state\x18\x01 \x01(\v2%.forgepoint.inference.v1.CircuitStateR\fcircuitState\"\x8f\x01\n" +
	"\x18ListCircuitStatesRequest\x12*\n" +
	"\x11model_name_filter\x18\x01 \x01(\tR\x0fmodelNameFilter\x12G\n" +
	"\n" +
	"pagination\x18\x02 \x01(\v2'.forgepoint.common.v1.PaginationRequestR\n" +
	"pagination\"\xb3\x01\n" +
	"\x19ListCircuitStatesResponse\x12L\n" +
	"\x0ecircuit_states\x18\x01 \x03(\v2%.forgepoint.inference.v1.CircuitStateR\rcircuitStates\x12H\n" +
	"\n" +
	"pagination\x18\x02 \x01(\v2(.forgepoint.common.v1.PaginationResponseR\n" +
	"pagination\"o\n" +
	"\n" +
	"TensorSpec\x12\x12\n" +
	"\x04name\x18\x01 \x01(\tR\x04name\x127\n" +
	"\x05dtype\x18\x02 \x01(\x0e2!.forgepoint.inference.v1.DataTypeR\x05dtype\x12\x14\n" +
	"\x05shape\x18\x03 \x03(\x03R\x05shape\"c\n" +
	"\vVersionInfo\x12\x18\n" +
	"\aversion\x18\x01 \x01(\tR\aversion\x12\x1d\n" +
	"\n" +
	"weight_bps\x18\x02 \x01(\x05R\tweightBps\x12\x1b\n" +
	"\tis_stable\x18\x03 \x01(\bR\bisStable\"4\n" +
	"\x13GetModelInfoRequest\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\"Y\n" +
	"\x14GetModelInfoResponse\x12A\n" +
	"\n" +
	"model_info\x18\x01 \x01(\v2\".forgepoint.inference.v1.ModelInfoR\tmodelInfo\"\xc2\x02\n" +
	"\tModelInfo\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\x12\x1d\n" +
	"\n" +
	"is_serving\x18\x02 \x01(\bR\tisServing\x12;\n" +
	"\x06inputs\x18\x03 \x03(\v2#.forgepoint.inference.v1.TensorSpecR\x06inputs\x12=\n" +
	"\aoutputs\x18\x04 \x03(\v2#.forgepoint.inference.v1.TensorSpecR\aoutputs\x12@\n" +
	"\bversions\x18\x05 \x03(\v2$.forgepoint.inference.v1.VersionInfoR\bversions\x129\n" +
	"\n" +
	"updated_at\x18\x06 \x01(\v2\x1a.google.protobuf.TimestampR\tupdatedAt*\xa7\x01\n" +
	"\bDataType\x12\x19\n" +
	"\x15DATA_TYPE_UNSPECIFIED\x10\x00\x12\x15\n" +
	"\x11DATA_TYPE_FLOAT32\x10\x01\x12\x15\n" +
	"\x11DATA_TYPE_FLOAT64\x10\x02\x12\x13\n" +
	"\x0fDATA_TYPE_INT32\x10\x03\x12\x13\n" +
	"\x0fDATA_TYPE_INT64\x10\x04\x12\x14\n" +
	"\x10DATA_TYPE_STRING\x10\x05\x12\x12\n" +
	"\x0eDATA_TYPE_BOOL\x10\x06*\x80\x01\n" +
	"\fTargetStatus\x12\x1d\n" +
	"\x19TARGET_STATUS_UNSPECIFIED\x10\x00\x12\x18\n" +
	"\x14TARGET_STATUS_ACTIVE\x10\x01\x12\x1a\n" +
	"\x16TARGET_STATUS_DRAINING\x10\x02\x12\x1b\n" +
	"\x17TARGET_STATUS_UNHEALTHY\x10\x03*\xa3\x01\n" +
	"\x13CircuitBreakerState\x12%\n" +
	"!CIRCUIT_BREAKER_STATE_UNSPECIFIED\x10\x00\x12 \n" +
	"\x1cCIRCUIT_BREAKER_STATE_CLOSED\x10\x01\x12\x1e\n" +
	"\x1aCIRCUIT_BREAKER_STATE_OPEN\x10\x02\x12#\n" +
	"\x1fCIRCUIT_BREAKER_STATE_HALF_OPEN\x10\x032\xc7\t\n" +
	"\x17InferenceGatewayService\x12\\\n" +
	"\aPredict\x12'.forgepoint.inference.v1.PredictRequest\x1a(.forgepoint.inference.v1.PredictResponse\x12k\n" +
	"\fBatchPredict\x12,.forgepoint.inference.v1.BatchPredictRequest\x1a-.forgepoint.inference.v1.BatchPredictResponse\x12p\n" +
	"\rStreamPredict\x12-.forgepoint.inference.v1.StreamPredictRequest\x1a..forgepoint.inference.v1.StreamPredictResponse0\x01\x12k\n" +
	"\fGetModelInfo\x12,.forgepoint.inference.v1.GetModelInfoRequest\x1a-.forgepoint.inference.v1.GetModelInfoResponse\x12_\n" +
	"\bGetRoute\x12(.forgepoint.inference.v1.GetRouteRequest\x1a).forgepoint.inference.v1.GetRouteResponse\x12e\n" +
	"\n" +
	"ListRoutes\x12*.forgepoint.inference.v1.ListRoutesRequest\x1a+.forgepoint.inference.v1.ListRoutesResponse\x12h\n" +
	"\vUpsertRoute\x12+.forgepoint.inference.v1.UpsertRouteRequest\x1a,.forgepoint.inference.v1.UpsertRouteResponse\x12t\n" +
	"\x0fSetTrafficSplit\x12/.forgepoint.inference.v1.SetTrafficSplitRequest\x1a0.forgepoint.inference.v1.SetTrafficSplitResponse\x12h\n" +
	"\vDeleteRoute\x12+.forgepoint.inference.v1.DeleteRouteRequest\x1a,.forgepoint.inference.v1.DeleteRouteResponse\x12t\n" +
	"\x0fGetCircuitState\x12/.forgepoint.inference.v1.GetCircuitStateRequest\x1a0.forgepoint.inference.v1.GetCircuitStateResponse\x12z\n" +
	"\x11ListCircuitStates\x121.forgepoint.inference.v1.ListCircuitStatesRequest\x1a2.forgepoint.inference.v1.ListCircuitStatesResponseBNZLgithub.com/abd-ulbasit/forgepoint/gen/go/forgepoint/inference/v1;inferencev1b\x06proto3"

var (
	file_forgepoint_inference_v1_inference_proto_rawDescOnce sync.Once
	file_forgepoint_inference_v1_inference_proto_rawDescData []byte
)

func file_forgepoint_inference_v1_inference_proto_rawDescGZIP() []byte {
	file_forgepoint_inference_v1_inference_proto_rawDescOnce.Do(func() {
		file_forgepoint_inference_v1_inference_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_forgepoint_inference_v1_inference_proto_rawDesc), len(file_forgepoint_inference_v1_inference_proto_rawDesc)))
	})
	return file_forgepoint_inference_v1_inference_proto_rawDescData
}

var file_forgepoint_inference_v1_inference_proto_enumTypes = make([]protoimpl.EnumInfo, 3)
var file_forgepoint_inference_v1_inference_proto_msgTypes = make([]protoimpl.MessageInfo, 36)
var file_forgepoint_inference_v1_inference_proto_goTypes = []any{
	(DataType)(0),                     // 0: forgepoint.inference.v1.DataType
	(TargetStatus)(0),                 // 1: forgepoint.inference.v1.TargetStatus
	(CircuitBreakerState)(0),          // 2: forgepoint.inference.v1.CircuitBreakerState
	(*TensorData)(nil),                // 3: forgepoint.inference.v1.TensorData
	(*RouteTarget)(nil),               // 4: forgepoint.inference.v1.RouteTarget
	(*Route)(nil),                     // 5: forgepoint.inference.v1.Route
	(*CircuitState)(nil),              // 6: forgepoint.inference.v1.CircuitState
	(*PredictRequest)(nil),            // 7: forgepoint.inference.v1.PredictRequest
	(*PredictResponse)(nil),           // 8: forgepoint.inference.v1.PredictResponse
	(*BatchPredictRequest)(nil),       // 9: forgepoint.inference.v1.BatchPredictRequest
	(*BatchPredictItem)(nil),          // 10: forgepoint.inference.v1.BatchPredictItem
	(*BatchPredictResponse)(nil),      // 11: forgepoint.inference.v1.BatchPredictResponse
	(*BatchPredictResult)(nil),        // 12: forgepoint.inference.v1.BatchPredictResult
	(*StreamPredictRequest)(nil),      // 13: forgepoint.inference.v1.StreamPredictRequest
	(*StreamPredictResponse)(nil),     // 14: forgepoint.inference.v1.StreamPredictResponse
	(*GetRouteRequest)(nil),           // 15: forgepoint.inference.v1.GetRouteRequest
	(*GetRouteResponse)(nil),          // 16: forgepoint.inference.v1.GetRouteResponse
	(*ListRoutesRequest)(nil),         // 17: forgepoint.inference.v1.ListRoutesRequest
	(*ListRoutesResponse)(nil),        // 18: forgepoint.inference.v1.ListRoutesResponse
	(*UpsertRouteRequest)(nil),        // 19: forgepoint.inference.v1.UpsertRouteRequest
	(*UpsertRouteResponse)(nil),       // 20: forgepoint.inference.v1.UpsertRouteResponse
	(*SetTrafficSplitRequest)(nil),    // 21: forgepoint.inference.v1.SetTrafficSplitRequest
	(*TrafficWeight)(nil),             // 22: forgepoint.inference.v1.TrafficWeight
	(*SetTrafficSplitResponse)(nil),   // 23: forgepoint.inference.v1.SetTrafficSplitResponse
	(*DeleteRouteRequest)(nil),        // 24: forgepoint.inference.v1.DeleteRouteRequest
	(*DeleteRouteResponse)(nil),       // 25: forgepoint.inference.v1.DeleteRouteResponse
	(*GetCircuitStateRequest)(nil),    // 26: forgepoint.inference.v1.GetCircuitStateRequest
	(*GetCircuitStateResponse)(nil),   // 27: forgepoint.inference.v1.GetCircuitStateResponse
	(*ListCircuitStatesRequest)(nil),  // 28: forgepoint.inference.v1.ListCircuitStatesRequest
	(*ListCircuitStatesResponse)(nil), // 29: forgepoint.inference.v1.ListCircuitStatesResponse
	(*TensorSpec)(nil),                // 30: forgepoint.inference.v1.TensorSpec
	(*VersionInfo)(nil),               // 31: forgepoint.inference.v1.VersionInfo
	(*GetModelInfoRequest)(nil),       // 32: forgepoint.inference.v1.GetModelInfoRequest
	(*GetModelInfoResponse)(nil),      // 33: forgepoint.inference.v1.GetModelInfoResponse
	(*ModelInfo)(nil),                 // 34: forgepoint.inference.v1.ModelInfo
	nil,                               // 35: forgepoint.inference.v1.PredictRequest.InputsEntry
	nil,                               // 36: forgepoint.inference.v1.PredictResponse.OutputsEntry
	nil,                               // 37: forgepoint.inference.v1.BatchPredictItem.InputsEntry
	nil,                               // 38: forgepoint.inference.v1.BatchPredictResult.OutputsEntry
	(*timestamppb.Timestamp)(nil),     // 39: google.protobuf.Timestamp
	(*durationpb.Duration)(nil),       // 40: google.protobuf.Duration
	(*v1.ErrorDetail)(nil),            // 41: forgepoint.common.v1.ErrorDetail
	(v11.InferenceFailureReason)(0),   // 42: forgepoint.events.v1.InferenceFailureReason
	(*v1.PaginationRequest)(nil),      // 43: forgepoint.common.v1.PaginationRequest
	(*v1.PaginationResponse)(nil),     // 44: forgepoint.common.v1.PaginationResponse
}
var file_forgepoint_inference_v1_inference_proto_depIdxs = []int32{
	0,  // 0: forgepoint.inference.v1.TensorData.dtype:type_name -> forgepoint.inference.v1.DataType
	1,  // 1: forgepoint.inference.v1.RouteTarget.status:type_name -> forgepoint.inference.v1.TargetStatus
	4,  // 2: forgepoint.inference.v1.Route.targets:type_name -> forgepoint.inference.v1.RouteTarget
	39, // 3: forgepoint.inference.v1.Route.updated_at:type_name -> google.protobuf.Timestamp
	2,  // 4: forgepoint.inference.v1.CircuitState.state:type_name -> forgepoint.inference.v1.CircuitBreakerState
	39, // 5: forgepoint.inference.v1.CircuitState.last_transition_at:type_name -> google.protobuf.Timestamp
	35, // 6: forgepoint.inference.v1.PredictRequest.inputs:type_name -> forgepoint.inference.v1.PredictRequest.InputsEntry
	36, // 7: forgepoint.inference.v1.PredictResponse.outputs:type_name -> forgepoint.inference.v1.PredictResponse.OutputsEntry
	40, // 8: forgepoint.inference.v1.PredictResponse.latency:type_name -> google.protobuf.Duration
	10, // 9: forgepoint.inference.v1.BatchPredictRequest.items:type_name -> forgepoint.inference.v1.BatchPredictItem
	37, // 10: forgepoint.inference.v1.BatchPredictItem.inputs:type_name -> forgepoint.inference.v1.BatchPredictItem.InputsEntry
	12, // 11: forgepoint.inference.v1.BatchPredictResponse.results:type_name -> forgepoint.inference.v1.BatchPredictResult
	38, // 12: forgepoint.inference.v1.BatchPredictResult.outputs:type_name -> forgepoint.inference.v1.BatchPredictResult.OutputsEntry
	41, // 13: forgepoint.inference.v1.BatchPredictResult.error:type_name -> forgepoint.common.v1.ErrorDetail
	42, // 14: forgepoint.inference.v1.BatchPredictResult.failure_reason:type_name -> forgepoint.events.v1.InferenceFailureReason
	10, // 15: forgepoint.inference.v1.StreamPredictRequest.items:type_name -> forgepoint.inference.v1.BatchPredictItem
	12, // 16: forgepoint.inference.v1.StreamPredictResponse.result:type_name -> forgepoint.inference.v1.BatchPredictResult
	5,  // 17: forgepoint.inference.v1.GetRouteResponse.route:type_name -> forgepoint.inference.v1.Route
	43, // 18: forgepoint.inference.v1.ListRoutesRequest.pagination:type_name -> forgepoint.common.v1.PaginationRequest
	5,  // 19: forgepoint.inference.v1.ListRoutesResponse.routes:type_name -> forgepoint.inference.v1.Route
	44, // 20: forgepoint.inference.v1.ListRoutesResponse.pagination:type_name -> forgepoint.common.v1.PaginationResponse
	4,  // 21: forgepoint.inference.v1.UpsertRouteRequest.targets:type_name -> forgepoint.inference.v1.RouteTarget
	5,  // 22: forgepoint.inference.v1.UpsertRouteResponse.route:type_name -> forgepoint.inference.v1.Route
	22, // 23: forgepoint.inference.v1.SetTrafficSplitRequest.weights:type_name -> forgepoint.inference.v1.TrafficWeight
	5,  // 24: forgepoint.inference.v1.SetTrafficSplitResponse.route:type_name -> forgepoint.inference.v1.Route
	6,  // 25: forgepoint.inference.v1.GetCircuitStateResponse.circuit_state:type_name -> forgepoint.inference.v1.CircuitState
	43, // 26: forgepoint.inference.v1.ListCircuitStatesRequest.pagination:type_name -> forgepoint.common.v1.PaginationRequest
	6,  // 27: forgepoint.inference.v1.ListCircuitStatesResponse.circuit_states:type_name -> forgepoint.inference.v1.CircuitState
	44, // 28: forgepoint.inference.v1.ListCircuitStatesResponse.pagination:type_name -> forgepoint.common.v1.PaginationResponse
	0,  // 29: forgepoint.inference.v1.TensorSpec.dtype:type_name -> forgepoint.inference.v1.DataType
	34, // 30: forgepoint.inference.v1.GetModelInfoResponse.model_info:type_name -> forgepoint.inference.v1.ModelInfo
	30, // 31: forgepoint.inference.v1.ModelInfo.inputs:type_name -> forgepoint.inference.v1.TensorSpec
	30, // 32: forgepoint.inference.v1.ModelInfo.outputs:type_name -> forgepoint.inference.v1.TensorSpec
	31, // 33: forgepoint.inference.v1.ModelInfo.versions:type_name -> forgepoint.inference.v1.VersionInfo
	39, // 34: forgepoint.inference.v1.ModelInfo.updated_at:type_name -> google.protobuf.Timestamp
	3,  // 35: forgepoint.inference.v1.PredictRequest.InputsEntry.value:type_name -> forgepoint.inference.v1.TensorData
	3,  // 36: forgepoint.inference.v1.PredictResponse.OutputsEntry.value:type_name -> forgepoint.inference.v1.TensorData
	3,  // 37: forgepoint.inference.v1.BatchPredictItem.InputsEntry.value:type_name -> forgepoint.inference.v1.TensorData
	3,  // 38: forgepoint.inference.v1.BatchPredictResult.OutputsEntry.value:type_name -> forgepoint.inference.v1.TensorData
	7,  // 39: forgepoint.inference.v1.InferenceGatewayService.Predict:input_type -> forgepoint.inference.v1.PredictRequest
	9,  // 40: forgepoint.inference.v1.InferenceGatewayService.BatchPredict:input_type -> forgepoint.inference.v1.BatchPredictRequest
	13, // 41: forgepoint.inference.v1.InferenceGatewayService.StreamPredict:input_type -> forgepoint.inference.v1.StreamPredictRequest
	32, // 42: forgepoint.inference.v1.InferenceGatewayService.GetModelInfo:input_type -> forgepoint.inference.v1.GetModelInfoRequest
	15, // 43: forgepoint.inference.v1.InferenceGatewayService.GetRoute:input_type -> forgepoint.inference.v1.GetRouteRequest
	17, // 44: forgepoint.inference.v1.InferenceGatewayService.ListRoutes:input_type -> forgepoint.inference.v1.ListRoutesRequest
	19, // 45: forgepoint.inference.v1.InferenceGatewayService.UpsertRoute:input_type -> forgepoint.inference.v1.UpsertRouteRequest
	21, // 46: forgepoint.inference.v1.InferenceGatewayService.SetTrafficSplit:input_type -> forgepoint.inference.v1.SetTrafficSplitRequest
	24, // 47: forgepoint.inference.v1.InferenceGatewayService.DeleteRoute:input_type -> forgepoint.inference.v1.DeleteRouteRequest
	26, // 48: forgepoint.inference.v1.InferenceGatewayService.GetCircuitState:input_type -> forgepoint.inference.v1.GetCircuitStateRequest
	28, // 49: forgepoint.inference.v1.InferenceGatewayService.ListCircuitStates:input_type -> forgepoint.inference.v1.ListCircuitStatesRequest
	8,  // 50: forgepoint.inference.v1.InferenceGatewayService.Predict:output_type -> forgepoint.inference.v1.PredictResponse
	11, // 51: forgepoint.inference.v1.InferenceGatewayService.BatchPredict:output_type -> forgepoint.inference.v1.BatchPredictResponse
	14, // 52: forgepoint.inference.v1.InferenceGatewayService.StreamPredict:output_type -> forgepoint.inference.v1.StreamPredictResponse
	33, // 53: forgepoint.inference.v1.InferenceGatewayService.GetModelInfo:output_type -> forgepoint.inference.v1.GetModelInfoResponse
	16, // 54: forgepoint.inference.v1.InferenceGatewayService.GetRoute:output_type -> forgepoint.inference.v1.GetRouteResponse
	18, // 55: forgepoint.inference.v1.InferenceGatewayService.ListRoutes:output_type -> forgepoint.inference.v1.ListRoutesResponse
	20, // 56: forgepoint.inference.v1.InferenceGatewayService.UpsertRoute:output_type -> forgepoint.inference.v1.UpsertRouteResponse
	23, // 57: forgepoint.inference.v1.InferenceGatewayService.SetTrafficSplit:output_type -> forgepoint.inference.v1.SetTrafficSplitResponse
	25, // 58: forgepoint.inference.v1.InferenceGatewayService.DeleteRoute:output_type -> forgepoint.inference.v1.DeleteRouteResponse
	27, // 59: forgepoint.inference.v1.InferenceGatewayService.GetCircuitState:output_type -> forgepoint.inference.v1.GetCircuitStateResponse
	29, // 60: forgepoint.inference.v1.InferenceGatewayService.ListCircuitStates:output_type -> forgepoint.inference.v1.ListCircuitStatesResponse
	50, // [50:61] is the sub-list for method output_type
	39, // [39:50] is the sub-list for method input_type
	39, // [39:39] is the sub-list for extension type_name
	39, // [39:39] is the sub-list for extension extendee
	0,  // [0:39] is the sub-list for field type_name
}

func init() { file_forgepoint_inference_v1_inference_proto_init() }
func file_forgepoint_inference_v1_inference_proto_init() {
	if File_forgepoint_inference_v1_inference_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_forgepoint_inference_v1_inference_proto_rawDesc), len(file_forgepoint_inference_v1_inference_proto_rawDesc)),
			NumEnums:      3,
			NumMessages:   36,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_forgepoint_inference_v1_inference_proto_goTypes,
		DependencyIndexes: file_forgepoint_inference_v1_inference_proto_depIdxs,
		EnumInfos:         file_forgepoint_inference_v1_inference_proto_enumTypes,
		MessageInfos:      file_forgepoint_inference_v1_inference_proto_msgTypes,
	}.Build()
	File_forgepoint_inference_v1_inference_proto = out.File
	file_forgepoint_inference_v1_inference_proto_goTypes = nil
	file_forgepoint_inference_v1_inference_proto_depIdxs = nil
}
