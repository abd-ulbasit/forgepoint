// ============================================================================
// Forgepoint Canonical Event Contract
// ============================================================================
//
// WHY THIS FILE EXISTS (read this first):
//   The 9 service protos were authored in parallel and each defined its OWN
//   ad-hoc event-payload messages inside its service proto. An adversarial
//   review found they DISAGREED — two services both claimed the "an inference
//   happened, bill it" event (inference.InferenceCompletedEvent vs
//   serving.PredictionCompletedEvent), the model-deploy lifecycle events the
//   gateway/serving consume were never defined anywhere, and the feature-write
//   event had two different names on the producer (FeaturesWritten) and the
//   consumer (FeaturesIngested) sides. The platform's async nervous system was
//   incoherent.
//
//   This package is the SINGLE SOURCE OF TRUTH for every NATS event payload on
//   the platform. Producers marshal one of these messages into
//   common.v1.EventEnvelope.data (a google.protobuf.Any) and publish it;
//   consumers unmarshal the same message by its Any type_url. One contract,
//   shared by both ends of every pipe. The per-service protos KEEP their RPC
//   request/response types but DELETE their inline event messages and depend on
//   these instead (see the per-service migration notes the fix agents will use).
//
// ----------------------------------------------------------------------------
// DESIGN — EVENT SCHEMA DECOUPLED FROM API SCHEMA (a mini schema registry)
// ----------------------------------------------------------------------------
//   These payloads do NOT import any service's domain/API messages. A
//   registry.ModelVersion, an inference.PredictRequest, a billing.Invoice — none
//   of them appear here. Every event carries the IDs + the specific fields its
//   consumers need as FLAT scalars/enums. WHY this discipline:
//
//   1. NO IMPORT CYCLES / NO COUPLING. If FeaturesWritten embedded
//      featurestore.FeatureView, then the Experiment Tracker (a consumer) would
//      have to compile-depend on the Feature Store's API proto just to read an
//      event. Multiply across 9 services and the "decoupled microservices" claim
//      collapses into one giant proto blob. Flat events keep each consumer
//      depending ONLY on this events package + common.
//
//   2. THE EVENT SCHEMA EVOLVES INDEPENDENTLY OF THE API SCHEMA. A service can
//      refactor its RPC types (rename a field on its internal ModelVersion)
//      without breaking the event bus, and vice-versa. The event is a PUBLISHED
//      CONTRACT with its own lifecycle and its own buf-breaking guarantee.
//      This is exactly what a schema registry (Confluent/Avro, AWS Glue) gives
//      you: the wire contract for events is a first-class, versioned artifact,
//      separate from any application's object model.
//
//   3. EVENTS ARE STABLE & SELF-DESCRIBING. A consumer can act on an event
//      WITHOUT a synchronous callback into the producer (which would re-couple
//      the services, add latency on the hot path, and risk reading state the
//      producer's own read-projection hasn't caught up to yet).
//
//   THE TRADEOFF — "FAT-BUT-FLAT EVENTS" vs "THIN EVENTS + CALLBACK":
//     - FAT-BUT-FLAT (our choice for action-driving events like
//       InferenceCompleted, ModelDriftDetected, UsageRecorded): carry enough
//       denormalized data that the consumer needs no callback. Cost: larger
//       messages, and a field duplicated from the producer's state can go stale
//       (acceptable — events are immutable facts about a past moment).
//     - THIN EVENT + CALLBACK (used where the full object is large or rarely
//       needed — e.g. FeaturesWritten carries entity ids + a version range, and
//       a consumer that wants the actual values calls GetOnlineFeatures): small
//       messages, but the consumer must call back, re-coupling it to the
//       producer and adding a synchronous dependency on the read path.
//     We pick per-event based on what the consumers actually need to ACT — fat
//     where a reaction must be immediate and self-contained, thin where the
//     event is just a "something changed, pull if you care" notification.
//
//   INTERVIEW FRAMING (likely probes):
//     - "Why not embed the domain message in the event?" → import cycles +
//       coupling + independent evolution; the event is a separate published
//       contract (schema-registry thinking).
//     - "Fat vs thin events?" → event-carried state transfer vs notification;
//       we choose per-event by consumer need; name the staleness tradeoff.
//     - "How do you avoid double-processing?" → EventEnvelope.id is the
//       transport dedupe key; each payload also carries a BUSINESS idempotency
//       handle (request_id, window_id, source_request_id) so consumers dedupe at
//       the domain level too. At-least-once delivery + idempotent consumers =
//       exactly-once EFFECT.
//
// ----------------------------------------------------------------------------
// SUBJECT NAMING CONVENTION (the one convention, applied everywhere)
// ----------------------------------------------------------------------------
//   GRAMMAR:   fp.<domain>.<event...>
//     - fp.            platform namespace (all Forgepoint subjects).
//     - <domain>       the RESOURCE the event is about, PLURAL (see below).
//     - <event...>     a dot-segmented past-tense event name; deeper sub-paths
//                      group related events (e.g. fp.models.version.created).
//
//   PLURAL DOMAIN SEGMENT — DECISION & JUSTIFICATION:
//     We use the PLURAL resource noun: fp.models.registered, NOT
//     fp.model.registered. WHY:
//       (a) The authoritative platform-design doc's NATS hierarchy already uses
//           plurals throughout (fp.models.>, fp.pipelines.>, fp.features.>,
//           fp.billing.>, fp.notifications.>). Picking plural makes the contract
//           match the approved design with zero churn to that doc.
//       (b) A subject is a STREAM of all events about a SET of resources, so the
//           plural ("the models stream") reads correctly: fp.models.> = "every
//           event about models". This is the AWS-SNS/EventBridge and Kafka-topic
//           convention (topics are named for the plural resource).
//       (c) Wildcard subscriptions read naturally: Notification subscribes to
//           fp.> ; Registry's projection to fp.models.> ; etc.
//     The only domains that aren't pluralizable are the activity/uncountable
//     ones (fp.inference.*) — left as-is; they're already "the inference
//     stream", not a countable resource. Consistency rule for the platform:
//     COUNTABLE RESOURCES ARE PLURAL; activity streams keep their natural noun.
//
//   PAST TENSE: events name something that ALREADY HAPPENED (registered,
//     completed, deployed, detected). Never imperative (don't name a subject
//     fp.models.register — that's a command, not an event).
//
//   OWNERSHIP NOTE — a DOMAIN segment is not a SERVICE name:
//     A subject's domain is the RESOURCE, and the PRODUCER may be a different
//     service than the domain suggests. The clearest example: drift events live
//     under fp.models.drift.detected (they're about a MODEL) but are produced by
//     the Model Monitor service, not the Registry. Likewise the model-deploy
//     lifecycle is about a model's DEPLOYMENT and is owned by the Pipeline
//     Orchestrator (the saga that performs the deploy), published under
//     fp.pipelines.model.* . The EventEnvelope.source field records the actual
//     producing service; the subject records the resource domain.
//
//   FULL SUBJECT REGISTRY (authoritative — every event on the platform):
//
//   ┌──────────────────────────────────┬───────────────────────────┬──────────────────────┬───────────────────────────────────────────────┐
//   │ SUBJECT                          │ PAYLOAD MESSAGE           │ PRODUCER             │ CONSUMERS                                       │
//   ├──────────────────────────────────┼───────────────────────────┼──────────────────────┼───────────────────────────────────────────────┤
//   │ fp.models.registered             │ ModelRegistered           │ registry             │ pipeline-orchestrator, experiment-tracker       │
//   │ fp.models.version.created        │ ModelVersionCreated       │ registry             │ experiment-tracker                              │
//   │ fp.models.version.ready          │ ModelVersionReady         │ registry             │ serving, billing, pipeline-orchestrator         │
//   │ fp.models.promoted               │ ModelPromoted             │ registry             │ inference-gateway, serving, monitor, billing    │
//   │ fp.models.archived               │ ModelArchived             │ registry             │ inference-gateway, serving, billing             │
//   │ fp.models.drift.detected         │ ModelDriftDetected        │ model-monitor        │ notification, experiment-tracker, pipeline-orch │
//   │ fp.pipelines.started             │ PipelineStarted           │ pipeline-orchestrator│ notification, experiment-tracker                │
//   │ fp.pipelines.step.completed      │ StepCompleted             │ pipeline-orchestrator│ experiment-tracker                              │
//   │ fp.pipelines.step.failed         │ StepFailed                │ pipeline-orchestrator│ notification                                    │
//   │ fp.pipelines.completed           │ PipelineCompleted         │ pipeline-orchestrator│ experiment-tracker, notification                │
//   │ fp.pipelines.failed              │ PipelineFailed            │ pipeline-orchestrator│ notification                                    │
//   │ fp.pipelines.compensation.trigg… │ CompensationTriggered     │ pipeline-orchestrator│ notification                                    │
//   │ fp.pipelines.model.deployed      │ ModelDeployed             │ pipeline-orchestrator│ inference-gateway, serving, experiment-tracker  │
//   │ fp.pipelines.model.undeployed    │ ModelUndeployed           │ pipeline-orchestrator│ inference-gateway, serving                      │
//   │ fp.inference.completed           │ InferenceCompleted        │ inference-gateway    │ billing, experiment-tracker, model-monitor      │
//   │ fp.inference.failed              │ InferenceFailed           │ inference-gateway    │ model-monitor (error-rate), notification        │
//   │ fp.features.view.defined         │ FeatureViewDefined        │ feature-store        │ experiment-tracker                              │
//   │ fp.features.written              │ FeaturesWritten           │ feature-store        │ experiment-tracker, model-monitor               │
//   │ fp.billing.usage.recorded        │ UsageRecorded             │ billing              │ experiment-tracker                              │
//   │ fp.billing.quota.exceeded        │ QuotaExceeded             │ billing              │ notification, inference-gateway                 │
//   │ fp.billing.invoice.generated     │ InvoiceGenerated          │ billing              │ notification                                    │
//   │ fp.experiments.run.created       │ RunCreated                │ experiment-tracker   │ notification                                    │
//   │ fp.experiments.run.finished      │ RunFinished               │ experiment-tracker   │ notification, experiment-tracker (leaderboard)  │
//   │ fp.notifications.delivered       │ NotificationDelivered     │ notification         │ experiment-tracker (delivery health)            │
//   │ fp.notifications.failed          │ NotificationFailed        │ notification         │ experiment-tracker, on-call escalation          │
//   │ fp.auth.user.created             │ UserCreated               │ auth                 │ notification, billing, experiment-tracker/audit │
//   │ fp.auth.apikey.rotated           │ ApiKeyRotated             │ auth                 │ notification, inference-gateway (cache evict)    │
//   └──────────────────────────────────┴───────────────────────────┴──────────────────────┴───────────────────────────────────────────────┘
//
//   (The full prose catalog with field-level rationale is docs/design/event-contract.md.)
//
// ----------------------------------------------------------------------------
// CONFLICTS RESOLVED IN THIS FILE (so the per-service fix agents align exactly)
// ----------------------------------------------------------------------------
//   1. ONE inference event. inference-gateway OWNS InferenceCompleted
//      (fp.inference.completed) — it is the entry point and alone knows latency,
//      routing/served_version, the billed principal (api_key_id), and canary.
//      serving.PredictionCompletedEvent is DELETED (a serving pod must not emit
//      a competing billing/metering event). Billing AND Model Monitor both
//      consume the gateway's single event. The prediction_id/keys are modeled so
//      Model Monitor can later JOIN delayed ground truth (request_id is the key).
//   2. Model-deploy lifecycle. pipeline-orchestrator OWNS ModelDeployed /
//      ModelUndeployed (fp.pipelines.model.deployed / .undeployed). These
//      payloads were MISSING entirely — added here. inference-gateway & serving
//      CONSUME them to add/remove routes and (un)load models. serving's
//      ModelLoadedEvent/ModelUnloadedEvent are DELETED — the AUTHORITATIVE
//      deploy lifecycle is the orchestrator's saga, not a serving-pod side note.
//   3. Registry gets ModelVersionReady (fp.models.version.ready) — the artifact-
//      upload-finished transition serving/billing consumers need. ModelVersionCreated
//      fires at row creation (PENDING_UPLOAD); ModelVersionReady fires when the
//      artifact is uploaded+verified (READY) and is what makes a version servable.
//   4. Feature event name: ONE name, FeaturesWritten (fp.features.written). The
//      design doc's "FeaturesIngested" and the experiment-tracker's consumed
//      "fp.features.ingested" are RECONCILED to fp.features.written. Fix agents
//      MUST update the experiment-tracker consumer subscription to fp.features.written.
//   5. Drift: model-monitor OWNS ModelDriftDetected (fp.models.drift.detected) —
//      closes serve→monitor→retrain. notification, experiment-tracker, and
//      pipeline-orchestrator consume it.
//   6. Billing subjects kept exactly as the design doc sanctions:
//      fp.billing.quota.exceeded, fp.billing.invoice.generated. UsageRecorded is
//      published on fp.billing.usage.recorded — the design doc's hierarchy lists
//      quota/invoice explicitly and UsageRecorded was implied by the outbox
//      example; we add fp.billing.usage.recorded under the sanctioned fp.billing.>
//      tree (documented as a doc addition in event-contract.md).
//   7. ONE subject convention: fp.<domain>.<event...>, PLURAL domain (justified
//      above). The prior fp.models.* vs fp.pipelines.* split is resolved by
//      OWNERSHIP-BY-RESOURCE: model lifecycle facts live under fp.models.*,
//      while the model-DEPLOYMENT lifecycle (performed by the saga) lives under
//      fp.pipelines.model.* — both consistent with the convention.
//
// VERSIONING: package path includes v1 (Buf/Google convention). A breaking event
// change requires a new forgepoint.events.v2 package; both coexist during a
// migration (publishers can dual-publish, consumers cut over). buf breaking
// (FILE level) guards this file just like every other proto on the platform.
// ============================================================================

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.11
// 	protoc        (unknown)
// source: forgepoint/events/v1/events.proto

package eventsv1

import (
	v1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	structpb "google.golang.org/protobuf/types/known/structpb"
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

// ModelStage mirrors registry.v1.ModelStage — a model VERSION's lifecycle state.
// Carried on ModelPromoted so a consumer (serving/monitor/billing) sees exactly
// which transition happened (e.g. only react when TO == PRODUCTION).
type ModelStage int32

const (
	ModelStage_MODEL_STAGE_UNSPECIFIED ModelStage = 0
	ModelStage_MODEL_STAGE_DEV         ModelStage = 1
	ModelStage_MODEL_STAGE_STAGING     ModelStage = 2
	ModelStage_MODEL_STAGE_PRODUCTION  ModelStage = 3
	ModelStage_MODEL_STAGE_ARCHIVED    ModelStage = 4
)

// Enum value maps for ModelStage.
var (
	ModelStage_name = map[int32]string{
		0: "MODEL_STAGE_UNSPECIFIED",
		1: "MODEL_STAGE_DEV",
		2: "MODEL_STAGE_STAGING",
		3: "MODEL_STAGE_PRODUCTION",
		4: "MODEL_STAGE_ARCHIVED",
	}
	ModelStage_value = map[string]int32{
		"MODEL_STAGE_UNSPECIFIED": 0,
		"MODEL_STAGE_DEV":         1,
		"MODEL_STAGE_STAGING":     2,
		"MODEL_STAGE_PRODUCTION":  3,
		"MODEL_STAGE_ARCHIVED":    4,
	}
)

func (x ModelStage) Enum() *ModelStage {
	p := new(ModelStage)
	*p = x
	return p
}

func (x ModelStage) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ModelStage) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_events_v1_events_proto_enumTypes[0].Descriptor()
}

func (ModelStage) Type() protoreflect.EnumType {
	return &file_forgepoint_events_v1_events_proto_enumTypes[0]
}

func (x ModelStage) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ModelStage.Descriptor instead.
func (ModelStage) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{0}
}

// PipelineType mirrors pipeline.v1.PipelineType — saga vs DAG vs batch. Carried
// on pipeline lifecycle events so a consumer can filter (e.g. Experiment Tracker
// only ingests TRAINING_DAG completions for metrics).
type PipelineType int32

const (
	PipelineType_PIPELINE_TYPE_UNSPECIFIED     PipelineType = 0
	PipelineType_PIPELINE_TYPE_DEPLOYMENT_SAGA PipelineType = 1
	PipelineType_PIPELINE_TYPE_TRAINING_DAG    PipelineType = 2
	PipelineType_PIPELINE_TYPE_BATCH_INFERENCE PipelineType = 3
)

// Enum value maps for PipelineType.
var (
	PipelineType_name = map[int32]string{
		0: "PIPELINE_TYPE_UNSPECIFIED",
		1: "PIPELINE_TYPE_DEPLOYMENT_SAGA",
		2: "PIPELINE_TYPE_TRAINING_DAG",
		3: "PIPELINE_TYPE_BATCH_INFERENCE",
	}
	PipelineType_value = map[string]int32{
		"PIPELINE_TYPE_UNSPECIFIED":     0,
		"PIPELINE_TYPE_DEPLOYMENT_SAGA": 1,
		"PIPELINE_TYPE_TRAINING_DAG":    2,
		"PIPELINE_TYPE_BATCH_INFERENCE": 3,
	}
)

func (x PipelineType) Enum() *PipelineType {
	p := new(PipelineType)
	*p = x
	return p
}

func (x PipelineType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (PipelineType) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_events_v1_events_proto_enumTypes[1].Descriptor()
}

func (PipelineType) Type() protoreflect.EnumType {
	return &file_forgepoint_events_v1_events_proto_enumTypes[1]
}

func (x PipelineType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use PipelineType.Descriptor instead.
func (PipelineType) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{1}
}

// StepType mirrors pipeline.v1.StepType — the kind of work a saga/DAG step does.
// Carried on step events so consumers can route on it (e.g. a REGISTER step
// completing matters to the Registry/Experiment Tracker more than a VALIDATE).
type StepType int32

const (
	StepType_STEP_TYPE_UNSPECIFIED StepType = 0
	StepType_STEP_TYPE_VALIDATE    StepType = 1
	StepType_STEP_TYPE_BUILD       StepType = 2
	StepType_STEP_TYPE_DEPLOY      StepType = 3
	StepType_STEP_TYPE_CANARY      StepType = 4
	StepType_STEP_TYPE_PROMOTE     StepType = 5
	StepType_STEP_TYPE_TRAIN       StepType = 6
	StepType_STEP_TYPE_EVALUATE    StepType = 7
	StepType_STEP_TYPE_REGISTER    StepType = 8
	StepType_STEP_TYPE_CUSTOM      StepType = 9
)

// Enum value maps for StepType.
var (
	StepType_name = map[int32]string{
		0: "STEP_TYPE_UNSPECIFIED",
		1: "STEP_TYPE_VALIDATE",
		2: "STEP_TYPE_BUILD",
		3: "STEP_TYPE_DEPLOY",
		4: "STEP_TYPE_CANARY",
		5: "STEP_TYPE_PROMOTE",
		6: "STEP_TYPE_TRAIN",
		7: "STEP_TYPE_EVALUATE",
		8: "STEP_TYPE_REGISTER",
		9: "STEP_TYPE_CUSTOM",
	}
	StepType_value = map[string]int32{
		"STEP_TYPE_UNSPECIFIED": 0,
		"STEP_TYPE_VALIDATE":    1,
		"STEP_TYPE_BUILD":       2,
		"STEP_TYPE_DEPLOY":      3,
		"STEP_TYPE_CANARY":      4,
		"STEP_TYPE_PROMOTE":     5,
		"STEP_TYPE_TRAIN":       6,
		"STEP_TYPE_EVALUATE":    7,
		"STEP_TYPE_REGISTER":    8,
		"STEP_TYPE_CUSTOM":      9,
	}
)

func (x StepType) Enum() *StepType {
	p := new(StepType)
	*p = x
	return p
}

func (x StepType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (StepType) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_events_v1_events_proto_enumTypes[2].Descriptor()
}

func (StepType) Type() protoreflect.EnumType {
	return &file_forgepoint_events_v1_events_proto_enumTypes[2]
}

func (x StepType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use StepType.Descriptor instead.
func (StepType) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{2}
}

// InferenceFailureReason mirrors inference.v1.FailureReason — WHY a prediction
// failed, which resilience pattern fired. Drives Model Monitor's error-rate
// signal and Notification routing (a capacity problem vs a backend-health
// problem call for different reactions).
type InferenceFailureReason int32

const (
	InferenceFailureReason_INFERENCE_FAILURE_REASON_UNSPECIFIED    InferenceFailureReason = 0
	InferenceFailureReason_INFERENCE_FAILURE_REASON_NO_ROUTE       InferenceFailureReason = 1
	InferenceFailureReason_INFERENCE_FAILURE_REASON_RATE_LIMITED   InferenceFailureReason = 2
	InferenceFailureReason_INFERENCE_FAILURE_REASON_BULKHEAD_FULL  InferenceFailureReason = 3
	InferenceFailureReason_INFERENCE_FAILURE_REASON_CIRCUIT_OPEN   InferenceFailureReason = 4
	InferenceFailureReason_INFERENCE_FAILURE_REASON_UPSTREAM_ERROR InferenceFailureReason = 5
	InferenceFailureReason_INFERENCE_FAILURE_REASON_TIMEOUT        InferenceFailureReason = 6
	InferenceFailureReason_INFERENCE_FAILURE_REASON_INVALID_INPUT  InferenceFailureReason = 7
	InferenceFailureReason_INFERENCE_FAILURE_REASON_QUOTA_EXCEEDED InferenceFailureReason = 8
)

// Enum value maps for InferenceFailureReason.
var (
	InferenceFailureReason_name = map[int32]string{
		0: "INFERENCE_FAILURE_REASON_UNSPECIFIED",
		1: "INFERENCE_FAILURE_REASON_NO_ROUTE",
		2: "INFERENCE_FAILURE_REASON_RATE_LIMITED",
		3: "INFERENCE_FAILURE_REASON_BULKHEAD_FULL",
		4: "INFERENCE_FAILURE_REASON_CIRCUIT_OPEN",
		5: "INFERENCE_FAILURE_REASON_UPSTREAM_ERROR",
		6: "INFERENCE_FAILURE_REASON_TIMEOUT",
		7: "INFERENCE_FAILURE_REASON_INVALID_INPUT",
		8: "INFERENCE_FAILURE_REASON_QUOTA_EXCEEDED",
	}
	InferenceFailureReason_value = map[string]int32{
		"INFERENCE_FAILURE_REASON_UNSPECIFIED":    0,
		"INFERENCE_FAILURE_REASON_NO_ROUTE":       1,
		"INFERENCE_FAILURE_REASON_RATE_LIMITED":   2,
		"INFERENCE_FAILURE_REASON_BULKHEAD_FULL":  3,
		"INFERENCE_FAILURE_REASON_CIRCUIT_OPEN":   4,
		"INFERENCE_FAILURE_REASON_UPSTREAM_ERROR": 5,
		"INFERENCE_FAILURE_REASON_TIMEOUT":        6,
		"INFERENCE_FAILURE_REASON_INVALID_INPUT":  7,
		"INFERENCE_FAILURE_REASON_QUOTA_EXCEEDED": 8,
	}
)

func (x InferenceFailureReason) Enum() *InferenceFailureReason {
	p := new(InferenceFailureReason)
	*p = x
	return p
}

func (x InferenceFailureReason) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (InferenceFailureReason) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_events_v1_events_proto_enumTypes[3].Descriptor()
}

func (InferenceFailureReason) Type() protoreflect.EnumType {
	return &file_forgepoint_events_v1_events_proto_enumTypes[3]
}

func (x InferenceFailureReason) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use InferenceFailureReason.Descriptor instead.
func (InferenceFailureReason) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{3}
}

// MeterType mirrors billing.v1.MeterType — the billable axis a usage record
// measures. Carried on UsageRecorded/QuotaExceeded so consumers (Experiment
// Tracker cost attribution, the gateway's quota cache) need no callback.
type MeterType int32

const (
	MeterType_METER_TYPE_UNSPECIFIED       MeterType = 0
	MeterType_METER_TYPE_INFERENCE_REQUEST MeterType = 1
	MeterType_METER_TYPE_INFERENCE_TOKENS  MeterType = 2
	MeterType_METER_TYPE_COMPUTE_SECONDS   MeterType = 3
	MeterType_METER_TYPE_STORAGE_BYTES     MeterType = 4
)

// Enum value maps for MeterType.
var (
	MeterType_name = map[int32]string{
		0: "METER_TYPE_UNSPECIFIED",
		1: "METER_TYPE_INFERENCE_REQUEST",
		2: "METER_TYPE_INFERENCE_TOKENS",
		3: "METER_TYPE_COMPUTE_SECONDS",
		4: "METER_TYPE_STORAGE_BYTES",
	}
	MeterType_value = map[string]int32{
		"METER_TYPE_UNSPECIFIED":       0,
		"METER_TYPE_INFERENCE_REQUEST": 1,
		"METER_TYPE_INFERENCE_TOKENS":  2,
		"METER_TYPE_COMPUTE_SECONDS":   3,
		"METER_TYPE_STORAGE_BYTES":     4,
	}
)

func (x MeterType) Enum() *MeterType {
	p := new(MeterType)
	*p = x
	return p
}

func (x MeterType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (MeterType) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_events_v1_events_proto_enumTypes[4].Descriptor()
}

func (MeterType) Type() protoreflect.EnumType {
	return &file_forgepoint_events_v1_events_proto_enumTypes[4]
}

func (x MeterType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use MeterType.Descriptor instead.
func (MeterType) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{4}
}

// RunStatus mirrors experiment.v1.RunStatus — terminal state of a tracked run.
// Carried on RunFinished so a consumer ranks/alerts without a GetRun callback.
type RunStatus int32

const (
	RunStatus_RUN_STATUS_UNSPECIFIED RunStatus = 0
	RunStatus_RUN_STATUS_RUNNING     RunStatus = 1
	RunStatus_RUN_STATUS_FINISHED    RunStatus = 2
	RunStatus_RUN_STATUS_FAILED      RunStatus = 3
	RunStatus_RUN_STATUS_KILLED      RunStatus = 4
)

// Enum value maps for RunStatus.
var (
	RunStatus_name = map[int32]string{
		0: "RUN_STATUS_UNSPECIFIED",
		1: "RUN_STATUS_RUNNING",
		2: "RUN_STATUS_FINISHED",
		3: "RUN_STATUS_FAILED",
		4: "RUN_STATUS_KILLED",
	}
	RunStatus_value = map[string]int32{
		"RUN_STATUS_UNSPECIFIED": 0,
		"RUN_STATUS_RUNNING":     1,
		"RUN_STATUS_FINISHED":    2,
		"RUN_STATUS_FAILED":      3,
		"RUN_STATUS_KILLED":      4,
	}
)

func (x RunStatus) Enum() *RunStatus {
	p := new(RunStatus)
	*p = x
	return p
}

func (x RunStatus) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (RunStatus) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_events_v1_events_proto_enumTypes[5].Descriptor()
}

func (RunStatus) Type() protoreflect.EnumType {
	return &file_forgepoint_events_v1_events_proto_enumTypes[5]
}

func (x RunStatus) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use RunStatus.Descriptor instead.
func (RunStatus) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{5}
}

// DriftType mirrors monitor.v1.DriftType — which drift signal breached.
type DriftType int32

const (
	DriftType_DRIFT_TYPE_UNSPECIFIED DriftType = 0
	DriftType_DRIFT_TYPE_DATA        DriftType = 1
	DriftType_DRIFT_TYPE_PREDICTION  DriftType = 2
	DriftType_DRIFT_TYPE_PERFORMANCE DriftType = 3
)

// Enum value maps for DriftType.
var (
	DriftType_name = map[int32]string{
		0: "DRIFT_TYPE_UNSPECIFIED",
		1: "DRIFT_TYPE_DATA",
		2: "DRIFT_TYPE_PREDICTION",
		3: "DRIFT_TYPE_PERFORMANCE",
	}
	DriftType_value = map[string]int32{
		"DRIFT_TYPE_UNSPECIFIED": 0,
		"DRIFT_TYPE_DATA":        1,
		"DRIFT_TYPE_PREDICTION":  2,
		"DRIFT_TYPE_PERFORMANCE": 3,
	}
)

func (x DriftType) Enum() *DriftType {
	p := new(DriftType)
	*p = x
	return p
}

func (x DriftType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (DriftType) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_events_v1_events_proto_enumTypes[6].Descriptor()
}

func (DriftType) Type() protoreflect.EnumType {
	return &file_forgepoint_events_v1_events_proto_enumTypes[6]
}

func (x DriftType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use DriftType.Descriptor instead.
func (DriftType) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{6}
}

// DriftMethod mirrors monitor.v1.DriftMethod — the statistical test used, so a
// drift score on an event is self-describing (PSI 0.3 != KS 0.3).
type DriftMethod int32

const (
	DriftMethod_DRIFT_METHOD_UNSPECIFIED DriftMethod = 0
	DriftMethod_DRIFT_METHOD_PSI         DriftMethod = 1
	DriftMethod_DRIFT_METHOD_KL          DriftMethod = 2
	DriftMethod_DRIFT_METHOD_KS          DriftMethod = 3
)

// Enum value maps for DriftMethod.
var (
	DriftMethod_name = map[int32]string{
		0: "DRIFT_METHOD_UNSPECIFIED",
		1: "DRIFT_METHOD_PSI",
		2: "DRIFT_METHOD_KL",
		3: "DRIFT_METHOD_KS",
	}
	DriftMethod_value = map[string]int32{
		"DRIFT_METHOD_UNSPECIFIED": 0,
		"DRIFT_METHOD_PSI":         1,
		"DRIFT_METHOD_KL":          2,
		"DRIFT_METHOD_KS":          3,
	}
)

func (x DriftMethod) Enum() *DriftMethod {
	p := new(DriftMethod)
	*p = x
	return p
}

func (x DriftMethod) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (DriftMethod) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_events_v1_events_proto_enumTypes[7].Descriptor()
}

func (DriftMethod) Type() protoreflect.EnumType {
	return &file_forgepoint_events_v1_events_proto_enumTypes[7]
}

func (x DriftMethod) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use DriftMethod.Descriptor instead.
func (DriftMethod) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{7}
}

// DriftSeverity mirrors monitor.v1.DriftSeverity — OK/WARNING/CRITICAL ladder.
// The retrain policy branches on this (CRITICAL fires auto-retrain).
type DriftSeverity int32

const (
	DriftSeverity_DRIFT_SEVERITY_UNSPECIFIED DriftSeverity = 0
	DriftSeverity_DRIFT_SEVERITY_OK          DriftSeverity = 1
	DriftSeverity_DRIFT_SEVERITY_WARNING     DriftSeverity = 2
	DriftSeverity_DRIFT_SEVERITY_CRITICAL    DriftSeverity = 3
)

// Enum value maps for DriftSeverity.
var (
	DriftSeverity_name = map[int32]string{
		0: "DRIFT_SEVERITY_UNSPECIFIED",
		1: "DRIFT_SEVERITY_OK",
		2: "DRIFT_SEVERITY_WARNING",
		3: "DRIFT_SEVERITY_CRITICAL",
	}
	DriftSeverity_value = map[string]int32{
		"DRIFT_SEVERITY_UNSPECIFIED": 0,
		"DRIFT_SEVERITY_OK":          1,
		"DRIFT_SEVERITY_WARNING":     2,
		"DRIFT_SEVERITY_CRITICAL":    3,
	}
)

func (x DriftSeverity) Enum() *DriftSeverity {
	p := new(DriftSeverity)
	*p = x
	return p
}

func (x DriftSeverity) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (DriftSeverity) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_events_v1_events_proto_enumTypes[8].Descriptor()
}

func (DriftSeverity) Type() protoreflect.EnumType {
	return &file_forgepoint_events_v1_events_proto_enumTypes[8]
}

func (x DriftSeverity) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use DriftSeverity.Descriptor instead.
func (DriftSeverity) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{8}
}

// NotificationChannel mirrors notification.v1.NotificationChannel — the
// transport a notification was delivered over. Carried on delivery events so
// observers see which channel succeeded/failed.
type NotificationChannel int32

const (
	NotificationChannel_NOTIFICATION_CHANNEL_UNSPECIFIED NotificationChannel = 0
	NotificationChannel_NOTIFICATION_CHANNEL_IN_APP      NotificationChannel = 1
	NotificationChannel_NOTIFICATION_CHANNEL_WEBHOOK     NotificationChannel = 2
	NotificationChannel_NOTIFICATION_CHANNEL_SLACK       NotificationChannel = 3
	NotificationChannel_NOTIFICATION_CHANNEL_EMAIL       NotificationChannel = 4
)

// Enum value maps for NotificationChannel.
var (
	NotificationChannel_name = map[int32]string{
		0: "NOTIFICATION_CHANNEL_UNSPECIFIED",
		1: "NOTIFICATION_CHANNEL_IN_APP",
		2: "NOTIFICATION_CHANNEL_WEBHOOK",
		3: "NOTIFICATION_CHANNEL_SLACK",
		4: "NOTIFICATION_CHANNEL_EMAIL",
	}
	NotificationChannel_value = map[string]int32{
		"NOTIFICATION_CHANNEL_UNSPECIFIED": 0,
		"NOTIFICATION_CHANNEL_IN_APP":      1,
		"NOTIFICATION_CHANNEL_WEBHOOK":     2,
		"NOTIFICATION_CHANNEL_SLACK":       3,
		"NOTIFICATION_CHANNEL_EMAIL":       4,
	}
)

func (x NotificationChannel) Enum() *NotificationChannel {
	p := new(NotificationChannel)
	*p = x
	return p
}

func (x NotificationChannel) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (NotificationChannel) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_events_v1_events_proto_enumTypes[9].Descriptor()
}

func (NotificationChannel) Type() protoreflect.EnumType {
	return &file_forgepoint_events_v1_events_proto_enumTypes[9]
}

func (x NotificationChannel) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use NotificationChannel.Descriptor instead.
func (NotificationChannel) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{9}
}

// PredictionSummary — a compact, privacy-conscious summary of a prediction's
// OUTPUT, carried on InferenceCompleted. WHY a summary, not the raw output
// tensor: the event is consumed by Billing, Experiment Tracker, and Model
// Monitor; none need (or should receive) raw outputs. Shipping full tensors on
// every prediction would flood NATS and leak potentially sensitive data to three
// services. Monitor gets enough distributional signal here for PREDICTION drift.
type PredictionSummary struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The predicted class label / argmax (classifier) or string-formatted scalar
	// (regressor). Empty if not applicable.
	TopLabel string `protobuf:"bytes,1,opt,name=top_label,json=topLabel,proto3" json:"top_label,omitempty"`
	// Confidence/probability of top_label (0.0–1.0); 0 for regression / no probs.
	TopScore float64 `protobuf:"fixed64,2,opt,name=top_score,json=topScore,proto3" json:"top_score,omitempty"`
	// Per-output simple stats (e.g. {"score_mean":0.31,"score_max":0.9}). Compact,
	// model-agnostic numeric summary for prediction-drift math — NOT raw outputs.
	OutputStats map[string]float64 `protobuf:"bytes,3,rep,name=output_stats,json=outputStats,proto3" json:"output_stats,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"fixed64,2,opt,name=value"`
	// Number of elements in the output (e.g. number of classes). Lets Monitor
	// track output-shape changes across versions cheaply.
	OutputCardinality int32 `protobuf:"varint,4,opt,name=output_cardinality,json=outputCardinality,proto3" json:"output_cardinality,omitempty"`
	unknownFields     protoimpl.UnknownFields
	sizeCache         protoimpl.SizeCache
}

func (x *PredictionSummary) Reset() {
	*x = PredictionSummary{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PredictionSummary) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PredictionSummary) ProtoMessage() {}

func (x *PredictionSummary) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PredictionSummary.ProtoReflect.Descriptor instead.
func (*PredictionSummary) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{0}
}

func (x *PredictionSummary) GetTopLabel() string {
	if x != nil {
		return x.TopLabel
	}
	return ""
}

func (x *PredictionSummary) GetTopScore() float64 {
	if x != nil {
		return x.TopScore
	}
	return 0
}

func (x *PredictionSummary) GetOutputStats() map[string]float64 {
	if x != nil {
		return x.OutputStats
	}
	return nil
}

func (x *PredictionSummary) GetOutputCardinality() int32 {
	if x != nil {
		return x.OutputCardinality
	}
	return 0
}

// FeatureSummary — a compact summary of a prediction's INPUT features, carried
// on InferenceCompleted for DATA-drift detection. Same privacy rationale as
// PredictionSummary: per-feature scalar summaries the Monitor folds into its
// PSI/KS windows, never the raw feature vector and never a join-key to a person.
type FeatureSummary struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Per-feature numeric value/summary keyed by feature name (e.g.
	// {"sepal_length":5.1}). High-dimensional inputs may be sampled/aggregated.
	FeatureValues map[string]float64 `protobuf:"bytes,1,rep,name=feature_values,json=featureValues,proto3" json:"feature_values,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"fixed64,2,opt,name=value"`
	// Total number of input features (dimensionality). Lets Monitor detect
	// schema/shape drift independent of the sampled values above.
	FeatureCount  int32 `protobuf:"varint,2,opt,name=feature_count,json=featureCount,proto3" json:"feature_count,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *FeatureSummary) Reset() {
	*x = FeatureSummary{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *FeatureSummary) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FeatureSummary) ProtoMessage() {}

func (x *FeatureSummary) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FeatureSummary.ProtoReflect.Descriptor instead.
func (*FeatureSummary) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{1}
}

func (x *FeatureSummary) GetFeatureValues() map[string]float64 {
	if x != nil {
		return x.FeatureValues
	}
	return nil
}

func (x *FeatureSummary) GetFeatureCount() int32 {
	if x != nil {
		return x.FeatureCount
	}
	return 0
}

// DriftMetric — one feature's/output's/metric's drift measurement. Carried
// (repeated) inside ModelDriftDetected so a consumer sees WHICH feature moved
// ("income PSI=0.41, everything else <0.05") — the actionable detail.
type DriftMetric struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// What was measured: a feature name (data drift), output/class name
	// (prediction drift), or metric name like "accuracy"/"f1" (performance).
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// Statistical method behind `score` (PSI/KL/KS) — travels with the score
	// because thresholds are method-specific.
	Method DriftMethod `protobuf:"varint,2,opt,name=method,proto3,enum=forgepoint.events.v1.DriftMethod" json:"method,omitempty"`
	// The computed drift score under `method`. Higher = more drift. Producer-computed
	// by the Monitor (server-authoritative; a client cannot fabricate a verdict).
	Score float64 `protobuf:"fixed64,3,opt,name=score,proto3" json:"score,omitempty"`
	// Baseline (training-time) summary value for context (e.g. baseline mean).
	BaselineValue float64 `protobuf:"fixed64,4,opt,name=baseline_value,json=baselineValue,proto3" json:"baseline_value,omitempty"`
	// Current-window summary value, paired with baseline_value.
	CurrentValue float64 `protobuf:"fixed64,5,opt,name=current_value,json=currentValue,proto3" json:"current_value,omitempty"`
	// Severity for THIS metric (vs the monitor's thresholds). The event's overall
	// severity is the max across metrics.
	Severity      DriftSeverity `protobuf:"varint,6,opt,name=severity,proto3,enum=forgepoint.events.v1.DriftSeverity" json:"severity,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *DriftMetric) Reset() {
	*x = DriftMetric{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DriftMetric) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DriftMetric) ProtoMessage() {}

func (x *DriftMetric) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DriftMetric.ProtoReflect.Descriptor instead.
func (*DriftMetric) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{2}
}

func (x *DriftMetric) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *DriftMetric) GetMethod() DriftMethod {
	if x != nil {
		return x.Method
	}
	return DriftMethod_DRIFT_METHOD_UNSPECIFIED
}

func (x *DriftMetric) GetScore() float64 {
	if x != nil {
		return x.Score
	}
	return 0
}

func (x *DriftMetric) GetBaselineValue() float64 {
	if x != nil {
		return x.BaselineValue
	}
	return 0
}

func (x *DriftMetric) GetCurrentValue() float64 {
	if x != nil {
		return x.CurrentValue
	}
	return 0
}

func (x *DriftMetric) GetSeverity() DriftSeverity {
	if x != nil {
		return x.Severity
	}
	return DriftSeverity_DRIFT_SEVERITY_UNSPECIFIED
}

// MetricPoint — one (key,value,step,ts) sample of an ML metric time-series.
// Carried (repeated) on RunFinished as the run's headline/final metrics so a
// consumer can rank/alert without a GetRun callback ("fat event").
type MetricPoint struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Metric name, e.g. "accuracy", "val_auc".
	Key string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	// The measured value.
	Value float64 `protobuf:"fixed64,2,opt,name=value,proto3" json:"value,omitempty"`
	// Monotonic training step/epoch index (the X axis). Producer-supplied.
	Step int64 `protobuf:"varint,3,opt,name=step,proto3" json:"step,omitempty"`
	// Wall-clock time of measurement (producer-stamped on ingest).
	Timestamp     *timestamppb.Timestamp `protobuf:"bytes,4,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *MetricPoint) Reset() {
	*x = MetricPoint{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *MetricPoint) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MetricPoint) ProtoMessage() {}

func (x *MetricPoint) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MetricPoint.ProtoReflect.Descriptor instead.
func (*MetricPoint) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{3}
}

func (x *MetricPoint) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *MetricPoint) GetValue() float64 {
	if x != nil {
		return x.Value
	}
	return 0
}

func (x *MetricPoint) GetStep() int64 {
	if x != nil {
		return x.Step
	}
	return 0
}

func (x *MetricPoint) GetTimestamp() *timestamppb.Timestamp {
	if x != nil {
		return x.Timestamp
	}
	return nil
}

// ModelRegistered → fp.models.registered
//
//	PRODUCER:  registry (after RegisterModel commits to Postgres).
//	CONSUMERS: pipeline-orchestrator (a registered model can become a pipeline
//	           input/target), experiment-tracker (link runs to the model).
//	WHY THESE FIELDS: identity (model_id/model_name) + ownership (owner_id/team)
//	are everything a consumer needs to associate downstream work without a
//	callback; framework/task_type let the UI/orchestrator route by model kind.
//	We carry FLAT fields rather than embedding registry.Model to keep the event
//	decoupled from the registry API (see DESIGN block).
type ModelRegistered struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Registry model UUID (stable handle other services key off).
	ModelId string `protobuf:"bytes,1,opt,name=model_id,json=modelId,proto3" json:"model_id,omitempty"`
	// Human-friendly model name within the team (the addressable handle).
	ModelName string `protobuf:"bytes,2,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// ML framework descriptor ("pytorch","onnx",...). Influences how serving loads.
	Framework string `protobuf:"bytes,3,opt,name=framework,proto3" json:"framework,omitempty"`
	// Task type ("classification","llm",...). For UI grouping / routing.
	TaskType string `protobuf:"bytes,4,opt,name=task_type,json=taskType,proto3" json:"task_type,omitempty"`
	// Registrant user id and owning team — for attribution (billing/notification)
	// and team-scoped consumers. Producer-derived from auth claims.
	OwnerId string `protobuf:"bytes,5,opt,name=owner_id,json=ownerId,proto3" json:"owner_id,omitempty"`
	Team    string `protobuf:"bytes,6,opt,name=team,proto3" json:"team,omitempty"`
	// When registration committed (producer clock).
	RegisteredAt  *timestamppb.Timestamp `protobuf:"bytes,7,opt,name=registered_at,json=registeredAt,proto3" json:"registered_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ModelRegistered) Reset() {
	*x = ModelRegistered{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ModelRegistered) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ModelRegistered) ProtoMessage() {}

func (x *ModelRegistered) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ModelRegistered.ProtoReflect.Descriptor instead.
func (*ModelRegistered) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{4}
}

func (x *ModelRegistered) GetModelId() string {
	if x != nil {
		return x.ModelId
	}
	return ""
}

func (x *ModelRegistered) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *ModelRegistered) GetFramework() string {
	if x != nil {
		return x.Framework
	}
	return ""
}

func (x *ModelRegistered) GetTaskType() string {
	if x != nil {
		return x.TaskType
	}
	return ""
}

func (x *ModelRegistered) GetOwnerId() string {
	if x != nil {
		return x.OwnerId
	}
	return ""
}

func (x *ModelRegistered) GetTeam() string {
	if x != nil {
		return x.Team
	}
	return ""
}

func (x *ModelRegistered) GetRegisteredAt() *timestamppb.Timestamp {
	if x != nil {
		return x.RegisteredAt
	}
	return nil
}

// ModelVersionCreated → fp.models.version.created
//
//	PRODUCER:  registry (after CreateVersion commits; status=PENDING_UPLOAD).
//	CONSUMERS: experiment-tracker (associate the training run that produced it).
//	IMPORTANT: this fires at ROW CREATION, before the artifact is uploaded. A
//	consumer that needs a USABLE artifact (serving, storage billing) must wait
//	for ModelVersionReady instead — that distinction is exactly why both events
//	exist (it was missing before; see conflict #3).
type ModelVersionCreated struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Owning model id + name (denormalized so consumers needn't join/look up).
	ModelId   string `protobuf:"bytes,1,opt,name=model_id,json=modelId,proto3" json:"model_id,omitempty"`
	ModelName string `protobuf:"bytes,2,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// Version UUID + human label (e.g. "1.2.0").
	VersionId string `protobuf:"bytes,3,opt,name=version_id,json=versionId,proto3" json:"version_id,omitempty"`
	Version   string `protobuf:"bytes,4,opt,name=version,proto3" json:"version,omitempty"`
	// Who created it (auth claims) — for lineage/audit.
	CreatedBy string `protobuf:"bytes,5,opt,name=created_by,json=createdBy,proto3" json:"created_by,omitempty"`
	// When the version row was created (producer clock). Artifact NOT yet present.
	CreatedAt     *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=created_at,json=createdAt,proto3" json:"created_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ModelVersionCreated) Reset() {
	*x = ModelVersionCreated{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ModelVersionCreated) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ModelVersionCreated) ProtoMessage() {}

func (x *ModelVersionCreated) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ModelVersionCreated.ProtoReflect.Descriptor instead.
func (*ModelVersionCreated) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{5}
}

func (x *ModelVersionCreated) GetModelId() string {
	if x != nil {
		return x.ModelId
	}
	return ""
}

func (x *ModelVersionCreated) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *ModelVersionCreated) GetVersionId() string {
	if x != nil {
		return x.VersionId
	}
	return ""
}

func (x *ModelVersionCreated) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *ModelVersionCreated) GetCreatedBy() string {
	if x != nil {
		return x.CreatedBy
	}
	return ""
}

func (x *ModelVersionCreated) GetCreatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CreatedAt
	}
	return nil
}

// ModelVersionReady → fp.models.version.ready
//
//	PRODUCER:  registry (when the artifact upload is confirmed + checksum-verified,
//	           flipping VersionStatus PENDING_UPLOAD → READY).
//	CONSUMERS: serving (a version is now loadable), billing (begin metering
//	           STORAGE_BYTES), pipeline-orchestrator (a deploy/promote saga
//	           waiting on artifact readiness can proceed).
//	WHY ADDED: ModelVersionCreated only signals intent; READY is the transition
//	that makes a version physically servable. Consumers that act on a real
//	artifact need this edge, and several explicitly do (see conflict #3).
//	WHY artifact_digest + size_bytes: serving verifies it loaded the exact
//	registered bytes (supply-chain integrity); billing meters storage on size —
//	both server-measured, never client-asserted.
type ModelVersionReady struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Owning model id + name.
	ModelId   string `protobuf:"bytes,1,opt,name=model_id,json=modelId,proto3" json:"model_id,omitempty"`
	ModelName string `protobuf:"bytes,2,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// Version UUID + human label that just became READY.
	VersionId string `protobuf:"bytes,3,opt,name=version_id,json=versionId,proto3" json:"version_id,omitempty"`
	Version   string `protobuf:"bytes,4,opt,name=version,proto3" json:"version,omitempty"`
	// Object-storage location of the artifact (e.g. "s3://fp-models/<m>/<v>.onnx").
	ArtifactPath string `protobuf:"bytes,5,opt,name=artifact_path,json=artifactPath,proto3" json:"artifact_path,omitempty"`
	// Content digest (e.g. "sha256:...") verified after upload — integrity handle.
	ArtifactDigest string `protobuf:"bytes,6,opt,name=artifact_digest,json=artifactDigest,proto3" json:"artifact_digest,omitempty"`
	// Artifact size in bytes (server-measured) — billing meters storage on this.
	SizeBytes int64 `protobuf:"varint,7,opt,name=size_bytes,json=sizeBytes,proto3" json:"size_bytes,omitempty"`
	// When the artifact became READY (producer clock).
	ReadyAt       *timestamppb.Timestamp `protobuf:"bytes,8,opt,name=ready_at,json=readyAt,proto3" json:"ready_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ModelVersionReady) Reset() {
	*x = ModelVersionReady{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ModelVersionReady) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ModelVersionReady) ProtoMessage() {}

func (x *ModelVersionReady) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ModelVersionReady.ProtoReflect.Descriptor instead.
func (*ModelVersionReady) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{6}
}

func (x *ModelVersionReady) GetModelId() string {
	if x != nil {
		return x.ModelId
	}
	return ""
}

func (x *ModelVersionReady) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *ModelVersionReady) GetVersionId() string {
	if x != nil {
		return x.VersionId
	}
	return ""
}

func (x *ModelVersionReady) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *ModelVersionReady) GetArtifactPath() string {
	if x != nil {
		return x.ArtifactPath
	}
	return ""
}

func (x *ModelVersionReady) GetArtifactDigest() string {
	if x != nil {
		return x.ArtifactDigest
	}
	return ""
}

func (x *ModelVersionReady) GetSizeBytes() int64 {
	if x != nil {
		return x.SizeBytes
	}
	return 0
}

func (x *ModelVersionReady) GetReadyAt() *timestamppb.Timestamp {
	if x != nil {
		return x.ReadyAt
	}
	return nil
}

// ModelPromoted → fp.models.promoted
//
//	PRODUCER:  registry (after PromoteVersion commits the atomic stage swap).
//	CONSUMERS: inference-gateway (update routing table to the new prod version),
//	           serving (reload/route to new weights), model-monitor (reset drift
//	           baseline to the new version's training distribution), billing
//	           (re-meter against the new version).
//	THE SINGLE-PRODUCTION INVARIANT: promoting B to PRODUCTION atomically demotes
//	the prior production version A to ARCHIVED. The event carries BOTH so a
//	consumer sees the whole swap and can tear down the old deployment.
//	WHY from_stage/to_stage: makes the event self-describing for audit and lets a
//	consumer ignore transitions it doesn't care about (Monitor reacts only when
//	to_stage == PRODUCTION).
type ModelPromoted struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The model whose production version changed.
	ModelId   string `protobuf:"bytes,1,opt,name=model_id,json=modelId,proto3" json:"model_id,omitempty"`
	ModelName string `protobuf:"bytes,2,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// The version that was promoted (id + label).
	VersionId string `protobuf:"bytes,3,opt,name=version_id,json=versionId,proto3" json:"version_id,omitempty"`
	Version   string `protobuf:"bytes,4,opt,name=version,proto3" json:"version,omitempty"`
	// The transition this promotion performed (both ends, for self-description).
	FromStage ModelStage `protobuf:"varint,5,opt,name=from_stage,json=fromStage,proto3,enum=forgepoint.events.v1.ModelStage" json:"from_stage,omitempty"`
	ToStage   ModelStage `protobuf:"varint,6,opt,name=to_stage,json=toStage,proto3,enum=forgepoint.events.v1.ModelStage" json:"to_stage,omitempty"`
	// If a prior production version was auto-demoted (single-prod invariant), its
	// id/label so consumers tear down the old deployment. Empty if none.
	DemotedVersionId string `protobuf:"bytes,7,opt,name=demoted_version_id,json=demotedVersionId,proto3" json:"demoted_version_id,omitempty"`
	DemotedVersion   string `protobuf:"bytes,8,opt,name=demoted_version,json=demotedVersion,proto3" json:"demoted_version,omitempty"`
	// Who initiated the promotion (auth claims) — audit.
	PromotedBy string `protobuf:"bytes,9,opt,name=promoted_by,json=promotedBy,proto3" json:"promoted_by,omitempty"`
	// When the swap committed (producer clock).
	PromotedAt    *timestamppb.Timestamp `protobuf:"bytes,10,opt,name=promoted_at,json=promotedAt,proto3" json:"promoted_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ModelPromoted) Reset() {
	*x = ModelPromoted{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ModelPromoted) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ModelPromoted) ProtoMessage() {}

func (x *ModelPromoted) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ModelPromoted.ProtoReflect.Descriptor instead.
func (*ModelPromoted) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{7}
}

func (x *ModelPromoted) GetModelId() string {
	if x != nil {
		return x.ModelId
	}
	return ""
}

func (x *ModelPromoted) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *ModelPromoted) GetVersionId() string {
	if x != nil {
		return x.VersionId
	}
	return ""
}

func (x *ModelPromoted) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *ModelPromoted) GetFromStage() ModelStage {
	if x != nil {
		return x.FromStage
	}
	return ModelStage_MODEL_STAGE_UNSPECIFIED
}

func (x *ModelPromoted) GetToStage() ModelStage {
	if x != nil {
		return x.ToStage
	}
	return ModelStage_MODEL_STAGE_UNSPECIFIED
}

func (x *ModelPromoted) GetDemotedVersionId() string {
	if x != nil {
		return x.DemotedVersionId
	}
	return ""
}

func (x *ModelPromoted) GetDemotedVersion() string {
	if x != nil {
		return x.DemotedVersion
	}
	return ""
}

func (x *ModelPromoted) GetPromotedBy() string {
	if x != nil {
		return x.PromotedBy
	}
	return ""
}

func (x *ModelPromoted) GetPromotedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.PromotedAt
	}
	return nil
}

// ModelArchived → fp.models.archived
//
//	PRODUCER:  registry (after DeleteModel soft-deletes the model + its versions).
//	CONSUMERS: inference-gateway (stop routing to it), serving (tear down running
//	           instances), billing (stop metering).
//	WHY model_name too: gateway/serving key routes/loads by name, so carrying it
//	saves every consumer a lookup at exactly the moment the model is going away.
type ModelArchived struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The archived model's id + name.
	ModelId   string `protobuf:"bytes,1,opt,name=model_id,json=modelId,proto3" json:"model_id,omitempty"`
	ModelName string `protobuf:"bytes,2,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// Who archived it (auth claims) — audit.
	ArchivedBy string `protobuf:"bytes,3,opt,name=archived_by,json=archivedBy,proto3" json:"archived_by,omitempty"`
	// When the archive committed (producer clock).
	ArchivedAt    *timestamppb.Timestamp `protobuf:"bytes,4,opt,name=archived_at,json=archivedAt,proto3" json:"archived_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ModelArchived) Reset() {
	*x = ModelArchived{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[8]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ModelArchived) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ModelArchived) ProtoMessage() {}

func (x *ModelArchived) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[8]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ModelArchived.ProtoReflect.Descriptor instead.
func (*ModelArchived) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{8}
}

func (x *ModelArchived) GetModelId() string {
	if x != nil {
		return x.ModelId
	}
	return ""
}

func (x *ModelArchived) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *ModelArchived) GetArchivedBy() string {
	if x != nil {
		return x.ArchivedBy
	}
	return ""
}

func (x *ModelArchived) GetArchivedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.ArchivedAt
	}
	return nil
}

// ModelDriftDetected → fp.models.drift.detected
//
//	PRODUCER:  model-monitor (a window breached a configured threshold). THE
//	           LOOP-CLOSER — Notification already subscribed to this before any
//	           producer existed; this is the producer.
//	CONSUMERS: notification (alert a human), experiment-tracker (record drift in
//	           the model's history), pipeline-orchestrator (on CRITICAL +
//	           auto_retrain, run the retrain pipeline → canary → promote).
//	WHY THIS LIVES UNDER fp.models.* THOUGH MONITOR PRODUCES IT: the subject
//	domain is the RESOURCE (a model), not the producing service. EventEnvelope.source
//	= "model-monitor" records the actual producer (see OWNERSHIP NOTE up top).
//	WHY headline scalars AND a full breakdown: model_name/drift_type/severity let
//	a consumer route/filter/template an alert WITHOUT deserializing everything;
//	the repeated DriftMetric breakdown + report_id give Experiment Tracker the
//	detail and a deep-link without a synchronous callback ("fat-but-flat").
//	GROUND-TRUTH JOIN: the report is over a window of request_ids the monitor
//	observed on InferenceCompleted; delayed ground truth is matched back by those
//	request_ids (SubmitGroundTruth), which is why InferenceCompleted carries
//	request_id as a stable join key.
type ModelDriftDetected struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The model + the exact serving version that drifted (version captured from the
	// inference events, so a report is tied to the precise version — vital under
	// canary traffic splitting).
	ModelName    string `protobuf:"bytes,1,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	ModelVersion string `protobuf:"bytes,2,opt,name=model_version,json=modelVersion,proto3" json:"model_version,omitempty"`
	// Headline drift type + severity, duplicated from the report for cheap routing
	// and alert templating without deserializing the breakdown.
	DriftType DriftType     `protobuf:"varint,3,opt,name=drift_type,json=driftType,proto3,enum=forgepoint.events.v1.DriftType" json:"drift_type,omitempty"`
	Severity  DriftSeverity `protobuf:"varint,4,opt,name=severity,proto3,enum=forgepoint.events.v1.DriftSeverity" json:"severity,omitempty"`
	// The drift report's stable id — deep-link handle + business-level dedupe key
	// (the report is itself idempotent on its window id).
	ReportId string `protobuf:"bytes,5,opt,name=report_id,json=reportId,proto3" json:"report_id,omitempty"`
	// Per-feature/per-output/per-metric breakdown — the actionable detail, so a
	// reactor (Experiment Tracker) records it without calling back into Monitor.
	Metrics []*DriftMetric `protobuf:"bytes,6,rep,name=metrics,proto3" json:"metrics,omitempty"`
	// Window bounds [start,end) and sample count — statistical context (PSI over 12
	// samples is far weaker than over 12,000) and timeline placement.
	WindowStart *timestamppb.Timestamp `protobuf:"bytes,7,opt,name=window_start,json=windowStart,proto3" json:"window_start,omitempty"`
	WindowEnd   *timestamppb.Timestamp `protobuf:"bytes,8,opt,name=window_end,json=windowEnd,proto3" json:"window_end,omitempty"`
	SampleCount int32                  `protobuf:"varint,9,opt,name=sample_count,json=sampleCount,proto3" json:"sample_count,omitempty"`
	// Closed-loop control fields. auto_retrain = is self-healing armed for this
	// model; retrain_pipeline_id = the Orchestrator pipeline to run (opaque id, NOT
	// a pipeline-proto import). Carried on the event so a reactor can close the loop
	// without a lookup.
	AutoRetrain       bool   `protobuf:"varint,10,opt,name=auto_retrain,json=autoRetrain,proto3" json:"auto_retrain,omitempty"`
	RetrainPipelineId string `protobuf:"bytes,11,opt,name=retrain_pipeline_id,json=retrainPipelineId,proto3" json:"retrain_pipeline_id,omitempty"`
	// Free-form drift context passed as input to the retrain pipeline (e.g.
	// {"top_drifted_feature":"income","psi":0.41}). Struct because each retrain
	// DAG's expected inputs differ — a typed message would couple this event to
	// every pipeline's input schema. Kept small + PII-free (summaries only).
	RetrainContext *structpb.Struct `protobuf:"bytes,12,opt,name=retrain_context,json=retrainContext,proto3" json:"retrain_context,omitempty"`
	// When the drift was detected / report computed (producer clock).
	DetectedAt    *timestamppb.Timestamp `protobuf:"bytes,13,opt,name=detected_at,json=detectedAt,proto3" json:"detected_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ModelDriftDetected) Reset() {
	*x = ModelDriftDetected{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[9]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ModelDriftDetected) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ModelDriftDetected) ProtoMessage() {}

func (x *ModelDriftDetected) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[9]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ModelDriftDetected.ProtoReflect.Descriptor instead.
func (*ModelDriftDetected) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{9}
}

func (x *ModelDriftDetected) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *ModelDriftDetected) GetModelVersion() string {
	if x != nil {
		return x.ModelVersion
	}
	return ""
}

func (x *ModelDriftDetected) GetDriftType() DriftType {
	if x != nil {
		return x.DriftType
	}
	return DriftType_DRIFT_TYPE_UNSPECIFIED
}

func (x *ModelDriftDetected) GetSeverity() DriftSeverity {
	if x != nil {
		return x.Severity
	}
	return DriftSeverity_DRIFT_SEVERITY_UNSPECIFIED
}

func (x *ModelDriftDetected) GetReportId() string {
	if x != nil {
		return x.ReportId
	}
	return ""
}

func (x *ModelDriftDetected) GetMetrics() []*DriftMetric {
	if x != nil {
		return x.Metrics
	}
	return nil
}

func (x *ModelDriftDetected) GetWindowStart() *timestamppb.Timestamp {
	if x != nil {
		return x.WindowStart
	}
	return nil
}

func (x *ModelDriftDetected) GetWindowEnd() *timestamppb.Timestamp {
	if x != nil {
		return x.WindowEnd
	}
	return nil
}

func (x *ModelDriftDetected) GetSampleCount() int32 {
	if x != nil {
		return x.SampleCount
	}
	return 0
}

func (x *ModelDriftDetected) GetAutoRetrain() bool {
	if x != nil {
		return x.AutoRetrain
	}
	return false
}

func (x *ModelDriftDetected) GetRetrainPipelineId() string {
	if x != nil {
		return x.RetrainPipelineId
	}
	return ""
}

func (x *ModelDriftDetected) GetRetrainContext() *structpb.Struct {
	if x != nil {
		return x.RetrainContext
	}
	return nil
}

func (x *ModelDriftDetected) GetDetectedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.DetectedAt
	}
	return nil
}

// PipelineStarted → fp.pipelines.started
//
//	CONSUMERS: notification (announce a run began), experiment-tracker (open a
//	           run record for the execution).
//	WHY triggered_by: distinguishes a human-initiated run from an automated one
//	(e.g. "model-monitor" for auto-retrain) — important for alert noise control.
type PipelineStarted struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The execution (run) id + the pipeline (template) it instantiates.
	ExecutionId string `protobuf:"bytes,1,opt,name=execution_id,json=executionId,proto3" json:"execution_id,omitempty"`
	PipelineId  string `protobuf:"bytes,2,opt,name=pipeline_id,json=pipelineId,proto3" json:"pipeline_id,omitempty"`
	// Saga vs DAG vs batch — lets consumers filter (e.g. only training DAGs).
	PipelineType PipelineType `protobuf:"varint,3,opt,name=pipeline_type,json=pipelineType,proto3,enum=forgepoint.events.v1.PipelineType" json:"pipeline_type,omitempty"`
	// Who/what triggered it (user_id or a service identity like "model-monitor").
	TriggeredBy string `protobuf:"bytes,4,opt,name=triggered_by,json=triggeredBy,proto3" json:"triggered_by,omitempty"`
	// When the run transitioned to RUNNING (producer clock).
	StartedAt     *timestamppb.Timestamp `protobuf:"bytes,5,opt,name=started_at,json=startedAt,proto3" json:"started_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *PipelineStarted) Reset() {
	*x = PipelineStarted{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[10]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PipelineStarted) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PipelineStarted) ProtoMessage() {}

func (x *PipelineStarted) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[10]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PipelineStarted.ProtoReflect.Descriptor instead.
func (*PipelineStarted) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{10}
}

func (x *PipelineStarted) GetExecutionId() string {
	if x != nil {
		return x.ExecutionId
	}
	return ""
}

func (x *PipelineStarted) GetPipelineId() string {
	if x != nil {
		return x.PipelineId
	}
	return ""
}

func (x *PipelineStarted) GetPipelineType() PipelineType {
	if x != nil {
		return x.PipelineType
	}
	return PipelineType_PIPELINE_TYPE_UNSPECIFIED
}

func (x *PipelineStarted) GetTriggeredBy() string {
	if x != nil {
		return x.TriggeredBy
	}
	return ""
}

func (x *PipelineStarted) GetStartedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.StartedAt
	}
	return nil
}

// StepCompleted → fp.pipelines.step.completed
//
//	CONSUMERS: experiment-tracker (record per-step progress/metrics).
//	WHY output (Struct): a step's small, step-specific result (e.g. a TRAIN
//	step's model artifact URI) so a consumer needn't call back. Struct because
//	the shape varies per StepType.
type StepCompleted struct {
	state       protoimpl.MessageState `protogen:"open.v1"`
	ExecutionId string                 `protobuf:"bytes,1,opt,name=execution_id,json=executionId,proto3" json:"execution_id,omitempty"`
	PipelineId  string                 `protobuf:"bytes,2,opt,name=pipeline_id,json=pipelineId,proto3" json:"pipeline_id,omitempty"`
	// Template step id that completed + its kind (so consumers route on it).
	StepId   string   `protobuf:"bytes,3,opt,name=step_id,json=stepId,proto3" json:"step_id,omitempty"`
	StepType StepType `protobuf:"varint,4,opt,name=step_type,json=stepType,proto3,enum=forgepoint.events.v1.StepType" json:"step_type,omitempty"`
	// Small step-specific output (e.g. {"model_uri":"..."}). Struct, not typed,
	// because it varies per step kind and per CUSTOM step.
	Output *structpb.Struct `protobuf:"bytes,5,opt,name=output,proto3" json:"output,omitempty"`
	// When the step finished (producer clock).
	CompletedAt   *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=completed_at,json=completedAt,proto3" json:"completed_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *StepCompleted) Reset() {
	*x = StepCompleted{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[11]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *StepCompleted) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StepCompleted) ProtoMessage() {}

func (x *StepCompleted) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[11]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StepCompleted.ProtoReflect.Descriptor instead.
func (*StepCompleted) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{11}
}

func (x *StepCompleted) GetExecutionId() string {
	if x != nil {
		return x.ExecutionId
	}
	return ""
}

func (x *StepCompleted) GetPipelineId() string {
	if x != nil {
		return x.PipelineId
	}
	return ""
}

func (x *StepCompleted) GetStepId() string {
	if x != nil {
		return x.StepId
	}
	return ""
}

func (x *StepCompleted) GetStepType() StepType {
	if x != nil {
		return x.StepType
	}
	return StepType_STEP_TYPE_UNSPECIFIED
}

func (x *StepCompleted) GetOutput() *structpb.Struct {
	if x != nil {
		return x.Output
	}
	return nil
}

func (x *StepCompleted) GetCompletedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CompletedAt
	}
	return nil
}

// StepFailed → fp.pipelines.step.failed
//
//	CONSUMERS: notification (surface the failing step for triage).
//	Emitted BEFORE compensation runs — it is the CAUSE; CompensationTriggered
//	marks the start of the UNDO phase.
type StepFailed struct {
	state       protoimpl.MessageState `protogen:"open.v1"`
	ExecutionId string                 `protobuf:"bytes,1,opt,name=execution_id,json=executionId,proto3" json:"execution_id,omitempty"`
	PipelineId  string                 `protobuf:"bytes,2,opt,name=pipeline_id,json=pipelineId,proto3" json:"pipeline_id,omitempty"`
	StepId      string                 `protobuf:"bytes,3,opt,name=step_id,json=stepId,proto3" json:"step_id,omitempty"`
	StepType    StepType               `protobuf:"varint,4,opt,name=step_type,json=stepType,proto3,enum=forgepoint.events.v1.StepType" json:"step_type,omitempty"`
	// Human-readable failure reason (triage/alerting; not end-user copy).
	Error string `protobuf:"bytes,5,opt,name=error,proto3" json:"error,omitempty"`
	// How many attempts were made before giving up.
	Attempts int32 `protobuf:"varint,6,opt,name=attempts,proto3" json:"attempts,omitempty"`
	// When the step failed (producer clock).
	FailedAt      *timestamppb.Timestamp `protobuf:"bytes,7,opt,name=failed_at,json=failedAt,proto3" json:"failed_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *StepFailed) Reset() {
	*x = StepFailed{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[12]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *StepFailed) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StepFailed) ProtoMessage() {}

func (x *StepFailed) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[12]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StepFailed.ProtoReflect.Descriptor instead.
func (*StepFailed) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{12}
}

func (x *StepFailed) GetExecutionId() string {
	if x != nil {
		return x.ExecutionId
	}
	return ""
}

func (x *StepFailed) GetPipelineId() string {
	if x != nil {
		return x.PipelineId
	}
	return ""
}

func (x *StepFailed) GetStepId() string {
	if x != nil {
		return x.StepId
	}
	return ""
}

func (x *StepFailed) GetStepType() StepType {
	if x != nil {
		return x.StepType
	}
	return StepType_STEP_TYPE_UNSPECIFIED
}

func (x *StepFailed) GetError() string {
	if x != nil {
		return x.Error
	}
	return ""
}

func (x *StepFailed) GetAttempts() int32 {
	if x != nil {
		return x.Attempts
	}
	return 0
}

func (x *StepFailed) GetFailedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.FailedAt
	}
	return nil
}

// CompensationTriggered → fp.pipelines.compensation.triggered
//
//	CONSUMERS: notification (a first-class "the saga is now rolling back" signal).
//	WHY a distinct event from StepFailed: a failure is the cause; this marks the
//	UNDO phase beginning. compensating_step_ids exposes the rollback PLAN (the
//	reverse-completion order steps will be undone in) so operators can watch it.
type CompensationTriggered struct {
	state       protoimpl.MessageState `protogen:"open.v1"`
	ExecutionId string                 `protobuf:"bytes,1,opt,name=execution_id,json=executionId,proto3" json:"execution_id,omitempty"`
	PipelineId  string                 `protobuf:"bytes,2,opt,name=pipeline_id,json=pipelineId,proto3" json:"pipeline_id,omitempty"`
	// The step whose failure triggered compensation.
	FailedStepId string `protobuf:"bytes,3,opt,name=failed_step_id,json=failedStepId,proto3" json:"failed_step_id,omitempty"`
	// The ordered step ids that will be compensated, in REVERSE completion order
	// (the order they'll actually be undone). Makes the rollback plan observable.
	CompensatingStepIds []string `protobuf:"bytes,4,rep,name=compensating_step_ids,json=compensatingStepIds,proto3" json:"compensating_step_ids,omitempty"`
	// When compensation began (producer clock).
	TriggeredAt   *timestamppb.Timestamp `protobuf:"bytes,5,opt,name=triggered_at,json=triggeredAt,proto3" json:"triggered_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *CompensationTriggered) Reset() {
	*x = CompensationTriggered{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[13]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CompensationTriggered) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CompensationTriggered) ProtoMessage() {}

func (x *CompensationTriggered) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[13]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CompensationTriggered.ProtoReflect.Descriptor instead.
func (*CompensationTriggered) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{13}
}

func (x *CompensationTriggered) GetExecutionId() string {
	if x != nil {
		return x.ExecutionId
	}
	return ""
}

func (x *CompensationTriggered) GetPipelineId() string {
	if x != nil {
		return x.PipelineId
	}
	return ""
}

func (x *CompensationTriggered) GetFailedStepId() string {
	if x != nil {
		return x.FailedStepId
	}
	return ""
}

func (x *CompensationTriggered) GetCompensatingStepIds() []string {
	if x != nil {
		return x.CompensatingStepIds
	}
	return nil
}

func (x *CompensationTriggered) GetTriggeredAt() *timestamppb.Timestamp {
	if x != nil {
		return x.TriggeredAt
	}
	return nil
}

// PipelineCompleted → fp.pipelines.completed
//
//	CONSUMERS: experiment-tracker (close out the run + record duration),
//	           notification (optional success announce).
type PipelineCompleted struct {
	state        protoimpl.MessageState `protogen:"open.v1"`
	ExecutionId  string                 `protobuf:"bytes,1,opt,name=execution_id,json=executionId,proto3" json:"execution_id,omitempty"`
	PipelineId   string                 `protobuf:"bytes,2,opt,name=pipeline_id,json=pipelineId,proto3" json:"pipeline_id,omitempty"`
	PipelineType PipelineType           `protobuf:"varint,3,opt,name=pipeline_type,json=pipelineType,proto3,enum=forgepoint.events.v1.PipelineType" json:"pipeline_type,omitempty"`
	// Total wall-clock duration of the run (for SLO/latency dashboards).
	Duration *durationpb.Duration `protobuf:"bytes,4,opt,name=duration,proto3" json:"duration,omitempty"`
	// When the run reached COMPLETED (producer clock).
	CompletedAt   *timestamppb.Timestamp `protobuf:"bytes,5,opt,name=completed_at,json=completedAt,proto3" json:"completed_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *PipelineCompleted) Reset() {
	*x = PipelineCompleted{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[14]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PipelineCompleted) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PipelineCompleted) ProtoMessage() {}

func (x *PipelineCompleted) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[14]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PipelineCompleted.ProtoReflect.Descriptor instead.
func (*PipelineCompleted) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{14}
}

func (x *PipelineCompleted) GetExecutionId() string {
	if x != nil {
		return x.ExecutionId
	}
	return ""
}

func (x *PipelineCompleted) GetPipelineId() string {
	if x != nil {
		return x.PipelineId
	}
	return ""
}

func (x *PipelineCompleted) GetPipelineType() PipelineType {
	if x != nil {
		return x.PipelineType
	}
	return PipelineType_PIPELINE_TYPE_UNSPECIFIED
}

func (x *PipelineCompleted) GetDuration() *durationpb.Duration {
	if x != nil {
		return x.Duration
	}
	return nil
}

func (x *PipelineCompleted) GetCompletedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CompletedAt
	}
	return nil
}

// PipelineFailed → fp.pipelines.failed
//
//	CONSUMERS: notification (turn into a page/alert).
//	WHY compensation_failed: a clean rollback vs a STUCK SAGA (orphaned side
//	effects) are different severities — Notification escalates the latter harder.
type PipelineFailed struct {
	state        protoimpl.MessageState `protogen:"open.v1"`
	ExecutionId  string                 `protobuf:"bytes,1,opt,name=execution_id,json=executionId,proto3" json:"execution_id,omitempty"`
	PipelineId   string                 `protobuf:"bytes,2,opt,name=pipeline_id,json=pipelineId,proto3" json:"pipeline_id,omitempty"`
	PipelineType PipelineType           `protobuf:"varint,3,opt,name=pipeline_type,json=pipelineType,proto3,enum=forgepoint.events.v1.PipelineType" json:"pipeline_type,omitempty"`
	// The step whose failure ultimately failed the run.
	FailedStepId string `protobuf:"bytes,4,opt,name=failed_step_id,json=failedStepId,proto3" json:"failed_step_id,omitempty"`
	// Failure summary for the alert.
	Error string `protobuf:"bytes,5,opt,name=error,proto3" json:"error,omitempty"`
	// True if compensation itself failed (a "stuck saga"). Escalate harder.
	CompensationFailed bool `protobuf:"varint,6,opt,name=compensation_failed,json=compensationFailed,proto3" json:"compensation_failed,omitempty"`
	// When the run reached terminal FAILED (producer clock).
	FailedAt      *timestamppb.Timestamp `protobuf:"bytes,7,opt,name=failed_at,json=failedAt,proto3" json:"failed_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *PipelineFailed) Reset() {
	*x = PipelineFailed{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[15]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PipelineFailed) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PipelineFailed) ProtoMessage() {}

func (x *PipelineFailed) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[15]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PipelineFailed.ProtoReflect.Descriptor instead.
func (*PipelineFailed) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{15}
}

func (x *PipelineFailed) GetExecutionId() string {
	if x != nil {
		return x.ExecutionId
	}
	return ""
}

func (x *PipelineFailed) GetPipelineId() string {
	if x != nil {
		return x.PipelineId
	}
	return ""
}

func (x *PipelineFailed) GetPipelineType() PipelineType {
	if x != nil {
		return x.PipelineType
	}
	return PipelineType_PIPELINE_TYPE_UNSPECIFIED
}

func (x *PipelineFailed) GetFailedStepId() string {
	if x != nil {
		return x.FailedStepId
	}
	return ""
}

func (x *PipelineFailed) GetError() string {
	if x != nil {
		return x.Error
	}
	return ""
}

func (x *PipelineFailed) GetCompensationFailed() bool {
	if x != nil {
		return x.CompensationFailed
	}
	return false
}

func (x *PipelineFailed) GetFailedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.FailedAt
	}
	return nil
}

// ModelDeployed → fp.pipelines.model.deployed
//
//	PRODUCER:  pipeline-orchestrator — the DEPLOYMENT SAGA OWNS this. (Was MISSING
//	           from every proto though the gateway/serving consume it; added here,
//	           conflict #2.)
//	CONSUMERS: inference-gateway (ADD a route/target → start splitting traffic to
//	           the version), serving (ensure the version is LOADED), experiment-tracker
//	           (record the deploy in the model's history).
//	WHY THIS LIVES UNDER fp.pipelines.* (not fp.models.*): the FACT being announced
//	is "the deployment saga deployed a version", produced and owned by the saga.
//	The model's own lifecycle facts (registered/promoted) live under fp.models.*;
//	the act of DEPLOYING (a workflow outcome) lives under the workflow domain.
//	This is the resolution of the prior fp.models.* vs fp.pipelines.* inconsistency.
//	WHY endpoint + weight_bps: the gateway needs the serving endpoint (resolved by
//	the saga, NOT client-supplied — SSRF guard) and the initial canary weight to
//	build the route target. traffic splitting is in basis points (0–10000).
type ModelDeployed struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The deployed model + the exact version now serving.
	ModelId   string `protobuf:"bytes,1,opt,name=model_id,json=modelId,proto3" json:"model_id,omitempty"`
	ModelName string `protobuf:"bytes,2,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	VersionId string `protobuf:"bytes,3,opt,name=version_id,json=versionId,proto3" json:"version_id,omitempty"`
	Version   string `protobuf:"bytes,4,opt,name=version,proto3" json:"version,omitempty"`
	// The model-serving endpoint for this version (e.g. "iris-v3.fp-models.svc:9090").
	// Resolved by the orchestrator from the deploy; the gateway uses it as the
	// route target's backend. Authoritative — never a client value.
	Endpoint string `protobuf:"bytes,5,opt,name=endpoint,proto3" json:"endpoint,omitempty"`
	// Initial traffic share for this version in basis points (0–10000). A canary
	// typically deploys at a small weight (e.g. 1000 = 10%) and is dialed up by the
	// saga's CANARY/PROMOTE steps via SetTrafficSplit.
	WeightBps int32 `protobuf:"varint,6,opt,name=weight_bps,json=weightBps,proto3" json:"weight_bps,omitempty"`
	// The execution that performed the deploy (lineage back to the saga).
	ExecutionId string `protobuf:"bytes,7,opt,name=execution_id,json=executionId,proto3" json:"execution_id,omitempty"`
	// When the deploy step completed (producer clock).
	DeployedAt    *timestamppb.Timestamp `protobuf:"bytes,8,opt,name=deployed_at,json=deployedAt,proto3" json:"deployed_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ModelDeployed) Reset() {
	*x = ModelDeployed{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[16]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ModelDeployed) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ModelDeployed) ProtoMessage() {}

func (x *ModelDeployed) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[16]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ModelDeployed.ProtoReflect.Descriptor instead.
func (*ModelDeployed) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{16}
}

func (x *ModelDeployed) GetModelId() string {
	if x != nil {
		return x.ModelId
	}
	return ""
}

func (x *ModelDeployed) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *ModelDeployed) GetVersionId() string {
	if x != nil {
		return x.VersionId
	}
	return ""
}

func (x *ModelDeployed) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *ModelDeployed) GetEndpoint() string {
	if x != nil {
		return x.Endpoint
	}
	return ""
}

func (x *ModelDeployed) GetWeightBps() int32 {
	if x != nil {
		return x.WeightBps
	}
	return 0
}

func (x *ModelDeployed) GetExecutionId() string {
	if x != nil {
		return x.ExecutionId
	}
	return ""
}

func (x *ModelDeployed) GetDeployedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.DeployedAt
	}
	return nil
}

// ModelUndeployed → fp.pipelines.model.undeployed
//
//	PRODUCER:  pipeline-orchestrator (a saga removed a version from serving — a
//	           rollback compensation, a scale-to-zero, or an explicit teardown).
//	CONSUMERS: inference-gateway (REMOVE the route target → stop traffic), serving
//	           (UNLOAD the model to free memory).
//	WHY reason: distinguishes an intentional teardown from a rollback so consumers
//	(and operators) don't alert on an expected removal.
type ModelUndeployed struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The model + version being removed from serving.
	ModelId   string `protobuf:"bytes,1,opt,name=model_id,json=modelId,proto3" json:"model_id,omitempty"`
	ModelName string `protobuf:"bytes,2,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	VersionId string `protobuf:"bytes,3,opt,name=version_id,json=versionId,proto3" json:"version_id,omitempty"`
	Version   string `protobuf:"bytes,4,opt,name=version,proto3" json:"version,omitempty"`
	// Why it was undeployed ("rollback","superseded","scale_to_zero","teardown").
	// Free-form but small; lets consumers suppress alerts on expected removals.
	Reason string `protobuf:"bytes,5,opt,name=reason,proto3" json:"reason,omitempty"`
	// The execution that performed the undeploy (lineage).
	ExecutionId string `protobuf:"bytes,6,opt,name=execution_id,json=executionId,proto3" json:"execution_id,omitempty"`
	// When the undeploy completed (producer clock).
	UndeployedAt  *timestamppb.Timestamp `protobuf:"bytes,7,opt,name=undeployed_at,json=undeployedAt,proto3" json:"undeployed_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ModelUndeployed) Reset() {
	*x = ModelUndeployed{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[17]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ModelUndeployed) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ModelUndeployed) ProtoMessage() {}

func (x *ModelUndeployed) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[17]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ModelUndeployed.ProtoReflect.Descriptor instead.
func (*ModelUndeployed) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{17}
}

func (x *ModelUndeployed) GetModelId() string {
	if x != nil {
		return x.ModelId
	}
	return ""
}

func (x *ModelUndeployed) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *ModelUndeployed) GetVersionId() string {
	if x != nil {
		return x.VersionId
	}
	return ""
}

func (x *ModelUndeployed) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *ModelUndeployed) GetReason() string {
	if x != nil {
		return x.Reason
	}
	return ""
}

func (x *ModelUndeployed) GetExecutionId() string {
	if x != nil {
		return x.ExecutionId
	}
	return ""
}

func (x *ModelUndeployed) GetUndeployedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.UndeployedAt
	}
	return nil
}

// InferenceCompleted → fp.inference.completed
//
//	PRODUCER:  inference-gateway — THE SINGLE CANONICAL inference event. The
//	           gateway is the entry point and the ONLY component that knows the
//	           full picture: end-to-end latency, which version actually served
//	           (post traffic-split / canary), the billed principal (api_key_id),
//	           and the request_id it minted. serving MUST NOT publish a competing
//	           metering event (serving.PredictionCompletedEvent is deleted — see
//	           conflict #1).
//	CONSUMERS: billing (meter usage — keys on api_key_id + request_id, dedupes on
//	           request_id), experiment-tracker (record A/B outcomes per served
//	           version), model-monitor (fold feature_summary into DATA-drift
//	           windows, prediction_summary into PREDICTION-drift windows, latency
//	           into performance signals; request_id is the JOIN KEY for delayed
//	           ground truth).
//	SECURITY/PII (this leaves the gateway over the bus): IDs + SUMMARIES only —
//	never raw input/output tensors, never an end-user identity. api_key_id is a
//	reference, not the secret. The summaries are statistical (see their docs).
//	IDEMPOTENCY: EventEnvelope.id dedupes redeliveries at the transport layer;
//	request_id is the BUSINESS dedupe handle (Billing won't double-meter a
//	request_id it already recorded — the NATS→DB half of exactly-once-in-effect).
type InferenceCompleted struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Gateway-minted request id (matches PredictResponse.request_id). The business
	// idempotency key for Billing AND the join key for Monitor's ground-truth
	// matching. SERVER-authoritative.
	RequestId string `protobuf:"bytes,1,opt,name=request_id,json=requestId,proto3" json:"request_id,omitempty"`
	// The model that served (registry id) + its public name (for dashboards).
	ModelId   string `protobuf:"bytes,2,opt,name=model_id,json=modelId,proto3" json:"model_id,omitempty"`
	ModelName string `protobuf:"bytes,3,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// The version that ACTUALLY served, post traffic-split. KEY for A/B analysis
	// and per-version metering/monitoring. SERVER-authoritative.
	Version string `protobuf:"bytes,4,opt,name=version,proto3" json:"version,omitempty"`
	// The API key that made the call (id, never the raw secret). Billing meters
	// against this (→ team/rate-plan). SERVER-stamped from the authed request.
	ApiKeyId string `protobuf:"bytes,5,opt,name=api_key_id,json=apiKeyId,proto3" json:"api_key_id,omitempty"`
	// True if this prediction was served by a CANARY target (a non-stable version
	// receiving a traffic slice). Lets Experiment Tracker bucket A/B outcomes and
	// Monitor weight canary traffic distinctly. SERVER-authoritative.
	IsCanary bool `protobuf:"varint,6,opt,name=is_canary,json=isCanary,proto3" json:"is_canary,omitempty"`
	// Observed backend latency, precise typed value + a denormalized whole-ms
	// convenience for simple consumers/Grafana panels (derived from `latency`).
	Latency   *durationpb.Duration `protobuf:"bytes,7,opt,name=latency,proto3" json:"latency,omitempty"`
	LatencyMs int64                `protobuf:"varint,8,opt,name=latency_ms,json=latencyMs,proto3" json:"latency_ms,omitempty"`
	// Token count for the call (prompt+completion), when applicable — lets Billing
	// meter METER_TYPE_INFERENCE_TOKENS off the single canonical event rather than
	// a competing serving event. 0 for non-token models.
	TokenCount int64 `protobuf:"varint,9,opt,name=token_count,json=tokenCount,proto3" json:"token_count,omitempty"`
	// Compact prediction summary for prediction-drift / A-B (NOT raw outputs).
	PredictionSummary *PredictionSummary `protobuf:"bytes,10,opt,name=prediction_summary,json=predictionSummary,proto3" json:"prediction_summary,omitempty"`
	// Compact input feature summary for data-drift detection (NOT raw inputs).
	FeatureSummary *FeatureSummary `protobuf:"bytes,11,opt,name=feature_summary,json=featureSummary,proto3" json:"feature_summary,omitempty"`
	// When the inference completed (gateway clock). SERVER-authoritative; drives
	// billing periods and time-windowed drift aggregation.
	CompletedAt   *timestamppb.Timestamp `protobuf:"bytes,12,opt,name=completed_at,json=completedAt,proto3" json:"completed_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *InferenceCompleted) Reset() {
	*x = InferenceCompleted{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[18]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *InferenceCompleted) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InferenceCompleted) ProtoMessage() {}

func (x *InferenceCompleted) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[18]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use InferenceCompleted.ProtoReflect.Descriptor instead.
func (*InferenceCompleted) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{18}
}

func (x *InferenceCompleted) GetRequestId() string {
	if x != nil {
		return x.RequestId
	}
	return ""
}

func (x *InferenceCompleted) GetModelId() string {
	if x != nil {
		return x.ModelId
	}
	return ""
}

func (x *InferenceCompleted) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *InferenceCompleted) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *InferenceCompleted) GetApiKeyId() string {
	if x != nil {
		return x.ApiKeyId
	}
	return ""
}

func (x *InferenceCompleted) GetIsCanary() bool {
	if x != nil {
		return x.IsCanary
	}
	return false
}

func (x *InferenceCompleted) GetLatency() *durationpb.Duration {
	if x != nil {
		return x.Latency
	}
	return nil
}

func (x *InferenceCompleted) GetLatencyMs() int64 {
	if x != nil {
		return x.LatencyMs
	}
	return 0
}

func (x *InferenceCompleted) GetTokenCount() int64 {
	if x != nil {
		return x.TokenCount
	}
	return 0
}

func (x *InferenceCompleted) GetPredictionSummary() *PredictionSummary {
	if x != nil {
		return x.PredictionSummary
	}
	return nil
}

func (x *InferenceCompleted) GetFeatureSummary() *FeatureSummary {
	if x != nil {
		return x.FeatureSummary
	}
	return nil
}

func (x *InferenceCompleted) GetCompletedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CompletedAt
	}
	return nil
}

// InferenceFailed → fp.inference.failed
//
//	PRODUCER:  inference-gateway.
//	CONSUMERS: model-monitor (error RATE is its own degradation signal — a
//	           spiking failure rate is a kind of drift), notification (alert on
//	           sustained failures).
//	WHY no summaries: on failure there may be no prediction to summarize and we
//	avoid echoing a possibly-malformed input. We carry the failure CLASSIFICATION
//	(which resilience pattern fired) instead, which is what drives the reaction.
//	WHY typically NOT billed: Billing generally does not meter failed calls;
//	carrying api_key_id still lets per-tenant error rates be computed.
type InferenceFailed struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Gateway-minted request id (idempotency + correlation).
	RequestId string `protobuf:"bytes,1,opt,name=request_id,json=requestId,proto3" json:"request_id,omitempty"`
	// The model the client targeted (+ id if routing resolved it).
	ModelId   string `protobuf:"bytes,2,opt,name=model_id,json=modelId,proto3" json:"model_id,omitempty"`
	ModelName string `protobuf:"bytes,3,opt,name=model_name,json=modelName,proto3" json:"model_name,omitempty"`
	// The version attempted, if routing got that far (empty if it failed before
	// version selection, e.g. NO_ROUTE).
	Version string `protobuf:"bytes,4,opt,name=version,proto3" json:"version,omitempty"`
	// The API key that made the call (id only) — per-tenant error rates / policy.
	ApiKeyId string `protobuf:"bytes,5,opt,name=api_key_id,json=apiKeyId,proto3" json:"api_key_id,omitempty"`
	// Why it failed — the resilience-pattern outcome (drives Monitor/alerts).
	Reason InferenceFailureReason `protobuf:"varint,6,opt,name=reason,proto3,enum=forgepoint.events.v1.InferenceFailureReason" json:"reason,omitempty"`
	// Structured error detail (reuses common.ErrorDetail so consumers handle it
	// uniformly with the sync API). Human-readable message + code, no PII.
	Error *v1.ErrorDetail `protobuf:"bytes,7,opt,name=error,proto3" json:"error,omitempty"`
	// When the failure occurred (gateway clock).
	FailedAt      *timestamppb.Timestamp `protobuf:"bytes,8,opt,name=failed_at,json=failedAt,proto3" json:"failed_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *InferenceFailed) Reset() {
	*x = InferenceFailed{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[19]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *InferenceFailed) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InferenceFailed) ProtoMessage() {}

func (x *InferenceFailed) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[19]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use InferenceFailed.ProtoReflect.Descriptor instead.
func (*InferenceFailed) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{19}
}

func (x *InferenceFailed) GetRequestId() string {
	if x != nil {
		return x.RequestId
	}
	return ""
}

func (x *InferenceFailed) GetModelId() string {
	if x != nil {
		return x.ModelId
	}
	return ""
}

func (x *InferenceFailed) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *InferenceFailed) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *InferenceFailed) GetApiKeyId() string {
	if x != nil {
		return x.ApiKeyId
	}
	return ""
}

func (x *InferenceFailed) GetReason() InferenceFailureReason {
	if x != nil {
		return x.Reason
	}
	return InferenceFailureReason_INFERENCE_FAILURE_REASON_UNSPECIFIED
}

func (x *InferenceFailed) GetError() *v1.ErrorDetail {
	if x != nil {
		return x.Error
	}
	return nil
}

func (x *InferenceFailed) GetFailedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.FailedAt
	}
	return nil
}

// FeatureViewDefined → fp.features.view.defined
//
//	PRODUCER:  feature-store (after DefineFeatureView appends a FeatureViewDefined
//	           event to the log / bumps schema_version).
//	CONSUMERS: experiment-tracker (correlate which feature schema versions fed a
//	           training run); audit subscribers (log schema changes).
type FeatureViewDefined struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The view's stable id + name (name for human-friendly routing/filtering).
	FeatureViewId   string `protobuf:"bytes,1,opt,name=feature_view_id,json=featureViewId,proto3" json:"feature_view_id,omitempty"`
	FeatureViewName string `protobuf:"bytes,2,opt,name=feature_view_name,json=featureViewName,proto3" json:"feature_view_name,omitempty"`
	// Schema version this event represents (1 on create, +1 on each evolve).
	SchemaVersion int64 `protobuf:"varint,3,opt,name=schema_version,json=schemaVersion,proto3" json:"schema_version,omitempty"`
	// Owning team (team-scoped consumers/audit). Producer-assigned from auth claims.
	OwnerTeam string `protobuf:"bytes,4,opt,name=owner_team,json=ownerTeam,proto3" json:"owner_team,omitempty"`
	// When the definition/evolution happened (producer clock).
	DefinedAt     *timestamppb.Timestamp `protobuf:"bytes,5,opt,name=defined_at,json=definedAt,proto3" json:"defined_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *FeatureViewDefined) Reset() {
	*x = FeatureViewDefined{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[20]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *FeatureViewDefined) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FeatureViewDefined) ProtoMessage() {}

func (x *FeatureViewDefined) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[20]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FeatureViewDefined.ProtoReflect.Descriptor instead.
func (*FeatureViewDefined) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{20}
}

func (x *FeatureViewDefined) GetFeatureViewId() string {
	if x != nil {
		return x.FeatureViewId
	}
	return ""
}

func (x *FeatureViewDefined) GetFeatureViewName() string {
	if x != nil {
		return x.FeatureViewName
	}
	return ""
}

func (x *FeatureViewDefined) GetSchemaVersion() int64 {
	if x != nil {
		return x.SchemaVersion
	}
	return 0
}

func (x *FeatureViewDefined) GetOwnerTeam() string {
	if x != nil {
		return x.OwnerTeam
	}
	return ""
}

func (x *FeatureViewDefined) GetDefinedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.DefinedAt
	}
	return nil
}

// FeaturesWritten → fp.features.written
//
//	PRODUCER:  feature-store (after a WriteFeatures append commits to the log).
//	CONSUMERS: experiment-tracker (record which feature versions fed a run),
//	           model-monitor (scope drift checks / cache invalidation to the
//	           changed entities).
//	NAME RESOLUTION (conflict #4): the platform settles on ONE name —
//	FeaturesWritten on subject fp.features.written. The design doc's
//	"FeaturesIngested" / "fp.features.ingested" and the experiment-tracker's
//	previously-consumed "fp.features.ingested" are RECONCILED to this. Fix agents
//	MUST repoint the experiment-tracker subscription to fp.features.written.
//	THIN BY DESIGN (the thin-event side of the tradeoff): carries the version
//	range + affected entity ids + a count, NOT the feature values themselves.
//	Events are for notification/correlation; a consumer that needs the actual
//	values calls GetOnlineFeatures/GetHistoricalFeatures (the read models). This
//	keeps NATS small and stops the bus becoming a second source of truth.
type FeaturesWritten struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The view that received the append (id + name; name is a subscriber convenience).
	FeatureViewId   string `protobuf:"bytes,1,opt,name=feature_view_id,json=featureViewId,proto3" json:"feature_view_id,omitempty"`
	FeatureViewName string `protobuf:"bytes,2,opt,name=feature_view_name,json=featureViewName,proto3" json:"feature_view_name,omitempty"`
	// Entity ids whose features changed in this batch. Consumers (Monitor) use them
	// to scope drift checks / invalidate caches. MAY be truncated for very large
	// batches — written_count remains the authoritative total.
	EntityIds []string `protobuf:"bytes,3,rep,name=entity_ids,json=entityIds,proto3" json:"entity_ids,omitempty"`
	// Total rows appended (authoritative even if entity_ids is truncated).
	WrittenCount int32 `protobuf:"varint,4,opt,name=written_count,json=writtenCount,proto3" json:"written_count,omitempty"`
	// Highest event-log version assigned by this append. A consumer can request
	// features as_of_version >= this to be sure it sees the new data.
	WrittenThroughVersion int64 `protobuf:"varint,5,opt,name=written_through_version,json=writtenThroughVersion,proto3" json:"written_through_version,omitempty"`
	// When the append committed (producer clock).
	WrittenAt     *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=written_at,json=writtenAt,proto3" json:"written_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *FeaturesWritten) Reset() {
	*x = FeaturesWritten{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[21]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *FeaturesWritten) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FeaturesWritten) ProtoMessage() {}

func (x *FeaturesWritten) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[21]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FeaturesWritten.ProtoReflect.Descriptor instead.
func (*FeaturesWritten) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{21}
}

func (x *FeaturesWritten) GetFeatureViewId() string {
	if x != nil {
		return x.FeatureViewId
	}
	return ""
}

func (x *FeaturesWritten) GetFeatureViewName() string {
	if x != nil {
		return x.FeatureViewName
	}
	return ""
}

func (x *FeaturesWritten) GetEntityIds() []string {
	if x != nil {
		return x.EntityIds
	}
	return nil
}

func (x *FeaturesWritten) GetWrittenCount() int32 {
	if x != nil {
		return x.WrittenCount
	}
	return 0
}

func (x *FeaturesWritten) GetWrittenThroughVersion() int64 {
	if x != nil {
		return x.WrittenThroughVersion
	}
	return 0
}

func (x *FeaturesWritten) GetWrittenAt() *timestamppb.Timestamp {
	if x != nil {
		return x.WrittenAt
	}
	return nil
}

// UsageRecorded → fp.billing.usage.recorded
//
//	PRODUCER:  billing (after a UsageRecord commits, via the outbox).
//	CONSUMERS: experiment-tracker (attribute cost to a run/model), usage dashboards.
//	DOC NOTE (conflict #6): the design doc's fp.billing.> hierarchy explicitly
//	lists quota.exceeded and invoice.generated; UsageRecorded was implied by the
//	outbox example. We add fp.billing.usage.recorded under the sanctioned tree
//	(recorded as a doc addition in event-contract.md).
//	FAT-BUT-FLAT: carries the full priced fact (team, meter, quantity,
//	server-computed cost) so a consumer needs no callback. cost is in MICRO units
//	of the currency (1e-6; 1_000_000 == 1.00) — exact integer money math.
type UsageRecorded struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Ledger record id (UUID) — business dedupe handle for consumers.
	RecordId string `protobuf:"bytes,1,opt,name=record_id,json=recordId,proto3" json:"record_id,omitempty"`
	// The team billed (producer-derived from the inference api_key / auth claims —
	// never a client value; naming the billed team is account-takeover-for-money).
	Team string `protobuf:"bytes,2,opt,name=team,proto3" json:"team,omitempty"`
	// The rate plan applied (pinned so the price is reproducible).
	RatePlanId string `protobuf:"bytes,3,opt,name=rate_plan_id,json=ratePlanId,proto3" json:"rate_plan_id,omitempty"`
	// What was metered + how many units (request count / tokens / compute-secs / bytes).
	MeterType MeterType `protobuf:"varint,4,opt,name=meter_type,json=meterType,proto3,enum=forgepoint.events.v1.MeterType" json:"meter_type,omitempty"`
	Quantity  int64     `protobuf:"varint,5,opt,name=quantity,proto3" json:"quantity,omitempty"`
	// Server-COMPUTED cost: amount in micro-units + ISO-4217 currency. Clients
	// never assert this; the server prices quantity against the plan.
	CostMicros   int64  `protobuf:"varint,6,opt,name=cost_micros,json=costMicros,proto3" json:"cost_micros,omitempty"`
	CurrencyCode string `protobuf:"bytes,7,opt,name=currency_code,json=currencyCode,proto3" json:"currency_code,omitempty"`
	// What produced the usage (attribution / per-model reports).
	ModelId      string `protobuf:"bytes,8,opt,name=model_id,json=modelId,proto3" json:"model_id,omitempty"`
	ModelVersion string `protobuf:"bytes,9,opt,name=model_version,json=modelVersion,proto3" json:"model_version,omitempty"`
	// The originating inference request id (lineage + the natural dedupe anchor:
	// two records must never share a source_request_id).
	SourceRequestId string `protobuf:"bytes,10,opt,name=source_request_id,json=sourceRequestId,proto3" json:"source_request_id,omitempty"`
	// When the metered action occurred (event time, not insert time).
	OccurredAt    *timestamppb.Timestamp `protobuf:"bytes,11,opt,name=occurred_at,json=occurredAt,proto3" json:"occurred_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UsageRecorded) Reset() {
	*x = UsageRecorded{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[22]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UsageRecorded) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UsageRecorded) ProtoMessage() {}

func (x *UsageRecorded) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[22]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UsageRecorded.ProtoReflect.Descriptor instead.
func (*UsageRecorded) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{22}
}

func (x *UsageRecorded) GetRecordId() string {
	if x != nil {
		return x.RecordId
	}
	return ""
}

func (x *UsageRecorded) GetTeam() string {
	if x != nil {
		return x.Team
	}
	return ""
}

func (x *UsageRecorded) GetRatePlanId() string {
	if x != nil {
		return x.RatePlanId
	}
	return ""
}

func (x *UsageRecorded) GetMeterType() MeterType {
	if x != nil {
		return x.MeterType
	}
	return MeterType_METER_TYPE_UNSPECIFIED
}

func (x *UsageRecorded) GetQuantity() int64 {
	if x != nil {
		return x.Quantity
	}
	return 0
}

func (x *UsageRecorded) GetCostMicros() int64 {
	if x != nil {
		return x.CostMicros
	}
	return 0
}

func (x *UsageRecorded) GetCurrencyCode() string {
	if x != nil {
		return x.CurrencyCode
	}
	return ""
}

func (x *UsageRecorded) GetModelId() string {
	if x != nil {
		return x.ModelId
	}
	return ""
}

func (x *UsageRecorded) GetModelVersion() string {
	if x != nil {
		return x.ModelVersion
	}
	return ""
}

func (x *UsageRecorded) GetSourceRequestId() string {
	if x != nil {
		return x.SourceRequestId
	}
	return ""
}

func (x *UsageRecorded) GetOccurredAt() *timestamppb.Timestamp {
	if x != nil {
		return x.OccurredAt
	}
	return nil
}

// QuotaExceeded → fp.billing.quota.exceeded
//
//	PRODUCER:  billing (when recorded usage pushes a team over its plan quota;
//	           written to the outbox in the SAME tx as the usage update).
//	CONSUMERS: notification (alert the team), inference-gateway (flip the team's
//	           Redis quota cache to "blocked" so subsequent inferences are
//	           rejected/throttled — eventual consistency is acceptable for the
//	           gateway pre-flight).
//	FAT enough to act: carries the limit AND current usage so the consumer can
//	render "1,012 / 1,000" and the gateway can decide policy without a callback.
type QuotaExceeded struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The team that exceeded quota + the plan whose quota was hit.
	Team       string `protobuf:"bytes,1,opt,name=team,proto3" json:"team,omitempty"`
	RatePlanId string `protobuf:"bytes,2,opt,name=rate_plan_id,json=ratePlanId,proto3" json:"rate_plan_id,omitempty"`
	// Which meter's quota was exceeded.
	MeterType MeterType `protobuf:"varint,3,opt,name=meter_type,json=meterType,proto3,enum=forgepoint.events.v1.MeterType" json:"meter_type,omitempty"`
	// The plan's cap for that meter (the limit crossed) and the team's current
	// usage (>= limit), so the consumer shows the ratio without a callback.
	QuotaLimit   int64 `protobuf:"varint,4,opt,name=quota_limit,json=quotaLimit,proto3" json:"quota_limit,omitempty"`
	CurrentUsage int64 `protobuf:"varint,5,opt,name=current_usage,json=currentUsage,proto3" json:"current_usage,omitempty"`
	// When the threshold was crossed (event time).
	OccurredAt    *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=occurred_at,json=occurredAt,proto3" json:"occurred_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *QuotaExceeded) Reset() {
	*x = QuotaExceeded{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[23]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *QuotaExceeded) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QuotaExceeded) ProtoMessage() {}

func (x *QuotaExceeded) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[23]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QuotaExceeded.ProtoReflect.Descriptor instead.
func (*QuotaExceeded) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{23}
}

func (x *QuotaExceeded) GetTeam() string {
	if x != nil {
		return x.Team
	}
	return ""
}

func (x *QuotaExceeded) GetRatePlanId() string {
	if x != nil {
		return x.RatePlanId
	}
	return ""
}

func (x *QuotaExceeded) GetMeterType() MeterType {
	if x != nil {
		return x.MeterType
	}
	return MeterType_METER_TYPE_UNSPECIFIED
}

func (x *QuotaExceeded) GetQuotaLimit() int64 {
	if x != nil {
		return x.QuotaLimit
	}
	return 0
}

func (x *QuotaExceeded) GetCurrentUsage() int64 {
	if x != nil {
		return x.CurrentUsage
	}
	return 0
}

func (x *QuotaExceeded) GetOccurredAt() *timestamppb.Timestamp {
	if x != nil {
		return x.OccurredAt
	}
	return nil
}

// InvoiceGenerated → fp.billing.invoice.generated
//
//	PRODUCER:  billing (when a period closes and an invoice goes DRAFT→FINALIZED;
//	           via the outbox).
//	CONSUMERS: notification (email/Slack the finalized invoice), external
//	           payment/accounting integrations.
//	FAT-BUT-FLAT: carries the headline invoice facts (number, team, period,
//	total) so Notification can render/send without a GetInvoice callback. The
//	full line-item breakdown stays in the Invoice read model (GetInvoice) — a
//	deliberate thin-tail: the alert needs the total, not every line.
type InvoiceGenerated struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Invoice UUID + the human-facing invoice number (safe to print/share).
	InvoiceId     string `protobuf:"bytes,1,opt,name=invoice_id,json=invoiceId,proto3" json:"invoice_id,omitempty"`
	InvoiceNumber string `protobuf:"bytes,2,opt,name=invoice_number,json=invoiceNumber,proto3" json:"invoice_number,omitempty"`
	// Team billed + the rate plan in effect for the period.
	Team       string `protobuf:"bytes,3,opt,name=team,proto3" json:"team,omitempty"`
	RatePlanId string `protobuf:"bytes,4,opt,name=rate_plan_id,json=ratePlanId,proto3" json:"rate_plan_id,omitempty"`
	// Billing period covered [start, end).
	PeriodStart *timestamppb.Timestamp `protobuf:"bytes,5,opt,name=period_start,json=periodStart,proto3" json:"period_start,omitempty"`
	PeriodEnd   *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=period_end,json=periodEnd,proto3" json:"period_end,omitempty"`
	// The invoice total: amount in micro-units + ISO-4217 currency (rounded to the
	// currency minor unit at finalization). Server-computed, authoritative.
	TotalMicros  int64  `protobuf:"varint,7,opt,name=total_micros,json=totalMicros,proto3" json:"total_micros,omitempty"`
	CurrencyCode string `protobuf:"bytes,8,opt,name=currency_code,json=currencyCode,proto3" json:"currency_code,omitempty"`
	// When the invoice was finalized (producer clock).
	FinalizedAt   *timestamppb.Timestamp `protobuf:"bytes,9,opt,name=finalized_at,json=finalizedAt,proto3" json:"finalized_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *InvoiceGenerated) Reset() {
	*x = InvoiceGenerated{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[24]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *InvoiceGenerated) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InvoiceGenerated) ProtoMessage() {}

func (x *InvoiceGenerated) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[24]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use InvoiceGenerated.ProtoReflect.Descriptor instead.
func (*InvoiceGenerated) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{24}
}

func (x *InvoiceGenerated) GetInvoiceId() string {
	if x != nil {
		return x.InvoiceId
	}
	return ""
}

func (x *InvoiceGenerated) GetInvoiceNumber() string {
	if x != nil {
		return x.InvoiceNumber
	}
	return ""
}

func (x *InvoiceGenerated) GetTeam() string {
	if x != nil {
		return x.Team
	}
	return ""
}

func (x *InvoiceGenerated) GetRatePlanId() string {
	if x != nil {
		return x.RatePlanId
	}
	return ""
}

func (x *InvoiceGenerated) GetPeriodStart() *timestamppb.Timestamp {
	if x != nil {
		return x.PeriodStart
	}
	return nil
}

func (x *InvoiceGenerated) GetPeriodEnd() *timestamppb.Timestamp {
	if x != nil {
		return x.PeriodEnd
	}
	return nil
}

func (x *InvoiceGenerated) GetTotalMicros() int64 {
	if x != nil {
		return x.TotalMicros
	}
	return 0
}

func (x *InvoiceGenerated) GetCurrencyCode() string {
	if x != nil {
		return x.CurrencyCode
	}
	return ""
}

func (x *InvoiceGenerated) GetFinalizedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.FinalizedAt
	}
	return nil
}

// RunCreated → fp.experiments.run.created
//
//	PRODUCER:  experiment-tracker (sync StartRun, or async materialization from a
//	           consumed platform event).
//	CONSUMERS: notification (optional "run started" surface); live dashboards.
//	WHY emit on create (not only finish): a live dashboard wants to show in-flight
//	runs immediately.
type RunCreated struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The run + the experiment it belongs to.
	RunId        string `protobuf:"bytes,1,opt,name=run_id,json=runId,proto3" json:"run_id,omitempty"`
	ExperimentId string `protobuf:"bytes,2,opt,name=experiment_id,json=experimentId,proto3" json:"experiment_id,omitempty"`
	// The model version it targets, if any (opaque Registry id — decoupled).
	ModelVersionId string `protobuf:"bytes,3,opt,name=model_version_id,json=modelVersionId,proto3" json:"model_version_id,omitempty"`
	// Optional human label (e.g. "lr=0.01 batch=64").
	DisplayName string `protobuf:"bytes,4,opt,name=display_name,json=displayName,proto3" json:"display_name,omitempty"`
	// When the run started (producer clock).
	StartedAt     *timestamppb.Timestamp `protobuf:"bytes,5,opt,name=started_at,json=startedAt,proto3" json:"started_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *RunCreated) Reset() {
	*x = RunCreated{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[25]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RunCreated) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RunCreated) ProtoMessage() {}

func (x *RunCreated) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[25]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RunCreated.ProtoReflect.Descriptor instead.
func (*RunCreated) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{25}
}

func (x *RunCreated) GetRunId() string {
	if x != nil {
		return x.RunId
	}
	return ""
}

func (x *RunCreated) GetExperimentId() string {
	if x != nil {
		return x.ExperimentId
	}
	return ""
}

func (x *RunCreated) GetModelVersionId() string {
	if x != nil {
		return x.ModelVersionId
	}
	return ""
}

func (x *RunCreated) GetDisplayName() string {
	if x != nil {
		return x.DisplayName
	}
	return ""
}

func (x *RunCreated) GetStartedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.StartedAt
	}
	return nil
}

// RunFinished → fp.experiments.run.finished
//
//	PRODUCER:  experiment-tracker (run reached a terminal state).
//	CONSUMERS: notification (alert on a finished/failed run), leaderboards/BFF
//	           cache invalidation.
//	FAT EVENT: carries final_metrics so a consumer ranks/alerts WITHOUT a GetRun
//	callback (terminal-run metrics are final, so staleness isn't a concern).
type RunFinished struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The run + experiment.
	RunId        string `protobuf:"bytes,1,opt,name=run_id,json=runId,proto3" json:"run_id,omitempty"`
	ExperimentId string `protobuf:"bytes,2,opt,name=experiment_id,json=experimentId,proto3" json:"experiment_id,omitempty"`
	// The model version it produced/evaluated, if any (opaque Registry id).
	ModelVersionId string `protobuf:"bytes,3,opt,name=model_version_id,json=modelVersionId,proto3" json:"model_version_id,omitempty"`
	// Terminal status (FINISHED / FAILED / KILLED).
	Status RunStatus `protobuf:"varint,4,opt,name=status,proto3,enum=forgepoint.events.v1.RunStatus" json:"status,omitempty"`
	// Denormalized headline metrics at finish (final/best per key). Lets a consumer
	// rank/alert without calling back.
	FinalMetrics []*MetricPoint `protobuf:"bytes,5,rep,name=final_metrics,json=finalMetrics,proto3" json:"final_metrics,omitempty"`
	// When the run ended (producer clock).
	EndedAt       *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=ended_at,json=endedAt,proto3" json:"ended_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *RunFinished) Reset() {
	*x = RunFinished{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[26]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RunFinished) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RunFinished) ProtoMessage() {}

func (x *RunFinished) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[26]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RunFinished.ProtoReflect.Descriptor instead.
func (*RunFinished) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{26}
}

func (x *RunFinished) GetRunId() string {
	if x != nil {
		return x.RunId
	}
	return ""
}

func (x *RunFinished) GetExperimentId() string {
	if x != nil {
		return x.ExperimentId
	}
	return ""
}

func (x *RunFinished) GetModelVersionId() string {
	if x != nil {
		return x.ModelVersionId
	}
	return ""
}

func (x *RunFinished) GetStatus() RunStatus {
	if x != nil {
		return x.Status
	}
	return RunStatus_RUN_STATUS_UNSPECIFIED
}

func (x *RunFinished) GetFinalMetrics() []*MetricPoint {
	if x != nil {
		return x.FinalMetrics
	}
	return nil
}

func (x *RunFinished) GetEndedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.EndedAt
	}
	return nil
}

// NotificationDelivered → fp.notifications.delivered
//
//	CONSUMERS: experiment-tracker / dashboards (alert volume + success rates).
type NotificationDelivered struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The notification that was delivered + who it went to.
	NotificationId  string `protobuf:"bytes,1,opt,name=notification_id,json=notificationId,proto3" json:"notification_id,omitempty"`
	RecipientUserId string `protobuf:"bytes,2,opt,name=recipient_user_id,json=recipientUserId,proto3" json:"recipient_user_id,omitempty"`
	// Which channel succeeded.
	Channel NotificationChannel `protobuf:"varint,3,opt,name=channel,proto3,enum=forgepoint.events.v1.NotificationChannel" json:"channel,omitempty"`
	// Provenance: the originating event's type (e.g. "fp.pipelines.failed").
	EventType string `protobuf:"bytes,4,opt,name=event_type,json=eventType,proto3" json:"event_type,omitempty"`
	// When delivery succeeded (producer clock).
	DeliveredAt   *timestamppb.Timestamp `protobuf:"bytes,5,opt,name=delivered_at,json=deliveredAt,proto3" json:"delivered_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *NotificationDelivered) Reset() {
	*x = NotificationDelivered{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[27]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *NotificationDelivered) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NotificationDelivered) ProtoMessage() {}

func (x *NotificationDelivered) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[27]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NotificationDelivered.ProtoReflect.Descriptor instead.
func (*NotificationDelivered) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{27}
}

func (x *NotificationDelivered) GetNotificationId() string {
	if x != nil {
		return x.NotificationId
	}
	return ""
}

func (x *NotificationDelivered) GetRecipientUserId() string {
	if x != nil {
		return x.RecipientUserId
	}
	return ""
}

func (x *NotificationDelivered) GetChannel() NotificationChannel {
	if x != nil {
		return x.Channel
	}
	return NotificationChannel_NOTIFICATION_CHANNEL_UNSPECIFIED
}

func (x *NotificationDelivered) GetEventType() string {
	if x != nil {
		return x.EventType
	}
	return ""
}

func (x *NotificationDelivered) GetDeliveredAt() *timestamppb.Timestamp {
	if x != nil {
		return x.DeliveredAt
	}
	return nil
}

// NotificationFailed → fp.notifications.failed
//
//	CONSUMERS: experiment-tracker / dashboards; on-call escalation / dead-letter.
//	WHY publish a failure event: makes "we failed to reach the user" observable
//	platform-wide and can feed an escalation flow.
type NotificationFailed struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The notification whose delivery failed + who we tried to reach.
	NotificationId  string `protobuf:"bytes,1,opt,name=notification_id,json=notificationId,proto3" json:"notification_id,omitempty"`
	RecipientUserId string `protobuf:"bytes,2,opt,name=recipient_user_id,json=recipientUserId,proto3" json:"recipient_user_id,omitempty"`
	// Which channel failed.
	Channel NotificationChannel `protobuf:"varint,3,opt,name=channel,proto3,enum=forgepoint.events.v1.NotificationChannel" json:"channel,omitempty"`
	// Provenance: the originating event's type.
	EventType string `protobuf:"bytes,4,opt,name=event_type,json=eventType,proto3" json:"event_type,omitempty"`
	// Attempts made before giving up (exhausted retry budget).
	Attempts int32 `protobuf:"varint,5,opt,name=attempts,proto3" json:"attempts,omitempty"`
	// Last failure detail (e.g. "503 from webhook", "circuit breaker open").
	ErrorMessage string `protobuf:"bytes,6,opt,name=error_message,json=errorMessage,proto3" json:"error_message,omitempty"`
	// When we gave up (producer clock).
	FailedAt      *timestamppb.Timestamp `protobuf:"bytes,7,opt,name=failed_at,json=failedAt,proto3" json:"failed_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *NotificationFailed) Reset() {
	*x = NotificationFailed{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[28]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *NotificationFailed) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NotificationFailed) ProtoMessage() {}

func (x *NotificationFailed) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[28]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NotificationFailed.ProtoReflect.Descriptor instead.
func (*NotificationFailed) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{28}
}

func (x *NotificationFailed) GetNotificationId() string {
	if x != nil {
		return x.NotificationId
	}
	return ""
}

func (x *NotificationFailed) GetRecipientUserId() string {
	if x != nil {
		return x.RecipientUserId
	}
	return ""
}

func (x *NotificationFailed) GetChannel() NotificationChannel {
	if x != nil {
		return x.Channel
	}
	return NotificationChannel_NOTIFICATION_CHANNEL_UNSPECIFIED
}

func (x *NotificationFailed) GetEventType() string {
	if x != nil {
		return x.EventType
	}
	return ""
}

func (x *NotificationFailed) GetAttempts() int32 {
	if x != nil {
		return x.Attempts
	}
	return 0
}

func (x *NotificationFailed) GetErrorMessage() string {
	if x != nil {
		return x.ErrorMessage
	}
	return ""
}

func (x *NotificationFailed) GetFailedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.FailedAt
	}
	return nil
}

// UserCreated → fp.auth.user.created
//
//	PRODUCER:  auth (after CreateUser commits the new account to Postgres).
//	CONSUMERS: notification (send a welcome / provisioning message), billing
//	           (open a usage account for the user's team), experiment-tracker /
//	           audit (record that an identity was provisioned). Consumers key off
//	           user_id (stable handle) and team (multi-tenancy scoping).
//	SECURITY/PII: identity ATTRIBUTES only (id, email, name, team, role) — never
//	the password hash or any credential. email is carried because notification
//	needs an address to reach; it is not a secret. SERVER-authoritative: every
//	field is producer-derived from the committed row, never a client assertion.
type UserCreated struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Auth user UUID — the stable handle every other service keys off.
	UserId string `protobuf:"bytes,1,opt,name=user_id,json=userId,proto3" json:"user_id,omitempty"`
	// Login email (the addressable identity). Carried so Notification can reach the
	// user without a callback. NOT a secret; never the password hash.
	Email string `protobuf:"bytes,2,opt,name=email,proto3" json:"email,omitempty"`
	// Display name (non-unique, human-friendly).
	Name string `protobuf:"bytes,3,opt,name=name,proto3" json:"name,omitempty"`
	// Owning team — the multi-tenancy scope. Billing opens the account against it;
	// team-scoped consumers filter on it. Producer-assigned, never client-supplied.
	Team string `protobuf:"bytes,4,opt,name=team,proto3" json:"team,omitempty"`
	// The role granted at creation (role NAME, e.g. "viewer"). Flat RBAC: a user
	// has exactly one role. Carried so an audit/consumer sees the initial grant
	// without a CheckPermission round-trip.
	Role string `protobuf:"bytes,5,opt,name=role,proto3" json:"role,omitempty"`
	// When the account was created (producer clock).
	CreatedAt     *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=created_at,json=createdAt,proto3" json:"created_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UserCreated) Reset() {
	*x = UserCreated{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[29]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UserCreated) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UserCreated) ProtoMessage() {}

func (x *UserCreated) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[29]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UserCreated.ProtoReflect.Descriptor instead.
func (*UserCreated) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{29}
}

func (x *UserCreated) GetUserId() string {
	if x != nil {
		return x.UserId
	}
	return ""
}

func (x *UserCreated) GetEmail() string {
	if x != nil {
		return x.Email
	}
	return ""
}

func (x *UserCreated) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *UserCreated) GetTeam() string {
	if x != nil {
		return x.Team
	}
	return ""
}

func (x *UserCreated) GetRole() string {
	if x != nil {
		return x.Role
	}
	return ""
}

func (x *UserCreated) GetCreatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CreatedAt
	}
	return nil
}

// ApiKeyRotated → fp.auth.apikey.rotated
//
//	PRODUCER:  auth (after an API key is minted — and, on a rotation, the prior
//	           key is revoked in the same logical operation; see the blue/green
//	           rotation note in the auth domain's APIKey model).
//	CONSUMERS: notification (tell the owner a new key was issued — a security-
//	           relevant event a human should see), audit/security log (record the
//	           credential lifecycle), inference-gateway / caches (proactively
//	           invalidate any cached validation for the REPLACED key so the
//	           revoked key stops working before its 30s validation-cache TTL
//	           would naturally expire).
//	SECURITY/PII (this is the whole point): we publish ONLY the key id + the
//	8-char display prefix (e.g. "fp_a1b2") and the owner/scopes — NEVER the raw
//	key. The raw key exists for exactly one moment in CreateAPIKey's return value
//	and is shown to the caller once (Stripe/GitHub-PAT model); it must never
//	touch the bus, a log, or the DB. A consumer that needs to know "which key"
//	uses key_id; a human-facing surface shows key_prefix.
//	WHY "rotated" (not "created"): the canonical event-contract names this
//	fp.auth.apikey.rotated. A plain create and a rotation (create-new +
//	revoke-old) are the same observable fact to consumers — "the set of valid
//	keys for this user changed". replaced_key_id distinguishes the two: empty on
//	a first/independent create, set to the revoked key's id on a true rotation,
//	so a cache-invalidating consumer knows exactly which key to evict.
type ApiKeyRotated struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The NEW API key's id (UUID) — the stable handle, NOT the secret.
	KeyId string `protobuf:"bytes,1,opt,name=key_id,json=keyId,proto3" json:"key_id,omitempty"`
	// The owning user (auth user id). Lets a consumer route the "new key issued"
	// notice to the right human and scope audit entries.
	UserId string `protobuf:"bytes,2,opt,name=user_id,json=userId,proto3" json:"user_id,omitempty"`
	// The new key's 8-char display prefix (e.g. "fp_a1b2") — safe to show/log; it
	// is NOT enough to authenticate. For human-facing surfaces only.
	KeyPrefix string `protobuf:"bytes,3,opt,name=key_prefix,json=keyPrefix,proto3" json:"key_prefix,omitempty"`
	// The new key's scopes ("resource:action" strings). Carried so a security/audit
	// consumer can see the granted capability set without a callback. The effective
	// grant is still role ∩ scope, enforced at validation time.
	Scopes []string `protobuf:"bytes,4,rep,name=scopes,proto3" json:"scopes,omitempty"`
	// The id of the key this one REPLACES (and that was revoked as part of the
	// rotation). Empty on a first/independent create; set on a true rotation. A
	// cache-invalidating consumer evicts exactly this key id.
	ReplacedKeyId string `protobuf:"bytes,5,opt,name=replaced_key_id,json=replacedKeyId,proto3" json:"replaced_key_id,omitempty"`
	// When the new key was issued / the rotation committed (producer clock).
	RotatedAt     *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=rotated_at,json=rotatedAt,proto3" json:"rotated_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ApiKeyRotated) Reset() {
	*x = ApiKeyRotated{}
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[30]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ApiKeyRotated) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ApiKeyRotated) ProtoMessage() {}

func (x *ApiKeyRotated) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_events_v1_events_proto_msgTypes[30]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ApiKeyRotated.ProtoReflect.Descriptor instead.
func (*ApiKeyRotated) Descriptor() ([]byte, []int) {
	return file_forgepoint_events_v1_events_proto_rawDescGZIP(), []int{30}
}

func (x *ApiKeyRotated) GetKeyId() string {
	if x != nil {
		return x.KeyId
	}
	return ""
}

func (x *ApiKeyRotated) GetUserId() string {
	if x != nil {
		return x.UserId
	}
	return ""
}

func (x *ApiKeyRotated) GetKeyPrefix() string {
	if x != nil {
		return x.KeyPrefix
	}
	return ""
}

func (x *ApiKeyRotated) GetScopes() []string {
	if x != nil {
		return x.Scopes
	}
	return nil
}

func (x *ApiKeyRotated) GetReplacedKeyId() string {
	if x != nil {
		return x.ReplacedKeyId
	}
	return ""
}

func (x *ApiKeyRotated) GetRotatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.RotatedAt
	}
	return nil
}

var File_forgepoint_events_v1_events_proto protoreflect.FileDescriptor

const file_forgepoint_events_v1_events_proto_rawDesc = "" +
	"\n" +
	"!forgepoint/events/v1/events.proto\x12\x14forgepoint.events.v1\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1egoogle/protobuf/duration.proto\x1a\x1cgoogle/protobuf/struct.proto\x1a!forgepoint/common/v1/common.proto\"\x99\x02\n" +
	"\x11PredictionSummary\x12\x1b\n" +
	"\ttop_label\x18\x01 \x01(\tR\btopLabel\x12\x1b\n" +
	"\ttop_score\x18\x02 \x01(\x01R\btopScore\x12[\n" +
	"\foutput_stats\x18\x03 \x03(\v28.forgepoint.events.v1.PredictionSummary.OutputStatsEntryR\voutputStats\x12-\n" +
	"\x12output_cardinality\x18\x04 \x01(\x05R\x11outputCardinality\x1a>\n" +
	"\x10OutputStatsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\x01R\x05value:\x028\x01\"\xd7\x01\n" +
	"\x0eFeatureSummary\x12^\n" +
	"\x0efeature_values\x18\x01 \x03(\v27.forgepoint.events.v1.FeatureSummary.FeatureValuesEntryR\rfeatureValues\x12#\n" +
	"\rfeature_count\x18\x02 \x01(\x05R\ffeatureCount\x1a@\n" +
	"\x12FeatureValuesEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\x01R\x05value:\x028\x01\"\xff\x01\n" +
	"\vDriftMetric\x12\x12\n" +
	"\x04name\x18\x01 \x01(\tR\x04name\x129\n" +
	"\x06method\x18\x02 \x01(\x0e2!.forgepoint.events.v1.DriftMethodR\x06method\x12\x14\n" +
	"\x05score\x18\x03 \x01(\x01R\x05score\x12%\n" +
	"\x0ebaseline_value\x18\x04 \x01(\x01R\rbaselineValue\x12#\n" +
	"\rcurrent_value\x18\x05 \x01(\x01R\fcurrentValue\x12?\n" +
	"\bseverity\x18\x06 \x01(\x0e2#.forgepoint.events.v1.DriftSeverityR\bseverity\"\x83\x01\n" +
	"\vMetricPoint\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\x01R\x05value\x12\x12\n" +
	"\x04step\x18\x03 \x01(\x03R\x04step\x128\n" +
	"\ttimestamp\x18\x04 \x01(\v2\x1a.google.protobuf.TimestampR\ttimestamp\"\xf6\x01\n" +
	"\x0fModelRegistered\x12\x19\n" +
	"\bmodel_id\x18\x01 \x01(\tR\amodelId\x12\x1d\n" +
	"\n" +
	"model_name\x18\x02 \x01(\tR\tmodelName\x12\x1c\n" +
	"\tframework\x18\x03 \x01(\tR\tframework\x12\x1b\n" +
	"\ttask_type\x18\x04 \x01(\tR\btaskType\x12\x19\n" +
	"\bowner_id\x18\x05 \x01(\tR\aownerId\x12\x12\n" +
	"\x04team\x18\x06 \x01(\tR\x04team\x12?\n" +
	"\rregistered_at\x18\a \x01(\v2\x1a.google.protobuf.TimestampR\fregisteredAt\"\xe2\x01\n" +
	"\x13ModelVersionCreated\x12\x19\n" +
	"\bmodel_id\x18\x01 \x01(\tR\amodelId\x12\x1d\n" +
	"\n" +
	"model_name\x18\x02 \x01(\tR\tmodelName\x12\x1d\n" +
	"\n" +
	"version_id\x18\x03 \x01(\tR\tversionId\x12\x18\n" +
	"\aversion\x18\x04 \x01(\tR\aversion\x12\x1d\n" +
	"\n" +
	"created_by\x18\x05 \x01(\tR\tcreatedBy\x129\n" +
	"\n" +
	"created_at\x18\x06 \x01(\v2\x1a.google.protobuf.TimestampR\tcreatedAt\"\xaa\x02\n" +
	"\x11ModelVersionReady\x12\x19\n" +
	"\bmodel_id\x18\x01 \x01(\tR\amodelId\x12\x1d\n" +
	"\n" +
	"model_name\x18\x02 \x01(\tR\tmodelName\x12\x1d\n" +
	"\n" +
	"version_id\x18\x03 \x01(\tR\tversionId\x12\x18\n" +
	"\aversion\x18\x04 \x01(\tR\aversion\x12#\n" +
	"\rartifact_path\x18\x05 \x01(\tR\fartifactPath\x12'\n" +
	"\x0fartifact_digest\x18\x06 \x01(\tR\x0eartifactDigest\x12\x1d\n" +
	"\n" +
	"size_bytes\x18\a \x01(\x03R\tsizeBytes\x125\n" +
	"\bready_at\x18\b \x01(\v2\x1a.google.protobuf.TimestampR\areadyAt\"\xb5\x03\n" +
	"\rModelPromoted\x12\x19\n" +
	"\bmodel_id\x18\x01 \x01(\tR\amodelId\x12\x1d\n" +
	"\n" +
	"model_name\x18\x02 \x01(\tR\tmodelName\x12\x1d\n" +
	"\n" +
	"version_id\x18\x03 \x01(\tR\tversionId\x12\x18\n" +
	"\aversion\x18\x04 \x01(\tR\aversion\x12?\n" +
	"\n" +
	"from_stage\x18\x05 \x01(\x0e2 .forgepoint.events.v1.ModelStageR\tfromStage\x12;\n" +
	"\bto_stage\x18\x06 \x01(\x0e2 .forgepoint.events.v1.ModelStageR\atoStage\x12,\n" +
	"\x12demoted_version_id\x18\a \x01(\tR\x10demotedVersionId\x12'\n" +
	"\x0fdemoted_version\x18\b \x01(\tR\x0edemotedVersion\x12\x1f\n" +
	"\vpromoted_by\x18\t \x01(\tR\n" +
	"promotedBy\x12;\n" +
	"\vpromoted_at\x18\n" +
	" \x01(\v2\x1a.google.protobuf.TimestampR\n" +
	"promotedAt\"\xa7\x01\n" +
	"\rModelArchived\x12\x19\n" +
	"\bmodel_id\x18\x01 \x01(\tR\amodelId\x12\x1d\n" +
	"\n" +
	"model_name\x18\x02 \x01(\tR\tmodelName\x12\x1f\n" +
	"\varchived_by\x18\x03 \x01(\tR\n" +
	"archivedBy\x12;\n" +
	"\varchived_at\x18\x04 \x01(\v2\x1a.google.protobuf.TimestampR\n" +
	"archivedAt\"\xa2\x05\n" +
	"\x12ModelDriftDetected\x12\x1d\n" +
	"\n" +
	"model_name\x18\x01 \x01(\tR\tmodelName\x12#\n" +
	"\rmodel_version\x18\x02 \x01(\tR\fmodelVersion\x12>\n" +
	"\n" +
	"drift_type\x18\x03 \x01(\x0e2\x1f.forgepoint.events.v1.DriftTypeR\tdriftType\x12?\n" +
	"\bseverity\x18\x04 \x01(\x0e2#.forgepoint.events.v1.DriftSeverityR\bseverity\x12\x1b\n" +
	"\treport_id\x18\x05 \x01(\tR\breportId\x12;\n" +
	"\ametrics\x18\x06 \x03(\v2!.forgepoint.events.v1.DriftMetricR\ametrics\x12=\n" +
	"\fwindow_start\x18\a \x01(\v2\x1a.google.protobuf.TimestampR\vwindowStart\x129\n" +
	"\n" +
	"window_end\x18\b \x01(\v2\x1a.google.protobuf.TimestampR\twindowEnd\x12!\n" +
	"\fsample_count\x18\t \x01(\x05R\vsampleCount\x12!\n" +
	"\fauto_retrain\x18\n" +
	" \x01(\bR\vautoRetrain\x12.\n" +
	"\x13retrain_pipeline_id\x18\v \x01(\tR\x11retrainPipelineId\x12@\n" +
	"\x0fretrain_context\x18\f \x01(\v2\x17.google.protobuf.StructR\x0eretrainContext\x12;\n" +
	"\vdetected_at\x18\r \x01(\v2\x1a.google.protobuf.TimestampR\n" +
	"detectedAt\"\xfc\x01\n" +
	"\x0fPipelineStarted\x12!\n" +
	"\fexecution_id\x18\x01 \x01(\tR\vexecutionId\x12\x1f\n" +
	"\vpipeline_id\x18\x02 \x01(\tR\n" +
	"pipelineId\x12G\n" +
	"\rpipeline_type\x18\x03 \x01(\x0e2\".forgepoint.events.v1.PipelineTypeR\fpipelineType\x12!\n" +
	"\ftriggered_by\x18\x04 \x01(\tR\vtriggeredBy\x129\n" +
	"\n" +
	"started_at\x18\x05 \x01(\v2\x1a.google.protobuf.TimestampR\tstartedAt\"\x99\x02\n" +
	"\rStepCompleted\x12!\n" +
	"\fexecution_id\x18\x01 \x01(\tR\vexecutionId\x12\x1f\n" +
	"\vpipeline_id\x18\x02 \x01(\tR\n" +
	"pipelineId\x12\x17\n" +
	"\astep_id\x18\x03 \x01(\tR\x06stepId\x12;\n" +
	"\tstep_type\x18\x04 \x01(\x0e2\x1e.forgepoint.events.v1.StepTypeR\bstepType\x12/\n" +
	"\x06output\x18\x05 \x01(\v2\x17.google.protobuf.StructR\x06output\x12=\n" +
	"\fcompleted_at\x18\x06 \x01(\v2\x1a.google.protobuf.TimestampR\vcompletedAt\"\x91\x02\n" +
	"\n" +
	"StepFailed\x12!\n" +
	"\fexecution_id\x18\x01 \x01(\tR\vexecutionId\x12\x1f\n" +
	"\vpipeline_id\x18\x02 \x01(\tR\n" +
	"pipelineId\x12\x17\n" +
	"\astep_id\x18\x03 \x01(\tR\x06stepId\x12;\n" +
	"\tstep_type\x18\x04 \x01(\x0e2\x1e.forgepoint.events.v1.StepTypeR\bstepType\x12\x14\n" +
	"\x05error\x18\x05 \x01(\tR\x05error\x12\x1a\n" +
	"\battempts\x18\x06 \x01(\x05R\battempts\x127\n" +
	"\tfailed_at\x18\a \x01(\v2\x1a.google.protobuf.TimestampR\bfailedAt\"\xf4\x01\n" +
	"\x15CompensationTriggered\x12!\n" +
	"\fexecution_id\x18\x01 \x01(\tR\vexecutionId\x12\x1f\n" +
	"\vpipeline_id\x18\x02 \x01(\tR\n" +
	"pipelineId\x12$\n" +
	"\x0efailed_step_id\x18\x03 \x01(\tR\ffailedStepId\x122\n" +
	"\x15compensating_step_ids\x18\x04 \x03(\tR\x13compensatingStepIds\x12=\n" +
	"\ftriggered_at\x18\x05 \x01(\v2\x1a.google.protobuf.TimestampR\vtriggeredAt\"\x96\x02\n" +
	"\x11PipelineCompleted\x12!\n" +
	"\fexecution_id\x18\x01 \x01(\tR\vexecutionId\x12\x1f\n" +
	"\vpipeline_id\x18\x02 \x01(\tR\n" +
	"pipelineId\x12G\n" +
	"\rpipeline_type\x18\x03 \x01(\x0e2\".forgepoint.events.v1.PipelineTypeR\fpipelineType\x125\n" +
	"\bduration\x18\x04 \x01(\v2\x19.google.protobuf.DurationR\bduration\x12=\n" +
	"\fcompleted_at\x18\x05 \x01(\v2\x1a.google.protobuf.TimestampR\vcompletedAt\"\xc3\x02\n" +
	"\x0ePipelineFailed\x12!\n" +
	"\fexecution_id\x18\x01 \x01(\tR\vexecutionId\x12\x1f\n" +
	"\vpipeline_id\x18\x02 \x01(\tR\n" +
	"pipelineId\x12G\n" +
	"\rpipeline_type\x18\x03 \x01(\x0e2\".forgepoint.events.v1.PipelineTypeR\fpipelineType\x12$\n" +
	"\x0efailed_step_id\x18\x04 \x01(\tR\ffailedStepId\x12\x14\n" +
	"\x05error\x18\x05 \x01(\tR\x05error\x12/\n" +
	"\x13compensation_failed\x18\x06 \x01(\bR\x12compensationFailed\x127\n" +
	"\tfailed_at\x18\a \x01(\v2\x1a.google.protobuf.TimestampR\bfailedAt\"\x9d\x02\n" +
	"\rModelDeployed\x12\x19\n" +
	"\bmodel_id\x18\x01 \x01(\tR\amodelId\x12\x1d\n" +
	"\n" +
	"model_name\x18\x02 \x01(\tR\tmodelName\x12\x1d\n" +
	"\n" +
	"version_id\x18\x03 \x01(\tR\tversionId\x12\x18\n" +
	"\aversion\x18\x04 \x01(\tR\aversion\x12\x1a\n" +
	"\bendpoint\x18\x05 \x01(\tR\bendpoint\x12\x1d\n" +
	"\n" +
	"weight_bps\x18\x06 \x01(\x05R\tweightBps\x12!\n" +
	"\fexecution_id\x18\a \x01(\tR\vexecutionId\x12;\n" +
	"\vdeployed_at\x18\b \x01(\v2\x1a.google.protobuf.TimestampR\n" +
	"deployedAt\"\x80\x02\n" +
	"\x0fModelUndeployed\x12\x19\n" +
	"\bmodel_id\x18\x01 \x01(\tR\amodelId\x12\x1d\n" +
	"\n" +
	"model_name\x18\x02 \x01(\tR\tmodelName\x12\x1d\n" +
	"\n" +
	"version_id\x18\x03 \x01(\tR\tversionId\x12\x18\n" +
	"\aversion\x18\x04 \x01(\tR\aversion\x12\x16\n" +
	"\x06reason\x18\x05 \x01(\tR\x06reason\x12!\n" +
	"\fexecution_id\x18\x06 \x01(\tR\vexecutionId\x12?\n" +
	"\rundeployed_at\x18\a \x01(\v2\x1a.google.protobuf.TimestampR\fundeployedAt\"\x9d\x04\n" +
	"\x12InferenceCompleted\x12\x1d\n" +
	"\n" +
	"request_id\x18\x01 \x01(\tR\trequestId\x12\x19\n" +
	"\bmodel_id\x18\x02 \x01(\tR\amodelId\x12\x1d\n" +
	"\n" +
	"model_name\x18\x03 \x01(\tR\tmodelName\x12\x18\n" +
	"\aversion\x18\x04 \x01(\tR\aversion\x12\x1c\n" +
	"\n" +
	"api_key_id\x18\x05 \x01(\tR\bapiKeyId\x12\x1b\n" +
	"\tis_canary\x18\x06 \x01(\bR\bisCanary\x123\n" +
	"\alatency\x18\a \x01(\v2\x19.google.protobuf.DurationR\alatency\x12\x1d\n" +
	"\n" +
	"latency_ms\x18\b \x01(\x03R\tlatencyMs\x12\x1f\n" +
	"\vtoken_count\x18\t \x01(\x03R\n" +
	"tokenCount\x12V\n" +
	"\x12prediction_summary\x18\n" +
	" \x01(\v2'.forgepoint.events.v1.PredictionSummaryR\x11predictionSummary\x12M\n" +
	"\x0ffeature_summary\x18\v \x01(\v2$.forgepoint.events.v1.FeatureSummaryR\x0efeatureSummary\x12=\n" +
	"\fcompleted_at\x18\f \x01(\v2\x1a.google.protobuf.TimestampR\vcompletedAt\"\xda\x02\n" +
	"\x0fInferenceFailed\x12\x1d\n" +
	"\n" +
	"request_id\x18\x01 \x01(\tR\trequestId\x12\x19\n" +
	"\bmodel_id\x18\x02 \x01(\tR\amodelId\x12\x1d\n" +
	"\n" +
	"model_name\x18\x03 \x01(\tR\tmodelName\x12\x18\n" +
	"\aversion\x18\x04 \x01(\tR\aversion\x12\x1c\n" +
	"\n" +
	"api_key_id\x18\x05 \x01(\tR\bapiKeyId\x12D\n" +
	"\x06reason\x18\x06 \x01(\x0e2,.forgepoint.events.v1.InferenceFailureReasonR\x06reason\x127\n" +
	"\x05error\x18\a \x01(\v2!.forgepoint.common.v1.ErrorDetailR\x05error\x127\n" +
	"\tfailed_at\x18\b \x01(\v2\x1a.google.protobuf.TimestampR\bfailedAt\"\xe9\x01\n" +
	"\x12FeatureViewDefined\x12&\n" +
	"\x0ffeature_view_id\x18\x01 \x01(\tR\rfeatureViewId\x12*\n" +
	"\x11feature_view_name\x18\x02 \x01(\tR\x0ffeatureViewName\x12%\n" +
	"\x0eschema_version\x18\x03 \x01(\x03R\rschemaVersion\x12\x1d\n" +
	"\n" +
	"owner_team\x18\x04 \x01(\tR\townerTeam\x129\n" +
	"\n" +
	"defined_at\x18\x05 \x01(\v2\x1a.google.protobuf.TimestampR\tdefinedAt\"\x9c\x02\n" +
	"\x0fFeaturesWritten\x12&\n" +
	"\x0ffeature_view_id\x18\x01 \x01(\tR\rfeatureViewId\x12*\n" +
	"\x11feature_view_name\x18\x02 \x01(\tR\x0ffeatureViewName\x12\x1d\n" +
	"\n" +
	"entity_ids\x18\x03 \x03(\tR\tentityIds\x12#\n" +
	"\rwritten_count\x18\x04 \x01(\x05R\fwrittenCount\x126\n" +
	"\x17written_through_version\x18\x05 \x01(\x03R\x15writtenThroughVersion\x129\n" +
	"\n" +
	"written_at\x18\x06 \x01(\v2\x1a.google.protobuf.TimestampR\twrittenAt\"\xad\x03\n" +
	"\rUsageRecorded\x12\x1b\n" +
	"\trecord_id\x18\x01 \x01(\tR\brecordId\x12\x12\n" +
	"\x04team\x18\x02 \x01(\tR\x04team\x12 \n" +
	"\frate_plan_id\x18\x03 \x01(\tR\n" +
	"ratePlanId\x12>\n" +
	"\n" +
	"meter_type\x18\x04 \x01(\x0e2\x1f.forgepoint.events.v1.MeterTypeR\tmeterType\x12\x1a\n" +
	"\bquantity\x18\x05 \x01(\x03R\bquantity\x12\x1f\n" +
	"\vcost_micros\x18\x06 \x01(\x03R\n" +
	"costMicros\x12#\n" +
	"\rcurrency_code\x18\a \x01(\tR\fcurrencyCode\x12\x19\n" +
	"\bmodel_id\x18\b \x01(\tR\amodelId\x12#\n" +
	"\rmodel_version\x18\t \x01(\tR\fmodelVersion\x12*\n" +
	"\x11source_request_id\x18\n" +
	" \x01(\tR\x0fsourceRequestId\x12;\n" +
	"\voccurred_at\x18\v \x01(\v2\x1a.google.protobuf.TimestampR\n" +
	"occurredAt\"\x88\x02\n" +
	"\rQuotaExceeded\x12\x12\n" +
	"\x04team\x18\x01 \x01(\tR\x04team\x12 \n" +
	"\frate_plan_id\x18\x02 \x01(\tR\n" +
	"ratePlanId\x12>\n" +
	"\n" +
	"meter_type\x18\x03 \x01(\x0e2\x1f.forgepoint.events.v1.MeterTypeR\tmeterType\x12\x1f\n" +
	"\vquota_limit\x18\x04 \x01(\x03R\n" +
	"quotaLimit\x12#\n" +
	"\rcurrent_usage\x18\x05 \x01(\x03R\fcurrentUsage\x12;\n" +
	"\voccurred_at\x18\x06 \x01(\v2\x1a.google.protobuf.TimestampR\n" +
	"occurredAt\"\x8f\x03\n" +
	"\x10InvoiceGenerated\x12\x1d\n" +
	"\n" +
	"invoice_id\x18\x01 \x01(\tR\tinvoiceId\x12%\n" +
	"\x0einvoice_number\x18\x02 \x01(\tR\rinvoiceNumber\x12\x12\n" +
	"\x04team\x18\x03 \x01(\tR\x04team\x12 \n" +
	"\frate_plan_id\x18\x04 \x01(\tR\n" +
	"ratePlanId\x12=\n" +
	"\fperiod_start\x18\x05 \x01(\v2\x1a.google.protobuf.TimestampR\vperiodStart\x129\n" +
	"\n" +
	"period_end\x18\x06 \x01(\v2\x1a.google.protobuf.TimestampR\tperiodEnd\x12!\n" +
	"\ftotal_micros\x18\a \x01(\x03R\vtotalMicros\x12#\n" +
	"\rcurrency_code\x18\b \x01(\tR\fcurrencyCode\x12=\n" +
	"\ffinalized_at\x18\t \x01(\v2\x1a.google.protobuf.TimestampR\vfinalizedAt\"\xd0\x01\n" +
	"\n" +
	"RunCreated\x12\x15\n" +
	"\x06run_id\x18\x01 \x01(\tR\x05runId\x12#\n" +
	"\rexperiment_id\x18\x02 \x01(\tR\fexperimentId\x12(\n" +
	"\x10model_version_id\x18\x03 \x01(\tR\x0emodelVersionId\x12!\n" +
	"\fdisplay_name\x18\x04 \x01(\tR\vdisplayName\x129\n" +
	"\n" +
	"started_at\x18\x05 \x01(\v2\x1a.google.protobuf.TimestampR\tstartedAt\"\xab\x02\n" +
	"\vRunFinished\x12\x15\n" +
	"\x06run_id\x18\x01 \x01(\tR\x05runId\x12#\n" +
	"\rexperiment_id\x18\x02 \x01(\tR\fexperimentId\x12(\n" +
	"\x10model_version_id\x18\x03 \x01(\tR\x0emodelVersionId\x127\n" +
	"\x06status\x18\x04 \x01(\x0e2\x1f.forgepoint.events.v1.RunStatusR\x06status\x12F\n" +
	"\rfinal_metrics\x18\x05 \x03(\v2!.forgepoint.events.v1.MetricPointR\ffinalMetrics\x125\n" +
	"\bended_at\x18\x06 \x01(\v2\x1a.google.protobuf.TimestampR\aendedAt\"\x8f\x02\n" +
	"\x15NotificationDelivered\x12'\n" +
	"\x0fnotification_id\x18\x01 \x01(\tR\x0enotificationId\x12*\n" +
	"\x11recipient_user_id\x18\x02 \x01(\tR\x0frecipientUserId\x12C\n" +
	"\achannel\x18\x03 \x01(\x0e2).forgepoint.events.v1.NotificationChannelR\achannel\x12\x1d\n" +
	"\n" +
	"event_type\x18\x04 \x01(\tR\teventType\x12=\n" +
	"\fdelivered_at\x18\x05 \x01(\v2\x1a.google.protobuf.TimestampR\vdeliveredAt\"\xc7\x02\n" +
	"\x12NotificationFailed\x12'\n" +
	"\x0fnotification_id\x18\x01 \x01(\tR\x0enotificationId\x12*\n" +
	"\x11recipient_user_id\x18\x02 \x01(\tR\x0frecipientUserId\x12C\n" +
	"\achannel\x18\x03 \x01(\x0e2).forgepoint.events.v1.NotificationChannelR\achannel\x12\x1d\n" +
	"\n" +
	"event_type\x18\x04 \x01(\tR\teventType\x12\x1a\n" +
	"\battempts\x18\x05 \x01(\x05R\battempts\x12#\n" +
	"\rerror_message\x18\x06 \x01(\tR\ferrorMessage\x127\n" +
	"\tfailed_at\x18\a \x01(\v2\x1a.google.protobuf.TimestampR\bfailedAt\"\xb3\x01\n" +
	"\vUserCreated\x12\x17\n" +
	"\auser_id\x18\x01 \x01(\tR\x06userId\x12\x14\n" +
	"\x05email\x18\x02 \x01(\tR\x05email\x12\x12\n" +
	"\x04name\x18\x03 \x01(\tR\x04name\x12\x12\n" +
	"\x04team\x18\x04 \x01(\tR\x04team\x12\x12\n" +
	"\x04role\x18\x05 \x01(\tR\x04role\x129\n" +
	"\n" +
	"created_at\x18\x06 \x01(\v2\x1a.google.protobuf.TimestampR\tcreatedAt\"\xd9\x01\n" +
	"\rApiKeyRotated\x12\x15\n" +
	"\x06key_id\x18\x01 \x01(\tR\x05keyId\x12\x17\n" +
	"\auser_id\x18\x02 \x01(\tR\x06userId\x12\x1d\n" +
	"\n" +
	"key_prefix\x18\x03 \x01(\tR\tkeyPrefix\x12\x16\n" +
	"\x06scopes\x18\x04 \x03(\tR\x06scopes\x12&\n" +
	"\x0freplaced_key_id\x18\x05 \x01(\tR\rreplacedKeyId\x129\n" +
	"\n" +
	"rotated_at\x18\x06 \x01(\v2\x1a.google.protobuf.TimestampR\trotatedAt*\x8d\x01\n" +
	"\n" +
	"ModelStage\x12\x1b\n" +
	"\x17MODEL_STAGE_UNSPECIFIED\x10\x00\x12\x13\n" +
	"\x0fMODEL_STAGE_DEV\x10\x01\x12\x17\n" +
	"\x13MODEL_STAGE_STAGING\x10\x02\x12\x1a\n" +
	"\x16MODEL_STAGE_PRODUCTION\x10\x03\x12\x18\n" +
	"\x14MODEL_STAGE_ARCHIVED\x10\x04*\x93\x01\n" +
	"\fPipelineType\x12\x1d\n" +
	"\x19PIPELINE_TYPE_UNSPECIFIED\x10\x00\x12!\n" +
	"\x1dPIPELINE_TYPE_DEPLOYMENT_SAGA\x10\x01\x12\x1e\n" +
	"\x1aPIPELINE_TYPE_TRAINING_DAG\x10\x02\x12!\n" +
	"\x1dPIPELINE_TYPE_BATCH_INFERENCE\x10\x03*\xf0\x01\n" +
	"\bStepType\x12\x19\n" +
	"\x15STEP_TYPE_UNSPECIFIED\x10\x00\x12\x16\n" +
	"\x12STEP_TYPE_VALIDATE\x10\x01\x12\x13\n" +
	"\x0fSTEP_TYPE_BUILD\x10\x02\x12\x14\n" +
	"\x10STEP_TYPE_DEPLOY\x10\x03\x12\x14\n" +
	"\x10STEP_TYPE_CANARY\x10\x04\x12\x15\n" +
	"\x11STEP_TYPE_PROMOTE\x10\x05\x12\x13\n" +
	"\x0fSTEP_TYPE_TRAIN\x10\x06\x12\x16\n" +
	"\x12STEP_TYPE_EVALUATE\x10\a\x12\x16\n" +
	"\x12STEP_TYPE_REGISTER\x10\b\x12\x14\n" +
	"\x10STEP_TYPE_CUSTOM\x10\t*\x97\x03\n" +
	"\x16InferenceFailureReason\x12(\n" +
	"$INFERENCE_FAILURE_REASON_UNSPECIFIED\x10\x00\x12%\n" +
	"!INFERENCE_FAILURE_REASON_NO_ROUTE\x10\x01\x12)\n" +
	"%INFERENCE_FAILURE_REASON_RATE_LIMITED\x10\x02\x12*\n" +
	"&INFERENCE_FAILURE_REASON_BULKHEAD_FULL\x10\x03\x12)\n" +
	"%INFERENCE_FAILURE_REASON_CIRCUIT_OPEN\x10\x04\x12+\n" +
	"'INFERENCE_FAILURE_REASON_UPSTREAM_ERROR\x10\x05\x12$\n" +
	" INFERENCE_FAILURE_REASON_TIMEOUT\x10\x06\x12*\n" +
	"&INFERENCE_FAILURE_REASON_INVALID_INPUT\x10\a\x12+\n" +
	"'INFERENCE_FAILURE_REASON_QUOTA_EXCEEDED\x10\b*\xa8\x01\n" +
	"\tMeterType\x12\x1a\n" +
	"\x16METER_TYPE_UNSPECIFIED\x10\x00\x12 \n" +
	"\x1cMETER_TYPE_INFERENCE_REQUEST\x10\x01\x12\x1f\n" +
	"\x1bMETER_TYPE_INFERENCE_TOKENS\x10\x02\x12\x1e\n" +
	"\x1aMETER_TYPE_COMPUTE_SECONDS\x10\x03\x12\x1c\n" +
	"\x18METER_TYPE_STORAGE_BYTES\x10\x04*\x86\x01\n" +
	"\tRunStatus\x12\x1a\n" +
	"\x16RUN_STATUS_UNSPECIFIED\x10\x00\x12\x16\n" +
	"\x12RUN_STATUS_RUNNING\x10\x01\x12\x17\n" +
	"\x13RUN_STATUS_FINISHED\x10\x02\x12\x15\n" +
	"\x11RUN_STATUS_FAILED\x10\x03\x12\x15\n" +
	"\x11RUN_STATUS_KILLED\x10\x04*s\n" +
	"\tDriftType\x12\x1a\n" +
	"\x16DRIFT_TYPE_UNSPECIFIED\x10\x00\x12\x13\n" +
	"\x0fDRIFT_TYPE_DATA\x10\x01\x12\x19\n" +
	"\x15DRIFT_TYPE_PREDICTION\x10\x02\x12\x1a\n" +
	"\x16DRIFT_TYPE_PERFORMANCE\x10\x03*k\n" +
	"\vDriftMethod\x12\x1c\n" +
	"\x18DRIFT_METHOD_UNSPECIFIED\x10\x00\x12\x14\n" +
	"\x10DRIFT_METHOD_PSI\x10\x01\x12\x13\n" +
	"\x0fDRIFT_METHOD_KL\x10\x02\x12\x13\n" +
	"\x0fDRIFT_METHOD_KS\x10\x03*\x7f\n" +
	"\rDriftSeverity\x12\x1e\n" +
	"\x1aDRIFT_SEVERITY_UNSPECIFIED\x10\x00\x12\x15\n" +
	"\x11DRIFT_SEVERITY_OK\x10\x01\x12\x1a\n" +
	"\x16DRIFT_SEVERITY_WARNING\x10\x02\x12\x1b\n" +
	"\x17DRIFT_SEVERITY_CRITICAL\x10\x03*\xbe\x01\n" +
	"\x13NotificationChannel\x12$\n" +
	" NOTIFICATION_CHANNEL_UNSPECIFIED\x10\x00\x12\x1f\n" +
	"\x1bNOTIFICATION_CHANNEL_IN_APP\x10\x01\x12 \n" +
	"\x1cNOTIFICATION_CHANNEL_WEBHOOK\x10\x02\x12\x1e\n" +
	"\x1aNOTIFICATION_CHANNEL_SLACK\x10\x03\x12\x1e\n" +
	"\x1aNOTIFICATION_CHANNEL_EMAIL\x10\x04BHZFgithub.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1;eventsv1b\x06proto3"

var (
	file_forgepoint_events_v1_events_proto_rawDescOnce sync.Once
	file_forgepoint_events_v1_events_proto_rawDescData []byte
)

func file_forgepoint_events_v1_events_proto_rawDescGZIP() []byte {
	file_forgepoint_events_v1_events_proto_rawDescOnce.Do(func() {
		file_forgepoint_events_v1_events_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_forgepoint_events_v1_events_proto_rawDesc), len(file_forgepoint_events_v1_events_proto_rawDesc)))
	})
	return file_forgepoint_events_v1_events_proto_rawDescData
}

var file_forgepoint_events_v1_events_proto_enumTypes = make([]protoimpl.EnumInfo, 10)
var file_forgepoint_events_v1_events_proto_msgTypes = make([]protoimpl.MessageInfo, 33)
var file_forgepoint_events_v1_events_proto_goTypes = []any{
	(ModelStage)(0),               // 0: forgepoint.events.v1.ModelStage
	(PipelineType)(0),             // 1: forgepoint.events.v1.PipelineType
	(StepType)(0),                 // 2: forgepoint.events.v1.StepType
	(InferenceFailureReason)(0),   // 3: forgepoint.events.v1.InferenceFailureReason
	(MeterType)(0),                // 4: forgepoint.events.v1.MeterType
	(RunStatus)(0),                // 5: forgepoint.events.v1.RunStatus
	(DriftType)(0),                // 6: forgepoint.events.v1.DriftType
	(DriftMethod)(0),              // 7: forgepoint.events.v1.DriftMethod
	(DriftSeverity)(0),            // 8: forgepoint.events.v1.DriftSeverity
	(NotificationChannel)(0),      // 9: forgepoint.events.v1.NotificationChannel
	(*PredictionSummary)(nil),     // 10: forgepoint.events.v1.PredictionSummary
	(*FeatureSummary)(nil),        // 11: forgepoint.events.v1.FeatureSummary
	(*DriftMetric)(nil),           // 12: forgepoint.events.v1.DriftMetric
	(*MetricPoint)(nil),           // 13: forgepoint.events.v1.MetricPoint
	(*ModelRegistered)(nil),       // 14: forgepoint.events.v1.ModelRegistered
	(*ModelVersionCreated)(nil),   // 15: forgepoint.events.v1.ModelVersionCreated
	(*ModelVersionReady)(nil),     // 16: forgepoint.events.v1.ModelVersionReady
	(*ModelPromoted)(nil),         // 17: forgepoint.events.v1.ModelPromoted
	(*ModelArchived)(nil),         // 18: forgepoint.events.v1.ModelArchived
	(*ModelDriftDetected)(nil),    // 19: forgepoint.events.v1.ModelDriftDetected
	(*PipelineStarted)(nil),       // 20: forgepoint.events.v1.PipelineStarted
	(*StepCompleted)(nil),         // 21: forgepoint.events.v1.StepCompleted
	(*StepFailed)(nil),            // 22: forgepoint.events.v1.StepFailed
	(*CompensationTriggered)(nil), // 23: forgepoint.events.v1.CompensationTriggered
	(*PipelineCompleted)(nil),     // 24: forgepoint.events.v1.PipelineCompleted
	(*PipelineFailed)(nil),        // 25: forgepoint.events.v1.PipelineFailed
	(*ModelDeployed)(nil),         // 26: forgepoint.events.v1.ModelDeployed
	(*ModelUndeployed)(nil),       // 27: forgepoint.events.v1.ModelUndeployed
	(*InferenceCompleted)(nil),    // 28: forgepoint.events.v1.InferenceCompleted
	(*InferenceFailed)(nil),       // 29: forgepoint.events.v1.InferenceFailed
	(*FeatureViewDefined)(nil),    // 30: forgepoint.events.v1.FeatureViewDefined
	(*FeaturesWritten)(nil),       // 31: forgepoint.events.v1.FeaturesWritten
	(*UsageRecorded)(nil),         // 32: forgepoint.events.v1.UsageRecorded
	(*QuotaExceeded)(nil),         // 33: forgepoint.events.v1.QuotaExceeded
	(*InvoiceGenerated)(nil),      // 34: forgepoint.events.v1.InvoiceGenerated
	(*RunCreated)(nil),            // 35: forgepoint.events.v1.RunCreated
	(*RunFinished)(nil),           // 36: forgepoint.events.v1.RunFinished
	(*NotificationDelivered)(nil), // 37: forgepoint.events.v1.NotificationDelivered
	(*NotificationFailed)(nil),    // 38: forgepoint.events.v1.NotificationFailed
	(*UserCreated)(nil),           // 39: forgepoint.events.v1.UserCreated
	(*ApiKeyRotated)(nil),         // 40: forgepoint.events.v1.ApiKeyRotated
	nil,                           // 41: forgepoint.events.v1.PredictionSummary.OutputStatsEntry
	nil,                           // 42: forgepoint.events.v1.FeatureSummary.FeatureValuesEntry
	(*timestamppb.Timestamp)(nil), // 43: google.protobuf.Timestamp
	(*structpb.Struct)(nil),       // 44: google.protobuf.Struct
	(*durationpb.Duration)(nil),   // 45: google.protobuf.Duration
	(*v1.ErrorDetail)(nil),        // 46: forgepoint.common.v1.ErrorDetail
}
var file_forgepoint_events_v1_events_proto_depIdxs = []int32{
	41, // 0: forgepoint.events.v1.PredictionSummary.output_stats:type_name -> forgepoint.events.v1.PredictionSummary.OutputStatsEntry
	42, // 1: forgepoint.events.v1.FeatureSummary.feature_values:type_name -> forgepoint.events.v1.FeatureSummary.FeatureValuesEntry
	7,  // 2: forgepoint.events.v1.DriftMetric.method:type_name -> forgepoint.events.v1.DriftMethod
	8,  // 3: forgepoint.events.v1.DriftMetric.severity:type_name -> forgepoint.events.v1.DriftSeverity
	43, // 4: forgepoint.events.v1.MetricPoint.timestamp:type_name -> google.protobuf.Timestamp
	43, // 5: forgepoint.events.v1.ModelRegistered.registered_at:type_name -> google.protobuf.Timestamp
	43, // 6: forgepoint.events.v1.ModelVersionCreated.created_at:type_name -> google.protobuf.Timestamp
	43, // 7: forgepoint.events.v1.ModelVersionReady.ready_at:type_name -> google.protobuf.Timestamp
	0,  // 8: forgepoint.events.v1.ModelPromoted.from_stage:type_name -> forgepoint.events.v1.ModelStage
	0,  // 9: forgepoint.events.v1.ModelPromoted.to_stage:type_name -> forgepoint.events.v1.ModelStage
	43, // 10: forgepoint.events.v1.ModelPromoted.promoted_at:type_name -> google.protobuf.Timestamp
	43, // 11: forgepoint.events.v1.ModelArchived.archived_at:type_name -> google.protobuf.Timestamp
	6,  // 12: forgepoint.events.v1.ModelDriftDetected.drift_type:type_name -> forgepoint.events.v1.DriftType
	8,  // 13: forgepoint.events.v1.ModelDriftDetected.severity:type_name -> forgepoint.events.v1.DriftSeverity
	12, // 14: forgepoint.events.v1.ModelDriftDetected.metrics:type_name -> forgepoint.events.v1.DriftMetric
	43, // 15: forgepoint.events.v1.ModelDriftDetected.window_start:type_name -> google.protobuf.Timestamp
	43, // 16: forgepoint.events.v1.ModelDriftDetected.window_end:type_name -> google.protobuf.Timestamp
	44, // 17: forgepoint.events.v1.ModelDriftDetected.retrain_context:type_name -> google.protobuf.Struct
	43, // 18: forgepoint.events.v1.ModelDriftDetected.detected_at:type_name -> google.protobuf.Timestamp
	1,  // 19: forgepoint.events.v1.PipelineStarted.pipeline_type:type_name -> forgepoint.events.v1.PipelineType
	43, // 20: forgepoint.events.v1.PipelineStarted.started_at:type_name -> google.protobuf.Timestamp
	2,  // 21: forgepoint.events.v1.StepCompleted.step_type:type_name -> forgepoint.events.v1.StepType
	44, // 22: forgepoint.events.v1.StepCompleted.output:type_name -> google.protobuf.Struct
	43, // 23: forgepoint.events.v1.StepCompleted.completed_at:type_name -> google.protobuf.Timestamp
	2,  // 24: forgepoint.events.v1.StepFailed.step_type:type_name -> forgepoint.events.v1.StepType
	43, // 25: forgepoint.events.v1.StepFailed.failed_at:type_name -> google.protobuf.Timestamp
	43, // 26: forgepoint.events.v1.CompensationTriggered.triggered_at:type_name -> google.protobuf.Timestamp
	1,  // 27: forgepoint.events.v1.PipelineCompleted.pipeline_type:type_name -> forgepoint.events.v1.PipelineType
	45, // 28: forgepoint.events.v1.PipelineCompleted.duration:type_name -> google.protobuf.Duration
	43, // 29: forgepoint.events.v1.PipelineCompleted.completed_at:type_name -> google.protobuf.Timestamp
	1,  // 30: forgepoint.events.v1.PipelineFailed.pipeline_type:type_name -> forgepoint.events.v1.PipelineType
	43, // 31: forgepoint.events.v1.PipelineFailed.failed_at:type_name -> google.protobuf.Timestamp
	43, // 32: forgepoint.events.v1.ModelDeployed.deployed_at:type_name -> google.protobuf.Timestamp
	43, // 33: forgepoint.events.v1.ModelUndeployed.undeployed_at:type_name -> google.protobuf.Timestamp
	45, // 34: forgepoint.events.v1.InferenceCompleted.latency:type_name -> google.protobuf.Duration
	10, // 35: forgepoint.events.v1.InferenceCompleted.prediction_summary:type_name -> forgepoint.events.v1.PredictionSummary
	11, // 36: forgepoint.events.v1.InferenceCompleted.feature_summary:type_name -> forgepoint.events.v1.FeatureSummary
	43, // 37: forgepoint.events.v1.InferenceCompleted.completed_at:type_name -> google.protobuf.Timestamp
	3,  // 38: forgepoint.events.v1.InferenceFailed.reason:type_name -> forgepoint.events.v1.InferenceFailureReason
	46, // 39: forgepoint.events.v1.InferenceFailed.error:type_name -> forgepoint.common.v1.ErrorDetail
	43, // 40: forgepoint.events.v1.InferenceFailed.failed_at:type_name -> google.protobuf.Timestamp
	43, // 41: forgepoint.events.v1.FeatureViewDefined.defined_at:type_name -> google.protobuf.Timestamp
	43, // 42: forgepoint.events.v1.FeaturesWritten.written_at:type_name -> google.protobuf.Timestamp
	4,  // 43: forgepoint.events.v1.UsageRecorded.meter_type:type_name -> forgepoint.events.v1.MeterType
	43, // 44: forgepoint.events.v1.UsageRecorded.occurred_at:type_name -> google.protobuf.Timestamp
	4,  // 45: forgepoint.events.v1.QuotaExceeded.meter_type:type_name -> forgepoint.events.v1.MeterType
	43, // 46: forgepoint.events.v1.QuotaExceeded.occurred_at:type_name -> google.protobuf.Timestamp
	43, // 47: forgepoint.events.v1.InvoiceGenerated.period_start:type_name -> google.protobuf.Timestamp
	43, // 48: forgepoint.events.v1.InvoiceGenerated.period_end:type_name -> google.protobuf.Timestamp
	43, // 49: forgepoint.events.v1.InvoiceGenerated.finalized_at:type_name -> google.protobuf.Timestamp
	43, // 50: forgepoint.events.v1.RunCreated.started_at:type_name -> google.protobuf.Timestamp
	5,  // 51: forgepoint.events.v1.RunFinished.status:type_name -> forgepoint.events.v1.RunStatus
	13, // 52: forgepoint.events.v1.RunFinished.final_metrics:type_name -> forgepoint.events.v1.MetricPoint
	43, // 53: forgepoint.events.v1.RunFinished.ended_at:type_name -> google.protobuf.Timestamp
	9,  // 54: forgepoint.events.v1.NotificationDelivered.channel:type_name -> forgepoint.events.v1.NotificationChannel
	43, // 55: forgepoint.events.v1.NotificationDelivered.delivered_at:type_name -> google.protobuf.Timestamp
	9,  // 56: forgepoint.events.v1.NotificationFailed.channel:type_name -> forgepoint.events.v1.NotificationChannel
	43, // 57: forgepoint.events.v1.NotificationFailed.failed_at:type_name -> google.protobuf.Timestamp
	43, // 58: forgepoint.events.v1.UserCreated.created_at:type_name -> google.protobuf.Timestamp
	43, // 59: forgepoint.events.v1.ApiKeyRotated.rotated_at:type_name -> google.protobuf.Timestamp
	60, // [60:60] is the sub-list for method output_type
	60, // [60:60] is the sub-list for method input_type
	60, // [60:60] is the sub-list for extension type_name
	60, // [60:60] is the sub-list for extension extendee
	0,  // [0:60] is the sub-list for field type_name
}

func init() { file_forgepoint_events_v1_events_proto_init() }
func file_forgepoint_events_v1_events_proto_init() {
	if File_forgepoint_events_v1_events_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_forgepoint_events_v1_events_proto_rawDesc), len(file_forgepoint_events_v1_events_proto_rawDesc)),
			NumEnums:      10,
			NumMessages:   33,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_forgepoint_events_v1_events_proto_goTypes,
		DependencyIndexes: file_forgepoint_events_v1_events_proto_depIdxs,
		EnumInfos:         file_forgepoint_events_v1_events_proto_enumTypes,
		MessageInfos:      file_forgepoint_events_v1_events_proto_msgTypes,
	}.Build()
	File_forgepoint_events_v1_events_proto = out.File
	file_forgepoint_events_v1_events_proto_goTypes = nil
	file_forgepoint_events_v1_events_proto_depIdxs = nil
}
