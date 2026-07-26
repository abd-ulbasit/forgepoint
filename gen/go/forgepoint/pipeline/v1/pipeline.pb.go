// ============================================================================
// Forgepoint Pipeline Orchestrator Proto Definitions
// ============================================================================
//
// WHY: The Pipeline Orchestrator is the "star service" of the platform — a
// GENERIC workflow engine that runs multi-step ML workflows. Two execution
// models live behind one API:
//
//   1. SAGA (orchestration + compensation) — for stateful, side-effecting
//      workflows like model DEPLOYMENT. Steps run in sequence; if a later step
//      fails, previously-completed steps are UNDONE in reverse order by running
//      their compensation steps. This is how we get "distributed transaction"
//      semantics without a 2-phase-commit coordinator.
//
//   2. DAG (directed acyclic graph) — for TRAINING workflows. Steps declare
//      `depends_on` edges; independent steps run in parallel (fan-out), and
//      barrier steps wait for all parents (fan-in). No global rollback —
//      failure propagates to dependents.
//
// PATTERN — Saga (Orchestration) vs Choreography:
//   ORCHESTRATION (this service): a CENTRAL coordinator (the orchestrator)
//   tells each participant what to do and decides when to compensate. The
//   control flow lives in ONE place — easy to reason about, easy to visualize,
//   easy to debug ("where is the saga stuck?"). Tradeoff: the orchestrator is
//   a coupling point and must be highly available + durable.
//
//   CHOREOGRAPHY (the Notification service uses this): no coordinator; each
//   service reacts to events and emits its own. Decoupled, but the workflow is
//   implicit — there is no single place that knows the whole flow, which makes
//   debugging and compensation ordering much harder.
//
//   WHY ORCHESTRATION FOR DEPLOYMENT: deployment compensation MUST run in a
//   strict reverse order (destroy serving instance only AFTER rolling back
//   traffic). A central orchestrator enforces that order deterministically;
//   choreography cannot guarantee it.
//
// REAL-WORLD COMPARISON:
//   - Temporal / Cadence: durable workflow engines — same "orchestrator owns
//     the state machine, persists every step" idea. Our saga is a hand-rolled,
//     domain-specific Temporal.
//   - AWS Step Functions: state machine of steps with retry/catch — closest
//     managed analogue to this service.
//   - Argo Workflows / Airflow: DAG executors for ML/data pipelines — analogous
//     to our DAG mode (TRAINING_DAG).
//   - Netflix Conductor: orchestration-based saga engine for microservices.
//
// DURABILITY: every StepExecution is persisted to Postgres
// BEFORE the step runs (write-ahead). If the orchestrator pod crashes mid-saga,
// on restart it loads the last persisted checkpoint and RESUMES — it does not
// restart the whole pipeline. The Helm chart runs this service with LEADER
// ELECTION (single active replica) so two pods never drive the same saga
// concurrently. The API here is the durable, queryable face of that state.
//
// CLOSED LOOP: this service is what makes Forgepoint a closed loop. The Model
// Monitor publishes fp.models.drift.detected (events.ModelDriftDetected); this
// service CONSUMES it and, on a CRITICAL report with auto_retrain armed, calls
// its OWN TriggerExecution on the model's retrain pipeline (retrain → canary →
// promote). That closes serve → monitor → retrain WITHOUT a synchronous callback
// — the orchestrator reacts to a fat-but-flat event, not a gRPC poke.
//
// EVENT CONTRACT (single source of truth): this service's NATS payloads are
// defined in forgepoint/events/v1/events.proto, NOT here. We deliberately do NOT
// re-declare event-payload messages in this API proto, and we do NOT even import
// events.proto into this file (the generated Go event types are imported by the
// Go publisher/consumer code instead — keeping the API-proto surface uncoupled
// from the event schema). WHY: an event is a PUBLISHED CONTRACT with its own
// lifecycle and its own buf-breaking guarantee, decoupled from the RPC types — a
// consumer (Notification, Experiment Tracker) depends only on the events package,
// never on this service's API proto. See the long DESIGN block in events.proto
// for the schema-registry reasoning. The handler maps this proto's domain enums
// (PipelineType, StepType) to/from the mirrored events enums at the publish
// boundary (a few lines of mechanical conversion — the price of decoupling).
//
//   PRODUCES (subject → events.v1 message):
//     fp.pipelines.started                  → events.PipelineStarted
//     fp.pipelines.step.completed           → events.StepCompleted
//     fp.pipelines.step.failed              → events.StepFailed
//     fp.pipelines.completed                → events.PipelineCompleted
//     fp.pipelines.failed                   → events.PipelineFailed
//     fp.pipelines.compensation.triggered   → events.CompensationTriggered
//     fp.pipelines.model.deployed           → events.ModelDeployed     (NEW — see below)
//     fp.pipelines.model.undeployed         → events.ModelUndeployed   (NEW — see below)
//
//   CONSUMES:
//     fp.models.drift.detected              → events.ModelDriftDetected
//       (on severity == CRITICAL && auto_retrain, TriggerExecution(retrain_pipeline_id))
//
// MODEL-DEPLOY LIFECYCLE OWNERSHIP (events.proto conflict #2): the DEPLOY /
// PROMOTE saga steps — and the compensation that rolls them back — are the
// AUTHORITATIVE source of "a model version is now (un)deployed". This service
// therefore OWNS and publishes events.ModelDeployed / events.ModelUndeployed
// under fp.pipelines.model.* (the deploy is a WORKFLOW outcome, so it lives in
// the pipelines domain even though it concerns a model — EventEnvelope.source
// records "pipeline-orchestrator" as the real producer). The Inference Gateway
// and Model Serving CONSUME these to add/remove routes and (un)load versions; a
// serving pod must NOT emit a competing ModelLoaded/Unloaded lifecycle event.
//   SSRF GUARD: events.ModelDeployed.endpoint is the serving
//   backend address. It is RESOLVED SERVER-SIDE by the DEPLOY executor (from the
//   model version + the K8s Service it created), NEVER taken from client-supplied
//   step `config`. Accepting a client URL as the route target would let a caller
//   point gateway traffic at an arbitrary internal host (SSRF). The executor only
//   ever emits an endpoint it constructed itself, against the fp-models namespace.
//
// VERSIONING: Package path includes v1 following Buf/Google convention.
// Breaking changes require a new forgepoint.pipeline.v2 package.
// ============================================================================

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.11
// 	protoc        (unknown)
// source: forgepoint/pipeline/v1/pipeline.proto

package pipelinev1

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

// ============================================================================
// PipelineType
// ============================================================================
//
// WHY: One service, two execution models. The type tells the engine HOW to run
// the steps — sequentially with compensation (saga) or as a parallel DAG.
//
// WHY NOT TWO SEPARATE SERVICES: because the durable state,
// crash recovery, persistence, and observability are identical for both; only
// the scheduling differs. Splitting would duplicate the hard 80% (durability)
// to vary the easy 20% (scheduling). The type field selects the strategy at
// runtime — Strategy pattern at the message level.
// ============================================================================
type PipelineType int32

const (
	// Proto3 requires a zero value; treat it as "unset / invalid". Buf STANDARD
	// (ENUM_ZERO_VALUE_SUFFIX) requires the _UNSPECIFIED suffix.
	PipelineType_PIPELINE_TYPE_UNSPECIFIED PipelineType = 0
	// Sequential saga with compensation. Used for model DEPLOYMENT:
	// validate → deploy → canary → promote, with rollback on failure.
	PipelineType_PIPELINE_TYPE_DEPLOYMENT_SAGA PipelineType = 1
	// Parallel DAG. Used for model TRAINING:
	// fetch → preprocess → (train fold 1..N in parallel) → aggregate → evaluate → register.
	PipelineType_PIPELINE_TYPE_TRAINING_DAG PipelineType = 2
	// Batch inference DAG (scoring a dataset). Also DAG-scheduled.
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
	return file_forgepoint_pipeline_v1_pipeline_proto_enumTypes[0].Descriptor()
}

func (PipelineType) Type() protoreflect.EnumType {
	return &file_forgepoint_pipeline_v1_pipeline_proto_enumTypes[0]
}

func (x PipelineType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use PipelineType.Descriptor instead.
func (PipelineType) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{0}
}

// ============================================================================
// StepType
// ============================================================================
//
// WHY: Each step maps to a concrete StepExecutor implementation in the engine
// (ExecutorRegistry: StepType → executor). The proto enumerates the KINDS of
// work the platform knows how to do; CUSTOM is the escape hatch.
//
// WHY AN ENUM AND NOT A FREE STRING: an enum is self-documenting, validated at
// the edge (unknown step types are rejected at CreatePipeline), and lets the
// executor registry be exhaustive. Tradeoff: adding a new step kind is a proto
// change. CUSTOM + a config map covers the long tail without a proto bump.
// ============================================================================
type StepType int32

const (
	StepType_STEP_TYPE_UNSPECIFIED StepType = 0
	// Saga (deployment) steps -------------------------------------------------
	// Verify the model exists and is in a deployable stage (calls Model Registry).
	StepType_STEP_TYPE_VALIDATE StepType = 1
	// Build/prepare a serving artifact or image.
	StepType_STEP_TYPE_BUILD StepType = 2
	// Create the K8s serving Deployment for a model version.
	StepType_STEP_TYPE_DEPLOY StepType = 3
	// Shift a small traffic % to the new version and watch metrics (Inference GW).
	StepType_STEP_TYPE_CANARY StepType = 4
	// Promote to 100% traffic once the canary passes.
	StepType_STEP_TYPE_PROMOTE StepType = 5
	// DAG (training) steps ----------------------------------------------------
	// Launch a K8s training Job.
	StepType_STEP_TYPE_TRAIN StepType = 6
	// Evaluate trained model metrics against thresholds.
	StepType_STEP_TYPE_EVALUATE StepType = 7
	// Register the resulting model version in the Model Registry.
	StepType_STEP_TYPE_REGISTER StepType = 8
	// Escape hatch: arbitrary step whose behavior is driven entirely by `config`.
	// Used for one-off / experimental steps without changing this proto.
	StepType_STEP_TYPE_CUSTOM StepType = 9
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
	return file_forgepoint_pipeline_v1_pipeline_proto_enumTypes[1].Descriptor()
}

func (StepType) Type() protoreflect.EnumType {
	return &file_forgepoint_pipeline_v1_pipeline_proto_enumTypes[1]
}

func (x StepType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use StepType.Descriptor instead.
func (StepType) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{1}
}

// ============================================================================
// ExecutionStatus
// ============================================================================
//
// WHY: This is the saga state machine, surfaced as an enum. The orchestrator
// transitions an Execution through these states and persists each transition.
//
// STATE MACHINE:
//
//	PENDING ──► RUNNING ──► COMPLETED            (happy path)
//	               │
//	               ├──► (step fails) ──► COMPENSATING ──► FAILED
//	               │                         (undo completed steps in reverse)
//	               │
//	               └──► (CancelExecution) ──► COMPENSATING ──► CANCELLED
//
//	- PENDING:      durably created, not yet scheduled.
//	- RUNNING:      executing steps (sequential saga, or parallel DAG levels).
//	- COMPENSATING: a step failed (or user cancelled); running compensation
//	                steps in REVERSE completion order.
//	- COMPLETED:    all steps succeeded.
//	- FAILED:       a step failed AND compensation finished. Terminal.
//	- CANCELLED:    user-requested stop; compensation done. Terminal.
//
// WHY SEPARATE FAILED vs CANCELLED: both are terminal and both run
// compensation, but the CAUSE matters for alerting and audit. FAILED pages an
// on-call human (SagaStuck/PipelineFailed alert → Notification service);
// CANCELLED is an expected operator action and should NOT page.
// ============================================================================
type ExecutionStatus int32

const (
	ExecutionStatus_EXECUTION_STATUS_UNSPECIFIED  ExecutionStatus = 0
	ExecutionStatus_EXECUTION_STATUS_PENDING      ExecutionStatus = 1
	ExecutionStatus_EXECUTION_STATUS_RUNNING      ExecutionStatus = 2
	ExecutionStatus_EXECUTION_STATUS_COMPENSATING ExecutionStatus = 3
	ExecutionStatus_EXECUTION_STATUS_COMPLETED    ExecutionStatus = 4
	ExecutionStatus_EXECUTION_STATUS_FAILED       ExecutionStatus = 5
	ExecutionStatus_EXECUTION_STATUS_CANCELLED    ExecutionStatus = 6
)

// Enum value maps for ExecutionStatus.
var (
	ExecutionStatus_name = map[int32]string{
		0: "EXECUTION_STATUS_UNSPECIFIED",
		1: "EXECUTION_STATUS_PENDING",
		2: "EXECUTION_STATUS_RUNNING",
		3: "EXECUTION_STATUS_COMPENSATING",
		4: "EXECUTION_STATUS_COMPLETED",
		5: "EXECUTION_STATUS_FAILED",
		6: "EXECUTION_STATUS_CANCELLED",
	}
	ExecutionStatus_value = map[string]int32{
		"EXECUTION_STATUS_UNSPECIFIED":  0,
		"EXECUTION_STATUS_PENDING":      1,
		"EXECUTION_STATUS_RUNNING":      2,
		"EXECUTION_STATUS_COMPENSATING": 3,
		"EXECUTION_STATUS_COMPLETED":    4,
		"EXECUTION_STATUS_FAILED":       5,
		"EXECUTION_STATUS_CANCELLED":    6,
	}
)

func (x ExecutionStatus) Enum() *ExecutionStatus {
	p := new(ExecutionStatus)
	*p = x
	return p
}

func (x ExecutionStatus) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ExecutionStatus) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_pipeline_v1_pipeline_proto_enumTypes[2].Descriptor()
}

func (ExecutionStatus) Type() protoreflect.EnumType {
	return &file_forgepoint_pipeline_v1_pipeline_proto_enumTypes[2]
}

func (x ExecutionStatus) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ExecutionStatus.Descriptor instead.
func (ExecutionStatus) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{2}
}

// ============================================================================
// StepStatus
// ============================================================================
//
// WHY: Each step has its own lifecycle, independent of the overall execution.
// This is the per-checkpoint status persisted in `step_executions` — the row
// the orchestrator reads on crash recovery to know where to resume.
//
// COMPENSATION SUB-STATES (the saga-specific bit):
//
//	When a saga rolls back, a previously COMPLETED step moves through:
//	  COMPLETED ──► COMPENSATING ──► COMPENSATED   (undo succeeded)
//	                             └─► COMPENSATION_FAILED (undo FAILED — danger!)
//
//	COMPENSATION_FAILED is the worst case in any saga: we could not undo a
//	side effect (e.g., failed to destroy a serving instance). This is a
//	"stuck saga" requiring human intervention; the engine surfaces it loudly
//	rather than silently leaving orphaned resources. The honest posture:
//	"compensation is best-effort and idempotent, and a failed compensation is
//	an alert, not a silent state".
//
// WHY SKIPPED EXISTS: in a DAG, if a parent fails, its not-yet-started
// dependents are marked SKIPPED (never ran) — distinct from FAILED (ran and
// errored). Keeping them separate makes the execution graph readable.
// ============================================================================
type StepStatus int32

const (
	StepStatus_STEP_STATUS_UNSPECIFIED StepStatus = 0
	// Created/persisted, not yet started (write-ahead checkpoint before run).
	StepStatus_STEP_STATUS_PENDING StepStatus = 1
	// Currently executing.
	StepStatus_STEP_STATUS_RUNNING StepStatus = 2
	// Finished successfully.
	StepStatus_STEP_STATUS_COMPLETED StepStatus = 3
	// Ran and errored.
	StepStatus_STEP_STATUS_FAILED StepStatus = 4
	// A dependency failed, so this step never ran (DAG failure propagation).
	StepStatus_STEP_STATUS_SKIPPED StepStatus = 5
	// Currently running this step's compensation (undo) action.
	StepStatus_STEP_STATUS_COMPENSATING StepStatus = 6
	// Compensation (undo) succeeded — the side effect was reversed.
	StepStatus_STEP_STATUS_COMPENSATED StepStatus = 7
	// Compensation FAILED — side effect could NOT be reversed. Needs a human.
	StepStatus_STEP_STATUS_COMPENSATION_FAILED StepStatus = 8
)

// Enum value maps for StepStatus.
var (
	StepStatus_name = map[int32]string{
		0: "STEP_STATUS_UNSPECIFIED",
		1: "STEP_STATUS_PENDING",
		2: "STEP_STATUS_RUNNING",
		3: "STEP_STATUS_COMPLETED",
		4: "STEP_STATUS_FAILED",
		5: "STEP_STATUS_SKIPPED",
		6: "STEP_STATUS_COMPENSATING",
		7: "STEP_STATUS_COMPENSATED",
		8: "STEP_STATUS_COMPENSATION_FAILED",
	}
	StepStatus_value = map[string]int32{
		"STEP_STATUS_UNSPECIFIED":         0,
		"STEP_STATUS_PENDING":             1,
		"STEP_STATUS_RUNNING":             2,
		"STEP_STATUS_COMPLETED":           3,
		"STEP_STATUS_FAILED":              4,
		"STEP_STATUS_SKIPPED":             5,
		"STEP_STATUS_COMPENSATING":        6,
		"STEP_STATUS_COMPENSATED":         7,
		"STEP_STATUS_COMPENSATION_FAILED": 8,
	}
)

func (x StepStatus) Enum() *StepStatus {
	p := new(StepStatus)
	*p = x
	return p
}

func (x StepStatus) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (StepStatus) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_pipeline_v1_pipeline_proto_enumTypes[3].Descriptor()
}

func (StepStatus) Type() protoreflect.EnumType {
	return &file_forgepoint_pipeline_v1_pipeline_proto_enumTypes[3]
}

func (x StepStatus) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use StepStatus.Descriptor instead.
func (StepStatus) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{3}
}

// ============================================================================
// StepDefinition
// ============================================================================
//
// WHY a "definition" vs an "execution": a StepDefinition is the STATIC template
// (part of a PipelineDefinition) — it describes what a step IS. A StepExecution
// (below) is a RUNTIME instance — what happened when a step RAN. Same
// definition can produce many executions (one per pipeline run). This
// template/instance split is the same one Airflow draws between a DAG and a
// DagRun, or Temporal between a Workflow and a WorkflowExecution.
//
// THE TWO EDGES THAT POWER BOTH MODES:
//   - `depends_on`            → DAG edges (topological sort, parallelism).
//   - `compensation_step_id`  → saga undo pointer (what to run to roll back).
//
// A single message carries both so one engine can run either mode; a saga
// typically uses linear depends_on + compensation pointers, a DAG uses rich
// depends_on + no compensation.
// ============================================================================
type StepDefinition struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Stable identifier for this step WITHIN its pipeline (e.g., "deploy",
	// "train-fold-1"). Referenced by other steps' depends_on / compensation_step_id.
	// Unique within a PipelineDefinition; NOT a global UUID.
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// Human-readable name for UI/logs. Not required to be unique.
	Name string `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	// What kind of work this step performs → selects the StepExecutor.
	Type StepType `protobuf:"varint,3,opt,name=type,proto3,enum=forgepoint.pipeline.v1.StepType" json:"type,omitempty"`
	// DAG edges: IDs of steps that must COMPLETE before this step may start.
	// Empty = a root step (can start immediately). The engine topologically
	// sorts on these and runs independent steps in parallel. A cycle here is
	// rejected at CreatePipeline (DAGs are acyclic by definition).
	DependsOn []string `protobuf:"bytes,4,rep,name=depends_on,json=dependsOn,proto3" json:"depends_on,omitempty"`
	// SAGA compensation pointer: the id of the step to run to UNDO this step if a
	// later step fails. Empty = nothing to compensate (e.g., a read-only VALIDATE
	// step). Example: a DEPLOY step's compensation_step_id points at a
	// "destroy-instance" step. Compensation runs in REVERSE completion order.
	CompensationStepId string `protobuf:"bytes,5,opt,name=compensation_step_id,json=compensationStepId,proto3" json:"compensation_step_id,omitempty"`
	// Free-form, step-specific configuration (e.g., canary traffic %, training
	// hyperparameters, target model_id/version). Struct (not a typed message)
	// because config shape varies per StepType and per CUSTOM step — typing every
	// variant would bloat this proto and couple it to executor internals.
	// SECURITY (untrusted input — executors MUST validate, never trust shape):
	//   - SSRF: config may name a target model_id/version, but it must NOT supply a
	//     raw serving URL/host/IP for the engine to call. The DEPLOY/CANARY
	//     executors RESOLVE the serving endpoint server-side (from the model
	//     version + the K8s Service in fp-models) and only ever talk to that
	//     allowlisted, internally-constructed address. A client-supplied endpoint
	//     would be a server-side request forgery vector — rejected at validation.
	//   - Resource caps: numeric config (e.g. parallel fold count, retry budget) is
	//     clamped to engine maxima so a config can't request unbounded fan-out.
	//   - No secrets here: config is echoed in GetExecution responses and step events.
	Config *structpb.Struct `protobuf:"bytes,6,opt,name=config,proto3" json:"config,omitempty"`
	// Optional per-step timeout. If the step runs longer, the engine fails it
	// (which, in a saga, triggers compensation). Zero/unset = use engine default.
	Timeout *durationpb.Duration `protobuf:"bytes,7,opt,name=timeout,proto3" json:"timeout,omitempty"`
	// Optional max retry attempts for transient failures before the step is
	// declared FAILED. Zero = no retries (fail fast). Retries are how a saga
	// tolerates flaky downstreams without immediately rolling everything back.
	MaxRetries    int32 `protobuf:"varint,8,opt,name=max_retries,json=maxRetries,proto3" json:"max_retries,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *StepDefinition) Reset() {
	*x = StepDefinition{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *StepDefinition) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StepDefinition) ProtoMessage() {}

func (x *StepDefinition) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StepDefinition.ProtoReflect.Descriptor instead.
func (*StepDefinition) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{0}
}

func (x *StepDefinition) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *StepDefinition) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *StepDefinition) GetType() StepType {
	if x != nil {
		return x.Type
	}
	return StepType_STEP_TYPE_UNSPECIFIED
}

func (x *StepDefinition) GetDependsOn() []string {
	if x != nil {
		return x.DependsOn
	}
	return nil
}

func (x *StepDefinition) GetCompensationStepId() string {
	if x != nil {
		return x.CompensationStepId
	}
	return ""
}

func (x *StepDefinition) GetConfig() *structpb.Struct {
	if x != nil {
		return x.Config
	}
	return nil
}

func (x *StepDefinition) GetTimeout() *durationpb.Duration {
	if x != nil {
		return x.Timeout
	}
	return nil
}

func (x *StepDefinition) GetMaxRetries() int32 {
	if x != nil {
		return x.MaxRetries
	}
	return 0
}

// ============================================================================
// PipelineDefinition
// ============================================================================
//
// WHY: The reusable template a user authors once and triggers many times. It is
// the "program"; an Execution is a "process" running that program.
//
// SECURITY — SERVER-AUTHORITATIVE FIELDS:
//
//	`id`, `created_by`, `created_at` are set by the SERVER, never accepted on
//	CreatePipeline (see CreatePipelineRequest). Accepting created_by from the
//	client would be a mass-assignment / identity-spoofing hole — a caller could
//	author a pipeline "as" another user. created_by is derived from the
//	authenticated principal (TokenClaims) in the auth interceptor.
//
// ============================================================================
type PipelineDefinition struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// UUID v4. Server-assigned, immutable. Primary key.
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// Human-readable pipeline name (e.g., "fraud-model-deploy"). Unique per team.
	Name string `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	// Saga vs DAG vs batch — selects the execution strategy.
	Type PipelineType `protobuf:"varint,3,opt,name=type,proto3,enum=forgepoint.pipeline.v1.PipelineType" json:"type,omitempty"`
	// The ordered/graph set of steps. For a saga this is effectively a sequence;
	// for a DAG the depends_on edges define the real shape.
	Steps []*StepDefinition `protobuf:"bytes,4,rep,name=steps,proto3" json:"steps,omitempty"`
	// SERVER-AUTHORITATIVE: user_id of the authenticated creator (from token
	// claims, not request input). Used for audit and ownership/RBAC checks.
	CreatedBy string `protobuf:"bytes,5,opt,name=created_by,json=createdBy,proto3" json:"created_by,omitempty"`
	// SERVER-AUTHORITATIVE: when the definition was created. Immutable.
	CreatedAt *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=created_at,json=createdAt,proto3" json:"created_at,omitempty"`
	// Team that owns this pipeline (namespacing for list/RBAC). Server-derived
	// from the creator's token claims, not free-form client input.
	Team          string `protobuf:"bytes,7,opt,name=team,proto3" json:"team,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *PipelineDefinition) Reset() {
	*x = PipelineDefinition{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PipelineDefinition) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PipelineDefinition) ProtoMessage() {}

func (x *PipelineDefinition) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PipelineDefinition.ProtoReflect.Descriptor instead.
func (*PipelineDefinition) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{1}
}

func (x *PipelineDefinition) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *PipelineDefinition) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *PipelineDefinition) GetType() PipelineType {
	if x != nil {
		return x.Type
	}
	return PipelineType_PIPELINE_TYPE_UNSPECIFIED
}

func (x *PipelineDefinition) GetSteps() []*StepDefinition {
	if x != nil {
		return x.Steps
	}
	return nil
}

func (x *PipelineDefinition) GetCreatedBy() string {
	if x != nil {
		return x.CreatedBy
	}
	return ""
}

func (x *PipelineDefinition) GetCreatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CreatedAt
	}
	return nil
}

func (x *PipelineDefinition) GetTeam() string {
	if x != nil {
		return x.Team
	}
	return ""
}

// ============================================================================
// StepExecution
// ============================================================================
//
// WHY: The runtime record of ONE step running within ONE execution. Persisted
// to `step_executions` — this is THE durability checkpoint. On crash recovery
// the engine reads the latest row per execution to resume from the last
// completed step. Surfacing it in the API lets the UI render a live, per-step
// timeline and lets operators see exactly where a saga is stuck.
// ============================================================================
type StepExecution struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// UUID v4 of this step-run (distinct from StepDefinition.id, which is the
	// template step id). One StepDefinition can yield many StepExecutions.
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// The Execution this step-run belongs to.
	ExecutionId string `protobuf:"bytes,2,opt,name=execution_id,json=executionId,proto3" json:"execution_id,omitempty"`
	// The StepDefinition.id (template id) this run corresponds to. Lets the UI
	// line up the runtime row with the static graph node.
	StepId string `protobuf:"bytes,3,opt,name=step_id,json=stepId,proto3" json:"step_id,omitempty"`
	// Current lifecycle state of this step (incl. compensation sub-states).
	Status StepStatus `protobuf:"varint,4,opt,name=status,proto3,enum=forgepoint.pipeline.v1.StepStatus" json:"status,omitempty"`
	// When this step started running. Unset while PENDING.
	StartedAt *timestamppb.Timestamp `protobuf:"bytes,5,opt,name=started_at,json=startedAt,proto3" json:"started_at,omitempty"`
	// When this step reached a terminal state. Unset while not terminal.
	CompletedAt *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=completed_at,json=completedAt,proto3" json:"completed_at,omitempty"`
	// Step OUTPUT, passed as input to dependent steps (DAG data flow) — e.g., a
	// TRAIN step emits a model artifact URI consumed by REGISTER. Struct because
	// the shape is step-specific. SECURITY: never put secrets here; outputs flow
	// to other steps and into responses.
	Output *structpb.Struct `protobuf:"bytes,7,opt,name=output,proto3" json:"output,omitempty"`
	// Human-readable error message when status is FAILED / COMPENSATION_FAILED.
	// For debugging/audit, NOT for end-user display. Empty on success.
	Error string `protobuf:"bytes,8,opt,name=error,proto3" json:"error,omitempty"`
	// How many times this step has been attempted (>= 1 once it has run).
	// Surfaces retry behavior to operators ("it succeeded on attempt 3").
	Attempt       int32 `protobuf:"varint,9,opt,name=attempt,proto3" json:"attempt,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *StepExecution) Reset() {
	*x = StepExecution{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *StepExecution) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StepExecution) ProtoMessage() {}

func (x *StepExecution) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StepExecution.ProtoReflect.Descriptor instead.
func (*StepExecution) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{2}
}

func (x *StepExecution) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *StepExecution) GetExecutionId() string {
	if x != nil {
		return x.ExecutionId
	}
	return ""
}

func (x *StepExecution) GetStepId() string {
	if x != nil {
		return x.StepId
	}
	return ""
}

func (x *StepExecution) GetStatus() StepStatus {
	if x != nil {
		return x.Status
	}
	return StepStatus_STEP_STATUS_UNSPECIFIED
}

func (x *StepExecution) GetStartedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.StartedAt
	}
	return nil
}

func (x *StepExecution) GetCompletedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CompletedAt
	}
	return nil
}

func (x *StepExecution) GetOutput() *structpb.Struct {
	if x != nil {
		return x.Output
	}
	return nil
}

func (x *StepExecution) GetError() string {
	if x != nil {
		return x.Error
	}
	return ""
}

func (x *StepExecution) GetAttempt() int32 {
	if x != nil {
		return x.Attempt
	}
	return 0
}

// ============================================================================
// Execution
// ============================================================================
//
// WHY: A single run of a PipelineDefinition — the saga/DAG "process" with its
// own status, timeline, and step records. This is the primary object clients
// poll (GetExecution), stream (WatchExecution), and list (ListExecutions).
//
// SECURITY: every field here is SERVER-AUTHORITATIVE. Clients never write an
// Execution directly; they only TriggerExecution (which references a pipeline
// id + input) and the server constructs/owns the Execution. status, timestamps,
// current_step, and triggered_by cannot be set by the client.
// ============================================================================
type Execution struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// UUID v4. Server-assigned, immutable.
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// The PipelineDefinition this run instantiates.
	PipelineId string `protobuf:"bytes,2,opt,name=pipeline_id,json=pipelineId,proto3" json:"pipeline_id,omitempty"`
	// The saga/DAG state machine value (see ExecutionStatus).
	Status ExecutionStatus `protobuf:"varint,3,opt,name=status,proto3,enum=forgepoint.pipeline.v1.ExecutionStatus" json:"status,omitempty"`
	// The StepDefinition.id currently executing (or last executed). For a DAG
	// with parallelism this is the most-recently-scheduled step — a coarse "where
	// are we" pointer; the full picture is in `step_executions`.
	CurrentStep string `protobuf:"bytes,4,opt,name=current_step,json=currentStep,proto3" json:"current_step,omitempty"`
	// Per-step runtime records. Populated on GetExecution/WatchExecution so the
	// caller can render the full timeline without N extra calls.
	StepExecutions []*StepExecution `protobuf:"bytes,5,rep,name=step_executions,json=stepExecutions,proto3" json:"step_executions,omitempty"`
	// SERVER-AUTHORITATIVE: who/what triggered this run. May be a user_id (manual
	// trigger) or a service identity (e.g., "model-monitor" for auto-retrain).
	// Derived from the authenticated caller, never from request input.
	TriggeredBy string `protobuf:"bytes,6,opt,name=triggered_by,json=triggeredBy,proto3" json:"triggered_by,omitempty"`
	// SERVER-AUTHORITATIVE: when the run started.
	StartedAt *timestamppb.Timestamp `protobuf:"bytes,7,opt,name=started_at,json=startedAt,proto3" json:"started_at,omitempty"`
	// SERVER-AUTHORITATIVE: when the run reached a terminal state. Unset while
	// PENDING/RUNNING/COMPENSATING.
	CompletedAt *timestamppb.Timestamp `protobuf:"bytes,8,opt,name=completed_at,json=completedAt,proto3" json:"completed_at,omitempty"`
	// The input payload the run was triggered with (e.g., the model_id to deploy,
	// or a drift report for auto-retrain). Echoed back for traceability. Struct
	// because input shape varies by pipeline. SECURITY: no secrets/PII here.
	Input *structpb.Struct `protobuf:"bytes,9,opt,name=input,proto3" json:"input,omitempty"`
	// Human-readable failure reason when status is FAILED. Empty otherwise.
	// Summarizes the failing step's error for quick triage.
	Error         string `protobuf:"bytes,10,opt,name=error,proto3" json:"error,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Execution) Reset() {
	*x = Execution{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Execution) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Execution) ProtoMessage() {}

func (x *Execution) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Execution.ProtoReflect.Descriptor instead.
func (*Execution) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{3}
}

func (x *Execution) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Execution) GetPipelineId() string {
	if x != nil {
		return x.PipelineId
	}
	return ""
}

func (x *Execution) GetStatus() ExecutionStatus {
	if x != nil {
		return x.Status
	}
	return ExecutionStatus_EXECUTION_STATUS_UNSPECIFIED
}

func (x *Execution) GetCurrentStep() string {
	if x != nil {
		return x.CurrentStep
	}
	return ""
}

func (x *Execution) GetStepExecutions() []*StepExecution {
	if x != nil {
		return x.StepExecutions
	}
	return nil
}

func (x *Execution) GetTriggeredBy() string {
	if x != nil {
		return x.TriggeredBy
	}
	return ""
}

func (x *Execution) GetStartedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.StartedAt
	}
	return nil
}

func (x *Execution) GetCompletedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CompletedAt
	}
	return nil
}

func (x *Execution) GetInput() *structpb.Struct {
	if x != nil {
		return x.Input
	}
	return nil
}

func (x *Execution) GetError() string {
	if x != nil {
		return x.Error
	}
	return ""
}

// CreatePipelineRequest authors a new pipeline template.
//
// SECURITY — anti-mass-assignment: we deliberately ACCEPT ONLY the
// user-authorable fields (name, type, steps). We do NOT accept id, created_by,
// created_at, or team — those are server-authoritative and derived from the
// authenticated principal. This prevents a caller from forging ownership or
// timestamps. (Same discipline as auth.proto's CreateUserRequest.)
type CreatePipelineRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Pipeline name. Must be unique within the caller's team.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// Execution strategy (saga / DAG / batch).
	Type PipelineType `protobuf:"varint,2,opt,name=type,proto3,enum=forgepoint.pipeline.v1.PipelineType" json:"type,omitempty"`
	// The step graph. The server validates: known StepTypes, depends_on refers to
	// real step ids, no cycles (DAG), compensation_step_id refers to real steps.
	// BATCH-SIZE CAP (contract, not just prose): the server REJECTS a definition
	// with more than 256 steps (INVALID_ARGUMENT). A pipeline graph this large is
	// almost always a mistake, and an unbounded graph is a DoS vector (the engine
	// persists a checkpoint row per step and topo-sorts the whole set). 256 is far
	// above any real ML workflow yet bounds the work the server commits to.
	Steps []*StepDefinition `protobuf:"bytes,3,rep,name=steps,proto3" json:"steps,omitempty"`
	// Caller-supplied idempotency key (typically a UUID) for the CREATE itself.
	// WHY mutating-create idempotency: a client retry after a network blip must not
	// create two pipelines with the same name (which would then fail the
	// uniqueness check confusingly, or — worse, under a race — both succeed). Same
	// key → the SAME PipelineDefinition is returned, never a duplicate. Empty = no
	// dedup (interactive one-off). Mirrors TriggerExecution's idempotency_key and
	// the Stripe idempotency-key pattern used across the platform.
	IdempotencyKey string `protobuf:"bytes,4,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *CreatePipelineRequest) Reset() {
	*x = CreatePipelineRequest{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CreatePipelineRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreatePipelineRequest) ProtoMessage() {}

func (x *CreatePipelineRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreatePipelineRequest.ProtoReflect.Descriptor instead.
func (*CreatePipelineRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{4}
}

func (x *CreatePipelineRequest) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *CreatePipelineRequest) GetType() PipelineType {
	if x != nil {
		return x.Type
	}
	return PipelineType_PIPELINE_TYPE_UNSPECIFIED
}

func (x *CreatePipelineRequest) GetSteps() []*StepDefinition {
	if x != nil {
		return x.Steps
	}
	return nil
}

func (x *CreatePipelineRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// CreatePipelineResponse wraps the created PipelineDefinition.
// WHY wrap (not return PipelineDefinition directly): Buf STANDARD
// (RPC_RESPONSE_STANDARD_NAME) requires "<Rpc>Response"; wrapping is also
// forward-compatible (we can add e.g. validation_warnings later without
// touching the shared PipelineDefinition domain message).
type CreatePipelineResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The newly created pipeline. id/created_by/created_at populated by server.
	Pipeline      *PipelineDefinition `protobuf:"bytes,1,opt,name=pipeline,proto3" json:"pipeline,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *CreatePipelineResponse) Reset() {
	*x = CreatePipelineResponse{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CreatePipelineResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreatePipelineResponse) ProtoMessage() {}

func (x *CreatePipelineResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreatePipelineResponse.ProtoReflect.Descriptor instead.
func (*CreatePipelineResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{5}
}

func (x *CreatePipelineResponse) GetPipeline() *PipelineDefinition {
	if x != nil {
		return x.Pipeline
	}
	return nil
}

// TriggerExecutionRequest starts a new run of an existing pipeline.
//
// WHY "Trigger" and not "Create": the client does not construct an Execution;
// it asks the orchestrator to START one. The orchestrator owns the resulting
// Execution's entire lifecycle (status, steps, timestamps).
//
// IDEMPOTENCY: triggering a deployment saga twice (e.g., a
// client retry after a network blip) would deploy the same model twice and
// could leave orphaned serving instances. The idempotency_key makes the trigger
// EXACTLY-ONCE from the caller's view: the server stores key → execution_id; a
// repeat with the same key returns the SAME Execution instead of starting a new
// one. This is the Stripe idempotency-key pattern, and it pairs with the
// idempotent NATS consumers used elsewhere on the platform.
type TriggerExecutionRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The pipeline template to run.
	PipelineId string `protobuf:"bytes,1,opt,name=pipeline_id,json=pipelineId,proto3" json:"pipeline_id,omitempty"`
	// Run input (e.g., {"model_id": "...", "version": "..."} for a deploy saga,
	// or a drift report for auto-retrain). Struct because shape is pipeline-
	// specific. SECURITY: validated by the engine; no secrets/PII.
	Input *structpb.Struct `protobuf:"bytes,2,opt,name=input,proto3" json:"input,omitempty"`
	// Caller-supplied idempotency key (typically a UUID). Same key → same
	// Execution returned, never a duplicate run. Strongly recommended for any
	// automated trigger (CI, Model Monitor auto-retrain). Empty = no dedup
	// (each call starts a fresh run — use only for interactive one-offs).
	IdempotencyKey string `protobuf:"bytes,3,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *TriggerExecutionRequest) Reset() {
	*x = TriggerExecutionRequest{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TriggerExecutionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TriggerExecutionRequest) ProtoMessage() {}

func (x *TriggerExecutionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TriggerExecutionRequest.ProtoReflect.Descriptor instead.
func (*TriggerExecutionRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{6}
}

func (x *TriggerExecutionRequest) GetPipelineId() string {
	if x != nil {
		return x.PipelineId
	}
	return ""
}

func (x *TriggerExecutionRequest) GetInput() *structpb.Struct {
	if x != nil {
		return x.Input
	}
	return nil
}

func (x *TriggerExecutionRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// TriggerExecutionResponse returns the freshly-created Execution (status
// PENDING/RUNNING). Wrapped per Buf STANDARD naming and for forward compat.
type TriggerExecutionResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The started execution. Clients typically follow up with WatchExecution.
	Execution     *Execution `protobuf:"bytes,1,opt,name=execution,proto3" json:"execution,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *TriggerExecutionResponse) Reset() {
	*x = TriggerExecutionResponse{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TriggerExecutionResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TriggerExecutionResponse) ProtoMessage() {}

func (x *TriggerExecutionResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TriggerExecutionResponse.ProtoReflect.Descriptor instead.
func (*TriggerExecutionResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{7}
}

func (x *TriggerExecutionResponse) GetExecution() *Execution {
	if x != nil {
		return x.Execution
	}
	return nil
}

// GetExecutionRequest fetches the current state of one run (point-in-time poll).
// WHY a poll RPC alongside the WatchExecution stream: not every caller wants a
// long-lived stream (e.g., the CLI `fp pipeline status <id>` does a single
// fetch; a load balancer health probe can't hold a stream). Get is the cheap,
// stateless read; Watch is for live UIs.
type GetExecutionRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// UUID of the execution to fetch.
	ExecutionId   string `protobuf:"bytes,1,opt,name=execution_id,json=executionId,proto3" json:"execution_id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetExecutionRequest) Reset() {
	*x = GetExecutionRequest{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[8]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetExecutionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetExecutionRequest) ProtoMessage() {}

func (x *GetExecutionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[8]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetExecutionRequest.ProtoReflect.Descriptor instead.
func (*GetExecutionRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{8}
}

func (x *GetExecutionRequest) GetExecutionId() string {
	if x != nil {
		return x.ExecutionId
	}
	return ""
}

// GetExecutionResponse wraps the Execution (incl. its step_executions).
type GetExecutionResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Execution     *Execution             `protobuf:"bytes,1,opt,name=execution,proto3" json:"execution,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetExecutionResponse) Reset() {
	*x = GetExecutionResponse{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[9]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetExecutionResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetExecutionResponse) ProtoMessage() {}

func (x *GetExecutionResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[9]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetExecutionResponse.ProtoReflect.Descriptor instead.
func (*GetExecutionResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{9}
}

func (x *GetExecutionResponse) GetExecution() *Execution {
	if x != nil {
		return x.Execution
	}
	return nil
}

// WatchExecutionRequest opens a live stream of updates for one execution.
// One request → many responses (server-streaming). See the service comment for
// WHY server-streaming is the right call here.
type WatchExecutionRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// UUID of the execution to watch.
	ExecutionId string `protobuf:"bytes,1,opt,name=execution_id,json=executionId,proto3" json:"execution_id,omitempty"`
	// If true, the server first emits the CURRENT full state as one or more
	// updates, THEN streams subsequent changes. This avoids a get-then-watch race
	// (a state change slipping between the snapshot read and the stream open).
	// If false, only changes occurring AFTER the stream opens are sent.
	IncludeCurrentState bool `protobuf:"varint,2,opt,name=include_current_state,json=includeCurrentState,proto3" json:"include_current_state,omitempty"`
	unknownFields       protoimpl.UnknownFields
	sizeCache           protoimpl.SizeCache
}

func (x *WatchExecutionRequest) Reset() {
	*x = WatchExecutionRequest{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[10]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *WatchExecutionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WatchExecutionRequest) ProtoMessage() {}

func (x *WatchExecutionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[10]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WatchExecutionRequest.ProtoReflect.Descriptor instead.
func (*WatchExecutionRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{10}
}

func (x *WatchExecutionRequest) GetExecutionId() string {
	if x != nil {
		return x.ExecutionId
	}
	return ""
}

func (x *WatchExecutionRequest) GetIncludeCurrentState() bool {
	if x != nil {
		return x.IncludeCurrentState
	}
	return false
}

// WatchExecutionResponse is ONE event in the WatchExecution stream.
//
// NAMING: buf's RPC_RESPONSE_STANDARD_NAME lint rule requires the response of
// rpc WatchExecution to be named WatchExecutionResponse. Because this is a
// SERVER-STREAMING RPC, each message on the stream is one of these — so the
// "Response" here is a single streamed update (a delta), not a one-shot reply.
//
// WHY a dedicated stream message (not just re-sending Execution): a stream is a
// sequence of DELTAS over time, and the consumer needs to know WHAT changed and
// WHEN. We send the full Execution snapshot (simple, idempotent for the UI to
// render) PLUS metadata: which step changed and a monotonic sequence number for
// gap detection / reconnection. This mirrors how the BFF bridges this stream to
// Server-Sent Events for the web UI (see implementation plan, M4).
//
// WHY NOT a oneof of fine-grained events (StepStarted/StepFinished/...): a
// full-snapshot-per-update is far simpler for clients — they just replace their
// view. The cost is slightly larger messages; for a handful of steps this is
// negligible and the simplicity is worth it. (Tradeoff explicitly chosen.)
type WatchExecutionResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The full current execution snapshot at the moment of this update. Clients
	// can render directly from this without tracking prior deltas.
	Execution *Execution `protobuf:"bytes,1,opt,name=execution,proto3" json:"execution,omitempty"`
	// The StepExecution that changed and caused this update, if any. Empty for
	// execution-level transitions (e.g., RUNNING → COMPLETED) that aren't tied to
	// a single step. Lets a UI highlight "the deploy step just finished".
	ChangedStep *StepExecution `protobuf:"bytes,2,opt,name=changed_step,json=changedStep,proto3" json:"changed_step,omitempty"`
	// Monotonic, per-execution sequence number. Lets a reconnecting client detect
	// gaps ("I last saw seq 7, the first message after reconnect is seq 10 — I
	// missed 8 and 9, do a GetExecution to resync"). Ordering/gap-detection is the
	// streaming equivalent of an event log offset (Kafka offset / NATS sequence).
	Sequence uint64 `protobuf:"varint,3,opt,name=sequence,proto3" json:"sequence,omitempty"`
	// When the server emitted this update.
	EmittedAt     *timestamppb.Timestamp `protobuf:"bytes,4,opt,name=emitted_at,json=emittedAt,proto3" json:"emitted_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *WatchExecutionResponse) Reset() {
	*x = WatchExecutionResponse{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[11]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *WatchExecutionResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WatchExecutionResponse) ProtoMessage() {}

func (x *WatchExecutionResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[11]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WatchExecutionResponse.ProtoReflect.Descriptor instead.
func (*WatchExecutionResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{11}
}

func (x *WatchExecutionResponse) GetExecution() *Execution {
	if x != nil {
		return x.Execution
	}
	return nil
}

func (x *WatchExecutionResponse) GetChangedStep() *StepExecution {
	if x != nil {
		return x.ChangedStep
	}
	return nil
}

func (x *WatchExecutionResponse) GetSequence() uint64 {
	if x != nil {
		return x.Sequence
	}
	return 0
}

func (x *WatchExecutionResponse) GetEmittedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.EmittedAt
	}
	return nil
}

// CancelExecutionRequest requests a graceful stop of an in-flight run.
//
// WHY cancel returns the Execution (not an empty response): cancellation is
// ASYNCHRONOUS — it transitions the run to COMPENSATING (undo completed steps)
// and only later to CANCELLED. Returning the Execution lets the caller see the
// immediate transition (e.g., RUNNING → COMPENSATING) and then Watch it settle.
type CancelExecutionRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// UUID of the execution to cancel.
	ExecutionId string `protobuf:"bytes,1,opt,name=execution_id,json=executionId,proto3" json:"execution_id,omitempty"`
	// Optional human-readable reason, recorded for audit ("superseded by retrain").
	Reason        string `protobuf:"bytes,2,opt,name=reason,proto3" json:"reason,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *CancelExecutionRequest) Reset() {
	*x = CancelExecutionRequest{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[12]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CancelExecutionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CancelExecutionRequest) ProtoMessage() {}

func (x *CancelExecutionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[12]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CancelExecutionRequest.ProtoReflect.Descriptor instead.
func (*CancelExecutionRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{12}
}

func (x *CancelExecutionRequest) GetExecutionId() string {
	if x != nil {
		return x.ExecutionId
	}
	return ""
}

func (x *CancelExecutionRequest) GetReason() string {
	if x != nil {
		return x.Reason
	}
	return ""
}

// CancelExecutionResponse wraps the (now COMPENSATING/CANCELLED) Execution.
type CancelExecutionResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Execution     *Execution             `protobuf:"bytes,1,opt,name=execution,proto3" json:"execution,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *CancelExecutionResponse) Reset() {
	*x = CancelExecutionResponse{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[13]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CancelExecutionResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CancelExecutionResponse) ProtoMessage() {}

func (x *CancelExecutionResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[13]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CancelExecutionResponse.ProtoReflect.Descriptor instead.
func (*CancelExecutionResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{13}
}

func (x *CancelExecutionResponse) GetExecution() *Execution {
	if x != nil {
		return x.Execution
	}
	return nil
}

// ListExecutionsRequest returns a paginated, filterable list of runs.
//
// PAGINATION: reuses forgepoint.common.v1.PaginationRequest for consistency
// across every list RPC on the platform (cursor-based; see common.proto for the
// WHY). SECURITY: the server CAPS page_size (default 20, max 100) regardless of
// the requested value, to prevent a client from requesting an unbounded page.
//
// TENANCY (anti-IDOR): there is deliberately NO team field on this request. The
// result set is ALWAYS scoped to the caller's team, derived SERVER-SIDE from the
// auth token claims — never from a client-supplied tenancy field. A client cannot
// list another team's executions by passing a different team. The pipeline_id /
// status_filter below only NARROW within that authorized scope.
type ListExecutionsRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Optional: only runs of this pipeline. Empty = all pipelines the caller's team
	// owns. A pipeline_id from another team yields an empty page (scope-filtered,
	// not an error — we don't confirm/deny existence of out-of-scope ids).
	PipelineId string `protobuf:"bytes,1,opt,name=pipeline_id,json=pipelineId,proto3" json:"pipeline_id,omitempty"`
	// Optional: only runs in this status (e.g., FAILED for a triage dashboard).
	// UNSPECIFIED = any status.
	StatusFilter ExecutionStatus `protobuf:"varint,2,opt,name=status_filter,json=statusFilter,proto3,enum=forgepoint.pipeline.v1.ExecutionStatus" json:"status_filter,omitempty"`
	// Cursor-based pagination. page_size defaults to 20, capped at 100 server-side.
	Pagination    *v1.PaginationRequest `protobuf:"bytes,3,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListExecutionsRequest) Reset() {
	*x = ListExecutionsRequest{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[14]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListExecutionsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListExecutionsRequest) ProtoMessage() {}

func (x *ListExecutionsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[14]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListExecutionsRequest.ProtoReflect.Descriptor instead.
func (*ListExecutionsRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{14}
}

func (x *ListExecutionsRequest) GetPipelineId() string {
	if x != nil {
		return x.PipelineId
	}
	return ""
}

func (x *ListExecutionsRequest) GetStatusFilter() ExecutionStatus {
	if x != nil {
		return x.StatusFilter
	}
	return ExecutionStatus_EXECUTION_STATUS_UNSPECIFIED
}

func (x *ListExecutionsRequest) GetPagination() *v1.PaginationRequest {
	if x != nil {
		return x.Pagination
	}
	return nil
}

type ListExecutionsResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The page of executions. NOTE: list items omit the per-step detail
	// (step_executions) to keep the page lightweight — fetch GetExecution for the
	// full timeline. (Documented contract; the server populates summary fields.)
	Executions []*Execution `protobuf:"bytes,1,rep,name=executions,proto3" json:"executions,omitempty"`
	// Pagination metadata: next_page_token + total_count.
	Pagination    *v1.PaginationResponse `protobuf:"bytes,2,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListExecutionsResponse) Reset() {
	*x = ListExecutionsResponse{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[15]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListExecutionsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListExecutionsResponse) ProtoMessage() {}

func (x *ListExecutionsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[15]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListExecutionsResponse.ProtoReflect.Descriptor instead.
func (*ListExecutionsResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{15}
}

func (x *ListExecutionsResponse) GetExecutions() []*Execution {
	if x != nil {
		return x.Executions
	}
	return nil
}

func (x *ListExecutionsResponse) GetPagination() *v1.PaginationResponse {
	if x != nil {
		return x.Pagination
	}
	return nil
}

// ListPipelinesRequest returns a paginated list of pipeline TEMPLATES (not
// runs). Same pagination/cap discipline as ListExecutions (page_size default 20,
// capped 100 server-side). TENANCY: like ListExecutions, results are scoped to
// the caller's team from auth claims — there is no client-supplied team field.
type ListPipelinesRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Optional: only pipelines of this type (e.g., only TRAINING_DAG).
	// UNSPECIFIED = any type.
	TypeFilter PipelineType `protobuf:"varint,1,opt,name=type_filter,json=typeFilter,proto3,enum=forgepoint.pipeline.v1.PipelineType" json:"type_filter,omitempty"`
	// Cursor-based pagination. page_size defaults to 20, capped at 100 server-side.
	Pagination    *v1.PaginationRequest `protobuf:"bytes,2,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListPipelinesRequest) Reset() {
	*x = ListPipelinesRequest{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[16]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListPipelinesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListPipelinesRequest) ProtoMessage() {}

func (x *ListPipelinesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[16]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListPipelinesRequest.ProtoReflect.Descriptor instead.
func (*ListPipelinesRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{16}
}

func (x *ListPipelinesRequest) GetTypeFilter() PipelineType {
	if x != nil {
		return x.TypeFilter
	}
	return PipelineType_PIPELINE_TYPE_UNSPECIFIED
}

func (x *ListPipelinesRequest) GetPagination() *v1.PaginationRequest {
	if x != nil {
		return x.Pagination
	}
	return nil
}

type ListPipelinesResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The page of pipeline definitions matching the request (within team/RBAC scope).
	Pipelines []*PipelineDefinition `protobuf:"bytes,1,rep,name=pipelines,proto3" json:"pipelines,omitempty"`
	// Pagination metadata.
	Pagination    *v1.PaginationResponse `protobuf:"bytes,2,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListPipelinesResponse) Reset() {
	*x = ListPipelinesResponse{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[17]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListPipelinesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListPipelinesResponse) ProtoMessage() {}

func (x *ListPipelinesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[17]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListPipelinesResponse.ProtoReflect.Descriptor instead.
func (*ListPipelinesResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{17}
}

func (x *ListPipelinesResponse) GetPipelines() []*PipelineDefinition {
	if x != nil {
		return x.Pipelines
	}
	return nil
}

func (x *ListPipelinesResponse) GetPagination() *v1.PaginationResponse {
	if x != nil {
		return x.Pagination
	}
	return nil
}

// GetPipelineRequest fetches ONE pipeline TEMPLATE by id.
// WHY this RPC exists (it was missing): the CLI (`fp pipeline get <id>`), the web
// UI's pipeline editor, and any client that wants to inspect a template's full
// step graph before triggering it need a by-id read. ListPipelines is for
// browsing; this is the point lookup. The server scopes the read to the caller's
// team/RBAC (a caller cannot fetch another team's pipeline by guessing its id).
type GetPipelineRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// UUID of the pipeline definition to fetch.
	PipelineId    string `protobuf:"bytes,1,opt,name=pipeline_id,json=pipelineId,proto3" json:"pipeline_id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetPipelineRequest) Reset() {
	*x = GetPipelineRequest{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[18]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetPipelineRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetPipelineRequest) ProtoMessage() {}

func (x *GetPipelineRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[18]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetPipelineRequest.ProtoReflect.Descriptor instead.
func (*GetPipelineRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{18}
}

func (x *GetPipelineRequest) GetPipelineId() string {
	if x != nil {
		return x.PipelineId
	}
	return ""
}

// GetPipelineResponse wraps the PipelineDefinition (incl. its full step graph).
type GetPipelineResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Pipeline      *PipelineDefinition    `protobuf:"bytes,1,opt,name=pipeline,proto3" json:"pipeline,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetPipelineResponse) Reset() {
	*x = GetPipelineResponse{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[19]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetPipelineResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetPipelineResponse) ProtoMessage() {}

func (x *GetPipelineResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[19]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetPipelineResponse.ProtoReflect.Descriptor instead.
func (*GetPipelineResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{19}
}

func (x *GetPipelineResponse) GetPipeline() *PipelineDefinition {
	if x != nil {
		return x.Pipeline
	}
	return nil
}

// UpdatePipelineRequest edits an existing pipeline TEMPLATE (name and/or steps).
//
// WHY this RPC exists (the CLI/UI need to edit a template without recreating it):
// without Update, the only way to change a pipeline is delete-and-recreate, which
// changes its id and orphans its execution history. Update keeps the id (and
// therefore the lineage of past runs) stable.
//
// SECURITY — anti-mass-assignment (same discipline as Create): we accept ONLY the
// user-authorable fields. id selects the target; name/type/steps are the editable
// payload. We do NOT accept created_by, created_at, or team — those stay
// server-authoritative and immutable; allowing a client to rewrite created_by
// would let it forge ownership on an existing record.
//
// IMMUTABILITY NOTE: `type` (saga vs DAG) is NOT editable — changing a pipeline's
// execution model out from under in-flight executions is unsafe, so the server
// rejects a type change (create a new pipeline instead). It is omitted from this
// request deliberately.
//
// CONCURRENCY: editing a template does NOT affect already-running executions —
// each Execution captures the step graph it started with (template/instance
// split). Only future TriggerExecutions see the new definition.
type UpdatePipelineRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// UUID of the pipeline to update (selects the target; not itself mutable).
	PipelineId string `protobuf:"bytes,1,opt,name=pipeline_id,json=pipelineId,proto3" json:"pipeline_id,omitempty"`
	// New human-readable name. Must remain unique within the caller's team.
	Name string `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	// The replacement step graph. Same validation + 256-step cap as CreatePipeline.
	// This is a FULL REPLACE of the steps (not a partial patch) — simpler to reason
	// about than field-level merge for a graph, and the whole graph is re-validated.
	Steps         []*StepDefinition `protobuf:"bytes,3,rep,name=steps,proto3" json:"steps,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UpdatePipelineRequest) Reset() {
	*x = UpdatePipelineRequest{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[20]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpdatePipelineRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdatePipelineRequest) ProtoMessage() {}

func (x *UpdatePipelineRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[20]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdatePipelineRequest.ProtoReflect.Descriptor instead.
func (*UpdatePipelineRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{20}
}

func (x *UpdatePipelineRequest) GetPipelineId() string {
	if x != nil {
		return x.PipelineId
	}
	return ""
}

func (x *UpdatePipelineRequest) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *UpdatePipelineRequest) GetSteps() []*StepDefinition {
	if x != nil {
		return x.Steps
	}
	return nil
}

// UpdatePipelineResponse wraps the updated PipelineDefinition.
type UpdatePipelineResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Pipeline      *PipelineDefinition    `protobuf:"bytes,1,opt,name=pipeline,proto3" json:"pipeline,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UpdatePipelineResponse) Reset() {
	*x = UpdatePipelineResponse{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[21]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpdatePipelineResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdatePipelineResponse) ProtoMessage() {}

func (x *UpdatePipelineResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[21]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdatePipelineResponse.ProtoReflect.Descriptor instead.
func (*UpdatePipelineResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{21}
}

func (x *UpdatePipelineResponse) GetPipeline() *PipelineDefinition {
	if x != nil {
		return x.Pipeline
	}
	return nil
}

// DeletePipelineRequest removes a pipeline TEMPLATE.
//
// WHY a dedicated RPC + WHY soft-delete: operators need to retire obsolete
// templates (the design/CLI call for a Delete). We SOFT-DELETE (archive) rather
// than hard-delete: past Executions reference this pipeline_id for audit/lineage,
// and hard-deleting would dangle those foreign keys and erase the history of what
// ran. An archived template is hidden from ListPipelines and cannot be triggered,
// but GetExecution on its historical runs still resolves the pipeline name.
//
// SAFETY: the server REJECTS deletion while the pipeline has a non-terminal
// Execution (PENDING/RUNNING/COMPENSATING) — you cannot retire a template whose
// saga is mid-flight (FAILED_PRECONDITION). Cancel those runs first.
type DeletePipelineRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// UUID of the pipeline to archive.
	PipelineId    string `protobuf:"bytes,1,opt,name=pipeline_id,json=pipelineId,proto3" json:"pipeline_id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *DeletePipelineRequest) Reset() {
	*x = DeletePipelineRequest{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[22]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DeletePipelineRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeletePipelineRequest) ProtoMessage() {}

func (x *DeletePipelineRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[22]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeletePipelineRequest.ProtoReflect.Descriptor instead.
func (*DeletePipelineRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{22}
}

func (x *DeletePipelineRequest) GetPipelineId() string {
	if x != nil {
		return x.PipelineId
	}
	return ""
}

// DeletePipelineResponse is an intentionally-empty, dedicated response.
// WHY a named empty message (not google.protobuf.Empty): Buf STANDARD
// (RPC_RESPONSE_STANDARD_NAME) requires a <Rpc>Response type, and a named message
// is forward-compatible — we can add fields later (e.g. archived_at) without a
// breaking signature change. google.protobuf.Empty can never grow a field.
type DeletePipelineResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *DeletePipelineResponse) Reset() {
	*x = DeletePipelineResponse{}
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[23]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DeletePipelineResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeletePipelineResponse) ProtoMessage() {}

func (x *DeletePipelineResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_pipeline_v1_pipeline_proto_msgTypes[23]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeletePipelineResponse.ProtoReflect.Descriptor instead.
func (*DeletePipelineResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP(), []int{23}
}

var File_forgepoint_pipeline_v1_pipeline_proto protoreflect.FileDescriptor

const file_forgepoint_pipeline_v1_pipeline_proto_rawDesc = "" +
	"\n" +
	"%forgepoint/pipeline/v1/pipeline.proto\x12\x16forgepoint.pipeline.v1\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1egoogle/protobuf/duration.proto\x1a\x1cgoogle/protobuf/struct.proto\x1a!forgepoint/common/v1/common.proto\"\xc2\x02\n" +
	"\x0eStepDefinition\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12\x12\n" +
	"\x04name\x18\x02 \x01(\tR\x04name\x124\n" +
	"\x04type\x18\x03 \x01(\x0e2 .forgepoint.pipeline.v1.StepTypeR\x04type\x12\x1d\n" +
	"\n" +
	"depends_on\x18\x04 \x03(\tR\tdependsOn\x120\n" +
	"\x14compensation_step_id\x18\x05 \x01(\tR\x12compensationStepId\x12/\n" +
	"\x06config\x18\x06 \x01(\v2\x17.google.protobuf.StructR\x06config\x123\n" +
	"\atimeout\x18\a \x01(\v2\x19.google.protobuf.DurationR\atimeout\x12\x1f\n" +
	"\vmax_retries\x18\b \x01(\x05R\n" +
	"maxRetries\"\x9e\x02\n" +
	"\x12PipelineDefinition\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12\x12\n" +
	"\x04name\x18\x02 \x01(\tR\x04name\x128\n" +
	"\x04type\x18\x03 \x01(\x0e2$.forgepoint.pipeline.v1.PipelineTypeR\x04type\x12<\n" +
	"\x05steps\x18\x04 \x03(\v2&.forgepoint.pipeline.v1.StepDefinitionR\x05steps\x12\x1d\n" +
	"\n" +
	"created_by\x18\x05 \x01(\tR\tcreatedBy\x129\n" +
	"\n" +
	"created_at\x18\x06 \x01(\v2\x1a.google.protobuf.TimestampR\tcreatedAt\x12\x12\n" +
	"\x04team\x18\a \x01(\tR\x04team\"\xf2\x02\n" +
	"\rStepExecution\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12!\n" +
	"\fexecution_id\x18\x02 \x01(\tR\vexecutionId\x12\x17\n" +
	"\astep_id\x18\x03 \x01(\tR\x06stepId\x12:\n" +
	"\x06status\x18\x04 \x01(\x0e2\".forgepoint.pipeline.v1.StepStatusR\x06status\x129\n" +
	"\n" +
	"started_at\x18\x05 \x01(\v2\x1a.google.protobuf.TimestampR\tstartedAt\x12=\n" +
	"\fcompleted_at\x18\x06 \x01(\v2\x1a.google.protobuf.TimestampR\vcompletedAt\x12/\n" +
	"\x06output\x18\a \x01(\v2\x17.google.protobuf.StructR\x06output\x12\x14\n" +
	"\x05error\x18\b \x01(\tR\x05error\x12\x18\n" +
	"\aattempt\x18\t \x01(\x05R\aattempt\"\xd2\x03\n" +
	"\tExecution\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12\x1f\n" +
	"\vpipeline_id\x18\x02 \x01(\tR\n" +
	"pipelineId\x12?\n" +
	"\x06status\x18\x03 \x01(\x0e2'.forgepoint.pipeline.v1.ExecutionStatusR\x06status\x12!\n" +
	"\fcurrent_step\x18\x04 \x01(\tR\vcurrentStep\x12N\n" +
	"\x0fstep_executions\x18\x05 \x03(\v2%.forgepoint.pipeline.v1.StepExecutionR\x0estepExecutions\x12!\n" +
	"\ftriggered_by\x18\x06 \x01(\tR\vtriggeredBy\x129\n" +
	"\n" +
	"started_at\x18\a \x01(\v2\x1a.google.protobuf.TimestampR\tstartedAt\x12=\n" +
	"\fcompleted_at\x18\b \x01(\v2\x1a.google.protobuf.TimestampR\vcompletedAt\x12-\n" +
	"\x05input\x18\t \x01(\v2\x17.google.protobuf.StructR\x05input\x12\x14\n" +
	"\x05error\x18\n" +
	" \x01(\tR\x05error\"\xcc\x01\n" +
	"\x15CreatePipelineRequest\x12\x12\n" +
	"\x04name\x18\x01 \x01(\tR\x04name\x128\n" +
	"\x04type\x18\x02 \x01(\x0e2$.forgepoint.pipeline.v1.PipelineTypeR\x04type\x12<\n" +
	"\x05steps\x18\x03 \x03(\v2&.forgepoint.pipeline.v1.StepDefinitionR\x05steps\x12'\n" +
	"\x0fidempotency_key\x18\x04 \x01(\tR\x0eidempotencyKey\"`\n" +
	"\x16CreatePipelineResponse\x12F\n" +
	"\bpipeline\x18\x01 \x01(\v2*.forgepoint.pipeline.v1.PipelineDefinitionR\bpipeline\"\x92\x01\n" +
	"\x17TriggerExecutionRequest\x12\x1f\n" +
	"\vpipeline_id\x18\x01 \x01(\tR\n" +
	"pipelineId\x12-\n" +
	"\x05input\x18\x02 \x01(\v2\x17.google.protobuf.StructR\x05input\x12'\n" +
	"\x0fidempotency_key\x18\x03 \x01(\tR\x0eidempotencyKey\"[\n" +
	"\x18TriggerExecutionResponse\x12?\n" +
	"\texecution\x18\x01 \x01(\v2!.forgepoint.pipeline.v1.ExecutionR\texecution\"8\n" +
	"\x13GetExecutionRequest\x12!\n" +
	"\fexecution_id\x18\x01 \x01(\tR\vexecutionId\"W\n" +
	"\x14GetExecutionResponse\x12?\n" +
	"\texecution\x18\x01 \x01(\v2!.forgepoint.pipeline.v1.ExecutionR\texecution\"n\n" +
	"\x15WatchExecutionRequest\x12!\n" +
	"\fexecution_id\x18\x01 \x01(\tR\vexecutionId\x122\n" +
	"\x15include_current_state\x18\x02 \x01(\bR\x13includeCurrentState\"\xfa\x01\n" +
	"\x16WatchExecutionResponse\x12?\n" +
	"\texecution\x18\x01 \x01(\v2!.forgepoint.pipeline.v1.ExecutionR\texecution\x12H\n" +
	"\fchanged_step\x18\x02 \x01(\v2%.forgepoint.pipeline.v1.StepExecutionR\vchangedStep\x12\x1a\n" +
	"\bsequence\x18\x03 \x01(\x04R\bsequence\x129\n" +
	"\n" +
	"emitted_at\x18\x04 \x01(\v2\x1a.google.protobuf.TimestampR\temittedAt\"S\n" +
	"\x16CancelExecutionRequest\x12!\n" +
	"\fexecution_id\x18\x01 \x01(\tR\vexecutionId\x12\x16\n" +
	"\x06reason\x18\x02 \x01(\tR\x06reason\"Z\n" +
	"\x17CancelExecutionResponse\x12?\n" +
	"\texecution\x18\x01 \x01(\v2!.forgepoint.pipeline.v1.ExecutionR\texecution\"\xcf\x01\n" +
	"\x15ListExecutionsRequest\x12\x1f\n" +
	"\vpipeline_id\x18\x01 \x01(\tR\n" +
	"pipelineId\x12L\n" +
	"\rstatus_filter\x18\x02 \x01(\x0e2'.forgepoint.pipeline.v1.ExecutionStatusR\fstatusFilter\x12G\n" +
	"\n" +
	"pagination\x18\x03 \x01(\v2'.forgepoint.common.v1.PaginationRequestR\n" +
	"pagination\"\xa5\x01\n" +
	"\x16ListExecutionsResponse\x12A\n" +
	"\n" +
	"executions\x18\x01 \x03(\v2!.forgepoint.pipeline.v1.ExecutionR\n" +
	"executions\x12H\n" +
	"\n" +
	"pagination\x18\x02 \x01(\v2(.forgepoint.common.v1.PaginationResponseR\n" +
	"pagination\"\xa6\x01\n" +
	"\x14ListPipelinesRequest\x12E\n" +
	"\vtype_filter\x18\x01 \x01(\x0e2$.forgepoint.pipeline.v1.PipelineTypeR\n" +
	"typeFilter\x12G\n" +
	"\n" +
	"pagination\x18\x02 \x01(\v2'.forgepoint.common.v1.PaginationRequestR\n" +
	"pagination\"\xab\x01\n" +
	"\x15ListPipelinesResponse\x12H\n" +
	"\tpipelines\x18\x01 \x03(\v2*.forgepoint.pipeline.v1.PipelineDefinitionR\tpipelines\x12H\n" +
	"\n" +
	"pagination\x18\x02 \x01(\v2(.forgepoint.common.v1.PaginationResponseR\n" +
	"pagination\"5\n" +
	"\x12GetPipelineRequest\x12\x1f\n" +
	"\vpipeline_id\x18\x01 \x01(\tR\n" +
	"pipelineId\"]\n" +
	"\x13GetPipelineResponse\x12F\n" +
	"\bpipeline\x18\x01 \x01(\v2*.forgepoint.pipeline.v1.PipelineDefinitionR\bpipeline\"\x8a\x01\n" +
	"\x15UpdatePipelineRequest\x12\x1f\n" +
	"\vpipeline_id\x18\x01 \x01(\tR\n" +
	"pipelineId\x12\x12\n" +
	"\x04name\x18\x02 \x01(\tR\x04name\x12<\n" +
	"\x05steps\x18\x03 \x03(\v2&.forgepoint.pipeline.v1.StepDefinitionR\x05steps\"`\n" +
	"\x16UpdatePipelineResponse\x12F\n" +
	"\bpipeline\x18\x01 \x01(\v2*.forgepoint.pipeline.v1.PipelineDefinitionR\bpipeline\"8\n" +
	"\x15DeletePipelineRequest\x12\x1f\n" +
	"\vpipeline_id\x18\x01 \x01(\tR\n" +
	"pipelineId\"\x18\n" +
	"\x16DeletePipelineResponse*\x93\x01\n" +
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
	"\x10STEP_TYPE_CUSTOM\x10\t*\xef\x01\n" +
	"\x0fExecutionStatus\x12 \n" +
	"\x1cEXECUTION_STATUS_UNSPECIFIED\x10\x00\x12\x1c\n" +
	"\x18EXECUTION_STATUS_PENDING\x10\x01\x12\x1c\n" +
	"\x18EXECUTION_STATUS_RUNNING\x10\x02\x12!\n" +
	"\x1dEXECUTION_STATUS_COMPENSATING\x10\x03\x12\x1e\n" +
	"\x1aEXECUTION_STATUS_COMPLETED\x10\x04\x12\x1b\n" +
	"\x17EXECUTION_STATUS_FAILED\x10\x05\x12\x1e\n" +
	"\x1aEXECUTION_STATUS_CANCELLED\x10\x06*\x87\x02\n" +
	"\n" +
	"StepStatus\x12\x1b\n" +
	"\x17STEP_STATUS_UNSPECIFIED\x10\x00\x12\x17\n" +
	"\x13STEP_STATUS_PENDING\x10\x01\x12\x17\n" +
	"\x13STEP_STATUS_RUNNING\x10\x02\x12\x19\n" +
	"\x15STEP_STATUS_COMPLETED\x10\x03\x12\x16\n" +
	"\x12STEP_STATUS_FAILED\x10\x04\x12\x17\n" +
	"\x13STEP_STATUS_SKIPPED\x10\x05\x12\x1c\n" +
	"\x18STEP_STATUS_COMPENSATING\x10\x06\x12\x1b\n" +
	"\x17STEP_STATUS_COMPENSATED\x10\a\x12#\n" +
	"\x1fSTEP_STATUS_COMPENSATION_FAILED\x10\b2\x80\t\n" +
	"\x1bPipelineOrchestratorService\x12o\n" +
	"\x0eCreatePipeline\x12-.forgepoint.pipeline.v1.CreatePipelineRequest\x1a..forgepoint.pipeline.v1.CreatePipelineResponse\x12l\n" +
	"\rListPipelines\x12,.forgepoint.pipeline.v1.ListPipelinesRequest\x1a-.forgepoint.pipeline.v1.ListPipelinesResponse\x12f\n" +
	"\vGetPipeline\x12*.forgepoint.pipeline.v1.GetPipelineRequest\x1a+.forgepoint.pipeline.v1.GetPipelineResponse\x12o\n" +
	"\x0eUpdatePipeline\x12-.forgepoint.pipeline.v1.UpdatePipelineRequest\x1a..forgepoint.pipeline.v1.UpdatePipelineResponse\x12o\n" +
	"\x0eDeletePipeline\x12-.forgepoint.pipeline.v1.DeletePipelineRequest\x1a..forgepoint.pipeline.v1.DeletePipelineResponse\x12u\n" +
	"\x10TriggerExecution\x12/.forgepoint.pipeline.v1.TriggerExecutionRequest\x1a0.forgepoint.pipeline.v1.TriggerExecutionResponse\x12i\n" +
	"\fGetExecution\x12+.forgepoint.pipeline.v1.GetExecutionRequest\x1a,.forgepoint.pipeline.v1.GetExecutionResponse\x12q\n" +
	"\x0eWatchExecution\x12-.forgepoint.pipeline.v1.WatchExecutionRequest\x1a..forgepoint.pipeline.v1.WatchExecutionResponse0\x01\x12r\n" +
	"\x0fCancelExecution\x12..forgepoint.pipeline.v1.CancelExecutionRequest\x1a/.forgepoint.pipeline.v1.CancelExecutionResponse\x12o\n" +
	"\x0eListExecutions\x12-.forgepoint.pipeline.v1.ListExecutionsRequest\x1a..forgepoint.pipeline.v1.ListExecutionsResponseBLZJgithub.com/abd-ulbasit/forgepoint/gen/go/forgepoint/pipeline/v1;pipelinev1b\x06proto3"

var (
	file_forgepoint_pipeline_v1_pipeline_proto_rawDescOnce sync.Once
	file_forgepoint_pipeline_v1_pipeline_proto_rawDescData []byte
)

func file_forgepoint_pipeline_v1_pipeline_proto_rawDescGZIP() []byte {
	file_forgepoint_pipeline_v1_pipeline_proto_rawDescOnce.Do(func() {
		file_forgepoint_pipeline_v1_pipeline_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_forgepoint_pipeline_v1_pipeline_proto_rawDesc), len(file_forgepoint_pipeline_v1_pipeline_proto_rawDesc)))
	})
	return file_forgepoint_pipeline_v1_pipeline_proto_rawDescData
}

var file_forgepoint_pipeline_v1_pipeline_proto_enumTypes = make([]protoimpl.EnumInfo, 4)
var file_forgepoint_pipeline_v1_pipeline_proto_msgTypes = make([]protoimpl.MessageInfo, 24)
var file_forgepoint_pipeline_v1_pipeline_proto_goTypes = []any{
	(PipelineType)(0),                // 0: forgepoint.pipeline.v1.PipelineType
	(StepType)(0),                    // 1: forgepoint.pipeline.v1.StepType
	(ExecutionStatus)(0),             // 2: forgepoint.pipeline.v1.ExecutionStatus
	(StepStatus)(0),                  // 3: forgepoint.pipeline.v1.StepStatus
	(*StepDefinition)(nil),           // 4: forgepoint.pipeline.v1.StepDefinition
	(*PipelineDefinition)(nil),       // 5: forgepoint.pipeline.v1.PipelineDefinition
	(*StepExecution)(nil),            // 6: forgepoint.pipeline.v1.StepExecution
	(*Execution)(nil),                // 7: forgepoint.pipeline.v1.Execution
	(*CreatePipelineRequest)(nil),    // 8: forgepoint.pipeline.v1.CreatePipelineRequest
	(*CreatePipelineResponse)(nil),   // 9: forgepoint.pipeline.v1.CreatePipelineResponse
	(*TriggerExecutionRequest)(nil),  // 10: forgepoint.pipeline.v1.TriggerExecutionRequest
	(*TriggerExecutionResponse)(nil), // 11: forgepoint.pipeline.v1.TriggerExecutionResponse
	(*GetExecutionRequest)(nil),      // 12: forgepoint.pipeline.v1.GetExecutionRequest
	(*GetExecutionResponse)(nil),     // 13: forgepoint.pipeline.v1.GetExecutionResponse
	(*WatchExecutionRequest)(nil),    // 14: forgepoint.pipeline.v1.WatchExecutionRequest
	(*WatchExecutionResponse)(nil),   // 15: forgepoint.pipeline.v1.WatchExecutionResponse
	(*CancelExecutionRequest)(nil),   // 16: forgepoint.pipeline.v1.CancelExecutionRequest
	(*CancelExecutionResponse)(nil),  // 17: forgepoint.pipeline.v1.CancelExecutionResponse
	(*ListExecutionsRequest)(nil),    // 18: forgepoint.pipeline.v1.ListExecutionsRequest
	(*ListExecutionsResponse)(nil),   // 19: forgepoint.pipeline.v1.ListExecutionsResponse
	(*ListPipelinesRequest)(nil),     // 20: forgepoint.pipeline.v1.ListPipelinesRequest
	(*ListPipelinesResponse)(nil),    // 21: forgepoint.pipeline.v1.ListPipelinesResponse
	(*GetPipelineRequest)(nil),       // 22: forgepoint.pipeline.v1.GetPipelineRequest
	(*GetPipelineResponse)(nil),      // 23: forgepoint.pipeline.v1.GetPipelineResponse
	(*UpdatePipelineRequest)(nil),    // 24: forgepoint.pipeline.v1.UpdatePipelineRequest
	(*UpdatePipelineResponse)(nil),   // 25: forgepoint.pipeline.v1.UpdatePipelineResponse
	(*DeletePipelineRequest)(nil),    // 26: forgepoint.pipeline.v1.DeletePipelineRequest
	(*DeletePipelineResponse)(nil),   // 27: forgepoint.pipeline.v1.DeletePipelineResponse
	(*structpb.Struct)(nil),          // 28: google.protobuf.Struct
	(*durationpb.Duration)(nil),      // 29: google.protobuf.Duration
	(*timestamppb.Timestamp)(nil),    // 30: google.protobuf.Timestamp
	(*v1.PaginationRequest)(nil),     // 31: forgepoint.common.v1.PaginationRequest
	(*v1.PaginationResponse)(nil),    // 32: forgepoint.common.v1.PaginationResponse
}
var file_forgepoint_pipeline_v1_pipeline_proto_depIdxs = []int32{
	1,  // 0: forgepoint.pipeline.v1.StepDefinition.type:type_name -> forgepoint.pipeline.v1.StepType
	28, // 1: forgepoint.pipeline.v1.StepDefinition.config:type_name -> google.protobuf.Struct
	29, // 2: forgepoint.pipeline.v1.StepDefinition.timeout:type_name -> google.protobuf.Duration
	0,  // 3: forgepoint.pipeline.v1.PipelineDefinition.type:type_name -> forgepoint.pipeline.v1.PipelineType
	4,  // 4: forgepoint.pipeline.v1.PipelineDefinition.steps:type_name -> forgepoint.pipeline.v1.StepDefinition
	30, // 5: forgepoint.pipeline.v1.PipelineDefinition.created_at:type_name -> google.protobuf.Timestamp
	3,  // 6: forgepoint.pipeline.v1.StepExecution.status:type_name -> forgepoint.pipeline.v1.StepStatus
	30, // 7: forgepoint.pipeline.v1.StepExecution.started_at:type_name -> google.protobuf.Timestamp
	30, // 8: forgepoint.pipeline.v1.StepExecution.completed_at:type_name -> google.protobuf.Timestamp
	28, // 9: forgepoint.pipeline.v1.StepExecution.output:type_name -> google.protobuf.Struct
	2,  // 10: forgepoint.pipeline.v1.Execution.status:type_name -> forgepoint.pipeline.v1.ExecutionStatus
	6,  // 11: forgepoint.pipeline.v1.Execution.step_executions:type_name -> forgepoint.pipeline.v1.StepExecution
	30, // 12: forgepoint.pipeline.v1.Execution.started_at:type_name -> google.protobuf.Timestamp
	30, // 13: forgepoint.pipeline.v1.Execution.completed_at:type_name -> google.protobuf.Timestamp
	28, // 14: forgepoint.pipeline.v1.Execution.input:type_name -> google.protobuf.Struct
	0,  // 15: forgepoint.pipeline.v1.CreatePipelineRequest.type:type_name -> forgepoint.pipeline.v1.PipelineType
	4,  // 16: forgepoint.pipeline.v1.CreatePipelineRequest.steps:type_name -> forgepoint.pipeline.v1.StepDefinition
	5,  // 17: forgepoint.pipeline.v1.CreatePipelineResponse.pipeline:type_name -> forgepoint.pipeline.v1.PipelineDefinition
	28, // 18: forgepoint.pipeline.v1.TriggerExecutionRequest.input:type_name -> google.protobuf.Struct
	7,  // 19: forgepoint.pipeline.v1.TriggerExecutionResponse.execution:type_name -> forgepoint.pipeline.v1.Execution
	7,  // 20: forgepoint.pipeline.v1.GetExecutionResponse.execution:type_name -> forgepoint.pipeline.v1.Execution
	7,  // 21: forgepoint.pipeline.v1.WatchExecutionResponse.execution:type_name -> forgepoint.pipeline.v1.Execution
	6,  // 22: forgepoint.pipeline.v1.WatchExecutionResponse.changed_step:type_name -> forgepoint.pipeline.v1.StepExecution
	30, // 23: forgepoint.pipeline.v1.WatchExecutionResponse.emitted_at:type_name -> google.protobuf.Timestamp
	7,  // 24: forgepoint.pipeline.v1.CancelExecutionResponse.execution:type_name -> forgepoint.pipeline.v1.Execution
	2,  // 25: forgepoint.pipeline.v1.ListExecutionsRequest.status_filter:type_name -> forgepoint.pipeline.v1.ExecutionStatus
	31, // 26: forgepoint.pipeline.v1.ListExecutionsRequest.pagination:type_name -> forgepoint.common.v1.PaginationRequest
	7,  // 27: forgepoint.pipeline.v1.ListExecutionsResponse.executions:type_name -> forgepoint.pipeline.v1.Execution
	32, // 28: forgepoint.pipeline.v1.ListExecutionsResponse.pagination:type_name -> forgepoint.common.v1.PaginationResponse
	0,  // 29: forgepoint.pipeline.v1.ListPipelinesRequest.type_filter:type_name -> forgepoint.pipeline.v1.PipelineType
	31, // 30: forgepoint.pipeline.v1.ListPipelinesRequest.pagination:type_name -> forgepoint.common.v1.PaginationRequest
	5,  // 31: forgepoint.pipeline.v1.ListPipelinesResponse.pipelines:type_name -> forgepoint.pipeline.v1.PipelineDefinition
	32, // 32: forgepoint.pipeline.v1.ListPipelinesResponse.pagination:type_name -> forgepoint.common.v1.PaginationResponse
	5,  // 33: forgepoint.pipeline.v1.GetPipelineResponse.pipeline:type_name -> forgepoint.pipeline.v1.PipelineDefinition
	4,  // 34: forgepoint.pipeline.v1.UpdatePipelineRequest.steps:type_name -> forgepoint.pipeline.v1.StepDefinition
	5,  // 35: forgepoint.pipeline.v1.UpdatePipelineResponse.pipeline:type_name -> forgepoint.pipeline.v1.PipelineDefinition
	8,  // 36: forgepoint.pipeline.v1.PipelineOrchestratorService.CreatePipeline:input_type -> forgepoint.pipeline.v1.CreatePipelineRequest
	20, // 37: forgepoint.pipeline.v1.PipelineOrchestratorService.ListPipelines:input_type -> forgepoint.pipeline.v1.ListPipelinesRequest
	22, // 38: forgepoint.pipeline.v1.PipelineOrchestratorService.GetPipeline:input_type -> forgepoint.pipeline.v1.GetPipelineRequest
	24, // 39: forgepoint.pipeline.v1.PipelineOrchestratorService.UpdatePipeline:input_type -> forgepoint.pipeline.v1.UpdatePipelineRequest
	26, // 40: forgepoint.pipeline.v1.PipelineOrchestratorService.DeletePipeline:input_type -> forgepoint.pipeline.v1.DeletePipelineRequest
	10, // 41: forgepoint.pipeline.v1.PipelineOrchestratorService.TriggerExecution:input_type -> forgepoint.pipeline.v1.TriggerExecutionRequest
	12, // 42: forgepoint.pipeline.v1.PipelineOrchestratorService.GetExecution:input_type -> forgepoint.pipeline.v1.GetExecutionRequest
	14, // 43: forgepoint.pipeline.v1.PipelineOrchestratorService.WatchExecution:input_type -> forgepoint.pipeline.v1.WatchExecutionRequest
	16, // 44: forgepoint.pipeline.v1.PipelineOrchestratorService.CancelExecution:input_type -> forgepoint.pipeline.v1.CancelExecutionRequest
	18, // 45: forgepoint.pipeline.v1.PipelineOrchestratorService.ListExecutions:input_type -> forgepoint.pipeline.v1.ListExecutionsRequest
	9,  // 46: forgepoint.pipeline.v1.PipelineOrchestratorService.CreatePipeline:output_type -> forgepoint.pipeline.v1.CreatePipelineResponse
	21, // 47: forgepoint.pipeline.v1.PipelineOrchestratorService.ListPipelines:output_type -> forgepoint.pipeline.v1.ListPipelinesResponse
	23, // 48: forgepoint.pipeline.v1.PipelineOrchestratorService.GetPipeline:output_type -> forgepoint.pipeline.v1.GetPipelineResponse
	25, // 49: forgepoint.pipeline.v1.PipelineOrchestratorService.UpdatePipeline:output_type -> forgepoint.pipeline.v1.UpdatePipelineResponse
	27, // 50: forgepoint.pipeline.v1.PipelineOrchestratorService.DeletePipeline:output_type -> forgepoint.pipeline.v1.DeletePipelineResponse
	11, // 51: forgepoint.pipeline.v1.PipelineOrchestratorService.TriggerExecution:output_type -> forgepoint.pipeline.v1.TriggerExecutionResponse
	13, // 52: forgepoint.pipeline.v1.PipelineOrchestratorService.GetExecution:output_type -> forgepoint.pipeline.v1.GetExecutionResponse
	15, // 53: forgepoint.pipeline.v1.PipelineOrchestratorService.WatchExecution:output_type -> forgepoint.pipeline.v1.WatchExecutionResponse
	17, // 54: forgepoint.pipeline.v1.PipelineOrchestratorService.CancelExecution:output_type -> forgepoint.pipeline.v1.CancelExecutionResponse
	19, // 55: forgepoint.pipeline.v1.PipelineOrchestratorService.ListExecutions:output_type -> forgepoint.pipeline.v1.ListExecutionsResponse
	46, // [46:56] is the sub-list for method output_type
	36, // [36:46] is the sub-list for method input_type
	36, // [36:36] is the sub-list for extension type_name
	36, // [36:36] is the sub-list for extension extendee
	0,  // [0:36] is the sub-list for field type_name
}

func init() { file_forgepoint_pipeline_v1_pipeline_proto_init() }
func file_forgepoint_pipeline_v1_pipeline_proto_init() {
	if File_forgepoint_pipeline_v1_pipeline_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_forgepoint_pipeline_v1_pipeline_proto_rawDesc), len(file_forgepoint_pipeline_v1_pipeline_proto_rawDesc)),
			NumEnums:      4,
			NumMessages:   24,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_forgepoint_pipeline_v1_pipeline_proto_goTypes,
		DependencyIndexes: file_forgepoint_pipeline_v1_pipeline_proto_depIdxs,
		EnumInfos:         file_forgepoint_pipeline_v1_pipeline_proto_enumTypes,
		MessageInfos:      file_forgepoint_pipeline_v1_pipeline_proto_msgTypes,
	}.Build()
	File_forgepoint_pipeline_v1_pipeline_proto = out.File
	file_forgepoint_pipeline_v1_pipeline_proto_goTypes = nil
	file_forgepoint_pipeline_v1_pipeline_proto_depIdxs = nil
}
