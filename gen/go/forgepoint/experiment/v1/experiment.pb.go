// ============================================================================
// Forgepoint Experiment Tracker Service Proto Definitions
// ============================================================================
//
// WHY: ML is empirical. You don't "write" a good model — you run dozens of
// experiments (different hyperparameters, features, architectures) and keep the
// one whose metrics win. The Experiment Tracker is the system of record for
// that loop: it stores every run's PARAMETERS (the knobs you set) and METRICS
// (the time-series numbers the run produced — loss per step, accuracy per
// epoch), and lets you COMPARE runs to pick a winner. This is Forgepoint's
// MLflow / Weights & Biases.
//
// WHAT'S HERE:
//   - Domain messages: Experiment, Run, Param, MetricPoint, MetricSeries
//   - RPCs: CreateExperiment/ListExperiments/GetExperiment/ArchiveExperiment,
//           StartRun/UpdateRunStatus/GetRun/ListRuns/DeleteRun,
//           LogMetrics (BATCH), LogParams, GetMetricHistory, CompareRuns,
//           SetRunArtifacts (free-form attachments)
//
// WHAT'S NOT HERE (deliberately): the event PAYLOAD messages this service
// publishes/consumes. They are NOT redefined locally — they live in the SINGLE
// canonical contract forgepoint/events/v1/events.proto and are imported below.
// This service PUBLISHES events.RunCreated (fp.experiments.run.created) and
// events.RunFinished (fp.experiments.run.finished); it CONSUMES a wide slice of
// the platform firehose (see the async-consumer note at the service definition).
// Defining event payloads here once led to producer/consumer schema drift across
// services (e.g. FeaturesWritten vs FeaturesIngested) — the events package fixes
// that by being the one place both ends of every pipe agree on.
//
// ============================================================================
// PATTERN — Event-Driven (Async Batch Ingestion)
// ============================================================================
//
// This service's defining trait is HOW metrics get in, not just the gRPC API.
// Two ingestion paths exist and the asymmetry is the whole point:
//
//   1) SYNC (this proto, the LogMetrics RPC): a training job's SDK calls
//      LogMetrics directly to push the loss/accuracy curve it's producing.
//      Used while a human or a training pod is actively driving a run.
//
//   2) ASYNC (NOT in this proto — it's a NATS consumer): the tracker also
//      SUBSCRIBES to high-volume platform events (the canonical subjects, see
//      forgepoint/events/v1/events.proto) — fp.inference.completed,
//      fp.models.version.created, fp.pipelines.step.completed,
//      fp.pipelines.completed, fp.features.written, fp.billing.usage.recorded,
//      fp.models.drift.detected, fp.pipelines.model.deployed,
//      fp.notifications.delivered/failed — buffers them in memory, and FLUSHES
//      to Postgres in batches (every N ms or every M events, whichever first).
//      If the DB falls behind, the buffer fills and the consumer NAKs / stops
//      acking, which makes NATS JetStream slow delivery: that is BACK-PRESSURE.
//
//      CRITICAL SUBJECT FIXES (event-contract alignment): the async consumer
//      subscribes to fp.features.WRITTEN (NOT the old fp.features.ingested — that
//      subject has no producer and would never deliver) and to the PLURAL
//      fp.pipelines.step.completed (NOT fp.pipeline.step.completed). These were
//      reconciled against events.proto so every subject we subscribe to actually
//      has a matching producer.
//
// WHY ASYNC FOR THE HOT PATH:
//   Inference happens thousands of times per second. If the inference gateway
//   had to make a synchronous "record this metric" RPC to the tracker on every
//   prediction, the tracker's latency and availability would be IN the serving
//   hot loop — a slow tracker would slow every prediction. By moving that to
//   fire-and-forget NATS events, serving never blocks on tracking. The tracker
//   absorbs spikes in its buffer and writes efficiently in batches (one
//   multi-row INSERT beats thousands of single-row INSERTs).
//
// WHY KEEP A SYNC LogMetrics RPC AT ALL THEN:
//   A training job legitimately wants its curve recorded promptly and wants an
//   immediate ack ("did my metric land?"). For that first-party, lower-volume
//   producer, a direct RPC is simpler than forcing it onto the event bus. The
//   RPC is still BATCH (repeated MetricPoint) so a job can flush 500 points in
//   one call — the same batching idea, just client-initiated.
//
// REAL-WORLD COMPARISON:
//   - MLflow: tracking server with log_metric/log_param; batches via log_batch.
//   - Weights & Biases: agent buffers locally and ships batches to the backend.
//   - Kafka Connect / ClickHouse async inserts: same batch-on-ingest idea for
//     high-cardinality time-series.
//
// DESIGN NOTES:
//   - "How do you handle metric volume?" → batch consumer + back-pressure via
//     NAK, time-partitioned Postgres table for the metrics.
//   - "Why is LogMetrics a unary batch RPC and not a client-streaming RPC?"
//     → see the LogMetrics comment; the short answer is idempotent retry +
//     a single transactional flush beat a long-lived stream for this shape.
//   - "How do runs connect to the rest of the platform?" → a Run carries a
//     model_version_id linking the experiment that produced a model to the
//     model artifact in the Registry, WITHOUT importing the registry proto.
//
// VERSIONING: Package path includes v1 (Buf/Google convention). Breaking
// changes require a new forgepoint.experiment.v2 package.
// ============================================================================

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.11
// 	protoc        (unknown)
// source: forgepoint/experiment/v1/experiment.proto

package experimentv1

import (
	v1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
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
// RunStatus
// ============================================================================
//
// WHY a finite-state enum (not a free-form string): a run has a small, known
// lifecycle, and downstream UI/queries switch on it ("show me failed runs").
// An enum makes the states self-documenting, prevents typos, and is the Buf-
// blessed way to model a closed set.
//
// LIFECYCLE (state machine):
//
//	RUNNING ──► FINISHED   (job completed; metrics are final)
//	   │
//	   ├──────► FAILED     (job crashed / errored)
//	   │
//	   └──────► KILLED     (cancelled by a user or orchestrator)
//
// RUNNING is the only non-terminal state. Status is SERVER-AUTHORITATIVE — a
// run is created in RUNNING by StartRun, and only the server (via
// UpdateRunStatus, or async on a PipelineFailed event) moves it to a terminal
// state. Clients never set status on creation (mass-assignment guard).
// ============================================================================
type RunStatus int32

const (
	// Required zero value (Buf ENUM_ZERO_VALUE_SUFFIX). Means "not set" — a run
	// is never legitimately in this state; treat it as a serialization bug.
	RunStatus_RUN_STATUS_UNSPECIFIED RunStatus = 0
	// The run is in progress. New MetricPoints are still expected.
	RunStatus_RUN_STATUS_RUNNING RunStatus = 1
	// The run completed successfully. Its metrics are final and comparable.
	RunStatus_RUN_STATUS_FINISHED RunStatus = 2
	// The run errored out (training crashed, OOM, bad data). Metrics partial.
	RunStatus_RUN_STATUS_FAILED RunStatus = 3
	// The run was cancelled by a user or the pipeline orchestrator.
	RunStatus_RUN_STATUS_KILLED RunStatus = 4
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
	return file_forgepoint_experiment_v1_experiment_proto_enumTypes[0].Descriptor()
}

func (RunStatus) Type() protoreflect.EnumType {
	return &file_forgepoint_experiment_v1_experiment_proto_enumTypes[0]
}

func (x RunStatus) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use RunStatus.Descriptor instead.
func (RunStatus) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{0}
}

// ============================================================================
// RunSource
// ============================================================================
//
// WHY: a run can be created by a human-driven training job (SYNC path,
// StartRun + LogMetrics) OR materialized from the async event stream (e.g.,
// the tracker observes fp.models.version.created and opens a run to attach
// production metrics to). Recording the source makes the two ingestion paths
// visible in the data — invaluable when debugging "why does this run have no
// params?" (answer: it came from an event, not a training job).
// ============================================================================
type RunSource int32

const (
	// Required zero value.
	RunSource_RUN_SOURCE_UNSPECIFIED RunSource = 0
	// Created via the sync gRPC API (StartRun) — a first-party training job/SDK.
	RunSource_RUN_SOURCE_API RunSource = 1
	// Materialized by the async NATS batch consumer from a platform event.
	RunSource_RUN_SOURCE_EVENT RunSource = 2
)

// Enum value maps for RunSource.
var (
	RunSource_name = map[int32]string{
		0: "RUN_SOURCE_UNSPECIFIED",
		1: "RUN_SOURCE_API",
		2: "RUN_SOURCE_EVENT",
	}
	RunSource_value = map[string]int32{
		"RUN_SOURCE_UNSPECIFIED": 0,
		"RUN_SOURCE_API":         1,
		"RUN_SOURCE_EVENT":       2,
	}
)

func (x RunSource) Enum() *RunSource {
	p := new(RunSource)
	*p = x
	return p
}

func (x RunSource) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (RunSource) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_experiment_v1_experiment_proto_enumTypes[1].Descriptor()
}

func (RunSource) Type() protoreflect.EnumType {
	return &file_forgepoint_experiment_v1_experiment_proto_enumTypes[1]
}

func (x RunSource) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use RunSource.Descriptor instead.
func (RunSource) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{1}
}

// ============================================================================
// Experiment
// ============================================================================
//
// WHY: an Experiment is a NAMED GROUPING of runs that share a goal — e.g.,
// "fraud-detector-v3 hyperparameter sweep". You compare runs WITHIN an
// experiment. This is the MLflow "experiment → runs" hierarchy exactly.
//
// OWNERSHIP / SECURITY: owner_id and team are SERVER-AUTHORITATIVE. They are
// set from the authenticated caller's TokenClaims (injected by the auth
// interceptor), never accepted on the create request. This prevents a caller
// from creating an experiment "owned" by someone else (mass-assignment).
// ============================================================================
type Experiment struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// UUID v4. Primary key, immutable. Server-generated.
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// Unique-per-team human name, e.g., "fraud-detector-v3-sweep".
	Name string `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	// Optional free-text description of what this experiment is testing.
	Description string `protobuf:"bytes,3,opt,name=description,proto3" json:"description,omitempty"`
	// Arbitrary key/value labels for filtering/organization
	// (e.g., {"project": "fraud", "owner_team": "risk"}). NOT for metrics.
	Tags map[string]string `protobuf:"bytes,4,rep,name=tags,proto3" json:"tags,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	// The user who created this experiment. SERVER-set from auth context.
	OwnerId string `protobuf:"bytes,5,opt,name=owner_id,json=ownerId,proto3" json:"owner_id,omitempty"`
	// The owning team (namespacing + access). SERVER-set from auth context.
	Team string `protobuf:"bytes,6,opt,name=team,proto3" json:"team,omitempty"`
	// When the experiment was created. SERVER-set, immutable.
	CreatedAt *timestamppb.Timestamp `protobuf:"bytes,7,opt,name=created_at,json=createdAt,proto3" json:"created_at,omitempty"`
	// When the experiment was last mutated (name/description/tags edit, or
	// archive). SERVER-set on every write. Lets clients cache/ETag and lets the
	// UI sort by recency.
	UpdatedAt *timestamppb.Timestamp `protobuf:"bytes,8,opt,name=updated_at,json=updatedAt,proto3" json:"updated_at,omitempty"`
	// SERVER-set archive timestamp. Unset = active. We SOFT-DELETE experiments
	// (set this, hide from default lists) rather than hard-delete: runs/metrics
	// are an audit trail and may be referenced by model lineage in the Registry.
	// ArchiveExperiment sets this; it is never client-supplied.
	ArchivedAt    *timestamppb.Timestamp `protobuf:"bytes,9,opt,name=archived_at,json=archivedAt,proto3" json:"archived_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Experiment) Reset() {
	*x = Experiment{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Experiment) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Experiment) ProtoMessage() {}

func (x *Experiment) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Experiment.ProtoReflect.Descriptor instead.
func (*Experiment) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{0}
}

func (x *Experiment) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Experiment) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Experiment) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *Experiment) GetTags() map[string]string {
	if x != nil {
		return x.Tags
	}
	return nil
}

func (x *Experiment) GetOwnerId() string {
	if x != nil {
		return x.OwnerId
	}
	return ""
}

func (x *Experiment) GetTeam() string {
	if x != nil {
		return x.Team
	}
	return ""
}

func (x *Experiment) GetCreatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CreatedAt
	}
	return nil
}

func (x *Experiment) GetUpdatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.UpdatedAt
	}
	return nil
}

func (x *Experiment) GetArchivedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.ArchivedAt
	}
	return nil
}

// ============================================================================
// Run
// ============================================================================
//
// WHY: a Run is ONE execution within an experiment — one training job, one set
// of hyperparameters, one resulting metric curve. It is the unit you compare.
//
// LINK TO MODEL VERSIONS (cross-service, decoupled):
//
//	model_version_id ties the run that PRODUCED a model to that model's
//	artifact in the Model Registry. We deliberately store it as an opaque
//	string ID, NOT by importing forgepoint.registry.v1 — services are
//	decoupled; the contract between them is the ID value plus async events, not
//	a compile-time proto dependency. This keeps the experiment tracker
//	buildable and deployable without the registry's proto, and lets either
//	evolve independently. (Same reasoning the design doc uses for "database
//	per service": no cross-service hard coupling.)
//
// WHY summary metrics live on the Run:
//
//	The full metric history is a time-series (potentially millions of points)
//	stored in a partitioned table and fetched on demand. But ListRuns and the
//	comparison UI need the HEADLINE number per run ("final accuracy = 0.94")
//	without reading the whole series. final_metrics is a denormalized snapshot
//	of the last/best value per metric key, written when the run finishes — a
//	read-optimization, the CQRS-flavored "projection" of the series.
//
// SECURITY: id, status, source, owner_id, started_at, ended_at, and
// final_metrics are all SERVER-AUTHORITATIVE — none are accepted from a client
// on StartRun/LogMetrics. A client only supplies params and metric points.
// ============================================================================
type Run struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// UUID v4. Primary key, immutable. Server-generated.
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// The experiment this run belongs to. Set at StartRun; immutable.
	ExperimentId string `protobuf:"bytes,2,opt,name=experiment_id,json=experimentId,proto3" json:"experiment_id,omitempty"`
	// Optional human label for the run, e.g., "lr=0.01 batch=64".
	// If empty, the UI falls back to a short form of the id.
	DisplayName string `protobuf:"bytes,3,opt,name=display_name,json=displayName,proto3" json:"display_name,omitempty"`
	// Current lifecycle state. SERVER-AUTHORITATIVE (see RunStatus).
	Status RunStatus `protobuf:"varint,4,opt,name=status,proto3,enum=forgepoint.experiment.v1.RunStatus" json:"status,omitempty"`
	// Whether this run came from the sync API or the async event stream.
	// SERVER-set based on which ingestion path created it.
	Source RunSource `protobuf:"varint,5,opt,name=source,proto3,enum=forgepoint.experiment.v1.RunSource" json:"source,omitempty"`
	// The model version this run produced (or evaluated), if any. Opaque ID into
	// the Model Registry. Empty for runs that don't yield a registered model.
	// See the cross-service note above on why this is a bare string.
	ModelVersionId string `protobuf:"bytes,6,opt,name=model_version_id,json=modelVersionId,proto3" json:"model_version_id,omitempty"`
	// The user who started this run. SERVER-set from auth context.
	OwnerId string `protobuf:"bytes,7,opt,name=owner_id,json=ownerId,proto3" json:"owner_id,omitempty"`
	// Hyperparameters / config for this run (the knobs). Typically set once at
	// StartRun and appended to via LogParams. Returned on GetRun.
	Params []*Param `protobuf:"bytes,8,rep,name=params,proto3" json:"params,omitempty"`
	// Denormalized headline metrics (final/best value per key), for cheap list
	// and compare reads. SERVER-computed from the metric series; NOT the full
	// history. Use CompareRuns / a metric-history read for the time-series.
	FinalMetrics []*MetricPoint `protobuf:"bytes,9,rep,name=final_metrics,json=finalMetrics,proto3" json:"final_metrics,omitempty"`
	// When the run started. SERVER-set at StartRun, immutable.
	StartedAt *timestamppb.Timestamp `protobuf:"bytes,10,opt,name=started_at,json=startedAt,proto3" json:"started_at,omitempty"`
	// When the run reached a terminal state. SERVER-set; unset while RUNNING.
	EndedAt *timestamppb.Timestamp `protobuf:"bytes,11,opt,name=ended_at,json=endedAt,proto3" json:"ended_at,omitempty"`
	// Free-form, JSON-shaped side-artifacts (confusion matrix, feature-importance,
	// artifact manifest, eval report). Attached via SetRunArtifacts; size-capped
	// server-side. Struct (not bytes) so it stays inspectable/renderable. Unset
	// for runs that never attach any. NOT a metrics channel — metrics are typed.
	Artifacts     *structpb.Struct `protobuf:"bytes,12,opt,name=artifacts,proto3" json:"artifacts,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Run) Reset() {
	*x = Run{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Run) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Run) ProtoMessage() {}

func (x *Run) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Run.ProtoReflect.Descriptor instead.
func (*Run) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{1}
}

func (x *Run) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Run) GetExperimentId() string {
	if x != nil {
		return x.ExperimentId
	}
	return ""
}

func (x *Run) GetDisplayName() string {
	if x != nil {
		return x.DisplayName
	}
	return ""
}

func (x *Run) GetStatus() RunStatus {
	if x != nil {
		return x.Status
	}
	return RunStatus_RUN_STATUS_UNSPECIFIED
}

func (x *Run) GetSource() RunSource {
	if x != nil {
		return x.Source
	}
	return RunSource_RUN_SOURCE_UNSPECIFIED
}

func (x *Run) GetModelVersionId() string {
	if x != nil {
		return x.ModelVersionId
	}
	return ""
}

func (x *Run) GetOwnerId() string {
	if x != nil {
		return x.OwnerId
	}
	return ""
}

func (x *Run) GetParams() []*Param {
	if x != nil {
		return x.Params
	}
	return nil
}

func (x *Run) GetFinalMetrics() []*MetricPoint {
	if x != nil {
		return x.FinalMetrics
	}
	return nil
}

func (x *Run) GetStartedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.StartedAt
	}
	return nil
}

func (x *Run) GetEndedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.EndedAt
	}
	return nil
}

func (x *Run) GetArtifacts() *structpb.Struct {
	if x != nil {
		return x.Artifacts
	}
	return nil
}

// ============================================================================
// Param
// ============================================================================
//
// WHY: a parameter is an INPUT to a run — a hyperparameter or config value set
// BEFORE/at run start (learning_rate=0.01, optimizer="adam", batch_size=64).
// Contrast with a metric, which is an OUTPUT measured DURING the run.
//
// WHY value is a string (not a oneof of typed scalars):
//
//	Params are heterogeneous (ints, floats, bools, enums, strings) and are
//	used for display, grouping, and equality comparison — never for math. A
//	single string column keeps the schema trivial and the API stable as new
//	param types appear. MLflow makes the same call (params are strings). If we
//	later need typed numeric params for range filtering, we add a typed field;
//	the string stays as the canonical display form.
//
// IMMUTABILITY: a param is write-once per key within a run. Re-logging the same
// key is rejected (or treated as idempotent if value matches) so a run's
// configuration can't silently change mid-flight.
// ============================================================================
type Param struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Parameter name, e.g., "learning_rate", "optimizer". Unique within a run.
	Key string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	// Stringified parameter value, e.g., "0.01", "adam", "true".
	Value         string `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Param) Reset() {
	*x = Param{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Param) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Param) ProtoMessage() {}

func (x *Param) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Param.ProtoReflect.Descriptor instead.
func (*Param) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{2}
}

func (x *Param) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *Param) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

// ============================================================================
// MetricPoint
// ============================================================================
//
// WHY this exact shape — (key, value, step, timestamp): a metric in ML is a
// TIME-SERIES, not a single number. "loss" isn't one value; it's a curve over
// training steps. Each point pins:
//   - key:   which metric ("loss", "accuracy", "val_auc")
//   - value: the measured number at this point
//   - step:  the monotonic training step/epoch/batch index (the X axis)
//   - timestamp: wall-clock time (server-set), for time-range queries
//
// WHY BOTH step AND timestamp:
//
//	step is the SEMANTIC X axis the user reasons about ("loss at epoch 10").
//	timestamp is the PHYSICAL axis for retention/partitioning and for
//	"accuracy over the last 30 days". The Postgres metrics table is RANGE-
//	partitioned by timestamp (see Phase 7.3) so old partitions can be dropped
//	cheaply — that's why timestamp is server-authoritative, not client-set.
//
// WHY value is double: ML metrics are real numbers. double is the natural fit
// and matches the DOUBLE PRECISION column in the storage schema.
// ============================================================================
type MetricPoint struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Metric name, e.g., "loss", "accuracy". Part of the time-series key.
	Key string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	// The measured value at this step.
	Value float64 `protobuf:"fixed64,2,opt,name=value,proto3" json:"value,omitempty"`
	// Monotonic step index (epoch/batch/iteration). The X axis of the curve.
	// Client-supplied: the training job knows its own step counter.
	Step int64 `protobuf:"varint,3,opt,name=step,proto3" json:"step,omitempty"`
	// Wall-clock time of measurement. SERVER-set on ingest so the partition key
	// and retention are under platform control (a client can't backdate metrics
	// into an old partition or the future). Echoed back on reads.
	Timestamp     *timestamppb.Timestamp `protobuf:"bytes,4,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *MetricPoint) Reset() {
	*x = MetricPoint{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *MetricPoint) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MetricPoint) ProtoMessage() {}

func (x *MetricPoint) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[3]
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
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{3}
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

// ============================================================================
// MetricSeries
// ============================================================================
//
// WHY: the columnar/grouped view of metrics for ONE key on ONE run — the shape
// a chart wants ("draw the loss curve"). CompareRuns returns these so a client
// can overlay the same metric across runs without re-grouping flat points.
// ============================================================================
type MetricSeries struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The metric name these points share, e.g., "accuracy".
	Key string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	// The ordered points (by step) for this metric on a single run.
	Points        []*MetricPoint `protobuf:"bytes,2,rep,name=points,proto3" json:"points,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *MetricSeries) Reset() {
	*x = MetricSeries{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *MetricSeries) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MetricSeries) ProtoMessage() {}

func (x *MetricSeries) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MetricSeries.ProtoReflect.Descriptor instead.
func (*MetricSeries) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{4}
}

func (x *MetricSeries) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *MetricSeries) GetPoints() []*MetricPoint {
	if x != nil {
		return x.Points
	}
	return nil
}

// CreateExperimentRequest carries ONLY client-owned fields. Note what is
// ABSENT: id, owner_id, team, created_at — all SERVER-authoritative (id is
// generated; owner_id/team come from the authenticated caller's claims;
// created_at is set on write). This is the mass-assignment guard.
type CreateExperimentRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Unique-per-team experiment name. Required.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// Optional description of the experiment's goal.
	Description string `protobuf:"bytes,2,opt,name=description,proto3" json:"description,omitempty"`
	// Optional organizational tags (key/value). Not metrics.
	Tags          map[string]string `protobuf:"bytes,3,rep,name=tags,proto3" json:"tags,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *CreateExperimentRequest) Reset() {
	*x = CreateExperimentRequest{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CreateExperimentRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateExperimentRequest) ProtoMessage() {}

func (x *CreateExperimentRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateExperimentRequest.ProtoReflect.Descriptor instead.
func (*CreateExperimentRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{5}
}

func (x *CreateExperimentRequest) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *CreateExperimentRequest) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *CreateExperimentRequest) GetTags() map[string]string {
	if x != nil {
		return x.Tags
	}
	return nil
}

// CreateExperimentResponse wraps the created Experiment.
// WHY wrap (not return Experiment directly): Buf RPC_RESPONSE_STANDARD_NAME
// requires "<Rpc>Response" naming, and wrapping lets us add fields later
// (e.g., a created-by-import flag) without mutating the shared Experiment type.
type CreateExperimentResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The newly created experiment, with server-set id/owner/team/created_at.
	Experiment    *Experiment `protobuf:"bytes,1,opt,name=experiment,proto3" json:"experiment,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *CreateExperimentResponse) Reset() {
	*x = CreateExperimentResponse{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CreateExperimentResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateExperimentResponse) ProtoMessage() {}

func (x *CreateExperimentResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateExperimentResponse.ProtoReflect.Descriptor instead.
func (*CreateExperimentResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{6}
}

func (x *CreateExperimentResponse) GetExperiment() *Experiment {
	if x != nil {
		return x.Experiment
	}
	return nil
}

type GetExperimentRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// UUID of the experiment to fetch.
	Id            string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetExperimentRequest) Reset() {
	*x = GetExperimentRequest{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetExperimentRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetExperimentRequest) ProtoMessage() {}

func (x *GetExperimentRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetExperimentRequest.ProtoReflect.Descriptor instead.
func (*GetExperimentRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{7}
}

func (x *GetExperimentRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

type GetExperimentResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The requested experiment. NOT_FOUND if it doesn't exist or the caller's
	// team has no access.
	Experiment    *Experiment `protobuf:"bytes,1,opt,name=experiment,proto3" json:"experiment,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetExperimentResponse) Reset() {
	*x = GetExperimentResponse{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[8]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetExperimentResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetExperimentResponse) ProtoMessage() {}

func (x *GetExperimentResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[8]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetExperimentResponse.ProtoReflect.Descriptor instead.
func (*GetExperimentResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{8}
}

func (x *GetExperimentResponse) GetExperiment() *Experiment {
	if x != nil {
		return x.Experiment
	}
	return nil
}

// ListExperimentsRequest reuses the common PaginationRequest for the same
// cursor-based shape every Forgepoint list RPC uses (see common.proto for the
// cursor-vs-offset rationale). Page size is capped SERVER-SIDE (default 20,
// max 100) to prevent a client from requesting an unbounded page.
type ListExperimentsRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Optional NARROWING filter only — NOT an authorization scope. The server
	// ALWAYS constrains results to the team(s) the caller's auth claims permit;
	// team_filter can only narrow WITHIN that permitted set, never widen it. WHY
	// the field still exists: an admin whose claims span several teams may want to
	// view one team's experiments. A client cannot use this to read another
	// team's data — the auth interceptor's claim, not this field, is the security
	// boundary. (Mass-assignment/IDOR guard: tenancy derives from claims.)
	TeamFilter string `protobuf:"bytes,1,opt,name=team_filter,json=teamFilter,proto3" json:"team_filter,omitempty"`
	// Include soft-archived experiments. Default false (active only). WHY a flag,
	// not a separate RPC: archive is a visibility toggle, not a different query.
	IncludeArchived bool `protobuf:"varint,2,opt,name=include_archived,json=includeArchived,proto3" json:"include_archived,omitempty"`
	// Cursor-based pagination. page_size defaults to 20, max 100 (server-enforced).
	// See common.PaginationRequest — the cap is enforced server-side, not trusted
	// from the client (an unbounded page is a DoS lever).
	Pagination    *v1.PaginationRequest `protobuf:"bytes,3,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListExperimentsRequest) Reset() {
	*x = ListExperimentsRequest{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[9]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListExperimentsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListExperimentsRequest) ProtoMessage() {}

func (x *ListExperimentsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[9]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListExperimentsRequest.ProtoReflect.Descriptor instead.
func (*ListExperimentsRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{9}
}

func (x *ListExperimentsRequest) GetTeamFilter() string {
	if x != nil {
		return x.TeamFilter
	}
	return ""
}

func (x *ListExperimentsRequest) GetIncludeArchived() bool {
	if x != nil {
		return x.IncludeArchived
	}
	return false
}

func (x *ListExperimentsRequest) GetPagination() *v1.PaginationRequest {
	if x != nil {
		return x.Pagination
	}
	return nil
}

type ListExperimentsResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The page of experiments.
	Experiments []*Experiment `protobuf:"bytes,1,rep,name=experiments,proto3" json:"experiments,omitempty"`
	// Pagination metadata (next_page_token + total_count).
	Pagination    *v1.PaginationResponse `protobuf:"bytes,2,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListExperimentsResponse) Reset() {
	*x = ListExperimentsResponse{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[10]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListExperimentsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListExperimentsResponse) ProtoMessage() {}

func (x *ListExperimentsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[10]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListExperimentsResponse.ProtoReflect.Descriptor instead.
func (*ListExperimentsResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{10}
}

func (x *ListExperimentsResponse) GetExperiments() []*Experiment {
	if x != nil {
		return x.Experiments
	}
	return nil
}

func (x *ListExperimentsResponse) GetPagination() *v1.PaginationResponse {
	if x != nil {
		return x.Pagination
	}
	return nil
}

// UpdateExperimentRequest edits the MUTABLE, client-owned fields of an
// experiment (name/description/tags). Note what is ABSENT and therefore
// NOT editable by a client: id (the target, but immutable as data), owner_id,
// team, created_at, archived_at — all server-authoritative. You cannot
// "re-home" an experiment to another owner/team via this RPC (mass-assignment
// guard). WHY a field mask: a client editing only the description must not have
// to round-trip name/tags and risk clobbering a concurrent edit.
type UpdateExperimentRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The experiment to update. The id selects the row; it is never changed.
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// New name (applied only if "name" is in update_fields).
	Name string `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	// New description (applied only if "description" is in update_fields).
	Description string `protobuf:"bytes,3,opt,name=description,proto3" json:"description,omitempty"`
	// New tags — REPLACES the whole map (applied only if "tags" is in
	// update_fields). WHY replace-not-merge: merge semantics make it impossible to
	// DELETE a tag; a full replace is unambiguous and the client sends the desired
	// final set.
	Tags map[string]string `protobuf:"bytes,4,rep,name=tags,proto3" json:"tags,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	// Field mask of which of {name, description, tags} to apply. WHY a string
	// list rather than google.protobuf.FieldMask: keeps the import surface minimal
	// and the allowed set is tiny and validated server-side; an unknown field name
	// is rejected with INVALID_ARGUMENT.
	UpdateFields  []string `protobuf:"bytes,5,rep,name=update_fields,json=updateFields,proto3" json:"update_fields,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UpdateExperimentRequest) Reset() {
	*x = UpdateExperimentRequest{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[11]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpdateExperimentRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdateExperimentRequest) ProtoMessage() {}

func (x *UpdateExperimentRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[11]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdateExperimentRequest.ProtoReflect.Descriptor instead.
func (*UpdateExperimentRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{11}
}

func (x *UpdateExperimentRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *UpdateExperimentRequest) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *UpdateExperimentRequest) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *UpdateExperimentRequest) GetTags() map[string]string {
	if x != nil {
		return x.Tags
	}
	return nil
}

func (x *UpdateExperimentRequest) GetUpdateFields() []string {
	if x != nil {
		return x.UpdateFields
	}
	return nil
}

type UpdateExperimentResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The experiment after the edit (updated_at refreshed).
	Experiment    *Experiment `protobuf:"bytes,1,opt,name=experiment,proto3" json:"experiment,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UpdateExperimentResponse) Reset() {
	*x = UpdateExperimentResponse{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[12]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpdateExperimentResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdateExperimentResponse) ProtoMessage() {}

func (x *UpdateExperimentResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[12]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdateExperimentResponse.ProtoReflect.Descriptor instead.
func (*UpdateExperimentResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{12}
}

func (x *UpdateExperimentResponse) GetExperiment() *Experiment {
	if x != nil {
		return x.Experiment
	}
	return nil
}

// ArchiveExperimentRequest soft-deletes an experiment: it sets archived_at and
// hides it from default lists, but PRESERVES its runs and metrics. WHY soft (not
// hard) delete: an experiment's runs are an audit trail and may be referenced by
// model-lineage in the Registry (a run carries the model_version_id it produced).
// Hard-deleting would orphan those references and destroy provenance. This is the
// design's "Delete/Archive where the design calls for it" done safely.
type ArchiveExperimentRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The experiment to archive. Must be visible to the caller's team.
	Id            string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ArchiveExperimentRequest) Reset() {
	*x = ArchiveExperimentRequest{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[13]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ArchiveExperimentRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ArchiveExperimentRequest) ProtoMessage() {}

func (x *ArchiveExperimentRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[13]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ArchiveExperimentRequest.ProtoReflect.Descriptor instead.
func (*ArchiveExperimentRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{13}
}

func (x *ArchiveExperimentRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

type ArchiveExperimentResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The experiment after archiving (archived_at now set).
	Experiment    *Experiment `protobuf:"bytes,1,opt,name=experiment,proto3" json:"experiment,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ArchiveExperimentResponse) Reset() {
	*x = ArchiveExperimentResponse{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[14]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ArchiveExperimentResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ArchiveExperimentResponse) ProtoMessage() {}

func (x *ArchiveExperimentResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[14]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ArchiveExperimentResponse.ProtoReflect.Descriptor instead.
func (*ArchiveExperimentResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{14}
}

func (x *ArchiveExperimentResponse) GetExperiment() *Experiment {
	if x != nil {
		return x.Experiment
	}
	return nil
}

// StartRunRequest opens a new run inside an experiment.
// SERVER-AUTHORITATIVE and therefore ABSENT here: id, status (always created
// RUNNING), source (set to API), owner_id (from claims), started_at,
// final_metrics. The client only declares which experiment, an optional label,
// the model version it targets, and its initial params.
type StartRunRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The experiment this run belongs to. Required; must exist and be visible
	// to the caller's team.
	ExperimentId string `protobuf:"bytes,1,opt,name=experiment_id,json=experimentId,proto3" json:"experiment_id,omitempty"`
	// Optional human label, e.g., "lr=0.01 batch=64".
	DisplayName string `protobuf:"bytes,2,opt,name=display_name,json=displayName,proto3" json:"display_name,omitempty"`
	// Optional model version this run will produce/evaluate. Opaque Registry ID;
	// see the Run.model_version_id note on why it's a bare string.
	ModelVersionId string `protobuf:"bytes,3,opt,name=model_version_id,json=modelVersionId,proto3" json:"model_version_id,omitempty"`
	// Initial hyperparameters for the run. More can be added via LogParams.
	Params []*Param `protobuf:"bytes,4,rep,name=params,proto3" json:"params,omitempty"`
	// Idempotency key (client-generated UUID). WHY: StartRun is a mutation a
	// client may retry after a network blip; without a dedup key a retry would
	// create a DUPLICATE run. The server records (idempotency_key → run_id) and
	// returns the SAME run on replay. This is the standard idempotent-create
	// pattern (Stripe's Idempotency-Key header). Optional but strongly advised
	// for automated callers.
	IdempotencyKey string `protobuf:"bytes,5,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *StartRunRequest) Reset() {
	*x = StartRunRequest{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[15]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *StartRunRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StartRunRequest) ProtoMessage() {}

func (x *StartRunRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[15]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StartRunRequest.ProtoReflect.Descriptor instead.
func (*StartRunRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{15}
}

func (x *StartRunRequest) GetExperimentId() string {
	if x != nil {
		return x.ExperimentId
	}
	return ""
}

func (x *StartRunRequest) GetDisplayName() string {
	if x != nil {
		return x.DisplayName
	}
	return ""
}

func (x *StartRunRequest) GetModelVersionId() string {
	if x != nil {
		return x.ModelVersionId
	}
	return ""
}

func (x *StartRunRequest) GetParams() []*Param {
	if x != nil {
		return x.Params
	}
	return nil
}

func (x *StartRunRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

type StartRunResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The newly started run (status = RUNNING, server-set fields populated).
	Run           *Run `protobuf:"bytes,1,opt,name=run,proto3" json:"run,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *StartRunResponse) Reset() {
	*x = StartRunResponse{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[16]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *StartRunResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StartRunResponse) ProtoMessage() {}

func (x *StartRunResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[16]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StartRunResponse.ProtoReflect.Descriptor instead.
func (*StartRunResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{16}
}

func (x *StartRunResponse) GetRun() *Run {
	if x != nil {
		return x.Run
	}
	return nil
}

type GetRunRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// UUID of the run to fetch.
	Id            string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetRunRequest) Reset() {
	*x = GetRunRequest{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[17]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetRunRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetRunRequest) ProtoMessage() {}

func (x *GetRunRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[17]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetRunRequest.ProtoReflect.Descriptor instead.
func (*GetRunRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{17}
}

func (x *GetRunRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

// GetRunResponse returns run metadata + params + the denormalized headline
// metrics + any attached artifacts. It does NOT return the full metric
// time-series (that can be huge); use GetMetricHistory for one run's curve, or
// CompareRuns to overlay several runs.
type GetRunResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The requested run. NOT_FOUND if missing or not visible to the caller.
	Run           *Run `protobuf:"bytes,1,opt,name=run,proto3" json:"run,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetRunResponse) Reset() {
	*x = GetRunResponse{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[18]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetRunResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetRunResponse) ProtoMessage() {}

func (x *GetRunResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[18]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetRunResponse.ProtoReflect.Descriptor instead.
func (*GetRunResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{18}
}

func (x *GetRunResponse) GetRun() *Run {
	if x != nil {
		return x.Run
	}
	return nil
}

// ListRunsRequest lists runs, normally scoped to one experiment, with optional
// status filtering. Paginated with the shared cursor type (page size capped
// server-side: default 20, max 100).
type ListRunsRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Filter to runs in this experiment. Required in practice (you compare runs
	// within an experiment); empty lists across the caller's team.
	ExperimentId string `protobuf:"bytes,1,opt,name=experiment_id,json=experimentId,proto3" json:"experiment_id,omitempty"`
	// Optional status filter, e.g., only RUN_STATUS_FINISHED for comparison.
	// RUN_STATUS_UNSPECIFIED = no status filter (return all).
	StatusFilter RunStatus `protobuf:"varint,2,opt,name=status_filter,json=statusFilter,proto3,enum=forgepoint.experiment.v1.RunStatus" json:"status_filter,omitempty"`
	// Cursor-based pagination. page_size defaults to 20, max 100 (server-enforced).
	Pagination    *v1.PaginationRequest `protobuf:"bytes,3,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListRunsRequest) Reset() {
	*x = ListRunsRequest{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[19]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListRunsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListRunsRequest) ProtoMessage() {}

func (x *ListRunsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[19]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListRunsRequest.ProtoReflect.Descriptor instead.
func (*ListRunsRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{19}
}

func (x *ListRunsRequest) GetExperimentId() string {
	if x != nil {
		return x.ExperimentId
	}
	return ""
}

func (x *ListRunsRequest) GetStatusFilter() RunStatus {
	if x != nil {
		return x.StatusFilter
	}
	return RunStatus_RUN_STATUS_UNSPECIFIED
}

func (x *ListRunsRequest) GetPagination() *v1.PaginationRequest {
	if x != nil {
		return x.Pagination
	}
	return nil
}

type ListRunsResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The page of runs. Each carries final_metrics (headline numbers) so a list
	// view can render leaderboards without fetching full metric series.
	Runs []*Run `protobuf:"bytes,1,rep,name=runs,proto3" json:"runs,omitempty"`
	// Pagination metadata.
	Pagination    *v1.PaginationResponse `protobuf:"bytes,2,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListRunsResponse) Reset() {
	*x = ListRunsResponse{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[20]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListRunsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListRunsResponse) ProtoMessage() {}

func (x *ListRunsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[20]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListRunsResponse.ProtoReflect.Descriptor instead.
func (*ListRunsResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{20}
}

func (x *ListRunsResponse) GetRuns() []*Run {
	if x != nil {
		return x.Runs
	}
	return nil
}

func (x *ListRunsResponse) GetPagination() *v1.PaginationResponse {
	if x != nil {
		return x.Pagination
	}
	return nil
}

// DeleteRunRequest removes a single run and ITS metrics/params. WHY this is
// allowed to HARD-delete where ArchiveExperiment is not: a run is the unit of
// experimental noise — a mis-launched job, a smoke test, a duplicate — and the
// design's run leaderboard is unusable if garbage runs can't be pruned. SAFETY
// RAIL: the server REJECTS deleting a run whose model_version_id is set and is
// still referenced by a live (non-archived) model version in the Registry — that
// would sever lineage; such a run must be Archived at the experiment level
// instead. A run with no downstream model reference is safe to delete.
// Only the run's owner or a team admin may delete (enforced from auth claims).
type DeleteRunRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The run to delete.
	RunId string `protobuf:"bytes,1,opt,name=run_id,json=runId,proto3" json:"run_id,omitempty"`
	// Idempotency: a retried delete after a network blip must not error just
	// because the run is already gone. WHY a key (not "treat NOT_FOUND as success"):
	// a key distinguishes "my earlier delete succeeded" from "someone else's run id
	// typo" — the server returns OK on replay of the SAME key, NOT_FOUND otherwise.
	IdempotencyKey string `protobuf:"bytes,2,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *DeleteRunRequest) Reset() {
	*x = DeleteRunRequest{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[21]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DeleteRunRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteRunRequest) ProtoMessage() {}

func (x *DeleteRunRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[21]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteRunRequest.ProtoReflect.Descriptor instead.
func (*DeleteRunRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{21}
}

func (x *DeleteRunRequest) GetRunId() string {
	if x != nil {
		return x.RunId
	}
	return ""
}

func (x *DeleteRunRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// DeleteRunResponse is a named-empty response (Buf forbids returning
// google.protobuf.Empty and forbids reusing a domain message as a response).
// Empty body = success; failures surface as gRPC status codes.
type DeleteRunResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *DeleteRunResponse) Reset() {
	*x = DeleteRunResponse{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[22]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DeleteRunResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteRunResponse) ProtoMessage() {}

func (x *DeleteRunResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[22]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteRunResponse.ProtoReflect.Descriptor instead.
func (*DeleteRunResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{22}
}

// UpdateRunStatusRequest transitions a run to a terminal state (FINISHED /
// FAILED / KILLED). WHY a dedicated RPC instead of a status field on some
// "UpdateRun": status is security-sensitive and state-machine-constrained, so
// it gets its own narrow, auditable mutation. The server validates the
// transition (e.g., you can't move a FINISHED run back to RUNNING) and stamps
// ended_at itself.
type UpdateRunStatusRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The run to transition.
	RunId string `protobuf:"bytes,1,opt,name=run_id,json=runId,proto3" json:"run_id,omitempty"`
	// The target terminal status. The server REJECTS illegal transitions and
	// rejects RUN_STATUS_RUNNING/UNSPECIFIED here (you can't "un-finish" a run).
	Status        RunStatus `protobuf:"varint,2,opt,name=status,proto3,enum=forgepoint.experiment.v1.RunStatus" json:"status,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UpdateRunStatusRequest) Reset() {
	*x = UpdateRunStatusRequest{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[23]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpdateRunStatusRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdateRunStatusRequest) ProtoMessage() {}

func (x *UpdateRunStatusRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[23]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdateRunStatusRequest.ProtoReflect.Descriptor instead.
func (*UpdateRunStatusRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{23}
}

func (x *UpdateRunStatusRequest) GetRunId() string {
	if x != nil {
		return x.RunId
	}
	return ""
}

func (x *UpdateRunStatusRequest) GetStatus() RunStatus {
	if x != nil {
		return x.Status
	}
	return RunStatus_RUN_STATUS_UNSPECIFIED
}

type UpdateRunStatusResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The run after the transition (ended_at now set, final_metrics computed).
	Run           *Run `protobuf:"bytes,1,opt,name=run,proto3" json:"run,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UpdateRunStatusResponse) Reset() {
	*x = UpdateRunStatusResponse{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[24]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpdateRunStatusResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdateRunStatusResponse) ProtoMessage() {}

func (x *UpdateRunStatusResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[24]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdateRunStatusResponse.ProtoReflect.Descriptor instead.
func (*UpdateRunStatusResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{24}
}

func (x *UpdateRunStatusResponse) GetRun() *Run {
	if x != nil {
		return x.Run
	}
	return nil
}

// ============================================================================
// LogMetrics  (BATCH — the high-throughput ingestion RPC)
// ============================================================================
//
// WHY a UNARY BATCH RPC (repeated MetricPoint) rather than client-streaming:
//
//	A training job produces metrics in bursts (e.g., 500 points per epoch). We
//	want each flush to be:
//	  - IDEMPOTENT & retryable: a unary call with an idempotency_key can be
//	    safely re-sent after a timeout; a long-lived client stream that dies
//	    mid-flight leaves ambiguous partial state.
//	  - ONE TRANSACTION: the server writes the whole batch in a single
//	    multi-row INSERT (one round trip, one commit) — far cheaper than a
//	    point-per-message stream and aligned with the async batch-write design.
//	Client-streaming would shine for an unbounded, never-ending feed, but
//	training flushes are naturally chunked, so batch-unary is the better fit.
//	(The truly unbounded, platform-wide metric feed is handled OFF this RPC by
//	the NATS batch consumer — see the pattern header.)
//
// CAP: the server bounds points-per-call (e.g., 1000) and returns
// INVALID_ARGUMENT above it, so one request can't blow up memory or a single
// transaction. Clients split larger flushes across calls.
//
// SERVER-AUTHORITATIVE: each MetricPoint.timestamp is (re)stamped server-side
// on ingest (partition key integrity); clients supply key/value/step only.
type LogMetricsRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The run these metrics belong to. Must be RUNNING (logging to a terminal
	// run is rejected — its metrics are final).
	RunId string `protobuf:"bytes,1,opt,name=run_id,json=runId,proto3" json:"run_id,omitempty"`
	// The batch of metric points. Capped server-side (e.g., 1000 per call).
	Points []*MetricPoint `protobuf:"bytes,2,rep,name=points,proto3" json:"points,omitempty"`
	// Idempotency key for this batch. WHY it matters here specifically: metric
	// ingestion is the highest-volume, most-retried mutation in the service. On
	// a retry, the server uses this key to avoid double-writing the same batch
	// (which would corrupt the curve with duplicate points). Mirrors the
	// idempotent-consumer guarantee the async path gets from EventEnvelope.id.
	IdempotencyKey string `protobuf:"bytes,3,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *LogMetricsRequest) Reset() {
	*x = LogMetricsRequest{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[25]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *LogMetricsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LogMetricsRequest) ProtoMessage() {}

func (x *LogMetricsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[25]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LogMetricsRequest.ProtoReflect.Descriptor instead.
func (*LogMetricsRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{25}
}

func (x *LogMetricsRequest) GetRunId() string {
	if x != nil {
		return x.RunId
	}
	return ""
}

func (x *LogMetricsRequest) GetPoints() []*MetricPoint {
	if x != nil {
		return x.Points
	}
	return nil
}

func (x *LogMetricsRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// LogMetricsResponse reports how many points were durably accepted. WHY return
// a count (not an empty ack): batch APIs should tell the caller what landed so
// it can reconcile (e.g., if the server clamped/deduped). accepted_count may be
// less than len(points) on idempotent replay (duplicates skipped).
type LogMetricsResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Number of metric points durably written by THIS call (post-dedup).
	AcceptedCount int32 `protobuf:"varint,1,opt,name=accepted_count,json=acceptedCount,proto3" json:"accepted_count,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *LogMetricsResponse) Reset() {
	*x = LogMetricsResponse{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[26]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *LogMetricsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LogMetricsResponse) ProtoMessage() {}

func (x *LogMetricsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[26]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LogMetricsResponse.ProtoReflect.Descriptor instead.
func (*LogMetricsResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{26}
}

func (x *LogMetricsResponse) GetAcceptedCount() int32 {
	if x != nil {
		return x.AcceptedCount
	}
	return 0
}

// LogParamsRequest appends hyperparameters to a run after StartRun (e.g., a job
// discovers a derived config value mid-setup). Params are write-once per key.
type LogParamsRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The run to attach params to. Must be RUNNING.
	RunId string `protobuf:"bytes,1,opt,name=run_id,json=runId,proto3" json:"run_id,omitempty"`
	// The params to record. Re-logging an existing key is rejected unless the
	// value is identical (idempotent), preventing silent config drift.
	Params        []*Param `protobuf:"bytes,2,rep,name=params,proto3" json:"params,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *LogParamsRequest) Reset() {
	*x = LogParamsRequest{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[27]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *LogParamsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LogParamsRequest) ProtoMessage() {}

func (x *LogParamsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[27]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LogParamsRequest.ProtoReflect.Descriptor instead.
func (*LogParamsRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{27}
}

func (x *LogParamsRequest) GetRunId() string {
	if x != nil {
		return x.RunId
	}
	return ""
}

func (x *LogParamsRequest) GetParams() []*Param {
	if x != nil {
		return x.Params
	}
	return nil
}

// LogParamsResponse reports how many params were newly recorded.
type LogParamsResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Number of params newly written (excludes idempotent no-op duplicates).
	AcceptedCount int32 `protobuf:"varint,1,opt,name=accepted_count,json=acceptedCount,proto3" json:"accepted_count,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *LogParamsResponse) Reset() {
	*x = LogParamsResponse{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[28]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *LogParamsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LogParamsResponse) ProtoMessage() {}

func (x *LogParamsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[28]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LogParamsResponse.ProtoReflect.Descriptor instead.
func (*LogParamsResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{28}
}

func (x *LogParamsResponse) GetAcceptedCount() int32 {
	if x != nil {
		return x.AcceptedCount
	}
	return 0
}

// CompareRunsRequest asks for the metric series of several runs side-by-side —
// the data behind "which model version performs better?". You pass the run IDs
// and (optionally) restrict to specific metric keys to keep the payload small.
type CompareRunsRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The runs to compare. Bounded server-side (e.g., max 20 runs) — comparing
	// hundreds of full curves at once is a payload/DoS hazard.
	RunIds []string `protobuf:"bytes,1,rep,name=run_ids,json=runIds,proto3" json:"run_ids,omitempty"`
	// Optional: only return these metric keys (e.g., ["accuracy", "val_auc"]).
	// Empty = return all metric keys present on the runs. Narrowing this is the
	// main lever for keeping the response small.
	MetricKeys    []string `protobuf:"bytes,2,rep,name=metric_keys,json=metricKeys,proto3" json:"metric_keys,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *CompareRunsRequest) Reset() {
	*x = CompareRunsRequest{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[29]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CompareRunsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CompareRunsRequest) ProtoMessage() {}

func (x *CompareRunsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[29]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CompareRunsRequest.ProtoReflect.Descriptor instead.
func (*CompareRunsRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{29}
}

func (x *CompareRunsRequest) GetRunIds() []string {
	if x != nil {
		return x.RunIds
	}
	return nil
}

func (x *CompareRunsRequest) GetMetricKeys() []string {
	if x != nil {
		return x.MetricKeys
	}
	return nil
}

// RunComparison is the per-run slice of a CompareRuns response: the run's
// params (so the UI can show "lr=0.01 vs lr=0.1") plus its metric series.
type RunComparison struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The run being compared (metadata, status, final_metrics).
	Run *Run `protobuf:"bytes,1,opt,name=run,proto3" json:"run,omitempty"`
	// The full metric series (per requested key) for this run — the curves.
	Series        []*MetricSeries `protobuf:"bytes,2,rep,name=series,proto3" json:"series,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *RunComparison) Reset() {
	*x = RunComparison{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[30]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RunComparison) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RunComparison) ProtoMessage() {}

func (x *RunComparison) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[30]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RunComparison.ProtoReflect.Descriptor instead.
func (*RunComparison) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{30}
}

func (x *RunComparison) GetRun() *Run {
	if x != nil {
		return x.Run
	}
	return nil
}

func (x *RunComparison) GetSeries() []*MetricSeries {
	if x != nil {
		return x.Series
	}
	return nil
}

// CompareRunsResponse returns one RunComparison per requested run (that exists
// and is visible), preserving request order where possible.
type CompareRunsResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// One entry per compared run.
	Comparisons   []*RunComparison `protobuf:"bytes,1,rep,name=comparisons,proto3" json:"comparisons,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *CompareRunsResponse) Reset() {
	*x = CompareRunsResponse{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[31]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CompareRunsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CompareRunsResponse) ProtoMessage() {}

func (x *CompareRunsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[31]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CompareRunsResponse.ProtoReflect.Descriptor instead.
func (*CompareRunsResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{31}
}

func (x *CompareRunsResponse) GetComparisons() []*RunComparison {
	if x != nil {
		return x.Comparisons
	}
	return nil
}

// ============================================================================
// GetMetricHistory  (the time-series read the rest of the proto refers to)
// ============================================================================
//
// WHY this exists as its own RPC: GetRun deliberately returns only the
// denormalized final_metrics (headline numbers), and CompareRuns is multi-run.
// Several places in this contract say "use a dedicated metric-history read for
// the curve" — this is that RPC. It returns the FULL time-series for ONE run,
// PAGINATED, because a single run's "loss" curve can be millions of points and
// must never be returned unbounded (that is both an OOM and a DoS hazard).
//
// WHY paginated rather than server-streaming: the metrics table is RANGE-
// partitioned by timestamp, so a cursor (page_token encoding the last
// (key,step,timestamp) seen) maps cleanly onto an indexed keyset scan and is
// trivially retryable. A server-stream would be fine too, but pagination reuses
// the platform-wide common.Pagination contract and lets a BFF/UI fetch one
// screenful at a time. (Streaming is reserved for genuinely push-shaped APIs,
// e.g. a future WatchRun; a historical read is pull-shaped.)
type GetMetricHistoryRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The run whose metric history to read.
	RunId string `protobuf:"bytes,1,opt,name=run_id,json=runId,proto3" json:"run_id,omitempty"`
	// Optional: restrict to these metric keys (e.g. ["loss","val_auc"]). Empty =
	// all keys on the run. The main lever for keeping the response bounded.
	MetricKeys []string `protobuf:"bytes,2,rep,name=metric_keys,json=metricKeys,proto3" json:"metric_keys,omitempty"`
	// Optional inclusive step window [min_step, max_step]. Both 0 = no step bound.
	// Lets a UI fetch "epochs 100–200" without scanning the whole curve.
	MinStep int64 `protobuf:"varint,3,opt,name=min_step,json=minStep,proto3" json:"min_step,omitempty"`
	MaxStep int64 `protobuf:"varint,4,opt,name=max_step,json=maxStep,proto3" json:"max_step,omitempty"`
	// Cursor pagination. page_size defaults to 1000 here (curves are dense), max
	// 5000 — a HIGHER cap than list RPCs because points are tiny and charts want
	// many; still server-enforced so the page can't be unbounded.
	Pagination    *v1.PaginationRequest `protobuf:"bytes,5,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetMetricHistoryRequest) Reset() {
	*x = GetMetricHistoryRequest{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[32]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetMetricHistoryRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetMetricHistoryRequest) ProtoMessage() {}

func (x *GetMetricHistoryRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[32]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetMetricHistoryRequest.ProtoReflect.Descriptor instead.
func (*GetMetricHistoryRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{32}
}

func (x *GetMetricHistoryRequest) GetRunId() string {
	if x != nil {
		return x.RunId
	}
	return ""
}

func (x *GetMetricHistoryRequest) GetMetricKeys() []string {
	if x != nil {
		return x.MetricKeys
	}
	return nil
}

func (x *GetMetricHistoryRequest) GetMinStep() int64 {
	if x != nil {
		return x.MinStep
	}
	return 0
}

func (x *GetMetricHistoryRequest) GetMaxStep() int64 {
	if x != nil {
		return x.MaxStep
	}
	return 0
}

func (x *GetMetricHistoryRequest) GetPagination() *v1.PaginationRequest {
	if x != nil {
		return x.Pagination
	}
	return nil
}

type GetMetricHistoryResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The requested points, grouped per metric key (chart-ready). Within a series,
	// points are ordered by step then timestamp.
	Series []*MetricSeries `protobuf:"bytes,1,rep,name=series,proto3" json:"series,omitempty"`
	// Pagination metadata. next_page_token encodes the last (key,step,timestamp)
	// read so the next page resumes via an indexed keyset scan.
	Pagination    *v1.PaginationResponse `protobuf:"bytes,2,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetMetricHistoryResponse) Reset() {
	*x = GetMetricHistoryResponse{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[33]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetMetricHistoryResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetMetricHistoryResponse) ProtoMessage() {}

func (x *GetMetricHistoryResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[33]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetMetricHistoryResponse.ProtoReflect.Descriptor instead.
func (*GetMetricHistoryResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{33}
}

func (x *GetMetricHistoryResponse) GetSeries() []*MetricSeries {
	if x != nil {
		return x.Series
	}
	return nil
}

func (x *GetMetricHistoryResponse) GetPagination() *v1.PaginationResponse {
	if x != nil {
		return x.Pagination
	}
	return nil
}

// ============================================================================
// SetRunArtifacts  (the typed home for the google.protobuf.Struct import)
// ============================================================================
//
// WHY this RPC exists (and why Struct): a run produces unstructured side-artifacts
// whose shape varies and isn't worth a schema change per kind — a confusion
// matrix, a feature-importance map, an artifact manifest, a small eval report.
// google.protobuf.Struct round-trips to/from JSON, so callers attach arbitrary
// JSON-shaped context WITHOUT us reaching for opaque bytes (which a UI can't
// render) and WITHOUT bloating the hot LogMetrics path with free-form data.
//
// SECURITY / SIZE: this is NOT a dumping ground. The server caps the serialized
// Struct size (e.g. 256 KiB) and REJECTS larger payloads with INVALID_ARGUMENT —
// large blobs belong in object storage with a URI referenced here, not inline on
// NATS-adjacent state. Artifacts are write-once-ish: re-setting the same key
// replaces it; keys are namespaced strings the run owns.
type SetRunArtifactsRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The run to attach artifacts to.
	RunId string `protobuf:"bytes,1,opt,name=run_id,json=runId,proto3" json:"run_id,omitempty"`
	// Free-form, JSON-shaped artifacts (e.g.
	// {"confusion_matrix": [[...]], "manifest_uri": "s3://..."}). Server-size-capped.
	// Struct (not bytes) so it is inspectable/renderable and stays human-readable.
	Artifacts *structpb.Struct `protobuf:"bytes,2,opt,name=artifacts,proto3" json:"artifacts,omitempty"`
	// Idempotency key — SetRunArtifacts is a mutation a client may retry; the same
	// key replays to the same effect rather than re-applying.
	IdempotencyKey string `protobuf:"bytes,3,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *SetRunArtifactsRequest) Reset() {
	*x = SetRunArtifactsRequest{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[34]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SetRunArtifactsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SetRunArtifactsRequest) ProtoMessage() {}

func (x *SetRunArtifactsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[34]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SetRunArtifactsRequest.ProtoReflect.Descriptor instead.
func (*SetRunArtifactsRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{34}
}

func (x *SetRunArtifactsRequest) GetRunId() string {
	if x != nil {
		return x.RunId
	}
	return ""
}

func (x *SetRunArtifactsRequest) GetArtifacts() *structpb.Struct {
	if x != nil {
		return x.Artifacts
	}
	return nil
}

func (x *SetRunArtifactsRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

type SetRunArtifactsResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The run after the attachment (carries the merged artifacts back for confirm).
	Run           *Run `protobuf:"bytes,1,opt,name=run,proto3" json:"run,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SetRunArtifactsResponse) Reset() {
	*x = SetRunArtifactsResponse{}
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[35]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SetRunArtifactsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SetRunArtifactsResponse) ProtoMessage() {}

func (x *SetRunArtifactsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_experiment_v1_experiment_proto_msgTypes[35]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SetRunArtifactsResponse.ProtoReflect.Descriptor instead.
func (*SetRunArtifactsResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP(), []int{35}
}

func (x *SetRunArtifactsResponse) GetRun() *Run {
	if x != nil {
		return x.Run
	}
	return nil
}

var File_forgepoint_experiment_v1_experiment_proto protoreflect.FileDescriptor

const file_forgepoint_experiment_v1_experiment_proto_rawDesc = "" +
	"\n" +
	")forgepoint/experiment/v1/experiment.proto\x12\x18forgepoint.experiment.v1\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1cgoogle/protobuf/struct.proto\x1a!forgepoint/common/v1/common.proto\"\xb1\x03\n" +
	"\n" +
	"Experiment\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12\x12\n" +
	"\x04name\x18\x02 \x01(\tR\x04name\x12 \n" +
	"\vdescription\x18\x03 \x01(\tR\vdescription\x12B\n" +
	"\x04tags\x18\x04 \x03(\v2..forgepoint.experiment.v1.Experiment.TagsEntryR\x04tags\x12\x19\n" +
	"\bowner_id\x18\x05 \x01(\tR\aownerId\x12\x12\n" +
	"\x04team\x18\x06 \x01(\tR\x04team\x129\n" +
	"\n" +
	"created_at\x18\a \x01(\v2\x1a.google.protobuf.TimestampR\tcreatedAt\x129\n" +
	"\n" +
	"updated_at\x18\b \x01(\v2\x1a.google.protobuf.TimestampR\tupdatedAt\x12;\n" +
	"\varchived_at\x18\t \x01(\v2\x1a.google.protobuf.TimestampR\n" +
	"archivedAt\x1a7\n" +
	"\tTagsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\tR\x05value:\x028\x01\"\xca\x04\n" +
	"\x03Run\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12#\n" +
	"\rexperiment_id\x18\x02 \x01(\tR\fexperimentId\x12!\n" +
	"\fdisplay_name\x18\x03 \x01(\tR\vdisplayName\x12;\n" +
	"\x06status\x18\x04 \x01(\x0e2#.forgepoint.experiment.v1.RunStatusR\x06status\x12;\n" +
	"\x06source\x18\x05 \x01(\x0e2#.forgepoint.experiment.v1.RunSourceR\x06source\x12(\n" +
	"\x10model_version_id\x18\x06 \x01(\tR\x0emodelVersionId\x12\x19\n" +
	"\bowner_id\x18\a \x01(\tR\aownerId\x127\n" +
	"\x06params\x18\b \x03(\v2\x1f.forgepoint.experiment.v1.ParamR\x06params\x12J\n" +
	"\rfinal_metrics\x18\t \x03(\v2%.forgepoint.experiment.v1.MetricPointR\ffinalMetrics\x129\n" +
	"\n" +
	"started_at\x18\n" +
	" \x01(\v2\x1a.google.protobuf.TimestampR\tstartedAt\x125\n" +
	"\bended_at\x18\v \x01(\v2\x1a.google.protobuf.TimestampR\aendedAt\x125\n" +
	"\tartifacts\x18\f \x01(\v2\x17.google.protobuf.StructR\tartifacts\"/\n" +
	"\x05Param\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\tR\x05value\"\x83\x01\n" +
	"\vMetricPoint\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\x01R\x05value\x12\x12\n" +
	"\x04step\x18\x03 \x01(\x03R\x04step\x128\n" +
	"\ttimestamp\x18\x04 \x01(\v2\x1a.google.protobuf.TimestampR\ttimestamp\"_\n" +
	"\fMetricSeries\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12=\n" +
	"\x06points\x18\x02 \x03(\v2%.forgepoint.experiment.v1.MetricPointR\x06points\"\xd9\x01\n" +
	"\x17CreateExperimentRequest\x12\x12\n" +
	"\x04name\x18\x01 \x01(\tR\x04name\x12 \n" +
	"\vdescription\x18\x02 \x01(\tR\vdescription\x12O\n" +
	"\x04tags\x18\x03 \x03(\v2;.forgepoint.experiment.v1.CreateExperimentRequest.TagsEntryR\x04tags\x1a7\n" +
	"\tTagsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\tR\x05value:\x028\x01\"`\n" +
	"\x18CreateExperimentResponse\x12D\n" +
	"\n" +
	"experiment\x18\x01 \x01(\v2$.forgepoint.experiment.v1.ExperimentR\n" +
	"experiment\"&\n" +
	"\x14GetExperimentRequest\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\"]\n" +
	"\x15GetExperimentResponse\x12D\n" +
	"\n" +
	"experiment\x18\x01 \x01(\v2$.forgepoint.experiment.v1.ExperimentR\n" +
	"experiment\"\xad\x01\n" +
	"\x16ListExperimentsRequest\x12\x1f\n" +
	"\vteam_filter\x18\x01 \x01(\tR\n" +
	"teamFilter\x12)\n" +
	"\x10include_archived\x18\x02 \x01(\bR\x0fincludeArchived\x12G\n" +
	"\n" +
	"pagination\x18\x03 \x01(\v2'.forgepoint.common.v1.PaginationRequestR\n" +
	"pagination\"\xab\x01\n" +
	"\x17ListExperimentsResponse\x12F\n" +
	"\vexperiments\x18\x01 \x03(\v2$.forgepoint.experiment.v1.ExperimentR\vexperiments\x12H\n" +
	"\n" +
	"pagination\x18\x02 \x01(\v2(.forgepoint.common.v1.PaginationResponseR\n" +
	"pagination\"\x8e\x02\n" +
	"\x17UpdateExperimentRequest\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12\x12\n" +
	"\x04name\x18\x02 \x01(\tR\x04name\x12 \n" +
	"\vdescription\x18\x03 \x01(\tR\vdescription\x12O\n" +
	"\x04tags\x18\x04 \x03(\v2;.forgepoint.experiment.v1.UpdateExperimentRequest.TagsEntryR\x04tags\x12#\n" +
	"\rupdate_fields\x18\x05 \x03(\tR\fupdateFields\x1a7\n" +
	"\tTagsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\tR\x05value:\x028\x01\"`\n" +
	"\x18UpdateExperimentResponse\x12D\n" +
	"\n" +
	"experiment\x18\x01 \x01(\v2$.forgepoint.experiment.v1.ExperimentR\n" +
	"experiment\"*\n" +
	"\x18ArchiveExperimentRequest\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\"a\n" +
	"\x19ArchiveExperimentResponse\x12D\n" +
	"\n" +
	"experiment\x18\x01 \x01(\v2$.forgepoint.experiment.v1.ExperimentR\n" +
	"experiment\"\xe5\x01\n" +
	"\x0fStartRunRequest\x12#\n" +
	"\rexperiment_id\x18\x01 \x01(\tR\fexperimentId\x12!\n" +
	"\fdisplay_name\x18\x02 \x01(\tR\vdisplayName\x12(\n" +
	"\x10model_version_id\x18\x03 \x01(\tR\x0emodelVersionId\x127\n" +
	"\x06params\x18\x04 \x03(\v2\x1f.forgepoint.experiment.v1.ParamR\x06params\x12'\n" +
	"\x0fidempotency_key\x18\x05 \x01(\tR\x0eidempotencyKey\"C\n" +
	"\x10StartRunResponse\x12/\n" +
	"\x03run\x18\x01 \x01(\v2\x1d.forgepoint.experiment.v1.RunR\x03run\"\x1f\n" +
	"\rGetRunRequest\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\"A\n" +
	"\x0eGetRunResponse\x12/\n" +
	"\x03run\x18\x01 \x01(\v2\x1d.forgepoint.experiment.v1.RunR\x03run\"\xc9\x01\n" +
	"\x0fListRunsRequest\x12#\n" +
	"\rexperiment_id\x18\x01 \x01(\tR\fexperimentId\x12H\n" +
	"\rstatus_filter\x18\x02 \x01(\x0e2#.forgepoint.experiment.v1.RunStatusR\fstatusFilter\x12G\n" +
	"\n" +
	"pagination\x18\x03 \x01(\v2'.forgepoint.common.v1.PaginationRequestR\n" +
	"pagination\"\x8f\x01\n" +
	"\x10ListRunsResponse\x121\n" +
	"\x04runs\x18\x01 \x03(\v2\x1d.forgepoint.experiment.v1.RunR\x04runs\x12H\n" +
	"\n" +
	"pagination\x18\x02 \x01(\v2(.forgepoint.common.v1.PaginationResponseR\n" +
	"pagination\"R\n" +
	"\x10DeleteRunRequest\x12\x15\n" +
	"\x06run_id\x18\x01 \x01(\tR\x05runId\x12'\n" +
	"\x0fidempotency_key\x18\x02 \x01(\tR\x0eidempotencyKey\"\x13\n" +
	"\x11DeleteRunResponse\"l\n" +
	"\x16UpdateRunStatusRequest\x12\x15\n" +
	"\x06run_id\x18\x01 \x01(\tR\x05runId\x12;\n" +
	"\x06status\x18\x02 \x01(\x0e2#.forgepoint.experiment.v1.RunStatusR\x06status\"J\n" +
	"\x17UpdateRunStatusResponse\x12/\n" +
	"\x03run\x18\x01 \x01(\v2\x1d.forgepoint.experiment.v1.RunR\x03run\"\x92\x01\n" +
	"\x11LogMetricsRequest\x12\x15\n" +
	"\x06run_id\x18\x01 \x01(\tR\x05runId\x12=\n" +
	"\x06points\x18\x02 \x03(\v2%.forgepoint.experiment.v1.MetricPointR\x06points\x12'\n" +
	"\x0fidempotency_key\x18\x03 \x01(\tR\x0eidempotencyKey\";\n" +
	"\x12LogMetricsResponse\x12%\n" +
	"\x0eaccepted_count\x18\x01 \x01(\x05R\racceptedCount\"b\n" +
	"\x10LogParamsRequest\x12\x15\n" +
	"\x06run_id\x18\x01 \x01(\tR\x05runId\x127\n" +
	"\x06params\x18\x02 \x03(\v2\x1f.forgepoint.experiment.v1.ParamR\x06params\":\n" +
	"\x11LogParamsResponse\x12%\n" +
	"\x0eaccepted_count\x18\x01 \x01(\x05R\racceptedCount\"N\n" +
	"\x12CompareRunsRequest\x12\x17\n" +
	"\arun_ids\x18\x01 \x03(\tR\x06runIds\x12\x1f\n" +
	"\vmetric_keys\x18\x02 \x03(\tR\n" +
	"metricKeys\"\x80\x01\n" +
	"\rRunComparison\x12/\n" +
	"\x03run\x18\x01 \x01(\v2\x1d.forgepoint.experiment.v1.RunR\x03run\x12>\n" +
	"\x06series\x18\x02 \x03(\v2&.forgepoint.experiment.v1.MetricSeriesR\x06series\"`\n" +
	"\x13CompareRunsResponse\x12I\n" +
	"\vcomparisons\x18\x01 \x03(\v2'.forgepoint.experiment.v1.RunComparisonR\vcomparisons\"\xd0\x01\n" +
	"\x17GetMetricHistoryRequest\x12\x15\n" +
	"\x06run_id\x18\x01 \x01(\tR\x05runId\x12\x1f\n" +
	"\vmetric_keys\x18\x02 \x03(\tR\n" +
	"metricKeys\x12\x19\n" +
	"\bmin_step\x18\x03 \x01(\x03R\aminStep\x12\x19\n" +
	"\bmax_step\x18\x04 \x01(\x03R\amaxStep\x12G\n" +
	"\n" +
	"pagination\x18\x05 \x01(\v2'.forgepoint.common.v1.PaginationRequestR\n" +
	"pagination\"\xa4\x01\n" +
	"\x18GetMetricHistoryResponse\x12>\n" +
	"\x06series\x18\x01 \x03(\v2&.forgepoint.experiment.v1.MetricSeriesR\x06series\x12H\n" +
	"\n" +
	"pagination\x18\x02 \x01(\v2(.forgepoint.common.v1.PaginationResponseR\n" +
	"pagination\"\x8f\x01\n" +
	"\x16SetRunArtifactsRequest\x12\x15\n" +
	"\x06run_id\x18\x01 \x01(\tR\x05runId\x125\n" +
	"\tartifacts\x18\x02 \x01(\v2\x17.google.protobuf.StructR\tartifacts\x12'\n" +
	"\x0fidempotency_key\x18\x03 \x01(\tR\x0eidempotencyKey\"J\n" +
	"\x17SetRunArtifactsResponse\x12/\n" +
	"\x03run\x18\x01 \x01(\v2\x1d.forgepoint.experiment.v1.RunR\x03run*\x86\x01\n" +
	"\tRunStatus\x12\x1a\n" +
	"\x16RUN_STATUS_UNSPECIFIED\x10\x00\x12\x16\n" +
	"\x12RUN_STATUS_RUNNING\x10\x01\x12\x17\n" +
	"\x13RUN_STATUS_FINISHED\x10\x02\x12\x15\n" +
	"\x11RUN_STATUS_FAILED\x10\x03\x12\x15\n" +
	"\x11RUN_STATUS_KILLED\x10\x04*Q\n" +
	"\tRunSource\x12\x1a\n" +
	"\x16RUN_SOURCE_UNSPECIFIED\x10\x00\x12\x12\n" +
	"\x0eRUN_SOURCE_API\x10\x01\x12\x14\n" +
	"\x10RUN_SOURCE_EVENT\x10\x022\xa7\r\n" +
	"\x18ExperimentTrackerService\x12y\n" +
	"\x10CreateExperiment\x121.forgepoint.experiment.v1.CreateExperimentRequest\x1a2.forgepoint.experiment.v1.CreateExperimentResponse\x12p\n" +
	"\rGetExperiment\x12..forgepoint.experiment.v1.GetExperimentRequest\x1a/.forgepoint.experiment.v1.GetExperimentResponse\x12v\n" +
	"\x0fListExperiments\x120.forgepoint.experiment.v1.ListExperimentsRequest\x1a1.forgepoint.experiment.v1.ListExperimentsResponse\x12y\n" +
	"\x10UpdateExperiment\x121.forgepoint.experiment.v1.UpdateExperimentRequest\x1a2.forgepoint.experiment.v1.UpdateExperimentResponse\x12|\n" +
	"\x11ArchiveExperiment\x122.forgepoint.experiment.v1.ArchiveExperimentRequest\x1a3.forgepoint.experiment.v1.ArchiveExperimentResponse\x12a\n" +
	"\bStartRun\x12).forgepoint.experiment.v1.StartRunRequest\x1a*.forgepoint.experiment.v1.StartRunResponse\x12v\n" +
	"\x0fUpdateRunStatus\x120.forgepoint.experiment.v1.UpdateRunStatusRequest\x1a1.forgepoint.experiment.v1.UpdateRunStatusResponse\x12[\n" +
	"\x06GetRun\x12'.forgepoint.experiment.v1.GetRunRequest\x1a(.forgepoint.experiment.v1.GetRunResponse\x12a\n" +
	"\bListRuns\x12).forgepoint.experiment.v1.ListRunsRequest\x1a*.forgepoint.experiment.v1.ListRunsResponse\x12d\n" +
	"\tDeleteRun\x12*.forgepoint.experiment.v1.DeleteRunRequest\x1a+.forgepoint.experiment.v1.DeleteRunResponse\x12g\n" +
	"\n" +
	"LogMetrics\x12+.forgepoint.experiment.v1.LogMetricsRequest\x1a,.forgepoint.experiment.v1.LogMetricsResponse\x12d\n" +
	"\tLogParams\x12*.forgepoint.experiment.v1.LogParamsRequest\x1a+.forgepoint.experiment.v1.LogParamsResponse\x12v\n" +
	"\x0fSetRunArtifacts\x120.forgepoint.experiment.v1.SetRunArtifactsRequest\x1a1.forgepoint.experiment.v1.SetRunArtifactsResponse\x12y\n" +
	"\x10GetMetricHistory\x121.forgepoint.experiment.v1.GetMetricHistoryRequest\x1a2.forgepoint.experiment.v1.GetMetricHistoryResponse\x12j\n" +
	"\vCompareRuns\x12,.forgepoint.experiment.v1.CompareRunsRequest\x1a-.forgepoint.experiment.v1.CompareRunsResponseBPZNgithub.com/abd-ulbasit/forgepoint/gen/go/forgepoint/experiment/v1;experimentv1b\x06proto3"

var (
	file_forgepoint_experiment_v1_experiment_proto_rawDescOnce sync.Once
	file_forgepoint_experiment_v1_experiment_proto_rawDescData []byte
)

func file_forgepoint_experiment_v1_experiment_proto_rawDescGZIP() []byte {
	file_forgepoint_experiment_v1_experiment_proto_rawDescOnce.Do(func() {
		file_forgepoint_experiment_v1_experiment_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_forgepoint_experiment_v1_experiment_proto_rawDesc), len(file_forgepoint_experiment_v1_experiment_proto_rawDesc)))
	})
	return file_forgepoint_experiment_v1_experiment_proto_rawDescData
}

var file_forgepoint_experiment_v1_experiment_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_forgepoint_experiment_v1_experiment_proto_msgTypes = make([]protoimpl.MessageInfo, 39)
var file_forgepoint_experiment_v1_experiment_proto_goTypes = []any{
	(RunStatus)(0),                    // 0: forgepoint.experiment.v1.RunStatus
	(RunSource)(0),                    // 1: forgepoint.experiment.v1.RunSource
	(*Experiment)(nil),                // 2: forgepoint.experiment.v1.Experiment
	(*Run)(nil),                       // 3: forgepoint.experiment.v1.Run
	(*Param)(nil),                     // 4: forgepoint.experiment.v1.Param
	(*MetricPoint)(nil),               // 5: forgepoint.experiment.v1.MetricPoint
	(*MetricSeries)(nil),              // 6: forgepoint.experiment.v1.MetricSeries
	(*CreateExperimentRequest)(nil),   // 7: forgepoint.experiment.v1.CreateExperimentRequest
	(*CreateExperimentResponse)(nil),  // 8: forgepoint.experiment.v1.CreateExperimentResponse
	(*GetExperimentRequest)(nil),      // 9: forgepoint.experiment.v1.GetExperimentRequest
	(*GetExperimentResponse)(nil),     // 10: forgepoint.experiment.v1.GetExperimentResponse
	(*ListExperimentsRequest)(nil),    // 11: forgepoint.experiment.v1.ListExperimentsRequest
	(*ListExperimentsResponse)(nil),   // 12: forgepoint.experiment.v1.ListExperimentsResponse
	(*UpdateExperimentRequest)(nil),   // 13: forgepoint.experiment.v1.UpdateExperimentRequest
	(*UpdateExperimentResponse)(nil),  // 14: forgepoint.experiment.v1.UpdateExperimentResponse
	(*ArchiveExperimentRequest)(nil),  // 15: forgepoint.experiment.v1.ArchiveExperimentRequest
	(*ArchiveExperimentResponse)(nil), // 16: forgepoint.experiment.v1.ArchiveExperimentResponse
	(*StartRunRequest)(nil),           // 17: forgepoint.experiment.v1.StartRunRequest
	(*StartRunResponse)(nil),          // 18: forgepoint.experiment.v1.StartRunResponse
	(*GetRunRequest)(nil),             // 19: forgepoint.experiment.v1.GetRunRequest
	(*GetRunResponse)(nil),            // 20: forgepoint.experiment.v1.GetRunResponse
	(*ListRunsRequest)(nil),           // 21: forgepoint.experiment.v1.ListRunsRequest
	(*ListRunsResponse)(nil),          // 22: forgepoint.experiment.v1.ListRunsResponse
	(*DeleteRunRequest)(nil),          // 23: forgepoint.experiment.v1.DeleteRunRequest
	(*DeleteRunResponse)(nil),         // 24: forgepoint.experiment.v1.DeleteRunResponse
	(*UpdateRunStatusRequest)(nil),    // 25: forgepoint.experiment.v1.UpdateRunStatusRequest
	(*UpdateRunStatusResponse)(nil),   // 26: forgepoint.experiment.v1.UpdateRunStatusResponse
	(*LogMetricsRequest)(nil),         // 27: forgepoint.experiment.v1.LogMetricsRequest
	(*LogMetricsResponse)(nil),        // 28: forgepoint.experiment.v1.LogMetricsResponse
	(*LogParamsRequest)(nil),          // 29: forgepoint.experiment.v1.LogParamsRequest
	(*LogParamsResponse)(nil),         // 30: forgepoint.experiment.v1.LogParamsResponse
	(*CompareRunsRequest)(nil),        // 31: forgepoint.experiment.v1.CompareRunsRequest
	(*RunComparison)(nil),             // 32: forgepoint.experiment.v1.RunComparison
	(*CompareRunsResponse)(nil),       // 33: forgepoint.experiment.v1.CompareRunsResponse
	(*GetMetricHistoryRequest)(nil),   // 34: forgepoint.experiment.v1.GetMetricHistoryRequest
	(*GetMetricHistoryResponse)(nil),  // 35: forgepoint.experiment.v1.GetMetricHistoryResponse
	(*SetRunArtifactsRequest)(nil),    // 36: forgepoint.experiment.v1.SetRunArtifactsRequest
	(*SetRunArtifactsResponse)(nil),   // 37: forgepoint.experiment.v1.SetRunArtifactsResponse
	nil,                               // 38: forgepoint.experiment.v1.Experiment.TagsEntry
	nil,                               // 39: forgepoint.experiment.v1.CreateExperimentRequest.TagsEntry
	nil,                               // 40: forgepoint.experiment.v1.UpdateExperimentRequest.TagsEntry
	(*timestamppb.Timestamp)(nil),     // 41: google.protobuf.Timestamp
	(*structpb.Struct)(nil),           // 42: google.protobuf.Struct
	(*v1.PaginationRequest)(nil),      // 43: forgepoint.common.v1.PaginationRequest
	(*v1.PaginationResponse)(nil),     // 44: forgepoint.common.v1.PaginationResponse
}
var file_forgepoint_experiment_v1_experiment_proto_depIdxs = []int32{
	38, // 0: forgepoint.experiment.v1.Experiment.tags:type_name -> forgepoint.experiment.v1.Experiment.TagsEntry
	41, // 1: forgepoint.experiment.v1.Experiment.created_at:type_name -> google.protobuf.Timestamp
	41, // 2: forgepoint.experiment.v1.Experiment.updated_at:type_name -> google.protobuf.Timestamp
	41, // 3: forgepoint.experiment.v1.Experiment.archived_at:type_name -> google.protobuf.Timestamp
	0,  // 4: forgepoint.experiment.v1.Run.status:type_name -> forgepoint.experiment.v1.RunStatus
	1,  // 5: forgepoint.experiment.v1.Run.source:type_name -> forgepoint.experiment.v1.RunSource
	4,  // 6: forgepoint.experiment.v1.Run.params:type_name -> forgepoint.experiment.v1.Param
	5,  // 7: forgepoint.experiment.v1.Run.final_metrics:type_name -> forgepoint.experiment.v1.MetricPoint
	41, // 8: forgepoint.experiment.v1.Run.started_at:type_name -> google.protobuf.Timestamp
	41, // 9: forgepoint.experiment.v1.Run.ended_at:type_name -> google.protobuf.Timestamp
	42, // 10: forgepoint.experiment.v1.Run.artifacts:type_name -> google.protobuf.Struct
	41, // 11: forgepoint.experiment.v1.MetricPoint.timestamp:type_name -> google.protobuf.Timestamp
	5,  // 12: forgepoint.experiment.v1.MetricSeries.points:type_name -> forgepoint.experiment.v1.MetricPoint
	39, // 13: forgepoint.experiment.v1.CreateExperimentRequest.tags:type_name -> forgepoint.experiment.v1.CreateExperimentRequest.TagsEntry
	2,  // 14: forgepoint.experiment.v1.CreateExperimentResponse.experiment:type_name -> forgepoint.experiment.v1.Experiment
	2,  // 15: forgepoint.experiment.v1.GetExperimentResponse.experiment:type_name -> forgepoint.experiment.v1.Experiment
	43, // 16: forgepoint.experiment.v1.ListExperimentsRequest.pagination:type_name -> forgepoint.common.v1.PaginationRequest
	2,  // 17: forgepoint.experiment.v1.ListExperimentsResponse.experiments:type_name -> forgepoint.experiment.v1.Experiment
	44, // 18: forgepoint.experiment.v1.ListExperimentsResponse.pagination:type_name -> forgepoint.common.v1.PaginationResponse
	40, // 19: forgepoint.experiment.v1.UpdateExperimentRequest.tags:type_name -> forgepoint.experiment.v1.UpdateExperimentRequest.TagsEntry
	2,  // 20: forgepoint.experiment.v1.UpdateExperimentResponse.experiment:type_name -> forgepoint.experiment.v1.Experiment
	2,  // 21: forgepoint.experiment.v1.ArchiveExperimentResponse.experiment:type_name -> forgepoint.experiment.v1.Experiment
	4,  // 22: forgepoint.experiment.v1.StartRunRequest.params:type_name -> forgepoint.experiment.v1.Param
	3,  // 23: forgepoint.experiment.v1.StartRunResponse.run:type_name -> forgepoint.experiment.v1.Run
	3,  // 24: forgepoint.experiment.v1.GetRunResponse.run:type_name -> forgepoint.experiment.v1.Run
	0,  // 25: forgepoint.experiment.v1.ListRunsRequest.status_filter:type_name -> forgepoint.experiment.v1.RunStatus
	43, // 26: forgepoint.experiment.v1.ListRunsRequest.pagination:type_name -> forgepoint.common.v1.PaginationRequest
	3,  // 27: forgepoint.experiment.v1.ListRunsResponse.runs:type_name -> forgepoint.experiment.v1.Run
	44, // 28: forgepoint.experiment.v1.ListRunsResponse.pagination:type_name -> forgepoint.common.v1.PaginationResponse
	0,  // 29: forgepoint.experiment.v1.UpdateRunStatusRequest.status:type_name -> forgepoint.experiment.v1.RunStatus
	3,  // 30: forgepoint.experiment.v1.UpdateRunStatusResponse.run:type_name -> forgepoint.experiment.v1.Run
	5,  // 31: forgepoint.experiment.v1.LogMetricsRequest.points:type_name -> forgepoint.experiment.v1.MetricPoint
	4,  // 32: forgepoint.experiment.v1.LogParamsRequest.params:type_name -> forgepoint.experiment.v1.Param
	3,  // 33: forgepoint.experiment.v1.RunComparison.run:type_name -> forgepoint.experiment.v1.Run
	6,  // 34: forgepoint.experiment.v1.RunComparison.series:type_name -> forgepoint.experiment.v1.MetricSeries
	32, // 35: forgepoint.experiment.v1.CompareRunsResponse.comparisons:type_name -> forgepoint.experiment.v1.RunComparison
	43, // 36: forgepoint.experiment.v1.GetMetricHistoryRequest.pagination:type_name -> forgepoint.common.v1.PaginationRequest
	6,  // 37: forgepoint.experiment.v1.GetMetricHistoryResponse.series:type_name -> forgepoint.experiment.v1.MetricSeries
	44, // 38: forgepoint.experiment.v1.GetMetricHistoryResponse.pagination:type_name -> forgepoint.common.v1.PaginationResponse
	42, // 39: forgepoint.experiment.v1.SetRunArtifactsRequest.artifacts:type_name -> google.protobuf.Struct
	3,  // 40: forgepoint.experiment.v1.SetRunArtifactsResponse.run:type_name -> forgepoint.experiment.v1.Run
	7,  // 41: forgepoint.experiment.v1.ExperimentTrackerService.CreateExperiment:input_type -> forgepoint.experiment.v1.CreateExperimentRequest
	9,  // 42: forgepoint.experiment.v1.ExperimentTrackerService.GetExperiment:input_type -> forgepoint.experiment.v1.GetExperimentRequest
	11, // 43: forgepoint.experiment.v1.ExperimentTrackerService.ListExperiments:input_type -> forgepoint.experiment.v1.ListExperimentsRequest
	13, // 44: forgepoint.experiment.v1.ExperimentTrackerService.UpdateExperiment:input_type -> forgepoint.experiment.v1.UpdateExperimentRequest
	15, // 45: forgepoint.experiment.v1.ExperimentTrackerService.ArchiveExperiment:input_type -> forgepoint.experiment.v1.ArchiveExperimentRequest
	17, // 46: forgepoint.experiment.v1.ExperimentTrackerService.StartRun:input_type -> forgepoint.experiment.v1.StartRunRequest
	25, // 47: forgepoint.experiment.v1.ExperimentTrackerService.UpdateRunStatus:input_type -> forgepoint.experiment.v1.UpdateRunStatusRequest
	19, // 48: forgepoint.experiment.v1.ExperimentTrackerService.GetRun:input_type -> forgepoint.experiment.v1.GetRunRequest
	21, // 49: forgepoint.experiment.v1.ExperimentTrackerService.ListRuns:input_type -> forgepoint.experiment.v1.ListRunsRequest
	23, // 50: forgepoint.experiment.v1.ExperimentTrackerService.DeleteRun:input_type -> forgepoint.experiment.v1.DeleteRunRequest
	27, // 51: forgepoint.experiment.v1.ExperimentTrackerService.LogMetrics:input_type -> forgepoint.experiment.v1.LogMetricsRequest
	29, // 52: forgepoint.experiment.v1.ExperimentTrackerService.LogParams:input_type -> forgepoint.experiment.v1.LogParamsRequest
	36, // 53: forgepoint.experiment.v1.ExperimentTrackerService.SetRunArtifacts:input_type -> forgepoint.experiment.v1.SetRunArtifactsRequest
	34, // 54: forgepoint.experiment.v1.ExperimentTrackerService.GetMetricHistory:input_type -> forgepoint.experiment.v1.GetMetricHistoryRequest
	31, // 55: forgepoint.experiment.v1.ExperimentTrackerService.CompareRuns:input_type -> forgepoint.experiment.v1.CompareRunsRequest
	8,  // 56: forgepoint.experiment.v1.ExperimentTrackerService.CreateExperiment:output_type -> forgepoint.experiment.v1.CreateExperimentResponse
	10, // 57: forgepoint.experiment.v1.ExperimentTrackerService.GetExperiment:output_type -> forgepoint.experiment.v1.GetExperimentResponse
	12, // 58: forgepoint.experiment.v1.ExperimentTrackerService.ListExperiments:output_type -> forgepoint.experiment.v1.ListExperimentsResponse
	14, // 59: forgepoint.experiment.v1.ExperimentTrackerService.UpdateExperiment:output_type -> forgepoint.experiment.v1.UpdateExperimentResponse
	16, // 60: forgepoint.experiment.v1.ExperimentTrackerService.ArchiveExperiment:output_type -> forgepoint.experiment.v1.ArchiveExperimentResponse
	18, // 61: forgepoint.experiment.v1.ExperimentTrackerService.StartRun:output_type -> forgepoint.experiment.v1.StartRunResponse
	26, // 62: forgepoint.experiment.v1.ExperimentTrackerService.UpdateRunStatus:output_type -> forgepoint.experiment.v1.UpdateRunStatusResponse
	20, // 63: forgepoint.experiment.v1.ExperimentTrackerService.GetRun:output_type -> forgepoint.experiment.v1.GetRunResponse
	22, // 64: forgepoint.experiment.v1.ExperimentTrackerService.ListRuns:output_type -> forgepoint.experiment.v1.ListRunsResponse
	24, // 65: forgepoint.experiment.v1.ExperimentTrackerService.DeleteRun:output_type -> forgepoint.experiment.v1.DeleteRunResponse
	28, // 66: forgepoint.experiment.v1.ExperimentTrackerService.LogMetrics:output_type -> forgepoint.experiment.v1.LogMetricsResponse
	30, // 67: forgepoint.experiment.v1.ExperimentTrackerService.LogParams:output_type -> forgepoint.experiment.v1.LogParamsResponse
	37, // 68: forgepoint.experiment.v1.ExperimentTrackerService.SetRunArtifacts:output_type -> forgepoint.experiment.v1.SetRunArtifactsResponse
	35, // 69: forgepoint.experiment.v1.ExperimentTrackerService.GetMetricHistory:output_type -> forgepoint.experiment.v1.GetMetricHistoryResponse
	33, // 70: forgepoint.experiment.v1.ExperimentTrackerService.CompareRuns:output_type -> forgepoint.experiment.v1.CompareRunsResponse
	56, // [56:71] is the sub-list for method output_type
	41, // [41:56] is the sub-list for method input_type
	41, // [41:41] is the sub-list for extension type_name
	41, // [41:41] is the sub-list for extension extendee
	0,  // [0:41] is the sub-list for field type_name
}

func init() { file_forgepoint_experiment_v1_experiment_proto_init() }
func file_forgepoint_experiment_v1_experiment_proto_init() {
	if File_forgepoint_experiment_v1_experiment_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_forgepoint_experiment_v1_experiment_proto_rawDesc), len(file_forgepoint_experiment_v1_experiment_proto_rawDesc)),
			NumEnums:      2,
			NumMessages:   39,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_forgepoint_experiment_v1_experiment_proto_goTypes,
		DependencyIndexes: file_forgepoint_experiment_v1_experiment_proto_depIdxs,
		EnumInfos:         file_forgepoint_experiment_v1_experiment_proto_enumTypes,
		MessageInfos:      file_forgepoint_experiment_v1_experiment_proto_msgTypes,
	}.Build()
	File_forgepoint_experiment_v1_experiment_proto = out.File
	file_forgepoint_experiment_v1_experiment_proto_goTypes = nil
	file_forgepoint_experiment_v1_experiment_proto_depIdxs = nil
}
