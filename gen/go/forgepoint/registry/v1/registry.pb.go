// ============================================================================
// Forgepoint Model Registry Service Proto Definitions
// ============================================================================
//
// WHY: The Model Registry is the system of record for ML models on the
// platform. It answers two distinct questions for two distinct audiences:
//   1. WRITE side (ML engineers, pipelines): "register this model, cut a new
//      version, promote v3 to production, archive this old model."
//   2. READ side (serving, gateway, UI, monitor): "what is the production
//      version of model X right now? list all models tagged team=search."
// These two access patterns have wildly different shapes and SLAs, which is
// exactly why this service is built on CQRS.
//
// PATTERN — CQRS (Command Query Responsibility Segregation):
//   COMMAND (write) path: RPCs that mutate state (RegisterModel, CreateVersion,
//   PromoteVersion, DeleteModel) write to the NORMALIZED PostgreSQL store —
//   the source of truth with strong consistency, FK integrity, transactions.
//   On commit, the service publishes a domain event to NATS.
//
//   QUERY (read) path: RPCs that only read (GetModel, ListModels, GetVersion,
//   ListVersions, SearchByTag) are served from a DENORMALIZED Redis projection
//   built by a NATS consumer reacting to those same events. Reads are O(1)
//   key lookups / sorted-set range scans — no joins, no table scans.
//
//   ASCII — the CQRS loop this API realizes:
//
//     WRITE                                            READ
//     ─────                                            ────
//     RegisterModel ─► Postgres (tx) ─► publish ─┐
//     CreateVersion ─►  (truth)        NATS event │
//     PromoteVersion ► commit                     │
//                                                 ▼
//                                    fp.models.>  projection consumer
//                                                 │  (idempotent)
//                                                 ▼
//                                          Redis projection
//                                                 ▲
//                            GetModel / ListModels / GetVersion ─┘
//                            SearchByTag  (eventually consistent)
//
//   WHY CQRS HERE:
//     - Read:write ratio is enormous. Every inference request path may ask
//       "what's the prod version of model X?" thousands of times per second;
//       models are registered/promoted rarely. Separating the stores lets us
//       scale and optimize each independently (Redis read replicas vs a single
//       Postgres primary for writes).
//     - The read model is shaped for the query ("latest production version")
//       so we never compute it at read time — the projection precomputes it.
//     - Tradeoff = EVENTUAL CONSISTENCY: after a write commits, there is a
//       brief window before the Redis projection reflects it. We accept this
//       and document it on every read RPC. Clients that need read-your-writes
//       (e.g., a CI step that registers then immediately deploys) must either
//       poll, or the orchestrator consumes the event directly rather than
//       polling the read API. See the "consistency" note on each query RPC.
//
//   ALTERNATIVES CONSIDERED:
//     - Single store (Postgres only), read and write from the same tables:
//       simplest, strongly consistent, no projection lag. Rejected as the
//       teaching vehicle (the whole point of this service is to demonstrate
//       CQRS) AND because the hot read ("prod version of X") would hammer
//       Postgres on the inference path.
//     - Event Sourcing (store events, not state): more powerful (full audit,
//       time-travel, rebuild any projection) but heavier. We reserve event
//       sourcing for the Feature Store. Registry stores current state in
//       Postgres and uses events only to feed the read projection — a lighter
//       "CQRS without event sourcing" variant, which is the common production
//       starting point (see Greg Young's guidance; also MLflow's registry is
//       essentially a normalized SQL store with a query layer on top).
//
// REAL-WORLD COMPARISON:
//   - MLflow Model Registry: models → versions → stages (None/Staging/
//     Production/Archived). Our stage enum mirrors this so the mental model
//     transfers directly. We add explicit events + a Redis read model.
//   - SageMaker Model Registry: model package groups → versioned packages with
//     approval status. Our PromoteVersion is the analog of approval transitions.
//   - Docker/OCI registries: name + immutable versioned artifacts addressed by
//     digest — same "named thing with immutable versions" core idea.
//
// SECURITY / MASS-ASSIGNMENT POSTURE (applies to every write RPC below):
//   Server-authoritative fields are NEVER accepted on write requests. The
//   caller does not get to set: id, owner_id, team (derived from the caller's
//   auth claims), stage/status (only PromoteVersion may change stage, only
//   ConfirmVersionUpload may change status), created_at/updated_at/archived_at,
//   or artifact_path/digest/size_bytes (MEASURED by the storage layer on upload,
//   never client-asserted — a forged digest defeats content integrity, a forged
//   size lets a tenant dodge storage billing). Accepting any of these would let a
//   caller forge ownership, backdate records, or jump a version straight to
//   production — classic mass-assignment escalation. We model only the genuinely
//   client-supplied fields on *Request messages and derive the rest server-side.
//
// TENANCY SCOPING (authz from claims, not the body):
//   There is intentionally NO `team` / `owner_id` field on any list/get/write
//   request. Team scoping and ownership are derived from the caller's validated
//   TokenClaims server-side, so a caller can neither list another team's models
//   nor register into another team's namespace. The few filter fields that DO
//   stay on requests (task_type_filter, framework_filter, tag key/value) only
//   NARROW within the caller's already-scoped view — they never widen it.
//
// PAGINATION / DoS BOUND (a contract invariant, not just per-RPC prose):
//   Every list RPC takes common.v1.PaginationRequest. page_size DEFAULTS to 20
//   and is HARD-CAPPED AT 100 server-side — a larger request is silently clamped
//   to 100, never honored. This is the platform-wide cap documented on
//   common.v1.PaginationRequest (default 20 / max 100) and it bounds Redis
//   ZRANGE/SMEMBERS work and response size (a memory/CPU DoS guard). proto3 has no
//   numeric-bound syntax and protovalidate is intentionally not a dependency here,
//   so the cap is expressed as this explicit, uniform contract clause and enforced
//   in the handler; it is NOT advisory. There are no batch-write RPCs in this
//   service, so no separate batch-size cap is needed.
//
// VERSIONING: Package path includes v1 (Buf/Google convention). Breaking
// changes require a new forgepoint.registry.v2 package; both coexist during
// migration. buf breaking (FILE level) guards this file.
// ============================================================================

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.11
// 	protoc        (unknown)
// source: forgepoint/registry/v1/registry.proto

package registryv1

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
// ModelStage — the lifecycle state of a *version*, not the model.
// ============================================================================
//
// WHY stage lives on the VERSION, not the Model:
//
//	"Production" is a property of a specific set of weights, not of the model
//	name. Model "fraud-detector" always exists; what changes over time is
//	WHICH version is serving production traffic. Putting the stage on
//	ModelVersion lets exactly one version per model occupy PRODUCTION while
//	older versions sit in ARCHIVED and newer candidates wait in STAGING.
//
// THE TRANSITION RULES (enforced server-side):
//
//	DEV ──► STAGING ──► PRODUCTION ──► ARCHIVED
//	 └──────────────────────────────────► ARCHIVED (abandon a candidate)
//	Promoting a version to PRODUCTION auto-demotes the *current* production
//	version of the same model to ARCHIVED (single-prod invariant). This is the
//	atomic swap that PromoteVersion performs and emits as ModelPromoted.
//
// WHY an enum, not a free string:
//
//	Closed set, validated at the wire boundary, prevents typos like "prod" vs
//	"production" silently fragmenting the read projection's stage index.
//	Buf STANDARD requires the _UNSPECIFIED zero value + ENUM_NAME prefix on
//	every value (so the int 0 never accidentally means "DEV").
//
// CANONICAL-EVENT MAPPING (the decoupling point):
//
//	This enum is the SERVICE/API enum. The event bus carries a MIRROR enum,
//	events.v1.ModelStage, with byte-identical values (UNSPECIFIED=0, DEV=1,
//	STAGING=2, PRODUCTION=3, ARCHIVED=4). They are kept numerically aligned on
//	purpose, but the handler still maps registry.ModelStage <-> events.ModelStage
//	explicitly at the publish/consume boundary rather than casting — so that if
//	the API enum ever evolves independently (a new internal stage that isn't a
//	published fact), the event contract is insulated. This is the same "event
//	schema is decoupled from API schema" discipline events.proto enforces.
//
// ============================================================================
type ModelStage int32

const (
	// Default zero value. Means "stage not set / unknown". A freshly created
	// version starts at MODEL_STAGE_DEV explicitly — the server never leaves a
	// version in UNSPECIFIED. Required by Buf ENUM_ZERO_VALUE_SUFFIX.
	ModelStage_MODEL_STAGE_UNSPECIFIED ModelStage = 0
	// Newly created version. Artifact may still be uploading. Not eligible for
	// serving. This is where every CreateVersion lands.
	ModelStage_MODEL_STAGE_DEV ModelStage = 1
	// Validated candidate undergoing pre-production checks (shadow traffic,
	// canary eval). Promotable to PRODUCTION.
	ModelStage_MODEL_STAGE_STAGING ModelStage = 2
	// The version currently serving production traffic. INVARIANT: at most one
	// PRODUCTION version per model at any time (enforced by PromoteVersion).
	ModelStage_MODEL_STAGE_PRODUCTION ModelStage = 3
	// Retired version. Kept for lineage/audit and rollback, but not served and
	// hidden from default list views. Terminal state.
	ModelStage_MODEL_STAGE_ARCHIVED ModelStage = 4
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
	return file_forgepoint_registry_v1_registry_proto_enumTypes[0].Descriptor()
}

func (ModelStage) Type() protoreflect.EnumType {
	return &file_forgepoint_registry_v1_registry_proto_enumTypes[0]
}

func (x ModelStage) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ModelStage.Descriptor instead.
func (ModelStage) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{0}
}

// ============================================================================
// VersionStatus — readiness of a version's artifact, orthogonal to stage.
// ============================================================================
//
// WHY separate from ModelStage:
//
//	Stage answers "where in the lifecycle is this version?" (a human/policy
//	decision). Status answers "is the artifact physically present and usable?"
//	(a system fact). A version can be in STAGING (stage) yet PENDING_UPLOAD
//	(status) because the weights haven't finished uploading to MinIO. Conflating
//	them would mean we couldn't represent "promoted but artifact still arriving".
//	Serving must check BOTH: stage == PRODUCTION AND status == READY.
//
// ============================================================================
type VersionStatus int32

const (
	// Required zero value. Unknown/unset status.
	VersionStatus_VERSION_STATUS_UNSPECIFIED VersionStatus = 0
	// Version row created; artifact upload not yet confirmed. CreateVersion
	// returns a version in this state alongside a presigned upload URL.
	VersionStatus_VERSION_STATUS_PENDING_UPLOAD VersionStatus = 1
	// Artifact present in object storage and validated (checksum verified).
	// Only READY versions are eligible to be promoted/served. The PENDING_UPLOAD
	// -> READY transition is driven by ConfirmVersionUpload (below) and is the
	// moment that publishes events.ModelVersionReady (fp.models.version.ready) —
	// the edge serving/billing/pipeline-orchestrator wait on (see conflict #3 in
	// events.proto). ModelVersionCreated fires earlier, at row creation; it is NOT
	// a signal the artifact is usable.
	VersionStatus_VERSION_STATUS_READY VersionStatus = 2
	// Upload failed, checksum mismatch, or validation error. Not promotable.
	// Set by ConfirmVersionUpload when verification fails (no ModelVersionReady
	// is emitted in that case — there is nothing servable to announce).
	VersionStatus_VERSION_STATUS_FAILED VersionStatus = 3
)

// Enum value maps for VersionStatus.
var (
	VersionStatus_name = map[int32]string{
		0: "VERSION_STATUS_UNSPECIFIED",
		1: "VERSION_STATUS_PENDING_UPLOAD",
		2: "VERSION_STATUS_READY",
		3: "VERSION_STATUS_FAILED",
	}
	VersionStatus_value = map[string]int32{
		"VERSION_STATUS_UNSPECIFIED":    0,
		"VERSION_STATUS_PENDING_UPLOAD": 1,
		"VERSION_STATUS_READY":          2,
		"VERSION_STATUS_FAILED":         3,
	}
)

func (x VersionStatus) Enum() *VersionStatus {
	p := new(VersionStatus)
	*p = x
	return p
}

func (x VersionStatus) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (VersionStatus) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_registry_v1_registry_proto_enumTypes[1].Descriptor()
}

func (VersionStatus) Type() protoreflect.EnumType {
	return &file_forgepoint_registry_v1_registry_proto_enumTypes[1]
}

func (x VersionStatus) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use VersionStatus.Descriptor instead.
func (VersionStatus) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{1}
}

// ============================================================================
// Model — the named, versioned thing. The aggregate root.
// ============================================================================
//
// WHY a Model has no inline stage/version fields except a denormalized pointer:
//
//	A Model is the stable identity ("fraud-detector"); its versions carry the
//	mutable lifecycle. We DO denormalize one read-optimized pointer onto the
//	read projection — latest_version / production_version — because the single
//	most common query is "give me the prod version of model X" and we don't
//	want clients to ListVersions and scan. That denormalization is a READ-MODEL
//	concern (Redis), surfaced here so GetModelResponse can include it.
//
// SECURITY: id, owner_id, team, created_at, updated_at, archived_at are all
// SERVER-AUTHORITATIVE — populated by the service, never accepted from clients.
// owner_id and team come from the caller's validated TokenClaims, not the body.
// ============================================================================
type Model struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// UUID v4. Primary key, immutable. Server-generated on RegisterModel.
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// Unique, human-friendly model name within a team namespace, e.g.
	// "fraud-detector". Used as the addressable handle by serving/gateway.
	// Client-supplied at registration; immutable afterward (rename = new model).
	Name string `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	// Free-text description. Client-supplied at registration; mutable afterward
	// via UpdateModel (see below). Name is NOT mutable (rename = new model).
	Description string `protobuf:"bytes,3,opt,name=description,proto3" json:"description,omitempty"`
	// The user_id of the registrant. SERVER-AUTHORITATIVE: derived from the
	// caller's auth claims, NOT from the request body. Identifies accountability
	// and is used by billing/notification to attribute cost and route alerts.
	OwnerId string `protobuf:"bytes,4,opt,name=owner_id,json=ownerId,proto3" json:"owner_id,omitempty"`
	// Team namespace (e.g., "ml-platform"). SERVER-AUTHORITATIVE: copied from the
	// caller's TokenClaims.team. Scopes visibility and billing. Never client-set,
	// so a caller cannot register a model into another team's namespace.
	Team string `protobuf:"bytes,5,opt,name=team,proto3" json:"team,omitempty"`
	// ML framework, e.g. "pytorch", "tensorflow", "sklearn", "onnx".
	// Client-supplied descriptive metadata; influences how serving loads it.
	Framework string `protobuf:"bytes,6,opt,name=framework,proto3" json:"framework,omitempty"`
	// Task type, e.g. "classification", "regression", "embedding", "llm".
	// Client-supplied descriptive metadata; used for UI grouping and routing.
	TaskType string `protobuf:"bytes,7,opt,name=task_type,json=taskType,proto3" json:"task_type,omitempty"`
	// Arbitrary key/value tags for search and organization, e.g.
	// {"domain": "fraud", "pii": "false"}. Drives SearchByTag's Redis tag sets.
	// Client-supplied (a map field is order-independent and dedupes keys, which
	// is exactly the semantics we want for tags — unlike repeated ModelTag).
	Tags map[string]string `protobuf:"bytes,8,rep,name=tags,proto3" json:"tags,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	// READ-MODEL CONVENIENCE (eventually consistent): the version string of this
	// model's current PRODUCTION version, or "" if none is in production. Lets a
	// client learn the serving version from a single GetModel without a second
	// call. Populated only on the read path (Redis projection).
	ProductionVersion string `protobuf:"bytes,9,opt,name=production_version,json=productionVersion,proto3" json:"production_version,omitempty"`
	// READ-MODEL CONVENIENCE (eventually consistent): the most recently created
	// version string regardless of stage. "" if the model has no versions yet.
	LatestVersion string `protobuf:"bytes,10,opt,name=latest_version,json=latestVersion,proto3" json:"latest_version,omitempty"`
	// When the model was first registered. SERVER-AUTHORITATIVE, immutable.
	CreatedAt *timestamppb.Timestamp `protobuf:"bytes,11,opt,name=created_at,json=createdAt,proto3" json:"created_at,omitempty"`
	// Last time the model row (or its denormalized pointers) changed.
	// SERVER-AUTHORITATIVE.
	UpdatedAt *timestamppb.Timestamp `protobuf:"bytes,12,opt,name=updated_at,json=updatedAt,proto3" json:"updated_at,omitempty"`
	// Set when the model is soft-deleted/archived; null/zero while active.
	// SERVER-AUTHORITATIVE. DeleteModel sets this rather than hard-deleting, so
	// lineage and audit survive (and the read projection can hide it from lists).
	ArchivedAt    *timestamppb.Timestamp `protobuf:"bytes,13,opt,name=archived_at,json=archivedAt,proto3" json:"archived_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Model) Reset() {
	*x = Model{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Model) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Model) ProtoMessage() {}

func (x *Model) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Model.ProtoReflect.Descriptor instead.
func (*Model) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{0}
}

func (x *Model) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Model) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Model) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *Model) GetOwnerId() string {
	if x != nil {
		return x.OwnerId
	}
	return ""
}

func (x *Model) GetTeam() string {
	if x != nil {
		return x.Team
	}
	return ""
}

func (x *Model) GetFramework() string {
	if x != nil {
		return x.Framework
	}
	return ""
}

func (x *Model) GetTaskType() string {
	if x != nil {
		return x.TaskType
	}
	return ""
}

func (x *Model) GetTags() map[string]string {
	if x != nil {
		return x.Tags
	}
	return nil
}

func (x *Model) GetProductionVersion() string {
	if x != nil {
		return x.ProductionVersion
	}
	return ""
}

func (x *Model) GetLatestVersion() string {
	if x != nil {
		return x.LatestVersion
	}
	return ""
}

func (x *Model) GetCreatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CreatedAt
	}
	return nil
}

func (x *Model) GetUpdatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.UpdatedAt
	}
	return nil
}

func (x *Model) GetArchivedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.ArchivedAt
	}
	return nil
}

// ============================================================================
// ModelVersion — an immutable, point-in-time snapshot of a model's artifact.
// ============================================================================
//
// WHY versions are immutable except for stage/status transitions:
//
//	The artifact (weights) and metrics captured at training time must never
//	change — that's the whole point of a registry (reproducibility, rollback).
//	The ONLY mutable aspects are the lifecycle stage (DEV→STAGING→PROD→ARCHIVED,
//	via PromoteVersion) and status (PENDING_UPLOAD→READY/FAILED, set when the
//	upload completes). Everything else is write-once.
//
// SECURITY: id, model_id binding, stage, status, artifact_path, artifact_digest,
// size_bytes, created_by, created_at are SERVER-AUTHORITATIVE. A client cannot
// hand us a version that is already PRODUCTION or claim an arbitrary digest.
// ============================================================================
type ModelVersion struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// UUID v4. Primary key for the version row. Server-generated.
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// FK to the owning Model.id. SERVER-AUTHORITATIVE: bound from the path/request
	// target, not a forgeable body field — you create a version *of a model*.
	ModelId string `protobuf:"bytes,2,opt,name=model_id,json=modelId,proto3" json:"model_id,omitempty"`
	// Human-facing version label, e.g. "1", "1.2.0", "2024-06-17-a". Unique per
	// model. SERVER-AUTHORITATIVE by default: the service assigns a monotonic
	// version unless the client explicitly requests one (see CreateVersionRequest).
	Version string `protobuf:"bytes,3,opt,name=version,proto3" json:"version,omitempty"`
	// Free-text changelog/notes for this version. Client-supplied at creation.
	Description string `protobuf:"bytes,4,opt,name=description,proto3" json:"description,omitempty"`
	// Object-storage location of the artifact (e.g. "s3://fp-models/<model>/<ver>/
	// model.onnx"). SERVER-AUTHORITATIVE: computed by the storage layer from the
	// presigned-upload flow. Clients never set where bytes land — preventing path
	// traversal / overwriting another model's artifact.
	ArtifactPath string `protobuf:"bytes,5,opt,name=artifact_path,json=artifactPath,proto3" json:"artifact_path,omitempty"`
	// Content digest (e.g. "sha256:..."), verified server-side after upload.
	// SERVER-AUTHORITATIVE. Enables content-addressable integrity + dedupe and
	// lets serving confirm it loaded exactly the registered bytes.
	ArtifactDigest string `protobuf:"bytes,6,opt,name=artifact_digest,json=artifactDigest,proto3" json:"artifact_digest,omitempty"`
	// Artifact size in bytes. SERVER-AUTHORITATIVE (measured on upload). Surfaced
	// for UI and for Billing to meter storage. A client-supplied size would let a
	// caller under-report storage usage — hence server-measured only.
	SizeBytes int64 `protobuf:"varint,7,opt,name=size_bytes,json=sizeBytes,proto3" json:"size_bytes,omitempty"`
	// Training/eval metrics as a free-form struct, e.g.
	// {"accuracy": 0.97, "auc": 0.99, "loss": 0.03}. google.protobuf.Struct (not
	// a fixed message) because metric sets vary per model/task and we don't want a
	// schema change every time a new metric appears. Client-supplied at creation;
	// stored in Postgres JSONB.
	Metrics *structpb.Struct `protobuf:"bytes,8,opt,name=metrics,proto3" json:"metrics,omitempty"`
	// Lifecycle stage. SERVER-AUTHORITATIVE: starts at MODEL_STAGE_DEV; only
	// PromoteVersion may advance it. Never accepted on CreateVersion (else a
	// caller could create straight into PRODUCTION, bypassing review).
	Stage ModelStage `protobuf:"varint,9,opt,name=stage,proto3,enum=forgepoint.registry.v1.ModelStage" json:"stage,omitempty"`
	// Artifact readiness. SERVER-AUTHORITATIVE: PENDING_UPLOAD on creation, flipped
	// to READY/FAILED by the storage layer once the upload is confirmed.
	Status VersionStatus `protobuf:"varint,10,opt,name=status,proto3,enum=forgepoint.registry.v1.VersionStatus" json:"status,omitempty"`
	// The user_id that created this version. SERVER-AUTHORITATIVE from auth claims.
	CreatedBy string `protobuf:"bytes,11,opt,name=created_by,json=createdBy,proto3" json:"created_by,omitempty"`
	// When this version was created. SERVER-AUTHORITATIVE, immutable.
	CreatedAt     *timestamppb.Timestamp `protobuf:"bytes,12,opt,name=created_at,json=createdAt,proto3" json:"created_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ModelVersion) Reset() {
	*x = ModelVersion{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ModelVersion) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ModelVersion) ProtoMessage() {}

func (x *ModelVersion) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ModelVersion.ProtoReflect.Descriptor instead.
func (*ModelVersion) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{1}
}

func (x *ModelVersion) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *ModelVersion) GetModelId() string {
	if x != nil {
		return x.ModelId
	}
	return ""
}

func (x *ModelVersion) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *ModelVersion) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *ModelVersion) GetArtifactPath() string {
	if x != nil {
		return x.ArtifactPath
	}
	return ""
}

func (x *ModelVersion) GetArtifactDigest() string {
	if x != nil {
		return x.ArtifactDigest
	}
	return ""
}

func (x *ModelVersion) GetSizeBytes() int64 {
	if x != nil {
		return x.SizeBytes
	}
	return 0
}

func (x *ModelVersion) GetMetrics() *structpb.Struct {
	if x != nil {
		return x.Metrics
	}
	return nil
}

func (x *ModelVersion) GetStage() ModelStage {
	if x != nil {
		return x.Stage
	}
	return ModelStage_MODEL_STAGE_UNSPECIFIED
}

func (x *ModelVersion) GetStatus() VersionStatus {
	if x != nil {
		return x.Status
	}
	return VersionStatus_VERSION_STATUS_UNSPECIFIED
}

func (x *ModelVersion) GetCreatedBy() string {
	if x != nil {
		return x.CreatedBy
	}
	return ""
}

func (x *ModelVersion) GetCreatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CreatedAt
	}
	return nil
}

// RegisterModelRequest carries ONLY client-owned fields. Note the deliberate
// absence of id, owner_id, team, timestamps, production_version, latest_version
// — all server-authoritative (see mass-assignment note in the header).
type RegisterModelRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Desired unique model name within the caller's team. Validated for format
	// and uniqueness server-side; collision → ALREADY_EXISTS.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// Optional human description.
	Description string `protobuf:"bytes,2,opt,name=description,proto3" json:"description,omitempty"`
	// Framework descriptor ("pytorch", "onnx", ...).
	Framework string `protobuf:"bytes,3,opt,name=framework,proto3" json:"framework,omitempty"`
	// Task type descriptor ("classification", "llm", ...).
	TaskType string `protobuf:"bytes,4,opt,name=task_type,json=taskType,proto3" json:"task_type,omitempty"`
	// Initial tags. Optional; more can be added later.
	Tags map[string]string `protobuf:"bytes,5,rep,name=tags,proto3" json:"tags,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	// IDEMPOTENCY KEY (write-safety). RegisterModel is a non-idempotent create:
	// a retried request (network blip, client retry) must NOT create a second
	// model. The server stores this client-generated UUID with the created row;
	// a repeat with the same key returns the original Model instead of erroring.
	// WHY surface it in the API rather than rely on name-uniqueness: name
	// collision would surface as ALREADY_EXISTS (an error the client must
	// special-case), whereas an idempotency key makes the retry a clean success
	// returning the same resource — the Stripe-style contract. Empty = the server
	// treats the call as one-shot (still protected by name uniqueness).
	IdempotencyKey string `protobuf:"bytes,6,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *RegisterModelRequest) Reset() {
	*x = RegisterModelRequest{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RegisterModelRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RegisterModelRequest) ProtoMessage() {}

func (x *RegisterModelRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RegisterModelRequest.ProtoReflect.Descriptor instead.
func (*RegisterModelRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{2}
}

func (x *RegisterModelRequest) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *RegisterModelRequest) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *RegisterModelRequest) GetFramework() string {
	if x != nil {
		return x.Framework
	}
	return ""
}

func (x *RegisterModelRequest) GetTaskType() string {
	if x != nil {
		return x.TaskType
	}
	return ""
}

func (x *RegisterModelRequest) GetTags() map[string]string {
	if x != nil {
		return x.Tags
	}
	return nil
}

func (x *RegisterModelRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// RegisterModelResponse wraps the created Model (write path returns the truth
// from Postgres, so it is immediately consistent for the registrant — the
// eventual-consistency caveat applies only to the READ RPCs below).
type RegisterModelResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The newly registered model. production_version/latest_version are empty
	// (no versions yet); owner_id/team/timestamps are server-populated.
	Model         *Model `protobuf:"bytes,1,opt,name=model,proto3" json:"model,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *RegisterModelResponse) Reset() {
	*x = RegisterModelResponse{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RegisterModelResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RegisterModelResponse) ProtoMessage() {}

func (x *RegisterModelResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RegisterModelResponse.ProtoReflect.Descriptor instead.
func (*RegisterModelResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{3}
}

func (x *RegisterModelResponse) GetModel() *Model {
	if x != nil {
		return x.Model
	}
	return nil
}

// ----------------------------------------------------------------------------
// UpdateModel (COMMAND / write path) — mutate the small mutable surface
// ----------------------------------------------------------------------------
//
// WHY a dedicated UpdateModel (the design/CLI need it, and the old code only
// hinted at it):
//
//	A Model's IDENTITY (id, name, owner_id, team) and its lineage timestamps are
//	immutable/server-owned, but two fields legitimately change over a model's
//	life: its human DESCRIPTION and its TAGS (re-classify "domain", flip a
//	"deprecated" flag). Without an UpdateModel the only way to fix a typo'd
//	description would be to re-register — losing the id and all version lineage.
//
// SECURITY / MASS-ASSIGNMENT (the whole reason this message is so small):
//
//	Only description and tags are accepted. name is OMITTED on purpose (renaming
//	an addressable handle out from under serving/gateway is a footgun — a rename
//	is modeled as a new model). id selects the target but is never itself
//	mutated. owner_id/team/stage/status/timestamps/artifact fields are absent so
//	a caller can NEVER reassign ownership, jump teams, or backdate via this RPC.
//
// PARTIAL-UPDATE SEMANTICS ("how do you patch in proto3?"):
//
//	proto3 scalars have no presence, so "field omitted" vs "field set to empty"
//	are indistinguishable for a bare string. We make the contract explicit with
//	update_mask-style booleans (update_description / replace_tags) rather than a
//	full google.protobuf.FieldMask, because the mutable surface is tiny and two
//	flags are clearer to read than a path-string mask. This avoids the classic
//	PATCH bug where an unset field silently clears stored data.
type UpdateModelRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The model to update. Required. Server-authoritative target binding (the
	// model must belong to the caller's team — enforced from auth claims).
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// New description. Applied ONLY when update_description is true (so an unset
	// description doesn't blank an existing one — see partial-update note above).
	Description string `protobuf:"bytes,2,opt,name=description,proto3" json:"description,omitempty"`
	// When true, `description` (even if empty) replaces the stored description.
	// When false, the description is left untouched.
	UpdateDescription bool `protobuf:"varint,3,opt,name=update_description,json=updateDescription,proto3" json:"update_description,omitempty"`
	// Replacement tag set. Applied ONLY when replace_tags is true. Tags are
	// REPLACED wholesale (not merged) — the simplest, least-surprising semantics
	// for a map; a caller that wants to add one tag sends the full desired set.
	Tags map[string]string `protobuf:"bytes,4,rep,name=tags,proto3" json:"tags,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	// When true, `tags` replaces the stored tag map (an empty map clears all
	// tags). When false, tags are left untouched.
	ReplaceTags bool `protobuf:"varint,5,opt,name=replace_tags,json=replaceTags,proto3" json:"replace_tags,omitempty"`
	// IDEMPOTENCY KEY: an update is naturally idempotent (applying the same patch
	// twice yields the same state), but the key still guards against a retry
	// re-emitting any change event / double-bumping updated_at. Optional.
	IdempotencyKey string `protobuf:"bytes,6,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *UpdateModelRequest) Reset() {
	*x = UpdateModelRequest{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpdateModelRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdateModelRequest) ProtoMessage() {}

func (x *UpdateModelRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdateModelRequest.ProtoReflect.Descriptor instead.
func (*UpdateModelRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{4}
}

func (x *UpdateModelRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *UpdateModelRequest) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *UpdateModelRequest) GetUpdateDescription() bool {
	if x != nil {
		return x.UpdateDescription
	}
	return false
}

func (x *UpdateModelRequest) GetTags() map[string]string {
	if x != nil {
		return x.Tags
	}
	return nil
}

func (x *UpdateModelRequest) GetReplaceTags() bool {
	if x != nil {
		return x.ReplaceTags
	}
	return false
}

func (x *UpdateModelRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// UpdateModelResponse returns the updated Model (write path → Postgres truth, so
// immediately consistent for the caller; the READ RPCs remain eventually
// consistent). NOTE: UpdateModel publishes NO canonical lifecycle event —
// description/tag edits are not in the events.proto contract (no consumer reacts
// to them), so there is nothing to emit. The Redis projection is refreshed by an
// internal projection refresh, not a published domain event.
type UpdateModelResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Model         *Model                 `protobuf:"bytes,1,opt,name=model,proto3" json:"model,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UpdateModelResponse) Reset() {
	*x = UpdateModelResponse{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpdateModelResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdateModelResponse) ProtoMessage() {}

func (x *UpdateModelResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdateModelResponse.ProtoReflect.Descriptor instead.
func (*UpdateModelResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{5}
}

func (x *UpdateModelResponse) GetModel() *Model {
	if x != nil {
		return x.Model
	}
	return nil
}

// GetModelRequest targets a model by id OR name (one of). We allow name lookup
// because callers (serving, CLI) usually know the human name, not the UUID.
type GetModelRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The model's UUID. Takes precedence if both are set.
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// The model's name within the caller's team. Used when id is empty.
	Name          string `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetModelRequest) Reset() {
	*x = GetModelRequest{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetModelRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetModelRequest) ProtoMessage() {}

func (x *GetModelRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetModelRequest.ProtoReflect.Descriptor instead.
func (*GetModelRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{6}
}

func (x *GetModelRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *GetModelRequest) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

// GetModelResponse wraps the Model from the READ projection.
// CONSISTENCY: served from Redis and therefore EVENTUALLY CONSISTENT — a model
// registered milliseconds ago may briefly 404 here until the projection catches
// up. Callers needing read-your-writes should react to the ModelRegistered
// event instead of polling.
type GetModelResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Model         *Model                 `protobuf:"bytes,1,opt,name=model,proto3" json:"model,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetModelResponse) Reset() {
	*x = GetModelResponse{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetModelResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetModelResponse) ProtoMessage() {}

func (x *GetModelResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetModelResponse.ProtoReflect.Descriptor instead.
func (*GetModelResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{7}
}

func (x *GetModelResponse) GetModel() *Model {
	if x != nil {
		return x.Model
	}
	return nil
}

// ListModelsRequest lists models in the caller's team (team scoping is applied
// server-side from auth claims — there is intentionally no team field here, so
// a caller cannot list another team's models).
type ListModelsRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Optional filter: only models whose task_type equals this value. Empty = all.
	TaskTypeFilter string `protobuf:"bytes,1,opt,name=task_type_filter,json=taskTypeFilter,proto3" json:"task_type_filter,omitempty"`
	// Optional filter: only models using this framework. Empty = all.
	FrameworkFilter string `protobuf:"bytes,2,opt,name=framework_filter,json=frameworkFilter,proto3" json:"framework_filter,omitempty"`
	// When true, ARCHIVED models are included. Default false hides them — the
	// common case is "show me active models". Mirrors the projection's choice to
	// drop archived models from the default models:list set.
	IncludeArchived bool `protobuf:"varint,3,opt,name=include_archived,json=includeArchived,proto3" json:"include_archived,omitempty"`
	// Cursor-based pagination (reused from common.proto). page_size defaults to
	// 20 and is CAPPED AT 100 server-side — a request for more is silently
	// clamped to 100 to bound Redis ZRANGE work and response size (DoS guard).
	Pagination    *v1.PaginationRequest `protobuf:"bytes,4,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListModelsRequest) Reset() {
	*x = ListModelsRequest{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[8]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListModelsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListModelsRequest) ProtoMessage() {}

func (x *ListModelsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[8]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListModelsRequest.ProtoReflect.Descriptor instead.
func (*ListModelsRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{8}
}

func (x *ListModelsRequest) GetTaskTypeFilter() string {
	if x != nil {
		return x.TaskTypeFilter
	}
	return ""
}

func (x *ListModelsRequest) GetFrameworkFilter() string {
	if x != nil {
		return x.FrameworkFilter
	}
	return ""
}

func (x *ListModelsRequest) GetIncludeArchived() bool {
	if x != nil {
		return x.IncludeArchived
	}
	return false
}

func (x *ListModelsRequest) GetPagination() *v1.PaginationRequest {
	if x != nil {
		return x.Pagination
	}
	return nil
}

type ListModelsResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The page of models, ordered newest-first by created_at (the projection's
	// sorted-set score). EVENTUALLY CONSISTENT (Redis read model).
	Models []*Model `protobuf:"bytes,1,rep,name=models,proto3" json:"models,omitempty"`
	// next_page_token + total_count. total_count may be -1 if computing the exact
	// count is expensive (see common.proto PaginationResponse).
	Pagination    *v1.PaginationResponse `protobuf:"bytes,2,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListModelsResponse) Reset() {
	*x = ListModelsResponse{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[9]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListModelsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListModelsResponse) ProtoMessage() {}

func (x *ListModelsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[9]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListModelsResponse.ProtoReflect.Descriptor instead.
func (*ListModelsResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{9}
}

func (x *ListModelsResponse) GetModels() []*Model {
	if x != nil {
		return x.Models
	}
	return nil
}

func (x *ListModelsResponse) GetPagination() *v1.PaginationResponse {
	if x != nil {
		return x.Pagination
	}
	return nil
}

// ----------------------------------------------------------------------------
// SearchByTag (QUERY / read path — paginated)
// ----------------------------------------------------------------------------
//
// WHY a dedicated RPC rather than a tag filter on ListModels:
//
//	Tag search hits a DIFFERENT Redis structure — the per-tag set
//	models:tag:{key}:{value} — which the projection maintains specifically so
//	tag lookups are O(members) set reads, not scans. A separate RPC makes that
//	distinct read shape explicit and keeps ListModels' filter surface small.
//
// ----------------------------------------------------------------------------
type SearchByTagRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Tag key to match, e.g. "domain".
	Key string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	// Tag value to match, e.g. "fraud". (key,value) together select the
	// models:tag:{key}:{value} set in the projection.
	Value string `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	// Cursor-based pagination; same 100-item server-side cap as ListModels.
	Pagination    *v1.PaginationRequest `protobuf:"bytes,3,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SearchByTagRequest) Reset() {
	*x = SearchByTagRequest{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[10]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SearchByTagRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SearchByTagRequest) ProtoMessage() {}

func (x *SearchByTagRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[10]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SearchByTagRequest.ProtoReflect.Descriptor instead.
func (*SearchByTagRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{10}
}

func (x *SearchByTagRequest) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *SearchByTagRequest) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

func (x *SearchByTagRequest) GetPagination() *v1.PaginationRequest {
	if x != nil {
		return x.Pagination
	}
	return nil
}

// SearchByTagResponse reuses the same paginated shape as ListModels for client
// uniformity. (Each list RPC still has its OWN response type to satisfy Buf's
// RPC_RESPONSE_STANDARD_NAME — we don't share ListModelsResponse.)
type SearchByTagResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Models        []*Model               `protobuf:"bytes,1,rep,name=models,proto3" json:"models,omitempty"`
	Pagination    *v1.PaginationResponse `protobuf:"bytes,2,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SearchByTagResponse) Reset() {
	*x = SearchByTagResponse{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[11]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SearchByTagResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SearchByTagResponse) ProtoMessage() {}

func (x *SearchByTagResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[11]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SearchByTagResponse.ProtoReflect.Descriptor instead.
func (*SearchByTagResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{11}
}

func (x *SearchByTagResponse) GetModels() []*Model {
	if x != nil {
		return x.Models
	}
	return nil
}

func (x *SearchByTagResponse) GetPagination() *v1.PaginationResponse {
	if x != nil {
		return x.Pagination
	}
	return nil
}

// CreateVersionRequest creates a new version of an existing model. The artifact
// itself is NOT in this message — bytes go to object storage via the presigned
// URL returned in the response (see GetUploadURL note). This keeps large blobs
// off the gRPC path (gRPC messages are capped ~4MB by default and models can be
// gigabytes).
type CreateVersionRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Target model. SERVER-AUTHORITATIVE binding for the version's model_id —
	// validated to exist and belong to the caller's team.
	ModelId string `protobuf:"bytes,1,opt,name=model_id,json=modelId,proto3" json:"model_id,omitempty"`
	// OPTIONAL explicit version label. Empty = the server auto-assigns the next
	// monotonic version (the safe default — prevents accidental clobbering and
	// version races). When provided, it must be unique for the model. We allow it
	// because some teams pin meaningful versions (e.g., a git SHA or semver from
	// CI). stage/status are NOT settable here.
	Version string `protobuf:"bytes,2,opt,name=version,proto3" json:"version,omitempty"`
	// Changelog/notes for this version.
	Description string `protobuf:"bytes,3,opt,name=description,proto3" json:"description,omitempty"`
	// Training/eval metrics (free-form struct). Stored as JSONB.
	Metrics *structpb.Struct `protobuf:"bytes,4,opt,name=metrics,proto3" json:"metrics,omitempty"`
	// IDEMPOTENCY KEY: a retried CreateVersion must not mint a duplicate version
	// (which would waste a version number and a storage slot). Same semantics as
	// RegisterModel.idempotency_key — repeat with the same key returns the
	// original version + its (still valid) upload URL.
	IdempotencyKey string `protobuf:"bytes,5,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	// How long the returned presigned upload URL should remain valid, in seconds.
	// Optional; server clamps to a sane max (e.g., 3600s) to limit the window an
	// upload credential is usable. 0 = server default.
	UploadUrlTtlSeconds int64 `protobuf:"varint,6,opt,name=upload_url_ttl_seconds,json=uploadUrlTtlSeconds,proto3" json:"upload_url_ttl_seconds,omitempty"`
	unknownFields       protoimpl.UnknownFields
	sizeCache           protoimpl.SizeCache
}

func (x *CreateVersionRequest) Reset() {
	*x = CreateVersionRequest{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[12]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CreateVersionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateVersionRequest) ProtoMessage() {}

func (x *CreateVersionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[12]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateVersionRequest.ProtoReflect.Descriptor instead.
func (*CreateVersionRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{12}
}

func (x *CreateVersionRequest) GetModelId() string {
	if x != nil {
		return x.ModelId
	}
	return ""
}

func (x *CreateVersionRequest) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *CreateVersionRequest) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *CreateVersionRequest) GetMetrics() *structpb.Struct {
	if x != nil {
		return x.Metrics
	}
	return nil
}

func (x *CreateVersionRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

func (x *CreateVersionRequest) GetUploadUrlTtlSeconds() int64 {
	if x != nil {
		return x.UploadUrlTtlSeconds
	}
	return 0
}

// CreateVersionResponse returns the new version (status=PENDING_UPLOAD,
// stage=DEV) AND a presigned URL the client PUTs the artifact bytes to.
// WHY bundle the URL here instead of a separate round-trip: the create-then-
// upload flow is always paired, so returning both avoids a second RPC and a
// race where the version exists but the client lost the URL.
type CreateVersionResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The created version. status will be VERSION_STATUS_PENDING_UPLOAD until the
	// upload is confirmed; stage is MODEL_STAGE_DEV.
	Version *ModelVersion `protobuf:"bytes,1,opt,name=version,proto3" json:"version,omitempty"`
	// Presigned PUT URL for the artifact. Single-use, time-limited. The bytes
	// never traverse this gRPC service — they go straight to MinIO/S3. After the
	// PUT completes, the client (or an S3/MinIO bucket-notification bridge) calls
	// ConfirmVersionUpload, which server-side verifies the object and flips status
	// to READY (publishing events.ModelVersionReady). If this URL expires before
	// the upload finishes, GetUploadURL re-issues a fresh one for the same version.
	UploadUrl string `protobuf:"bytes,2,opt,name=upload_url,json=uploadUrl,proto3" json:"upload_url,omitempty"`
	// When upload_url stops being valid. Client must complete the PUT before this
	// (or call GetUploadURL for a fresh URL).
	UploadUrlExpiresAt *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=upload_url_expires_at,json=uploadUrlExpiresAt,proto3" json:"upload_url_expires_at,omitempty"`
	unknownFields      protoimpl.UnknownFields
	sizeCache          protoimpl.SizeCache
}

func (x *CreateVersionResponse) Reset() {
	*x = CreateVersionResponse{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[13]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CreateVersionResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateVersionResponse) ProtoMessage() {}

func (x *CreateVersionResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[13]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateVersionResponse.ProtoReflect.Descriptor instead.
func (*CreateVersionResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{13}
}

func (x *CreateVersionResponse) GetVersion() *ModelVersion {
	if x != nil {
		return x.Version
	}
	return nil
}

func (x *CreateVersionResponse) GetUploadUrl() string {
	if x != nil {
		return x.UploadUrl
	}
	return ""
}

func (x *CreateVersionResponse) GetUploadUrlExpiresAt() *timestamppb.Timestamp {
	if x != nil {
		return x.UploadUrlExpiresAt
	}
	return nil
}

// ----------------------------------------------------------------------------
// GetUploadURL (STORAGE) — (re)issue a presigned upload URL for a version
// ----------------------------------------------------------------------------
//
// WHY a standalone RPC when CreateVersion already returns an upload URL:
//
//	The create-then-PUT flow can break between the two steps — the presigned URL
//	expires (TTL elapsed), the client crashed before uploading, or a CI runner
//	was preempted. Without a way to RE-ISSUE the URL, the only recovery would be
//	to mint a NEW version (wasting a version number and a storage slot). This RPC
//	re-issues a fresh presigned PUT for an EXISTING version that is still
//	PENDING_UPLOAD, making the upload step resumable. It is idempotent/read-only
//	with respect to registry STATE (it grants a credential; it does not change
//	the version row), so no idempotency key is needed.
//
// SECURITY: rejected with FAILED_PRECONDITION if the version is already READY
//
//	(no overwriting a verified artifact) or FAILED. The storage KEY is computed
//	server-side from the version's identity — never client-supplied — so this
//	cannot be used to obtain a write credential for an arbitrary object (path
//	traversal / cross-model overwrite guard).
type GetUploadURLRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The PENDING_UPLOAD version to (re)issue an upload URL for. Required.
	VersionId string `protobuf:"bytes,1,opt,name=version_id,json=versionId,proto3" json:"version_id,omitempty"`
	// Requested URL validity in seconds; server clamps to a max (e.g. 3600s) to
	// bound how long the write credential is usable. 0 = server default.
	TtlSeconds    int64 `protobuf:"varint,2,opt,name=ttl_seconds,json=ttlSeconds,proto3" json:"ttl_seconds,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetUploadURLRequest) Reset() {
	*x = GetUploadURLRequest{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[14]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetUploadURLRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetUploadURLRequest) ProtoMessage() {}

func (x *GetUploadURLRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[14]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetUploadURLRequest.ProtoReflect.Descriptor instead.
func (*GetUploadURLRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{14}
}

func (x *GetUploadURLRequest) GetVersionId() string {
	if x != nil {
		return x.VersionId
	}
	return ""
}

func (x *GetUploadURLRequest) GetTtlSeconds() int64 {
	if x != nil {
		return x.TtlSeconds
	}
	return 0
}

type GetUploadURLResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Fresh presigned PUT URL for the artifact. Single-use, time-limited. Bytes go
	// straight to MinIO/S3 — never through this control-plane service.
	UploadUrl string `protobuf:"bytes,1,opt,name=upload_url,json=uploadUrl,proto3" json:"upload_url,omitempty"`
	// When upload_url stops being valid.
	UploadUrlExpiresAt *timestamppb.Timestamp `protobuf:"bytes,2,opt,name=upload_url_expires_at,json=uploadUrlExpiresAt,proto3" json:"upload_url_expires_at,omitempty"`
	unknownFields      protoimpl.UnknownFields
	sizeCache          protoimpl.SizeCache
}

func (x *GetUploadURLResponse) Reset() {
	*x = GetUploadURLResponse{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[15]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetUploadURLResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetUploadURLResponse) ProtoMessage() {}

func (x *GetUploadURLResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[15]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetUploadURLResponse.ProtoReflect.Descriptor instead.
func (*GetUploadURLResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{15}
}

func (x *GetUploadURLResponse) GetUploadUrl() string {
	if x != nil {
		return x.UploadUrl
	}
	return ""
}

func (x *GetUploadURLResponse) GetUploadUrlExpiresAt() *timestamppb.Timestamp {
	if x != nil {
		return x.UploadUrlExpiresAt
	}
	return nil
}

// ----------------------------------------------------------------------------
// ConfirmVersionUpload (COMMAND / write path) — the PENDING_UPLOAD → READY edge
// ----------------------------------------------------------------------------
//
// WHY this RPC exists (the missing READY trigger):
//
//	CreateVersion lands a version at status=PENDING_UPLOAD and hands back a
//	presigned PUT URL; the bytes then flow DIRECTLY to MinIO/S3, bypassing this
//	service. Something must tell the registry "the upload finished, go verify
//	it" so the status can advance to READY (or FAILED). That trigger is this RPC.
//	It is what publishes events.ModelVersionReady (fp.models.version.ready) — the
//	edge serving/billing/pipeline-orchestrator consume (conflict #3). Before this
//	RPC existed, the proto's prose claimed "the storage layer flips status to
//	READY" but exposed no surface to do it — a real gap this fix closes.
//
// WHO CALLS IT: typically the uploader (CLI/CI) after a successful PUT, OR an
//
//	S3/MinIO bucket-notification webhook bridged into this RPC. Either way the
//	server RE-VERIFIES the object server-side (existence + checksum + size) —
//	it does NOT trust the caller's claim that the upload succeeded.
//
// SECURITY / MASS-ASSIGNMENT (critical): the caller does NOT supply
//
//	artifact_path, artifact_digest, or size_bytes. Those are MEASURED by the
//	server from the stored object. Accepting a client-asserted digest would let
//	a caller register a "verified" artifact whose bytes don't match — defeating
//	the entire content-integrity guarantee — and a client-asserted size would let
//	a tenant under-report storage to dodge billing. The ONLY input is which
//	version to confirm. The expected_digest field below is an OPTIONAL assertion
//	the server CHECKS AGAINST (cross-check), never one it stores blindly.
type ConfirmVersionUploadRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The PENDING_UPLOAD version whose artifact upload just completed. Required.
	VersionId string `protobuf:"bytes,1,opt,name=version_id,json=versionId,proto3" json:"version_id,omitempty"`
	// OPTIONAL client-asserted content digest ("sha256:..."). If set, the server
	// computes the object's digest and FAILS the confirmation (status→FAILED) on
	// mismatch — a cross-check, not a stored value. If empty, the server simply
	// records the digest it computes. Never trusted as the source of truth.
	ExpectedDigest string `protobuf:"bytes,2,opt,name=expected_digest,json=expectedDigest,proto3" json:"expected_digest,omitempty"`
	// IDEMPOTENCY KEY: confirmation triggers the ModelVersionReady event and
	// billing's storage metering. A retry with the same key is a no-op returning
	// the current version state — so a duplicate webhook delivery cannot double-fire
	// ModelVersionReady or double-meter storage. Optional but recommended.
	IdempotencyKey string `protobuf:"bytes,3,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *ConfirmVersionUploadRequest) Reset() {
	*x = ConfirmVersionUploadRequest{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[16]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ConfirmVersionUploadRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ConfirmVersionUploadRequest) ProtoMessage() {}

func (x *ConfirmVersionUploadRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[16]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ConfirmVersionUploadRequest.ProtoReflect.Descriptor instead.
func (*ConfirmVersionUploadRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{16}
}

func (x *ConfirmVersionUploadRequest) GetVersionId() string {
	if x != nil {
		return x.VersionId
	}
	return ""
}

func (x *ConfirmVersionUploadRequest) GetExpectedDigest() string {
	if x != nil {
		return x.ExpectedDigest
	}
	return ""
}

func (x *ConfirmVersionUploadRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// ConfirmVersionUploadResponse returns the version with status now READY (on a
// successful verify) or FAILED (on checksum/size mismatch or missing object).
// On READY, the server has populated artifact_path/artifact_digest/size_bytes —
// all server-measured — and has published events.ModelVersionReady.
type ConfirmVersionUploadResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The version reflecting its post-verification status (READY or FAILED) and,
	// when READY, the server-measured artifact_path/artifact_digest/size_bytes.
	Version       *ModelVersion `protobuf:"bytes,1,opt,name=version,proto3" json:"version,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ConfirmVersionUploadResponse) Reset() {
	*x = ConfirmVersionUploadResponse{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[17]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ConfirmVersionUploadResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ConfirmVersionUploadResponse) ProtoMessage() {}

func (x *ConfirmVersionUploadResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[17]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ConfirmVersionUploadResponse.ProtoReflect.Descriptor instead.
func (*ConfirmVersionUploadResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{17}
}

func (x *ConfirmVersionUploadResponse) GetVersion() *ModelVersion {
	if x != nil {
		return x.Version
	}
	return nil
}

// GetVersionRequest fetches one version, either by its version id, or by the
// (model_id, version-label) pair — whichever the caller has on hand.
type GetVersionRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The version's UUID. Takes precedence if set.
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// Alternative lookup: the owning model id...
	ModelId string `protobuf:"bytes,2,opt,name=model_id,json=modelId,proto3" json:"model_id,omitempty"`
	// ...plus the version label (e.g. "1.2.0"). Used together when id is empty.
	Version       string `protobuf:"bytes,3,opt,name=version,proto3" json:"version,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetVersionRequest) Reset() {
	*x = GetVersionRequest{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[18]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetVersionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetVersionRequest) ProtoMessage() {}

func (x *GetVersionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[18]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetVersionRequest.ProtoReflect.Descriptor instead.
func (*GetVersionRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{18}
}

func (x *GetVersionRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *GetVersionRequest) GetModelId() string {
	if x != nil {
		return x.ModelId
	}
	return ""
}

func (x *GetVersionRequest) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

// GetVersionResponse wraps the ModelVersion from the read projection.
// CONSISTENCY: EVENTUALLY CONSISTENT (Redis).
type GetVersionResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Version       *ModelVersion          `protobuf:"bytes,1,opt,name=version,proto3" json:"version,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetVersionResponse) Reset() {
	*x = GetVersionResponse{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[19]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetVersionResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetVersionResponse) ProtoMessage() {}

func (x *GetVersionResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[19]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetVersionResponse.ProtoReflect.Descriptor instead.
func (*GetVersionResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{19}
}

func (x *GetVersionResponse) GetVersion() *ModelVersion {
	if x != nil {
		return x.Version
	}
	return nil
}

type ListVersionsRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The model whose versions to list. Required.
	ModelId string `protobuf:"bytes,1,opt,name=model_id,json=modelId,proto3" json:"model_id,omitempty"`
	// Optional filter: only versions in this stage (e.g., only PRODUCTION).
	// MODEL_STAGE_UNSPECIFIED (the default/zero) means "any stage".
	StageFilter ModelStage `protobuf:"varint,2,opt,name=stage_filter,json=stageFilter,proto3,enum=forgepoint.registry.v1.ModelStage" json:"stage_filter,omitempty"`
	// Cursor-based pagination; 100-item server-side cap.
	Pagination    *v1.PaginationRequest `protobuf:"bytes,3,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListVersionsRequest) Reset() {
	*x = ListVersionsRequest{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[20]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListVersionsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListVersionsRequest) ProtoMessage() {}

func (x *ListVersionsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[20]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListVersionsRequest.ProtoReflect.Descriptor instead.
func (*ListVersionsRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{20}
}

func (x *ListVersionsRequest) GetModelId() string {
	if x != nil {
		return x.ModelId
	}
	return ""
}

func (x *ListVersionsRequest) GetStageFilter() ModelStage {
	if x != nil {
		return x.StageFilter
	}
	return ModelStage_MODEL_STAGE_UNSPECIFIED
}

func (x *ListVersionsRequest) GetPagination() *v1.PaginationRequest {
	if x != nil {
		return x.Pagination
	}
	return nil
}

type ListVersionsResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Versions ordered newest-first. EVENTUALLY CONSISTENT (Redis read model).
	Versions      []*ModelVersion        `protobuf:"bytes,1,rep,name=versions,proto3" json:"versions,omitempty"`
	Pagination    *v1.PaginationResponse `protobuf:"bytes,2,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListVersionsResponse) Reset() {
	*x = ListVersionsResponse{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[21]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListVersionsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListVersionsResponse) ProtoMessage() {}

func (x *ListVersionsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[21]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListVersionsResponse.ProtoReflect.Descriptor instead.
func (*ListVersionsResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{21}
}

func (x *ListVersionsResponse) GetVersions() []*ModelVersion {
	if x != nil {
		return x.Versions
	}
	return nil
}

func (x *ListVersionsResponse) GetPagination() *v1.PaginationResponse {
	if x != nil {
		return x.Pagination
	}
	return nil
}

// ----------------------------------------------------------------------------
// PromoteVersion (COMMAND / write path) — the stage state machine
// ----------------------------------------------------------------------------
//
// WHY one PromoteVersion RPC instead of separate Promote/Demote/Archive RPCs:
//
//	The lifecycle is a single state machine; modeling it as "set target stage,
//	server validates the transition" keeps one authoritative transition
//	validator rather than scattering rules across four RPCs. The server rejects
//	illegal transitions (e.g., DEV→PRODUCTION skipping STAGING, or promoting a
//	non-READY version) with FAILED_PRECONDITION.
//
// THE SINGLE-PRODUCTION INVARIANT:
//
//	Promoting version B of model M to PRODUCTION must ATOMICALLY demote the
//	current production version A of M to ARCHIVED — there is never a moment with
//	two production versions. This happens in ONE Postgres transaction; the
//	resulting ModelPromoted event carries BOTH the newly-promoted version and
//	the demoted one so downstream consumers (serving must reload, billing must
//	re-meter, monitor must re-baseline) see the swap atomically.
//
// ----------------------------------------------------------------------------
type PromoteVersionRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The version to transition. SERVER-AUTHORITATIVE binding to its model.
	VersionId string `protobuf:"bytes,1,opt,name=version_id,json=versionId,proto3" json:"version_id,omitempty"`
	// The desired target stage. The server validates the transition is legal from
	// the version's current stage and that the version is READY when targeting
	// STAGING/PRODUCTION. This is the ONLY place stage is client-influenced — and
	// even here it's a *request* the server may reject, not a direct write.
	TargetStage ModelStage `protobuf:"varint,2,opt,name=target_stage,json=targetStage,proto3,enum=forgepoint.registry.v1.ModelStage" json:"target_stage,omitempty"`
	// IDEMPOTENCY KEY: promotion triggers side effects (serving reload, billing
	// re-meter). A duplicate promote with the same key is a no-op returning the
	// current state, so a client retry can't re-fire those side effects or emit a
	// second ModelPromoted event.
	IdempotencyKey string `protobuf:"bytes,3,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *PromoteVersionRequest) Reset() {
	*x = PromoteVersionRequest{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[22]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PromoteVersionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PromoteVersionRequest) ProtoMessage() {}

func (x *PromoteVersionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[22]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PromoteVersionRequest.ProtoReflect.Descriptor instead.
func (*PromoteVersionRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{22}
}

func (x *PromoteVersionRequest) GetVersionId() string {
	if x != nil {
		return x.VersionId
	}
	return ""
}

func (x *PromoteVersionRequest) GetTargetStage() ModelStage {
	if x != nil {
		return x.TargetStage
	}
	return ModelStage_MODEL_STAGE_UNSPECIFIED
}

func (x *PromoteVersionRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// PromoteVersionResponse returns the version in its new stage, plus the version
// that was demoted (if any) so the caller sees the full atomic swap result.
type PromoteVersionResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The promoted version reflecting its new stage.
	Version *ModelVersion `protobuf:"bytes,1,opt,name=version,proto3" json:"version,omitempty"`
	// The previously-production version that was auto-demoted to ARCHIVED by this
	// promotion, or unset if there was no prior production version. Lets the
	// caller (and audit log) record the exact swap.
	DemotedVersion *ModelVersion `protobuf:"bytes,2,opt,name=demoted_version,json=demotedVersion,proto3" json:"demoted_version,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *PromoteVersionResponse) Reset() {
	*x = PromoteVersionResponse{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[23]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PromoteVersionResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PromoteVersionResponse) ProtoMessage() {}

func (x *PromoteVersionResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[23]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PromoteVersionResponse.ProtoReflect.Descriptor instead.
func (*PromoteVersionResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{23}
}

func (x *PromoteVersionResponse) GetVersion() *ModelVersion {
	if x != nil {
		return x.Version
	}
	return nil
}

func (x *PromoteVersionResponse) GetDemotedVersion() *ModelVersion {
	if x != nil {
		return x.DemotedVersion
	}
	return nil
}

// ----------------------------------------------------------------------------
// DeleteModel (COMMAND / write path) — soft delete / archive-all
// ----------------------------------------------------------------------------
//
// WHY soft delete, not hard delete:
//
//	Models have downstream lineage (experiments, billing records, served
//	deployments). Hard-deleting would orphan those references and destroy audit
//	history. DeleteModel sets the model's archived_at, archives all its
//	versions, and removes it from active read lists — but the rows survive for
//	compliance and potential restore. This is the same posture as "soft delete"
//	in Stripe/most SaaS registries.
//
// ----------------------------------------------------------------------------
type DeleteModelRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The model to archive (soft-delete). Required.
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// IDEMPOTENCY KEY: deleting an already-deleted model is naturally idempotent,
	// but the key also guards against a retry re-emitting the ModelArchived event
	// (which would double-notify). Optional but recommended.
	IdempotencyKey string `protobuf:"bytes,2,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *DeleteModelRequest) Reset() {
	*x = DeleteModelRequest{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[24]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DeleteModelRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteModelRequest) ProtoMessage() {}

func (x *DeleteModelRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[24]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteModelRequest.ProtoReflect.Descriptor instead.
func (*DeleteModelRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{24}
}

func (x *DeleteModelRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *DeleteModelRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// DeleteModelResponse is a NAMED EMPTY message (not google.protobuf.Empty) to
// satisfy Buf RPC_RESPONSE_STANDARD_NAME and stay forward-compatible: if we
// later want to return archived_at or a count of archived versions, we add
// fields here without changing the RPC signature. See auth.proto's
// RevokeAPIKeyResponse for the same rationale.
type DeleteModelResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *DeleteModelResponse) Reset() {
	*x = DeleteModelResponse{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[25]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DeleteModelResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteModelResponse) ProtoMessage() {}

func (x *DeleteModelResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[25]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteModelResponse.ProtoReflect.Descriptor instead.
func (*DeleteModelResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{25}
}

// ----------------------------------------------------------------------------
// GetDownloadURL (QUERY-ish / storage) — fetch artifact bytes out of band
// ----------------------------------------------------------------------------
//
// WHY a presigned DOWNLOAD URL rather than streaming bytes through gRPC:
//
//	Symmetric with the upload flow — artifacts can be gigabytes; routing them
//	through this control-plane service would blow the gRPC message limit and
//	waste CPU/bandwidth. Serving and the CLI fetch the artifact directly from
//	MinIO/S3 using a short-lived, scoped credential. This RPC does NOT mutate
//	state, so it is safe to retry and needs no idempotency key.
//
// ----------------------------------------------------------------------------
type GetDownloadURLRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The version whose artifact to download. Must be READY.
	VersionId string `protobuf:"bytes,1,opt,name=version_id,json=versionId,proto3" json:"version_id,omitempty"`
	// Requested validity of the URL in seconds; server clamps to a max. 0 =
	// server default. Short TTLs limit how long a leaked URL is usable.
	TtlSeconds    int64 `protobuf:"varint,2,opt,name=ttl_seconds,json=ttlSeconds,proto3" json:"ttl_seconds,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetDownloadURLRequest) Reset() {
	*x = GetDownloadURLRequest{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[26]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetDownloadURLRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetDownloadURLRequest) ProtoMessage() {}

func (x *GetDownloadURLRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[26]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetDownloadURLRequest.ProtoReflect.Descriptor instead.
func (*GetDownloadURLRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{26}
}

func (x *GetDownloadURLRequest) GetVersionId() string {
	if x != nil {
		return x.VersionId
	}
	return ""
}

func (x *GetDownloadURLRequest) GetTtlSeconds() int64 {
	if x != nil {
		return x.TtlSeconds
	}
	return 0
}

type GetDownloadURLResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Presigned GET URL for the artifact. Time-limited, scoped to this object.
	DownloadUrl string `protobuf:"bytes,1,opt,name=download_url,json=downloadUrl,proto3" json:"download_url,omitempty"`
	// When the URL expires.
	ExpiresAt *timestamppb.Timestamp `protobuf:"bytes,2,opt,name=expires_at,json=expiresAt,proto3" json:"expires_at,omitempty"`
	// Content digest of what will be downloaded, so the client can verify
	// integrity after fetching (matches ModelVersion.artifact_digest).
	ArtifactDigest string `protobuf:"bytes,3,opt,name=artifact_digest,json=artifactDigest,proto3" json:"artifact_digest,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *GetDownloadURLResponse) Reset() {
	*x = GetDownloadURLResponse{}
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[27]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetDownloadURLResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetDownloadURLResponse) ProtoMessage() {}

func (x *GetDownloadURLResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_registry_v1_registry_proto_msgTypes[27]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetDownloadURLResponse.ProtoReflect.Descriptor instead.
func (*GetDownloadURLResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_registry_v1_registry_proto_rawDescGZIP(), []int{27}
}

func (x *GetDownloadURLResponse) GetDownloadUrl() string {
	if x != nil {
		return x.DownloadUrl
	}
	return ""
}

func (x *GetDownloadURLResponse) GetExpiresAt() *timestamppb.Timestamp {
	if x != nil {
		return x.ExpiresAt
	}
	return nil
}

func (x *GetDownloadURLResponse) GetArtifactDigest() string {
	if x != nil {
		return x.ArtifactDigest
	}
	return ""
}

var File_forgepoint_registry_v1_registry_proto protoreflect.FileDescriptor

const file_forgepoint_registry_v1_registry_proto_rawDesc = "" +
	"\n" +
	"%forgepoint/registry/v1/registry.proto\x12\x16forgepoint.registry.v1\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1cgoogle/protobuf/struct.proto\x1a!forgepoint/common/v1/common.proto\"\xb6\x04\n" +
	"\x05Model\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12\x12\n" +
	"\x04name\x18\x02 \x01(\tR\x04name\x12 \n" +
	"\vdescription\x18\x03 \x01(\tR\vdescription\x12\x19\n" +
	"\bowner_id\x18\x04 \x01(\tR\aownerId\x12\x12\n" +
	"\x04team\x18\x05 \x01(\tR\x04team\x12\x1c\n" +
	"\tframework\x18\x06 \x01(\tR\tframework\x12\x1b\n" +
	"\ttask_type\x18\a \x01(\tR\btaskType\x12;\n" +
	"\x04tags\x18\b \x03(\v2'.forgepoint.registry.v1.Model.TagsEntryR\x04tags\x12-\n" +
	"\x12production_version\x18\t \x01(\tR\x11productionVersion\x12%\n" +
	"\x0elatest_version\x18\n" +
	" \x01(\tR\rlatestVersion\x129\n" +
	"\n" +
	"created_at\x18\v \x01(\v2\x1a.google.protobuf.TimestampR\tcreatedAt\x129\n" +
	"\n" +
	"updated_at\x18\f \x01(\v2\x1a.google.protobuf.TimestampR\tupdatedAt\x12;\n" +
	"\varchived_at\x18\r \x01(\v2\x1a.google.protobuf.TimestampR\n" +
	"archivedAt\x1a7\n" +
	"\tTagsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\tR\x05value:\x028\x01\"\xe8\x03\n" +
	"\fModelVersion\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12\x19\n" +
	"\bmodel_id\x18\x02 \x01(\tR\amodelId\x12\x18\n" +
	"\aversion\x18\x03 \x01(\tR\aversion\x12 \n" +
	"\vdescription\x18\x04 \x01(\tR\vdescription\x12#\n" +
	"\rartifact_path\x18\x05 \x01(\tR\fartifactPath\x12'\n" +
	"\x0fartifact_digest\x18\x06 \x01(\tR\x0eartifactDigest\x12\x1d\n" +
	"\n" +
	"size_bytes\x18\a \x01(\x03R\tsizeBytes\x121\n" +
	"\ametrics\x18\b \x01(\v2\x17.google.protobuf.StructR\ametrics\x128\n" +
	"\x05stage\x18\t \x01(\x0e2\".forgepoint.registry.v1.ModelStageR\x05stage\x12=\n" +
	"\x06status\x18\n" +
	" \x01(\x0e2%.forgepoint.registry.v1.VersionStatusR\x06status\x12\x1d\n" +
	"\n" +
	"created_by\x18\v \x01(\tR\tcreatedBy\x129\n" +
	"\n" +
	"created_at\x18\f \x01(\v2\x1a.google.protobuf.TimestampR\tcreatedAt\"\xb5\x02\n" +
	"\x14RegisterModelRequest\x12\x12\n" +
	"\x04name\x18\x01 \x01(\tR\x04name\x12 \n" +
	"\vdescription\x18\x02 \x01(\tR\vdescription\x12\x1c\n" +
	"\tframework\x18\x03 \x01(\tR\tframework\x12\x1b\n" +
	"\ttask_type\x18\x04 \x01(\tR\btaskType\x12J\n" +
	"\x04tags\x18\x05 \x03(\v26.forgepoint.registry.v1.RegisterModelRequest.TagsEntryR\x04tags\x12'\n" +
	"\x0fidempotency_key\x18\x06 \x01(\tR\x0eidempotencyKey\x1a7\n" +
	"\tTagsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\tR\x05value:\x028\x01\"L\n" +
	"\x15RegisterModelResponse\x123\n" +
	"\x05model\x18\x01 \x01(\v2\x1d.forgepoint.registry.v1.ModelR\x05model\"\xc4\x02\n" +
	"\x12UpdateModelRequest\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12 \n" +
	"\vdescription\x18\x02 \x01(\tR\vdescription\x12-\n" +
	"\x12update_description\x18\x03 \x01(\bR\x11updateDescription\x12H\n" +
	"\x04tags\x18\x04 \x03(\v24.forgepoint.registry.v1.UpdateModelRequest.TagsEntryR\x04tags\x12!\n" +
	"\freplace_tags\x18\x05 \x01(\bR\vreplaceTags\x12'\n" +
	"\x0fidempotency_key\x18\x06 \x01(\tR\x0eidempotencyKey\x1a7\n" +
	"\tTagsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\tR\x05value:\x028\x01\"J\n" +
	"\x13UpdateModelResponse\x123\n" +
	"\x05model\x18\x01 \x01(\v2\x1d.forgepoint.registry.v1.ModelR\x05model\"5\n" +
	"\x0fGetModelRequest\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12\x12\n" +
	"\x04name\x18\x02 \x01(\tR\x04name\"G\n" +
	"\x10GetModelResponse\x123\n" +
	"\x05model\x18\x01 \x01(\v2\x1d.forgepoint.registry.v1.ModelR\x05model\"\xdc\x01\n" +
	"\x11ListModelsRequest\x12(\n" +
	"\x10task_type_filter\x18\x01 \x01(\tR\x0etaskTypeFilter\x12)\n" +
	"\x10framework_filter\x18\x02 \x01(\tR\x0fframeworkFilter\x12)\n" +
	"\x10include_archived\x18\x03 \x01(\bR\x0fincludeArchived\x12G\n" +
	"\n" +
	"pagination\x18\x04 \x01(\v2'.forgepoint.common.v1.PaginationRequestR\n" +
	"pagination\"\x95\x01\n" +
	"\x12ListModelsResponse\x125\n" +
	"\x06models\x18\x01 \x03(\v2\x1d.forgepoint.registry.v1.ModelR\x06models\x12H\n" +
	"\n" +
	"pagination\x18\x02 \x01(\v2(.forgepoint.common.v1.PaginationResponseR\n" +
	"pagination\"\x85\x01\n" +
	"\x12SearchByTagRequest\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\tR\x05value\x12G\n" +
	"\n" +
	"pagination\x18\x03 \x01(\v2'.forgepoint.common.v1.PaginationRequestR\n" +
	"pagination\"\x96\x01\n" +
	"\x13SearchByTagResponse\x125\n" +
	"\x06models\x18\x01 \x03(\v2\x1d.forgepoint.registry.v1.ModelR\x06models\x12H\n" +
	"\n" +
	"pagination\x18\x02 \x01(\v2(.forgepoint.common.v1.PaginationResponseR\n" +
	"pagination\"\xfe\x01\n" +
	"\x14CreateVersionRequest\x12\x19\n" +
	"\bmodel_id\x18\x01 \x01(\tR\amodelId\x12\x18\n" +
	"\aversion\x18\x02 \x01(\tR\aversion\x12 \n" +
	"\vdescription\x18\x03 \x01(\tR\vdescription\x121\n" +
	"\ametrics\x18\x04 \x01(\v2\x17.google.protobuf.StructR\ametrics\x12'\n" +
	"\x0fidempotency_key\x18\x05 \x01(\tR\x0eidempotencyKey\x123\n" +
	"\x16upload_url_ttl_seconds\x18\x06 \x01(\x03R\x13uploadUrlTtlSeconds\"\xc5\x01\n" +
	"\x15CreateVersionResponse\x12>\n" +
	"\aversion\x18\x01 \x01(\v2$.forgepoint.registry.v1.ModelVersionR\aversion\x12\x1d\n" +
	"\n" +
	"upload_url\x18\x02 \x01(\tR\tuploadUrl\x12M\n" +
	"\x15upload_url_expires_at\x18\x03 \x01(\v2\x1a.google.protobuf.TimestampR\x12uploadUrlExpiresAt\"U\n" +
	"\x13GetUploadURLRequest\x12\x1d\n" +
	"\n" +
	"version_id\x18\x01 \x01(\tR\tversionId\x12\x1f\n" +
	"\vttl_seconds\x18\x02 \x01(\x03R\n" +
	"ttlSeconds\"\x84\x01\n" +
	"\x14GetUploadURLResponse\x12\x1d\n" +
	"\n" +
	"upload_url\x18\x01 \x01(\tR\tuploadUrl\x12M\n" +
	"\x15upload_url_expires_at\x18\x02 \x01(\v2\x1a.google.protobuf.TimestampR\x12uploadUrlExpiresAt\"\x8e\x01\n" +
	"\x1bConfirmVersionUploadRequest\x12\x1d\n" +
	"\n" +
	"version_id\x18\x01 \x01(\tR\tversionId\x12'\n" +
	"\x0fexpected_digest\x18\x02 \x01(\tR\x0eexpectedDigest\x12'\n" +
	"\x0fidempotency_key\x18\x03 \x01(\tR\x0eidempotencyKey\"^\n" +
	"\x1cConfirmVersionUploadResponse\x12>\n" +
	"\aversion\x18\x01 \x01(\v2$.forgepoint.registry.v1.ModelVersionR\aversion\"X\n" +
	"\x11GetVersionRequest\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12\x19\n" +
	"\bmodel_id\x18\x02 \x01(\tR\amodelId\x12\x18\n" +
	"\aversion\x18\x03 \x01(\tR\aversion\"T\n" +
	"\x12GetVersionResponse\x12>\n" +
	"\aversion\x18\x01 \x01(\v2$.forgepoint.registry.v1.ModelVersionR\aversion\"\xc0\x01\n" +
	"\x13ListVersionsRequest\x12\x19\n" +
	"\bmodel_id\x18\x01 \x01(\tR\amodelId\x12E\n" +
	"\fstage_filter\x18\x02 \x01(\x0e2\".forgepoint.registry.v1.ModelStageR\vstageFilter\x12G\n" +
	"\n" +
	"pagination\x18\x03 \x01(\v2'.forgepoint.common.v1.PaginationRequestR\n" +
	"pagination\"\xa2\x01\n" +
	"\x14ListVersionsResponse\x12@\n" +
	"\bversions\x18\x01 \x03(\v2$.forgepoint.registry.v1.ModelVersionR\bversions\x12H\n" +
	"\n" +
	"pagination\x18\x02 \x01(\v2(.forgepoint.common.v1.PaginationResponseR\n" +
	"pagination\"\xa6\x01\n" +
	"\x15PromoteVersionRequest\x12\x1d\n" +
	"\n" +
	"version_id\x18\x01 \x01(\tR\tversionId\x12E\n" +
	"\ftarget_stage\x18\x02 \x01(\x0e2\".forgepoint.registry.v1.ModelStageR\vtargetStage\x12'\n" +
	"\x0fidempotency_key\x18\x03 \x01(\tR\x0eidempotencyKey\"\xa7\x01\n" +
	"\x16PromoteVersionResponse\x12>\n" +
	"\aversion\x18\x01 \x01(\v2$.forgepoint.registry.v1.ModelVersionR\aversion\x12M\n" +
	"\x0fdemoted_version\x18\x02 \x01(\v2$.forgepoint.registry.v1.ModelVersionR\x0edemotedVersion\"M\n" +
	"\x12DeleteModelRequest\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12'\n" +
	"\x0fidempotency_key\x18\x02 \x01(\tR\x0eidempotencyKey\"\x15\n" +
	"\x13DeleteModelResponse\"W\n" +
	"\x15GetDownloadURLRequest\x12\x1d\n" +
	"\n" +
	"version_id\x18\x01 \x01(\tR\tversionId\x12\x1f\n" +
	"\vttl_seconds\x18\x02 \x01(\x03R\n" +
	"ttlSeconds\"\x9f\x01\n" +
	"\x16GetDownloadURLResponse\x12!\n" +
	"\fdownload_url\x18\x01 \x01(\tR\vdownloadUrl\x129\n" +
	"\n" +
	"expires_at\x18\x02 \x01(\v2\x1a.google.protobuf.TimestampR\texpiresAt\x12'\n" +
	"\x0fartifact_digest\x18\x03 \x01(\tR\x0eartifactDigest*\x8d\x01\n" +
	"\n" +
	"ModelStage\x12\x1b\n" +
	"\x17MODEL_STAGE_UNSPECIFIED\x10\x00\x12\x13\n" +
	"\x0fMODEL_STAGE_DEV\x10\x01\x12\x17\n" +
	"\x13MODEL_STAGE_STAGING\x10\x02\x12\x1a\n" +
	"\x16MODEL_STAGE_PRODUCTION\x10\x03\x12\x18\n" +
	"\x14MODEL_STAGE_ARCHIVED\x10\x04*\x87\x01\n" +
	"\rVersionStatus\x12\x1e\n" +
	"\x1aVERSION_STATUS_UNSPECIFIED\x10\x00\x12!\n" +
	"\x1dVERSION_STATUS_PENDING_UPLOAD\x10\x01\x12\x18\n" +
	"\x14VERSION_STATUS_READY\x10\x02\x12\x19\n" +
	"\x15VERSION_STATUS_FAILED\x10\x032\x8a\v\n" +
	"\x0fRegistryService\x12l\n" +
	"\rRegisterModel\x12,.forgepoint.registry.v1.RegisterModelRequest\x1a-.forgepoint.registry.v1.RegisterModelResponse\x12f\n" +
	"\vUpdateModel\x12*.forgepoint.registry.v1.UpdateModelRequest\x1a+.forgepoint.registry.v1.UpdateModelResponse\x12l\n" +
	"\rCreateVersion\x12,.forgepoint.registry.v1.CreateVersionRequest\x1a-.forgepoint.registry.v1.CreateVersionResponse\x12\x81\x01\n" +
	"\x14ConfirmVersionUpload\x123.forgepoint.registry.v1.ConfirmVersionUploadRequest\x1a4.forgepoint.registry.v1.ConfirmVersionUploadResponse\x12o\n" +
	"\x0ePromoteVersion\x12-.forgepoint.registry.v1.PromoteVersionRequest\x1a..forgepoint.registry.v1.PromoteVersionResponse\x12f\n" +
	"\vDeleteModel\x12*.forgepoint.registry.v1.DeleteModelRequest\x1a+.forgepoint.registry.v1.DeleteModelResponse\x12]\n" +
	"\bGetModel\x12'.forgepoint.registry.v1.GetModelRequest\x1a(.forgepoint.registry.v1.GetModelResponse\x12c\n" +
	"\n" +
	"ListModels\x12).forgepoint.registry.v1.ListModelsRequest\x1a*.forgepoint.registry.v1.ListModelsResponse\x12f\n" +
	"\vSearchByTag\x12*.forgepoint.registry.v1.SearchByTagRequest\x1a+.forgepoint.registry.v1.SearchByTagResponse\x12c\n" +
	"\n" +
	"GetVersion\x12).forgepoint.registry.v1.GetVersionRequest\x1a*.forgepoint.registry.v1.GetVersionResponse\x12i\n" +
	"\fListVersions\x12+.forgepoint.registry.v1.ListVersionsRequest\x1a,.forgepoint.registry.v1.ListVersionsResponse\x12i\n" +
	"\fGetUploadURL\x12+.forgepoint.registry.v1.GetUploadURLRequest\x1a,.forgepoint.registry.v1.GetUploadURLResponse\x12o\n" +
	"\x0eGetDownloadURL\x12-.forgepoint.registry.v1.GetDownloadURLRequest\x1a..forgepoint.registry.v1.GetDownloadURLResponseBLZJgithub.com/abd-ulbasit/forgepoint/gen/go/forgepoint/registry/v1;registryv1b\x06proto3"

var (
	file_forgepoint_registry_v1_registry_proto_rawDescOnce sync.Once
	file_forgepoint_registry_v1_registry_proto_rawDescData []byte
)

func file_forgepoint_registry_v1_registry_proto_rawDescGZIP() []byte {
	file_forgepoint_registry_v1_registry_proto_rawDescOnce.Do(func() {
		file_forgepoint_registry_v1_registry_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_forgepoint_registry_v1_registry_proto_rawDesc), len(file_forgepoint_registry_v1_registry_proto_rawDesc)))
	})
	return file_forgepoint_registry_v1_registry_proto_rawDescData
}

var file_forgepoint_registry_v1_registry_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_forgepoint_registry_v1_registry_proto_msgTypes = make([]protoimpl.MessageInfo, 31)
var file_forgepoint_registry_v1_registry_proto_goTypes = []any{
	(ModelStage)(0),                      // 0: forgepoint.registry.v1.ModelStage
	(VersionStatus)(0),                   // 1: forgepoint.registry.v1.VersionStatus
	(*Model)(nil),                        // 2: forgepoint.registry.v1.Model
	(*ModelVersion)(nil),                 // 3: forgepoint.registry.v1.ModelVersion
	(*RegisterModelRequest)(nil),         // 4: forgepoint.registry.v1.RegisterModelRequest
	(*RegisterModelResponse)(nil),        // 5: forgepoint.registry.v1.RegisterModelResponse
	(*UpdateModelRequest)(nil),           // 6: forgepoint.registry.v1.UpdateModelRequest
	(*UpdateModelResponse)(nil),          // 7: forgepoint.registry.v1.UpdateModelResponse
	(*GetModelRequest)(nil),              // 8: forgepoint.registry.v1.GetModelRequest
	(*GetModelResponse)(nil),             // 9: forgepoint.registry.v1.GetModelResponse
	(*ListModelsRequest)(nil),            // 10: forgepoint.registry.v1.ListModelsRequest
	(*ListModelsResponse)(nil),           // 11: forgepoint.registry.v1.ListModelsResponse
	(*SearchByTagRequest)(nil),           // 12: forgepoint.registry.v1.SearchByTagRequest
	(*SearchByTagResponse)(nil),          // 13: forgepoint.registry.v1.SearchByTagResponse
	(*CreateVersionRequest)(nil),         // 14: forgepoint.registry.v1.CreateVersionRequest
	(*CreateVersionResponse)(nil),        // 15: forgepoint.registry.v1.CreateVersionResponse
	(*GetUploadURLRequest)(nil),          // 16: forgepoint.registry.v1.GetUploadURLRequest
	(*GetUploadURLResponse)(nil),         // 17: forgepoint.registry.v1.GetUploadURLResponse
	(*ConfirmVersionUploadRequest)(nil),  // 18: forgepoint.registry.v1.ConfirmVersionUploadRequest
	(*ConfirmVersionUploadResponse)(nil), // 19: forgepoint.registry.v1.ConfirmVersionUploadResponse
	(*GetVersionRequest)(nil),            // 20: forgepoint.registry.v1.GetVersionRequest
	(*GetVersionResponse)(nil),           // 21: forgepoint.registry.v1.GetVersionResponse
	(*ListVersionsRequest)(nil),          // 22: forgepoint.registry.v1.ListVersionsRequest
	(*ListVersionsResponse)(nil),         // 23: forgepoint.registry.v1.ListVersionsResponse
	(*PromoteVersionRequest)(nil),        // 24: forgepoint.registry.v1.PromoteVersionRequest
	(*PromoteVersionResponse)(nil),       // 25: forgepoint.registry.v1.PromoteVersionResponse
	(*DeleteModelRequest)(nil),           // 26: forgepoint.registry.v1.DeleteModelRequest
	(*DeleteModelResponse)(nil),          // 27: forgepoint.registry.v1.DeleteModelResponse
	(*GetDownloadURLRequest)(nil),        // 28: forgepoint.registry.v1.GetDownloadURLRequest
	(*GetDownloadURLResponse)(nil),       // 29: forgepoint.registry.v1.GetDownloadURLResponse
	nil,                                  // 30: forgepoint.registry.v1.Model.TagsEntry
	nil,                                  // 31: forgepoint.registry.v1.RegisterModelRequest.TagsEntry
	nil,                                  // 32: forgepoint.registry.v1.UpdateModelRequest.TagsEntry
	(*timestamppb.Timestamp)(nil),        // 33: google.protobuf.Timestamp
	(*structpb.Struct)(nil),              // 34: google.protobuf.Struct
	(*v1.PaginationRequest)(nil),         // 35: forgepoint.common.v1.PaginationRequest
	(*v1.PaginationResponse)(nil),        // 36: forgepoint.common.v1.PaginationResponse
}
var file_forgepoint_registry_v1_registry_proto_depIdxs = []int32{
	30, // 0: forgepoint.registry.v1.Model.tags:type_name -> forgepoint.registry.v1.Model.TagsEntry
	33, // 1: forgepoint.registry.v1.Model.created_at:type_name -> google.protobuf.Timestamp
	33, // 2: forgepoint.registry.v1.Model.updated_at:type_name -> google.protobuf.Timestamp
	33, // 3: forgepoint.registry.v1.Model.archived_at:type_name -> google.protobuf.Timestamp
	34, // 4: forgepoint.registry.v1.ModelVersion.metrics:type_name -> google.protobuf.Struct
	0,  // 5: forgepoint.registry.v1.ModelVersion.stage:type_name -> forgepoint.registry.v1.ModelStage
	1,  // 6: forgepoint.registry.v1.ModelVersion.status:type_name -> forgepoint.registry.v1.VersionStatus
	33, // 7: forgepoint.registry.v1.ModelVersion.created_at:type_name -> google.protobuf.Timestamp
	31, // 8: forgepoint.registry.v1.RegisterModelRequest.tags:type_name -> forgepoint.registry.v1.RegisterModelRequest.TagsEntry
	2,  // 9: forgepoint.registry.v1.RegisterModelResponse.model:type_name -> forgepoint.registry.v1.Model
	32, // 10: forgepoint.registry.v1.UpdateModelRequest.tags:type_name -> forgepoint.registry.v1.UpdateModelRequest.TagsEntry
	2,  // 11: forgepoint.registry.v1.UpdateModelResponse.model:type_name -> forgepoint.registry.v1.Model
	2,  // 12: forgepoint.registry.v1.GetModelResponse.model:type_name -> forgepoint.registry.v1.Model
	35, // 13: forgepoint.registry.v1.ListModelsRequest.pagination:type_name -> forgepoint.common.v1.PaginationRequest
	2,  // 14: forgepoint.registry.v1.ListModelsResponse.models:type_name -> forgepoint.registry.v1.Model
	36, // 15: forgepoint.registry.v1.ListModelsResponse.pagination:type_name -> forgepoint.common.v1.PaginationResponse
	35, // 16: forgepoint.registry.v1.SearchByTagRequest.pagination:type_name -> forgepoint.common.v1.PaginationRequest
	2,  // 17: forgepoint.registry.v1.SearchByTagResponse.models:type_name -> forgepoint.registry.v1.Model
	36, // 18: forgepoint.registry.v1.SearchByTagResponse.pagination:type_name -> forgepoint.common.v1.PaginationResponse
	34, // 19: forgepoint.registry.v1.CreateVersionRequest.metrics:type_name -> google.protobuf.Struct
	3,  // 20: forgepoint.registry.v1.CreateVersionResponse.version:type_name -> forgepoint.registry.v1.ModelVersion
	33, // 21: forgepoint.registry.v1.CreateVersionResponse.upload_url_expires_at:type_name -> google.protobuf.Timestamp
	33, // 22: forgepoint.registry.v1.GetUploadURLResponse.upload_url_expires_at:type_name -> google.protobuf.Timestamp
	3,  // 23: forgepoint.registry.v1.ConfirmVersionUploadResponse.version:type_name -> forgepoint.registry.v1.ModelVersion
	3,  // 24: forgepoint.registry.v1.GetVersionResponse.version:type_name -> forgepoint.registry.v1.ModelVersion
	0,  // 25: forgepoint.registry.v1.ListVersionsRequest.stage_filter:type_name -> forgepoint.registry.v1.ModelStage
	35, // 26: forgepoint.registry.v1.ListVersionsRequest.pagination:type_name -> forgepoint.common.v1.PaginationRequest
	3,  // 27: forgepoint.registry.v1.ListVersionsResponse.versions:type_name -> forgepoint.registry.v1.ModelVersion
	36, // 28: forgepoint.registry.v1.ListVersionsResponse.pagination:type_name -> forgepoint.common.v1.PaginationResponse
	0,  // 29: forgepoint.registry.v1.PromoteVersionRequest.target_stage:type_name -> forgepoint.registry.v1.ModelStage
	3,  // 30: forgepoint.registry.v1.PromoteVersionResponse.version:type_name -> forgepoint.registry.v1.ModelVersion
	3,  // 31: forgepoint.registry.v1.PromoteVersionResponse.demoted_version:type_name -> forgepoint.registry.v1.ModelVersion
	33, // 32: forgepoint.registry.v1.GetDownloadURLResponse.expires_at:type_name -> google.protobuf.Timestamp
	4,  // 33: forgepoint.registry.v1.RegistryService.RegisterModel:input_type -> forgepoint.registry.v1.RegisterModelRequest
	6,  // 34: forgepoint.registry.v1.RegistryService.UpdateModel:input_type -> forgepoint.registry.v1.UpdateModelRequest
	14, // 35: forgepoint.registry.v1.RegistryService.CreateVersion:input_type -> forgepoint.registry.v1.CreateVersionRequest
	18, // 36: forgepoint.registry.v1.RegistryService.ConfirmVersionUpload:input_type -> forgepoint.registry.v1.ConfirmVersionUploadRequest
	24, // 37: forgepoint.registry.v1.RegistryService.PromoteVersion:input_type -> forgepoint.registry.v1.PromoteVersionRequest
	26, // 38: forgepoint.registry.v1.RegistryService.DeleteModel:input_type -> forgepoint.registry.v1.DeleteModelRequest
	8,  // 39: forgepoint.registry.v1.RegistryService.GetModel:input_type -> forgepoint.registry.v1.GetModelRequest
	10, // 40: forgepoint.registry.v1.RegistryService.ListModels:input_type -> forgepoint.registry.v1.ListModelsRequest
	12, // 41: forgepoint.registry.v1.RegistryService.SearchByTag:input_type -> forgepoint.registry.v1.SearchByTagRequest
	20, // 42: forgepoint.registry.v1.RegistryService.GetVersion:input_type -> forgepoint.registry.v1.GetVersionRequest
	22, // 43: forgepoint.registry.v1.RegistryService.ListVersions:input_type -> forgepoint.registry.v1.ListVersionsRequest
	16, // 44: forgepoint.registry.v1.RegistryService.GetUploadURL:input_type -> forgepoint.registry.v1.GetUploadURLRequest
	28, // 45: forgepoint.registry.v1.RegistryService.GetDownloadURL:input_type -> forgepoint.registry.v1.GetDownloadURLRequest
	5,  // 46: forgepoint.registry.v1.RegistryService.RegisterModel:output_type -> forgepoint.registry.v1.RegisterModelResponse
	7,  // 47: forgepoint.registry.v1.RegistryService.UpdateModel:output_type -> forgepoint.registry.v1.UpdateModelResponse
	15, // 48: forgepoint.registry.v1.RegistryService.CreateVersion:output_type -> forgepoint.registry.v1.CreateVersionResponse
	19, // 49: forgepoint.registry.v1.RegistryService.ConfirmVersionUpload:output_type -> forgepoint.registry.v1.ConfirmVersionUploadResponse
	25, // 50: forgepoint.registry.v1.RegistryService.PromoteVersion:output_type -> forgepoint.registry.v1.PromoteVersionResponse
	27, // 51: forgepoint.registry.v1.RegistryService.DeleteModel:output_type -> forgepoint.registry.v1.DeleteModelResponse
	9,  // 52: forgepoint.registry.v1.RegistryService.GetModel:output_type -> forgepoint.registry.v1.GetModelResponse
	11, // 53: forgepoint.registry.v1.RegistryService.ListModels:output_type -> forgepoint.registry.v1.ListModelsResponse
	13, // 54: forgepoint.registry.v1.RegistryService.SearchByTag:output_type -> forgepoint.registry.v1.SearchByTagResponse
	21, // 55: forgepoint.registry.v1.RegistryService.GetVersion:output_type -> forgepoint.registry.v1.GetVersionResponse
	23, // 56: forgepoint.registry.v1.RegistryService.ListVersions:output_type -> forgepoint.registry.v1.ListVersionsResponse
	17, // 57: forgepoint.registry.v1.RegistryService.GetUploadURL:output_type -> forgepoint.registry.v1.GetUploadURLResponse
	29, // 58: forgepoint.registry.v1.RegistryService.GetDownloadURL:output_type -> forgepoint.registry.v1.GetDownloadURLResponse
	46, // [46:59] is the sub-list for method output_type
	33, // [33:46] is the sub-list for method input_type
	33, // [33:33] is the sub-list for extension type_name
	33, // [33:33] is the sub-list for extension extendee
	0,  // [0:33] is the sub-list for field type_name
}

func init() { file_forgepoint_registry_v1_registry_proto_init() }
func file_forgepoint_registry_v1_registry_proto_init() {
	if File_forgepoint_registry_v1_registry_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_forgepoint_registry_v1_registry_proto_rawDesc), len(file_forgepoint_registry_v1_registry_proto_rawDesc)),
			NumEnums:      2,
			NumMessages:   31,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_forgepoint_registry_v1_registry_proto_goTypes,
		DependencyIndexes: file_forgepoint_registry_v1_registry_proto_depIdxs,
		EnumInfos:         file_forgepoint_registry_v1_registry_proto_enumTypes,
		MessageInfos:      file_forgepoint_registry_v1_registry_proto_msgTypes,
	}.Build()
	File_forgepoint_registry_v1_registry_proto = out.File
	file_forgepoint_registry_v1_registry_proto_goTypes = nil
	file_forgepoint_registry_v1_registry_proto_depIdxs = nil
}
