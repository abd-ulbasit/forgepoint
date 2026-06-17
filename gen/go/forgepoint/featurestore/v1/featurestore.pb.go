// ============================================================================
// Forgepoint Feature Store Service Proto Definitions
// ============================================================================
//
// WHY: The Feature Store is the system of record for the FEATURES that models
// consume — both at training time (offline, historical, reproducible) and at
// inference time (online, low-latency, "latest value now"). It decouples the
// teams that PRODUCE features (data/ML engineers running pipelines) from the
// teams/services that CONSUME them (training jobs, the inference gateway), so
// neither has to re-implement feature computation or worry about train/serve
// skew. Real-world parallels: Feast, Tecton, AWS SageMaker Feature Store,
// Databricks Feature Store.
//
// PATTERN — EVENT SOURCING:
//   This service is the platform's canonical Event Sourcing example. The core
//   idea: we DO NOT store current state and mutate it in place. Instead, every
//   change is recorded as an immutable, append-only EVENT in a log. The
//   "current value" of any feature is a DERIVED PROJECTION — the result of
//   folding (replaying) the events for an entity. State is a cache of the log,
//   never the source of truth.
//
//     WRITE PATH (commands → events):
//       WriteFeatures  ─► append FeaturesWritten event(s) to feature_events
//                         (id, feature_view_id, entity_id, version, data, ts)
//       DefineFeatureView ─► append FeatureViewDefined event
//
//     READ PATH (queries → projections, NEVER hit the raw log on the hot path):
//       GetOnlineFeatures    ─► Redis hash  feature:{view}:{entity}  (latest)
//       GetHistoricalFeatures─► Postgres "as-of" projection (point-in-time)
//       ListFeatureViews     ─► feature_views metadata projection
//
//   ASCII — the log is the truth, views are caches built from it:
//
//      commands                 append-only EVENT LOG (truth)
//     ┌──────────┐   write      ┌──────────────────────────────┐
//     │WriteFeat.│ ───────────► │ v1 FeaturesWritten {e1,...}  │
//     │DefineView│              │ v2 FeaturesWritten {e1,...}  │
//     └──────────┘              │ v3 FeaturesWritten {e2,...}  │
//                               └───────────────┬──────────────┘
//                         project / fold        │  (also emits NATS events)
//             ┌─────────────────────────────────┼───────────────────────┐
//             ▼                                  ▼                        ▼
//      Redis ONLINE view              Postgres OFFLINE view        NATS subscribers
//   feature:{view}:{entity}      DISTINCT ON(entity) ... DESC   (Experiment Tracker,
//   (GetOnlineFeatures)          (GetHistoricalFeatures)         Model Monitor, ...)
//
// WHY EVENT SOURCING HERE (not just an UPDATE table):
//   1. REPRODUCIBILITY — ML's killer requirement. To retrain or debug a model,
//      you must reconstruct EXACTLY the feature values that existed at training
//      time. An UPDATE-in-place table loses history; an append-only log keeps
//      every version, so GetHistoricalFeatures(as_of=T) is a pure query, not an
//      archaeology project. This is the whole reason feature stores exist.
//   2. TEMPORAL / POINT-IN-TIME queries fall out for free (event before T).
//   3. REBUILDABLE VIEWS — if Redis is flushed or a projection bug is fixed, we
//      replay the log to rebuild online+offline views. The cache can always be
//      regenerated; the log cannot be regenerated.
//   4. AUDIT — every feature value's full lineage is inspectable.
//
// TRADEOFFS / ALTERNATIVES (interview-critical — be ready to defend):
//   - vs CRUD table (UPDATE in place): simplest, but destroys history →
//     impossible point-in-time reads, no reproducibility. Disqualifying here.
//   - vs CDC/temporal tables (Postgres system-versioned rows): you DO keep
//     history, but state is still primary and events are a side-effect; harder
//     to publish typed domain events and to rebuild arbitrary projections.
//   - STORAGE COST: the log grows forever. Mitigation = snapshots (periodic
//     materialized checkpoints so replay starts mid-log, not from genesis) and
//     compaction/retention on very old event versions. Snapshots are an
//     implementation detail of the read side, NOT part of this API contract.
//   - EVENTUAL CONSISTENCY: the online (Redis) projection is updated just after
//     the event is appended, so a GetOnlineFeatures immediately after a
//     WriteFeatures MAY briefly miss it. We document this and expose
//     `as_of_version` in reads so callers can detect staleness.
//
// VERSIONING: package path includes v1 (Buf/Google convention). Breaking
// changes require a new forgepoint.featurestore.v2 package.
//
// NOTE ON NAMING vs the platform design doc:
//   The design doc sketches CreateFeatureSet/IngestFeatures; this contract uses
//   the clearer, consumer-facing names DefineFeatureView/WriteFeatures and the
//   "FeatureView" entity (a named, versioned schema over an entity). The domain
//   is identical — a FeatureView is the "feature set" of the design doc.
// ============================================================================

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.11
// 	protoc        (unknown)
// source: forgepoint/featurestore/v1/featurestore.proto

package featurestorev1

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
// FeatureValueType
// ============================================================================
//
// WHY an enum (not a free-form string): the FeatureView schema declares the
// expected type of each feature. The server validates incoming WriteFeatures
// payloads against these types — rejecting, e.g., a string where a DOUBLE is
// declared. Catching type drift at the boundary prevents silently corrupt
// features from poisoning training data, which is one of the worst, hardest-to-
// debug failure modes in ML ("the model got worse and nobody knows why").
//
// WHY THESE TYPES: they cover the ML feature primitives. Embeddings/vectors are
// represented as DOUBLE_LIST. Free-form/structured blobs fall back to STRUCT.
// Buf STANDARD requires the zero value to be *_UNSPECIFIED and every value to be
// prefixed with the enum name.
// ============================================================================
type FeatureValueType int32

const (
	// Default/unset — an invalid schema declaration. Server rejects on define.
	FeatureValueType_FEATURE_VALUE_TYPE_UNSPECIFIED FeatureValueType = 0
	// 64-bit signed integer (counts, IDs-as-features, ordinals).
	FeatureValueType_FEATURE_VALUE_TYPE_INT64 FeatureValueType = 1
	// IEEE-754 double (most numeric ML features: amounts, ratios, scores).
	FeatureValueType_FEATURE_VALUE_TYPE_DOUBLE FeatureValueType = 2
	// UTF-8 string (categoricals before encoding, raw text features).
	FeatureValueType_FEATURE_VALUE_TYPE_STRING FeatureValueType = 3
	// Boolean flag feature.
	FeatureValueType_FEATURE_VALUE_TYPE_BOOL FeatureValueType = 4
	// Wall-clock time feature (e.g., "last_login_at"). Encoded as Timestamp.
	FeatureValueType_FEATURE_VALUE_TYPE_TIMESTAMP FeatureValueType = 5
	// Vector of doubles — the canonical shape for embeddings. Validation may also
	// assert a fixed dimensionality declared in FeatureSpec.dimension.
	FeatureValueType_FEATURE_VALUE_TYPE_DOUBLE_LIST FeatureValueType = 6
	// Arbitrary nested structure (google.protobuf.Struct). Escape hatch for
	// structured features that don't fit the primitives. Use sparingly — typed
	// features are validated and indexable; structs are opaque.
	FeatureValueType_FEATURE_VALUE_TYPE_STRUCT FeatureValueType = 7
)

// Enum value maps for FeatureValueType.
var (
	FeatureValueType_name = map[int32]string{
		0: "FEATURE_VALUE_TYPE_UNSPECIFIED",
		1: "FEATURE_VALUE_TYPE_INT64",
		2: "FEATURE_VALUE_TYPE_DOUBLE",
		3: "FEATURE_VALUE_TYPE_STRING",
		4: "FEATURE_VALUE_TYPE_BOOL",
		5: "FEATURE_VALUE_TYPE_TIMESTAMP",
		6: "FEATURE_VALUE_TYPE_DOUBLE_LIST",
		7: "FEATURE_VALUE_TYPE_STRUCT",
	}
	FeatureValueType_value = map[string]int32{
		"FEATURE_VALUE_TYPE_UNSPECIFIED": 0,
		"FEATURE_VALUE_TYPE_INT64":       1,
		"FEATURE_VALUE_TYPE_DOUBLE":      2,
		"FEATURE_VALUE_TYPE_STRING":      3,
		"FEATURE_VALUE_TYPE_BOOL":        4,
		"FEATURE_VALUE_TYPE_TIMESTAMP":   5,
		"FEATURE_VALUE_TYPE_DOUBLE_LIST": 6,
		"FEATURE_VALUE_TYPE_STRUCT":      7,
	}
)

func (x FeatureValueType) Enum() *FeatureValueType {
	p := new(FeatureValueType)
	*p = x
	return p
}

func (x FeatureValueType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (FeatureValueType) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_featurestore_v1_featurestore_proto_enumTypes[0].Descriptor()
}

func (FeatureValueType) Type() protoreflect.EnumType {
	return &file_forgepoint_featurestore_v1_featurestore_proto_enumTypes[0]
}

func (x FeatureValueType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use FeatureValueType.Descriptor instead.
func (FeatureValueType) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_featurestore_v1_featurestore_proto_rawDescGZIP(), []int{0}
}

// ============================================================================
// Entity
// ============================================================================
//
// WHY: An Entity is the THING features describe and the KEY features are looked
// up by — e.g., a "user", a "merchant", a "device". Every FeatureView is keyed
// by exactly one Entity, and every feature value belongs to a specific entity
// instance (entity_id, e.g., user "u_123"). At inference the gateway says "give
// me features for user u_123"; the entity tells the store how to interpret
// "u_123".
//
// WHY MODEL IT EXPLICITLY (not just a string key): naming the join key is what
// lets multiple FeatureViews compose at serving time — a model can pull the
// "user_credit" view and the "user_activity" view and join them on the shared
// `user` entity. This mirrors Feast's first-class Entity concept.
//
// SECURITY: `name` and `join_key` are caller-provided identifiers (safe). There
// are no owner/credential fields here — ownership lives on FeatureView and is
// SERVER-assigned (see below) to avoid mass-assignment.
// ============================================================================
type Entity struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Stable, unique entity name within the team: "user", "merchant", "device".
	// Referenced by FeatureView.entity. Immutable once a view uses it.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// The logical join key column this entity is identified by, e.g., "user_id".
	// Documents how an entity_id maps onto upstream data for offline joins.
	JoinKey string `protobuf:"bytes,2,opt,name=join_key,json=joinKey,proto3" json:"join_key,omitempty"`
	// Human-readable description for the catalog/UI.
	Description   string `protobuf:"bytes,3,opt,name=description,proto3" json:"description,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Entity) Reset() {
	*x = Entity{}
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Entity) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Entity) ProtoMessage() {}

func (x *Entity) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Entity.ProtoReflect.Descriptor instead.
func (*Entity) Descriptor() ([]byte, []int) {
	return file_forgepoint_featurestore_v1_featurestore_proto_rawDescGZIP(), []int{0}
}

func (x *Entity) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Entity) GetJoinKey() string {
	if x != nil {
		return x.JoinKey
	}
	return ""
}

func (x *Entity) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

// ============================================================================
// FeatureSpec
// ============================================================================
//
// WHY: One feature's schema — its name, type, and optional shape constraints.
// A FeatureView holds a list of these. The server uses FeatureSpecs to VALIDATE
// every WriteFeatures payload, which is the contract that prevents train/serve
// skew and silent data corruption.
// ============================================================================
type FeatureSpec struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Feature name, unique within its FeatureView: "txn_count_7d", "embedding".
	// Becomes a key in the FeatureValues map written/served for an entity.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// Declared value type. Server validates written values against this.
	ValueType FeatureValueType `protobuf:"varint,2,opt,name=value_type,json=valueType,proto3,enum=forgepoint.featurestore.v1.FeatureValueType" json:"value_type,omitempty"`
	// Human-readable description (units, semantics) for the catalog/UI.
	Description string `protobuf:"bytes,3,opt,name=description,proto3" json:"description,omitempty"`
	// For DOUBLE_LIST features: required vector length (e.g., 768 for an
	// embedding). 0 = unconstrained/not-applicable. Lets the server reject
	// wrong-dimension vectors before they reach training.
	Dimension     int32 `protobuf:"varint,4,opt,name=dimension,proto3" json:"dimension,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *FeatureSpec) Reset() {
	*x = FeatureSpec{}
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *FeatureSpec) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FeatureSpec) ProtoMessage() {}

func (x *FeatureSpec) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FeatureSpec.ProtoReflect.Descriptor instead.
func (*FeatureSpec) Descriptor() ([]byte, []int) {
	return file_forgepoint_featurestore_v1_featurestore_proto_rawDescGZIP(), []int{1}
}

func (x *FeatureSpec) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *FeatureSpec) GetValueType() FeatureValueType {
	if x != nil {
		return x.ValueType
	}
	return FeatureValueType_FEATURE_VALUE_TYPE_UNSPECIFIED
}

func (x *FeatureSpec) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *FeatureSpec) GetDimension() int32 {
	if x != nil {
		return x.Dimension
	}
	return 0
}

// ============================================================================
// FeatureView
// ============================================================================
//
// WHY: A FeatureView is the unit of definition and serving — a NAMED, VERSIONED
// schema (a set of FeatureSpecs) over a single Entity. It is the design doc's
// "feature set". Consumers request features by (feature_view, entity_id); the
// view tells the store which features exist and how to validate/serve them.
//
// IMMUTABILITY + VERSIONING (event-sourcing tie-in): a FeatureView's schema is
// itself defined by an event (FeatureViewDefined). `schema_version` increments
// when the schema changes (a new FeatureViewDefined event), so historical reads
// can be interpreted against the schema that was in effect at the time. We
// favor additive schema evolution (add features) over breaking changes.
//
// SECURITY — SERVER-AUTHORITATIVE FIELDS (NOT accepted on define/write):
//
//	id, owner_user_id, owner_team, schema_version, created_at, updated_at are
//	all set by the SERVER from the authenticated principal / clock / sequence.
//	The client never sends them on DefineFeatureViewRequest. This avoids
//	mass-assignment (a caller forging owner_team to access another team's data
//	or back-dating created_at). The auth interceptor's TokenClaims is the only
//	trusted source of owner identity.
//
// ============================================================================
type FeatureView struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// UUID v4. SERVER-assigned. Stable handle used by all read/write RPCs.
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// Unique, human-readable view name within the team: "user_credit_features".
	Name string `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	// Human-readable description for the catalog/UI.
	Description string `protobuf:"bytes,3,opt,name=description,proto3" json:"description,omitempty"`
	// The Entity this view is keyed by. Every feature row belongs to one
	// entity instance (entity_id) of this entity's type.
	Entity *Entity `protobuf:"bytes,4,opt,name=entity,proto3" json:"entity,omitempty"`
	// The schema: the features this view contains. Server validates writes
	// against these specs.
	Features []*FeatureSpec `protobuf:"bytes,5,rep,name=features,proto3" json:"features,omitempty"`
	// Monotonic schema version. SERVER-assigned. Increments on each schema
	// change (each FeatureViewDefined event for this name). Lets historical
	// reads reason about which schema was in effect.
	SchemaVersion int64 `protobuf:"varint,6,opt,name=schema_version,json=schemaVersion,proto3" json:"schema_version,omitempty"`
	// Owning user (UUID). SERVER-assigned from the authenticated caller. Never
	// accepted from the client (mass-assignment guard).
	OwnerUserId string `protobuf:"bytes,7,opt,name=owner_user_id,json=ownerUserId,proto3" json:"owner_user_id,omitempty"`
	// Owning team label. SERVER-assigned from TokenClaims.team. Used for
	// team-scoped authorization on reads/writes. Never client-supplied.
	OwnerTeam string `protobuf:"bytes,8,opt,name=owner_team,json=ownerTeam,proto3" json:"owner_team,omitempty"`
	// When this view was first defined. SERVER-assigned. Immutable.
	CreatedAt *timestamppb.Timestamp `protobuf:"bytes,9,opt,name=created_at,json=createdAt,proto3" json:"created_at,omitempty"`
	// When this view's schema was last changed. SERVER-assigned.
	UpdatedAt     *timestamppb.Timestamp `protobuf:"bytes,10,opt,name=updated_at,json=updatedAt,proto3" json:"updated_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *FeatureView) Reset() {
	*x = FeatureView{}
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *FeatureView) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FeatureView) ProtoMessage() {}

func (x *FeatureView) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FeatureView.ProtoReflect.Descriptor instead.
func (*FeatureView) Descriptor() ([]byte, []int) {
	return file_forgepoint_featurestore_v1_featurestore_proto_rawDescGZIP(), []int{2}
}

func (x *FeatureView) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *FeatureView) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *FeatureView) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *FeatureView) GetEntity() *Entity {
	if x != nil {
		return x.Entity
	}
	return nil
}

func (x *FeatureView) GetFeatures() []*FeatureSpec {
	if x != nil {
		return x.Features
	}
	return nil
}

func (x *FeatureView) GetSchemaVersion() int64 {
	if x != nil {
		return x.SchemaVersion
	}
	return 0
}

func (x *FeatureView) GetOwnerUserId() string {
	if x != nil {
		return x.OwnerUserId
	}
	return ""
}

func (x *FeatureView) GetOwnerTeam() string {
	if x != nil {
		return x.OwnerTeam
	}
	return ""
}

func (x *FeatureView) GetCreatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CreatedAt
	}
	return nil
}

func (x *FeatureView) GetUpdatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.UpdatedAt
	}
	return nil
}

// ============================================================================
// FeatureValues
// ============================================================================
//
// WHY a map<string, Value>: a single entity's feature row is a set of
// name→value pairs. We use google.protobuf.Value (the well-known type) so any
// declared FeatureValueType — number, string, bool, list (DOUBLE_LIST), or
// nested struct — maps onto one wire representation, while the FeatureView's
// FeatureSpecs carry the authoritative type. This keeps the message generic
// over feature types without an enormous oneof per feature.
//
//	WHY NOT bytes: opaque bytes would lose the ability to inspect/validate/serve
//	individual features and would force every consumer to agree on an encoding.
//	WHY NOT a fixed message per view: views are user-defined at runtime; we
//	can't generate a proto type per view.
//
// The Timestamp here is the EVENT TIME — when these feature values became true
// (NOT when they were ingested). Point-in-time correctness depends on event
// time, so it is a first-class field, not server wall-clock. (Ingest/append
// time is recorded separately by the server in the event log.)
// ============================================================================
type FeatureValues struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The entity instance these values describe, e.g., "u_123". Interpreted
	// against the FeatureView's Entity.join_key.
	EntityId string `protobuf:"bytes,1,opt,name=entity_id,json=entityId,proto3" json:"entity_id,omitempty"`
	// name → value for each feature in this row. Keys must match FeatureSpec
	// names in the view; the server validates value kinds against declared types.
	Values map[string]*structpb.Value `protobuf:"bytes,2,rep,name=values,proto3" json:"values,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	// EVENT TIME: when these values became valid in the real world. This is the
	// timestamp point-in-time queries compare against (GetHistoricalFeatures
	// as_of). Caller-supplied because only the producer knows the true event time
	// (e.g., backfilling yesterday's features). If omitted on write, the server
	// defaults it to ingest time and says so.
	EventTime     *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=event_time,json=eventTime,proto3" json:"event_time,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *FeatureValues) Reset() {
	*x = FeatureValues{}
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *FeatureValues) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FeatureValues) ProtoMessage() {}

func (x *FeatureValues) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FeatureValues.ProtoReflect.Descriptor instead.
func (*FeatureValues) Descriptor() ([]byte, []int) {
	return file_forgepoint_featurestore_v1_featurestore_proto_rawDescGZIP(), []int{3}
}

func (x *FeatureValues) GetEntityId() string {
	if x != nil {
		return x.EntityId
	}
	return ""
}

func (x *FeatureValues) GetValues() map[string]*structpb.Value {
	if x != nil {
		return x.Values
	}
	return nil
}

func (x *FeatureValues) GetEventTime() *timestamppb.Timestamp {
	if x != nil {
		return x.EventTime
	}
	return nil
}

// ============================================================================
// FeatureVector
// ============================================================================
//
// WHY a distinct read-side type (vs reusing FeatureValues): a served feature
// vector carries PROVENANCE the write side doesn't — which schema_version and
// which event/log version produced it, and the projection's own timestamp.
// Returning the version that served the value lets callers (and the Model
// Monitor) detect staleness and reproduce reads. This separation (write command
// shape vs read projection shape) is idiomatic CQRS, the natural companion to
// event sourcing.
// ============================================================================
type FeatureVector struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The entity instance this vector is for.
	EntityId string `protobuf:"bytes,1,opt,name=entity_id,json=entityId,proto3" json:"entity_id,omitempty"`
	// name → value for the requested features. Missing requested features are
	// simply absent here (see GetOnlineFeaturesResponse.missing_features).
	Values map[string]*structpb.Value `protobuf:"bytes,2,rep,name=values,proto3" json:"values,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	// The event-log version that produced these values (the latest event applied
	// for this entity, at or before as_of for historical reads). Lets the caller
	// pin/repro a read and detect staleness against later writes.
	AsOfVersion int64 `protobuf:"varint,3,opt,name=as_of_version,json=asOfVersion,proto3" json:"as_of_version,omitempty"`
	// Event time of the values served (the producing event's event_time).
	EventTime *timestamppb.Timestamp `protobuf:"bytes,4,opt,name=event_time,json=eventTime,proto3" json:"event_time,omitempty"`
	// The FeatureView schema_version these values were validated against.
	SchemaVersion int64 `protobuf:"varint,5,opt,name=schema_version,json=schemaVersion,proto3" json:"schema_version,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *FeatureVector) Reset() {
	*x = FeatureVector{}
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *FeatureVector) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FeatureVector) ProtoMessage() {}

func (x *FeatureVector) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FeatureVector.ProtoReflect.Descriptor instead.
func (*FeatureVector) Descriptor() ([]byte, []int) {
	return file_forgepoint_featurestore_v1_featurestore_proto_rawDescGZIP(), []int{4}
}

func (x *FeatureVector) GetEntityId() string {
	if x != nil {
		return x.EntityId
	}
	return ""
}

func (x *FeatureVector) GetValues() map[string]*structpb.Value {
	if x != nil {
		return x.Values
	}
	return nil
}

func (x *FeatureVector) GetAsOfVersion() int64 {
	if x != nil {
		return x.AsOfVersion
	}
	return 0
}

func (x *FeatureVector) GetEventTime() *timestamppb.Timestamp {
	if x != nil {
		return x.EventTime
	}
	return nil
}

func (x *FeatureVector) GetSchemaVersion() int64 {
	if x != nil {
		return x.SchemaVersion
	}
	return 0
}

// DefineFeatureViewRequest declares (or evolves) a FeatureView schema.
//
// EVENT-SOURCING SEMANTICS: this is a COMMAND that, on success, appends a
// FeatureViewDefined event. Calling it again for the same `name` with a changed
// schema appends another FeatureViewDefined and bumps schema_version (additive
// evolution preferred). The append, not an UPDATE, is the source of truth.
//
// SECURITY: only schema-shaping inputs are accepted. id, owner_user_id,
// owner_team, schema_version, created_at, updated_at are SERVER-assigned (the
// owner from TokenClaims) — deliberately NOT in this request to block
// mass-assignment.
type DefineFeatureViewRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Unique view name within the caller's team. First call creates it; a later
	// call with the same name evolves the schema (new schema_version).
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// Human-readable description for the catalog.
	Description string `protobuf:"bytes,2,opt,name=description,proto3" json:"description,omitempty"`
	// The Entity this view is keyed by (name + join_key). Defining the entity
	// inline keeps the view self-contained for M2; a first-class entity registry
	// is a possible later refinement.
	Entity *Entity `protobuf:"bytes,3,opt,name=entity,proto3" json:"entity,omitempty"`
	// The feature schema (specs). Server validates: non-empty, unique names,
	// valid value_types, sane dimensions.
	Features []*FeatureSpec `protobuf:"bytes,4,rep,name=features,proto3" json:"features,omitempty"`
	// IDEMPOTENCY KEY (interview-critical for "exactly-once intent"):
	//
	//	DefineFeatureView mutates state by appending an event. A network retry
	//	after a server-side success (response lost) must NOT append a duplicate
	//	FeatureViewDefined / spuriously bump schema_version. The server records
	//	processed idempotency_keys; a repeat returns the SAME FeatureView. UUID
	//	v4 generated by the client per logical attempt (not per retry).
	IdempotencyKey string `protobuf:"bytes,5,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *DefineFeatureViewRequest) Reset() {
	*x = DefineFeatureViewRequest{}
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DefineFeatureViewRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DefineFeatureViewRequest) ProtoMessage() {}

func (x *DefineFeatureViewRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DefineFeatureViewRequest.ProtoReflect.Descriptor instead.
func (*DefineFeatureViewRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_featurestore_v1_featurestore_proto_rawDescGZIP(), []int{5}
}

func (x *DefineFeatureViewRequest) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *DefineFeatureViewRequest) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *DefineFeatureViewRequest) GetEntity() *Entity {
	if x != nil {
		return x.Entity
	}
	return nil
}

func (x *DefineFeatureViewRequest) GetFeatures() []*FeatureSpec {
	if x != nil {
		return x.Features
	}
	return nil
}

func (x *DefineFeatureViewRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// DefineFeatureViewResponse wraps the resulting FeatureView (with all
// SERVER-assigned fields populated). Wrapped, not returned bare, per Buf
// RPC_RESPONSE_STANDARD_NAME and for forward-compatibility (e.g., adding a
// `created` bool to distinguish create vs evolve later).
type DefineFeatureViewResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The defined/evolved view, including SERVER-assigned id, owner, version,
	// timestamps.
	FeatureView   *FeatureView `protobuf:"bytes,1,opt,name=feature_view,json=featureView,proto3" json:"feature_view,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *DefineFeatureViewResponse) Reset() {
	*x = DefineFeatureViewResponse{}
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DefineFeatureViewResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DefineFeatureViewResponse) ProtoMessage() {}

func (x *DefineFeatureViewResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DefineFeatureViewResponse.ProtoReflect.Descriptor instead.
func (*DefineFeatureViewResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_featurestore_v1_featurestore_proto_rawDescGZIP(), []int{6}
}

func (x *DefineFeatureViewResponse) GetFeatureView() *FeatureView {
	if x != nil {
		return x.FeatureView
	}
	return nil
}

// ListFeatureViewsRequest pages the FeatureView catalog (a read projection over
// FeatureViewDefined events). Reuses common.v1.PaginationRequest for a uniform
// cursor-based shape across the platform (see common.proto for the WHY).
type ListFeatureViewsRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Optional: case-insensitive substring filter on view name. Empty = no
	// filter. Team scoping is applied SERVER-side from TokenClaims.team (NOT a
	// client field) so a caller cannot enumerate another team's catalog.
	NameFilter string `protobuf:"bytes,1,opt,name=name_filter,json=nameFilter,proto3" json:"name_filter,omitempty"`
	// Cursor-based pagination. page_size defaults to 20; the server CAPS it at
	// 100 (oversized requests are clamped, not rejected) to bound response size
	// and prevent enumeration/DoS.
	Pagination    *v1.PaginationRequest `protobuf:"bytes,2,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListFeatureViewsRequest) Reset() {
	*x = ListFeatureViewsRequest{}
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListFeatureViewsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListFeatureViewsRequest) ProtoMessage() {}

func (x *ListFeatureViewsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListFeatureViewsRequest.ProtoReflect.Descriptor instead.
func (*ListFeatureViewsRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_featurestore_v1_featurestore_proto_rawDescGZIP(), []int{7}
}

func (x *ListFeatureViewsRequest) GetNameFilter() string {
	if x != nil {
		return x.NameFilter
	}
	return ""
}

func (x *ListFeatureViewsRequest) GetPagination() *v1.PaginationRequest {
	if x != nil {
		return x.Pagination
	}
	return nil
}

type ListFeatureViewsResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The page of feature views (catalog projection). Schemas are included so a
	// UI/SDK can render the catalog without N follow-up calls.
	FeatureViews []*FeatureView `protobuf:"bytes,1,rep,name=feature_views,json=featureViews,proto3" json:"feature_views,omitempty"`
	// next_page_token + total_count. total_count may be -1 when expensive.
	Pagination    *v1.PaginationResponse `protobuf:"bytes,2,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListFeatureViewsResponse) Reset() {
	*x = ListFeatureViewsResponse{}
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[8]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListFeatureViewsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListFeatureViewsResponse) ProtoMessage() {}

func (x *ListFeatureViewsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[8]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListFeatureViewsResponse.ProtoReflect.Descriptor instead.
func (*ListFeatureViewsResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_featurestore_v1_featurestore_proto_rawDescGZIP(), []int{8}
}

func (x *ListFeatureViewsResponse) GetFeatureViews() []*FeatureView {
	if x != nil {
		return x.FeatureViews
	}
	return nil
}

func (x *ListFeatureViewsResponse) GetPagination() *v1.PaginationResponse {
	if x != nil {
		return x.Pagination
	}
	return nil
}

// WriteFeaturesRequest appends new feature values — the primary EVENT-SOURCING
// write. It records FeaturesWritten event(s) into the append-only log; it does
// NOT update rows in place. Subsequent reads see these via the materialized
// views the server updates from the new events.
//
// WHY BATCH (repeated FeatureValues): feature pipelines write thousands of rows
// per run. One row per RPC would be chatty and lose write atomicity. A batch
// appends as one logical, ordered set of events (one log version range),
// matching how producers actually emit features.
//
// SECURITY: the caller may write only to views their team owns (enforced
// server-side from TokenClaims). No owner/version/append-time fields are
// accepted — those are SERVER-assigned. event_time inside FeatureValues IS
// caller-supplied (it is domain data the producer owns, not a security field).
type WriteFeaturesRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Target view (UUID, from FeatureView.id). The view's schema validates the
	// payload. Using the immutable id (not name) avoids ambiguity if a view is
	// later renamed.
	FeatureViewId string `protobuf:"bytes,1,opt,name=feature_view_id,json=featureViewId,proto3" json:"feature_view_id,omitempty"`
	// The feature rows to append. Each entry's `values` are validated against the
	// view's FeatureSpecs (names + types + dimensions). A type mismatch fails the
	// WHOLE batch (INVALID_ARGUMENT) so partial-corrupt writes can't slip in.
	Features []*FeatureValues `protobuf:"bytes,2,rep,name=features,proto3" json:"features,omitempty"`
	// IDEMPOTENCY KEY: a retried WriteFeatures (lost response) must not append the
	// same rows twice — duplicate feature events would corrupt counts and
	// point-in-time history. The server deduplicates on this key (UUID v4 per
	// logical batch) and returns the original result on replay. This is how an
	// event-sourced write achieves at-least-once delivery with exactly-once
	// EFFECT.
	IdempotencyKey string `protobuf:"bytes,3,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *WriteFeaturesRequest) Reset() {
	*x = WriteFeaturesRequest{}
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[9]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *WriteFeaturesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WriteFeaturesRequest) ProtoMessage() {}

func (x *WriteFeaturesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[9]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WriteFeaturesRequest.ProtoReflect.Descriptor instead.
func (*WriteFeaturesRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_featurestore_v1_featurestore_proto_rawDescGZIP(), []int{9}
}

func (x *WriteFeaturesRequest) GetFeatureViewId() string {
	if x != nil {
		return x.FeatureViewId
	}
	return ""
}

func (x *WriteFeaturesRequest) GetFeatures() []*FeatureValues {
	if x != nil {
		return x.Features
	}
	return nil
}

func (x *WriteFeaturesRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// WriteFeaturesResponse confirms the append and returns the assigned log
// version range, so producers can correlate, audit, and (if needed) wait for
// the online projection to catch up to written_through_version.
type WriteFeaturesResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Number of feature rows (events) appended.
	WrittenCount int32 `protobuf:"varint,1,opt,name=written_count,json=writtenCount,proto3" json:"written_count,omitempty"`
	// The highest event-log version assigned by this append. Reads observing an
	// as_of_version >= this value are guaranteed to include this batch — useful
	// for read-your-writes checks against the eventually-consistent online view.
	WrittenThroughVersion int64 `protobuf:"varint,2,opt,name=written_through_version,json=writtenThroughVersion,proto3" json:"written_through_version,omitempty"`
	unknownFields         protoimpl.UnknownFields
	sizeCache             protoimpl.SizeCache
}

func (x *WriteFeaturesResponse) Reset() {
	*x = WriteFeaturesResponse{}
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[10]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *WriteFeaturesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WriteFeaturesResponse) ProtoMessage() {}

func (x *WriteFeaturesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[10]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WriteFeaturesResponse.ProtoReflect.Descriptor instead.
func (*WriteFeaturesResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_featurestore_v1_featurestore_proto_rawDescGZIP(), []int{10}
}

func (x *WriteFeaturesResponse) GetWrittenCount() int32 {
	if x != nil {
		return x.WrittenCount
	}
	return 0
}

func (x *WriteFeaturesResponse) GetWrittenThroughVersion() int64 {
	if x != nil {
		return x.WrittenThroughVersion
	}
	return 0
}

// GetOnlineFeaturesRequest reads the LATEST feature values for entities from the
// low-latency online projection (Redis hash feature:{view}:{entity}). This is
// the inference hot path: the Inference Gateway calls it per prediction, so it
// must be fast and is served from the cache projection, never by replaying the
// log.
//
// PATTERN NOTE: this is the "read model" of CQRS over the event log. It is
// eventually consistent with WriteFeatures (see eventual-consistency note in
// the file header); as_of_version in the response lets callers detect lag.
type GetOnlineFeaturesRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Target view (UUID).
	FeatureViewId string `protobuf:"bytes,1,opt,name=feature_view_id,json=featureViewId,proto3" json:"feature_view_id,omitempty"`
	// Entity instances to fetch (e.g., ["u_123","u_456"]). Batched to amortize
	// round-trips for models that score many entities at once. Server CAPS the
	// batch size (documented limit, e.g., 1000) to bound latency/payload.
	EntityIds []string `protobuf:"bytes,2,rep,name=entity_ids,json=entityIds,proto3" json:"entity_ids,omitempty"`
	// Optional projection: only return these feature names. Empty = all features
	// in the view. Lets a model fetch just the columns it needs (smaller payload,
	// less Redis work).
	FeatureNames  []string `protobuf:"bytes,3,rep,name=feature_names,json=featureNames,proto3" json:"feature_names,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetOnlineFeaturesRequest) Reset() {
	*x = GetOnlineFeaturesRequest{}
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[11]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetOnlineFeaturesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetOnlineFeaturesRequest) ProtoMessage() {}

func (x *GetOnlineFeaturesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[11]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetOnlineFeaturesRequest.ProtoReflect.Descriptor instead.
func (*GetOnlineFeaturesRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_featurestore_v1_featurestore_proto_rawDescGZIP(), []int{11}
}

func (x *GetOnlineFeaturesRequest) GetFeatureViewId() string {
	if x != nil {
		return x.FeatureViewId
	}
	return ""
}

func (x *GetOnlineFeaturesRequest) GetEntityIds() []string {
	if x != nil {
		return x.EntityIds
	}
	return nil
}

func (x *GetOnlineFeaturesRequest) GetFeatureNames() []string {
	if x != nil {
		return x.FeatureNames
	}
	return nil
}

type GetOnlineFeaturesResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// One vector per found entity (order not guaranteed). Each carries its
	// as_of_version/event_time/schema_version provenance.
	Vectors []*FeatureVector `protobuf:"bytes,1,rep,name=vectors,proto3" json:"vectors,omitempty"`
	// Entity ids requested but absent in the online view (never written, or
	// evicted). Returned explicitly (rather than silently dropped) so the caller
	// can decide: use a default, fall back to GetHistoricalFeatures, or error.
	MissingEntityIds []string `protobuf:"bytes,2,rep,name=missing_entity_ids,json=missingEntityIds,proto3" json:"missing_entity_ids,omitempty"`
	unknownFields    protoimpl.UnknownFields
	sizeCache        protoimpl.SizeCache
}

func (x *GetOnlineFeaturesResponse) Reset() {
	*x = GetOnlineFeaturesResponse{}
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[12]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetOnlineFeaturesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetOnlineFeaturesResponse) ProtoMessage() {}

func (x *GetOnlineFeaturesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[12]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetOnlineFeaturesResponse.ProtoReflect.Descriptor instead.
func (*GetOnlineFeaturesResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_featurestore_v1_featurestore_proto_rawDescGZIP(), []int{12}
}

func (x *GetOnlineFeaturesResponse) GetVectors() []*FeatureVector {
	if x != nil {
		return x.Vectors
	}
	return nil
}

func (x *GetOnlineFeaturesResponse) GetMissingEntityIds() []string {
	if x != nil {
		return x.MissingEntityIds
	}
	return nil
}

// GetHistoricalFeaturesRequest performs a POINT-IN-TIME ("as-of") read against
// the offline projection of the event log: for each entity, the feature values
// from the latest event with event_time <= as_of. This is the reproducibility
// workhorse — training jobs use it to assemble the exact feature snapshot that
// existed when labels were observed, eliminating LABEL LEAKAGE (using features
// computed AFTER the prediction time).
//
// WHY UNARY (not server-streaming) for M2: training feature retrieval is
// typically a bounded set of (entity, timestamp) lookups assembled into a
// dataset, and the result is paginated. A large-scale "point-in-time JOIN"
// producing millions of rows is a batch/offline job (write to object storage,
// return a URI) rather than a synchronous stream — a deliberate later
// refinement, called out here so the unary choice is defensible.
type GetHistoricalFeaturesRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Target view (UUID).
	FeatureViewId string `protobuf:"bytes,1,opt,name=feature_view_id,json=featureViewId,proto3" json:"feature_view_id,omitempty"`
	// Entity instances to retrieve as-of the timestamp. Server CAPS batch size
	// (documented) to bound the scan.
	EntityIds []string `protobuf:"bytes,2,rep,name=entity_ids,json=entityIds,proto3" json:"entity_ids,omitempty"`
	// POINT-IN-TIME cutoff. For each entity, return the values from the latest
	// event with event_time <= as_of. REQUIRED — a missing as_of would make the
	// query non-reproducible (it would mean "now", which changes over time).
	AsOf *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=as_of,json=asOf,proto3" json:"as_of,omitempty"`
	// Optional feature-name projection. Empty = all features in the view.
	FeatureNames []string `protobuf:"bytes,4,rep,name=feature_names,json=featureNames,proto3" json:"feature_names,omitempty"`
	// Cursor-based pagination over the entity result set (large training pulls
	// page through results). page_size capped at 100 server-side.
	Pagination    *v1.PaginationRequest `protobuf:"bytes,5,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetHistoricalFeaturesRequest) Reset() {
	*x = GetHistoricalFeaturesRequest{}
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[13]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetHistoricalFeaturesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetHistoricalFeaturesRequest) ProtoMessage() {}

func (x *GetHistoricalFeaturesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[13]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetHistoricalFeaturesRequest.ProtoReflect.Descriptor instead.
func (*GetHistoricalFeaturesRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_featurestore_v1_featurestore_proto_rawDescGZIP(), []int{13}
}

func (x *GetHistoricalFeaturesRequest) GetFeatureViewId() string {
	if x != nil {
		return x.FeatureViewId
	}
	return ""
}

func (x *GetHistoricalFeaturesRequest) GetEntityIds() []string {
	if x != nil {
		return x.EntityIds
	}
	return nil
}

func (x *GetHistoricalFeaturesRequest) GetAsOf() *timestamppb.Timestamp {
	if x != nil {
		return x.AsOf
	}
	return nil
}

func (x *GetHistoricalFeaturesRequest) GetFeatureNames() []string {
	if x != nil {
		return x.FeatureNames
	}
	return nil
}

func (x *GetHistoricalFeaturesRequest) GetPagination() *v1.PaginationRequest {
	if x != nil {
		return x.Pagination
	}
	return nil
}

type GetHistoricalFeaturesResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// One vector per entity that had at least one event at or before `as_of`.
	// Each vector's as_of_version is the log version of the event that satisfied
	// the point-in-time read (so the snapshot is exactly reproducible).
	Vectors []*FeatureVector `protobuf:"bytes,1,rep,name=vectors,proto3" json:"vectors,omitempty"`
	// Requested entities with NO event at or before `as_of` (they did not exist
	// yet at that time). Explicit, so training code can drop or default them.
	MissingEntityIds []string `protobuf:"bytes,2,rep,name=missing_entity_ids,json=missingEntityIds,proto3" json:"missing_entity_ids,omitempty"`
	// next_page_token + total_count for the entity result set.
	Pagination    *v1.PaginationResponse `protobuf:"bytes,3,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetHistoricalFeaturesResponse) Reset() {
	*x = GetHistoricalFeaturesResponse{}
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[14]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetHistoricalFeaturesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetHistoricalFeaturesResponse) ProtoMessage() {}

func (x *GetHistoricalFeaturesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[14]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetHistoricalFeaturesResponse.ProtoReflect.Descriptor instead.
func (*GetHistoricalFeaturesResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_featurestore_v1_featurestore_proto_rawDescGZIP(), []int{14}
}

func (x *GetHistoricalFeaturesResponse) GetVectors() []*FeatureVector {
	if x != nil {
		return x.Vectors
	}
	return nil
}

func (x *GetHistoricalFeaturesResponse) GetMissingEntityIds() []string {
	if x != nil {
		return x.MissingEntityIds
	}
	return nil
}

func (x *GetHistoricalFeaturesResponse) GetPagination() *v1.PaginationResponse {
	if x != nil {
		return x.Pagination
	}
	return nil
}

// FeatureViewDefined is published after a FeatureView is created or its schema
// evolves (subject: fp.features.view.defined).
type FeatureViewDefined struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The view's stable id.
	FeatureViewId string `protobuf:"bytes,1,opt,name=feature_view_id,json=featureViewId,proto3" json:"feature_view_id,omitempty"`
	// The view name (human-friendly, for routing/filtering by subscribers).
	Name string `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	// The schema version this event represents (1 on create, +1 on each evolve).
	SchemaVersion int64 `protobuf:"varint,3,opt,name=schema_version,json=schemaVersion,proto3" json:"schema_version,omitempty"`
	// Owning team (for team-scoped consumers/audit). SERVER-assigned, as on the
	// FeatureView itself.
	OwnerTeam string `protobuf:"bytes,4,opt,name=owner_team,json=ownerTeam,proto3" json:"owner_team,omitempty"`
	// When the definition/evolution happened.
	OccurredAt    *timestamppb.Timestamp `protobuf:"bytes,5,opt,name=occurred_at,json=occurredAt,proto3" json:"occurred_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *FeatureViewDefined) Reset() {
	*x = FeatureViewDefined{}
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[15]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *FeatureViewDefined) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FeatureViewDefined) ProtoMessage() {}

func (x *FeatureViewDefined) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[15]
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
	return file_forgepoint_featurestore_v1_featurestore_proto_rawDescGZIP(), []int{15}
}

func (x *FeatureViewDefined) GetFeatureViewId() string {
	if x != nil {
		return x.FeatureViewId
	}
	return ""
}

func (x *FeatureViewDefined) GetName() string {
	if x != nil {
		return x.Name
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

func (x *FeatureViewDefined) GetOccurredAt() *timestamppb.Timestamp {
	if x != nil {
		return x.OccurredAt
	}
	return nil
}

// FeaturesWritten is published after a WriteFeatures append commits
// (subject: fp.features.written). THIN by design (see note above): it announces
// "these entities in this view changed up to this version" so consumers can
// react and, if needed, pull the actual values from the read models.
type FeaturesWritten struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The view that received the append.
	FeatureViewId string `protobuf:"bytes,1,opt,name=feature_view_id,json=featureViewId,proto3" json:"feature_view_id,omitempty"`
	// The view name (convenience for subscribers filtering by name).
	FeatureViewName string `protobuf:"bytes,2,opt,name=feature_view_name,json=featureViewName,proto3" json:"feature_view_name,omitempty"`
	// The entity ids whose features changed in this batch. Consumers (e.g., the
	// Model Monitor) use these to scope drift checks / cache invalidation.
	// For very large batches this MAY be truncated by the server; written_count
	// remains the authoritative total.
	EntityIds []string `protobuf:"bytes,3,rep,name=entity_ids,json=entityIds,proto3" json:"entity_ids,omitempty"`
	// Total rows appended in this batch (authoritative even if entity_ids above
	// is truncated for message-size reasons).
	WrittenCount int32 `protobuf:"varint,4,opt,name=written_count,json=writtenCount,proto3" json:"written_count,omitempty"`
	// Highest event-log version assigned by this append. A consumer can request
	// features as_of_version >= this to be sure it sees the new data.
	WrittenThroughVersion int64 `protobuf:"varint,5,opt,name=written_through_version,json=writtenThroughVersion,proto3" json:"written_through_version,omitempty"`
	// When the append committed (server time).
	OccurredAt    *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=occurred_at,json=occurredAt,proto3" json:"occurred_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *FeaturesWritten) Reset() {
	*x = FeaturesWritten{}
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[16]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *FeaturesWritten) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FeaturesWritten) ProtoMessage() {}

func (x *FeaturesWritten) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_featurestore_v1_featurestore_proto_msgTypes[16]
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
	return file_forgepoint_featurestore_v1_featurestore_proto_rawDescGZIP(), []int{16}
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

func (x *FeaturesWritten) GetOccurredAt() *timestamppb.Timestamp {
	if x != nil {
		return x.OccurredAt
	}
	return nil
}

var File_forgepoint_featurestore_v1_featurestore_proto protoreflect.FileDescriptor

const file_forgepoint_featurestore_v1_featurestore_proto_rawDesc = "" +
	"\n" +
	"-forgepoint/featurestore/v1/featurestore.proto\x12\x1aforgepoint.featurestore.v1\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1cgoogle/protobuf/struct.proto\x1a!forgepoint/common/v1/common.proto\"Y\n" +
	"\x06Entity\x12\x12\n" +
	"\x04name\x18\x01 \x01(\tR\x04name\x12\x19\n" +
	"\bjoin_key\x18\x02 \x01(\tR\ajoinKey\x12 \n" +
	"\vdescription\x18\x03 \x01(\tR\vdescription\"\xae\x01\n" +
	"\vFeatureSpec\x12\x12\n" +
	"\x04name\x18\x01 \x01(\tR\x04name\x12K\n" +
	"\n" +
	"value_type\x18\x02 \x01(\x0e2,.forgepoint.featurestore.v1.FeatureValueTypeR\tvalueType\x12 \n" +
	"\vdescription\x18\x03 \x01(\tR\vdescription\x12\x1c\n" +
	"\tdimension\x18\x04 \x01(\x05R\tdimension\"\xb4\x03\n" +
	"\vFeatureView\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12\x12\n" +
	"\x04name\x18\x02 \x01(\tR\x04name\x12 \n" +
	"\vdescription\x18\x03 \x01(\tR\vdescription\x12:\n" +
	"\x06entity\x18\x04 \x01(\v2\".forgepoint.featurestore.v1.EntityR\x06entity\x12C\n" +
	"\bfeatures\x18\x05 \x03(\v2'.forgepoint.featurestore.v1.FeatureSpecR\bfeatures\x12%\n" +
	"\x0eschema_version\x18\x06 \x01(\x03R\rschemaVersion\x12\"\n" +
	"\rowner_user_id\x18\a \x01(\tR\vownerUserId\x12\x1d\n" +
	"\n" +
	"owner_team\x18\b \x01(\tR\townerTeam\x129\n" +
	"\n" +
	"created_at\x18\t \x01(\v2\x1a.google.protobuf.TimestampR\tcreatedAt\x129\n" +
	"\n" +
	"updated_at\x18\n" +
	" \x01(\v2\x1a.google.protobuf.TimestampR\tupdatedAt\"\x89\x02\n" +
	"\rFeatureValues\x12\x1b\n" +
	"\tentity_id\x18\x01 \x01(\tR\bentityId\x12M\n" +
	"\x06values\x18\x02 \x03(\v25.forgepoint.featurestore.v1.FeatureValues.ValuesEntryR\x06values\x129\n" +
	"\n" +
	"event_time\x18\x03 \x01(\v2\x1a.google.protobuf.TimestampR\teventTime\x1aQ\n" +
	"\vValuesEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12,\n" +
	"\x05value\x18\x02 \x01(\v2\x16.google.protobuf.ValueR\x05value:\x028\x01\"\xd4\x02\n" +
	"\rFeatureVector\x12\x1b\n" +
	"\tentity_id\x18\x01 \x01(\tR\bentityId\x12M\n" +
	"\x06values\x18\x02 \x03(\v25.forgepoint.featurestore.v1.FeatureVector.ValuesEntryR\x06values\x12\"\n" +
	"\ras_of_version\x18\x03 \x01(\x03R\vasOfVersion\x129\n" +
	"\n" +
	"event_time\x18\x04 \x01(\v2\x1a.google.protobuf.TimestampR\teventTime\x12%\n" +
	"\x0eschema_version\x18\x05 \x01(\x03R\rschemaVersion\x1aQ\n" +
	"\vValuesEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12,\n" +
	"\x05value\x18\x02 \x01(\v2\x16.google.protobuf.ValueR\x05value:\x028\x01\"\xfa\x01\n" +
	"\x18DefineFeatureViewRequest\x12\x12\n" +
	"\x04name\x18\x01 \x01(\tR\x04name\x12 \n" +
	"\vdescription\x18\x02 \x01(\tR\vdescription\x12:\n" +
	"\x06entity\x18\x03 \x01(\v2\".forgepoint.featurestore.v1.EntityR\x06entity\x12C\n" +
	"\bfeatures\x18\x04 \x03(\v2'.forgepoint.featurestore.v1.FeatureSpecR\bfeatures\x12'\n" +
	"\x0fidempotency_key\x18\x05 \x01(\tR\x0eidempotencyKey\"g\n" +
	"\x19DefineFeatureViewResponse\x12J\n" +
	"\ffeature_view\x18\x01 \x01(\v2'.forgepoint.featurestore.v1.FeatureViewR\vfeatureView\"\x83\x01\n" +
	"\x17ListFeatureViewsRequest\x12\x1f\n" +
	"\vname_filter\x18\x01 \x01(\tR\n" +
	"nameFilter\x12G\n" +
	"\n" +
	"pagination\x18\x02 \x01(\v2'.forgepoint.common.v1.PaginationRequestR\n" +
	"pagination\"\xb2\x01\n" +
	"\x18ListFeatureViewsResponse\x12L\n" +
	"\rfeature_views\x18\x01 \x03(\v2'.forgepoint.featurestore.v1.FeatureViewR\ffeatureViews\x12H\n" +
	"\n" +
	"pagination\x18\x02 \x01(\v2(.forgepoint.common.v1.PaginationResponseR\n" +
	"pagination\"\xae\x01\n" +
	"\x14WriteFeaturesRequest\x12&\n" +
	"\x0ffeature_view_id\x18\x01 \x01(\tR\rfeatureViewId\x12E\n" +
	"\bfeatures\x18\x02 \x03(\v2).forgepoint.featurestore.v1.FeatureValuesR\bfeatures\x12'\n" +
	"\x0fidempotency_key\x18\x03 \x01(\tR\x0eidempotencyKey\"t\n" +
	"\x15WriteFeaturesResponse\x12#\n" +
	"\rwritten_count\x18\x01 \x01(\x05R\fwrittenCount\x126\n" +
	"\x17written_through_version\x18\x02 \x01(\x03R\x15writtenThroughVersion\"\x86\x01\n" +
	"\x18GetOnlineFeaturesRequest\x12&\n" +
	"\x0ffeature_view_id\x18\x01 \x01(\tR\rfeatureViewId\x12\x1d\n" +
	"\n" +
	"entity_ids\x18\x02 \x03(\tR\tentityIds\x12#\n" +
	"\rfeature_names\x18\x03 \x03(\tR\ffeatureNames\"\x8e\x01\n" +
	"\x19GetOnlineFeaturesResponse\x12C\n" +
	"\avectors\x18\x01 \x03(\v2).forgepoint.featurestore.v1.FeatureVectorR\avectors\x12,\n" +
	"\x12missing_entity_ids\x18\x02 \x03(\tR\x10missingEntityIds\"\x84\x02\n" +
	"\x1cGetHistoricalFeaturesRequest\x12&\n" +
	"\x0ffeature_view_id\x18\x01 \x01(\tR\rfeatureViewId\x12\x1d\n" +
	"\n" +
	"entity_ids\x18\x02 \x03(\tR\tentityIds\x12/\n" +
	"\x05as_of\x18\x03 \x01(\v2\x1a.google.protobuf.TimestampR\x04asOf\x12#\n" +
	"\rfeature_names\x18\x04 \x03(\tR\ffeatureNames\x12G\n" +
	"\n" +
	"pagination\x18\x05 \x01(\v2'.forgepoint.common.v1.PaginationRequestR\n" +
	"pagination\"\xdc\x01\n" +
	"\x1dGetHistoricalFeaturesResponse\x12C\n" +
	"\avectors\x18\x01 \x03(\v2).forgepoint.featurestore.v1.FeatureVectorR\avectors\x12,\n" +
	"\x12missing_entity_ids\x18\x02 \x03(\tR\x10missingEntityIds\x12H\n" +
	"\n" +
	"pagination\x18\x03 \x01(\v2(.forgepoint.common.v1.PaginationResponseR\n" +
	"pagination\"\xd3\x01\n" +
	"\x12FeatureViewDefined\x12&\n" +
	"\x0ffeature_view_id\x18\x01 \x01(\tR\rfeatureViewId\x12\x12\n" +
	"\x04name\x18\x02 \x01(\tR\x04name\x12%\n" +
	"\x0eschema_version\x18\x03 \x01(\x03R\rschemaVersion\x12\x1d\n" +
	"\n" +
	"owner_team\x18\x04 \x01(\tR\townerTeam\x12;\n" +
	"\voccurred_at\x18\x05 \x01(\v2\x1a.google.protobuf.TimestampR\n" +
	"occurredAt\"\x9e\x02\n" +
	"\x0fFeaturesWritten\x12&\n" +
	"\x0ffeature_view_id\x18\x01 \x01(\tR\rfeatureViewId\x12*\n" +
	"\x11feature_view_name\x18\x02 \x01(\tR\x0ffeatureViewName\x12\x1d\n" +
	"\n" +
	"entity_ids\x18\x03 \x03(\tR\tentityIds\x12#\n" +
	"\rwritten_count\x18\x04 \x01(\x05R\fwrittenCount\x126\n" +
	"\x17written_through_version\x18\x05 \x01(\x03R\x15writtenThroughVersion\x12;\n" +
	"\voccurred_at\x18\x06 \x01(\v2\x1a.google.protobuf.TimestampR\n" +
	"occurredAt*\x94\x02\n" +
	"\x10FeatureValueType\x12\"\n" +
	"\x1eFEATURE_VALUE_TYPE_UNSPECIFIED\x10\x00\x12\x1c\n" +
	"\x18FEATURE_VALUE_TYPE_INT64\x10\x01\x12\x1d\n" +
	"\x19FEATURE_VALUE_TYPE_DOUBLE\x10\x02\x12\x1d\n" +
	"\x19FEATURE_VALUE_TYPE_STRING\x10\x03\x12\x1b\n" +
	"\x17FEATURE_VALUE_TYPE_BOOL\x10\x04\x12 \n" +
	"\x1cFEATURE_VALUE_TYPE_TIMESTAMP\x10\x05\x12\"\n" +
	"\x1eFEATURE_VALUE_TYPE_DOUBLE_LIST\x10\x06\x12\x1d\n" +
	"\x19FEATURE_VALUE_TYPE_STRUCT\x10\a2\x9f\x05\n" +
	"\x13FeatureStoreService\x12\x80\x01\n" +
	"\x11DefineFeatureView\x124.forgepoint.featurestore.v1.DefineFeatureViewRequest\x1a5.forgepoint.featurestore.v1.DefineFeatureViewResponse\x12}\n" +
	"\x10ListFeatureViews\x123.forgepoint.featurestore.v1.ListFeatureViewsRequest\x1a4.forgepoint.featurestore.v1.ListFeatureViewsResponse\x12t\n" +
	"\rWriteFeatures\x120.forgepoint.featurestore.v1.WriteFeaturesRequest\x1a1.forgepoint.featurestore.v1.WriteFeaturesResponse\x12\x80\x01\n" +
	"\x11GetOnlineFeatures\x124.forgepoint.featurestore.v1.GetOnlineFeaturesRequest\x1a5.forgepoint.featurestore.v1.GetOnlineFeaturesResponse\x12\x8c\x01\n" +
	"\x15GetHistoricalFeatures\x128.forgepoint.featurestore.v1.GetHistoricalFeaturesRequest\x1a9.forgepoint.featurestore.v1.GetHistoricalFeaturesResponseBTZRgithub.com/abd-ulbasit/forgepoint/gen/go/forgepoint/featurestore/v1;featurestorev1b\x06proto3"

var (
	file_forgepoint_featurestore_v1_featurestore_proto_rawDescOnce sync.Once
	file_forgepoint_featurestore_v1_featurestore_proto_rawDescData []byte
)

func file_forgepoint_featurestore_v1_featurestore_proto_rawDescGZIP() []byte {
	file_forgepoint_featurestore_v1_featurestore_proto_rawDescOnce.Do(func() {
		file_forgepoint_featurestore_v1_featurestore_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_forgepoint_featurestore_v1_featurestore_proto_rawDesc), len(file_forgepoint_featurestore_v1_featurestore_proto_rawDesc)))
	})
	return file_forgepoint_featurestore_v1_featurestore_proto_rawDescData
}

var file_forgepoint_featurestore_v1_featurestore_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_forgepoint_featurestore_v1_featurestore_proto_msgTypes = make([]protoimpl.MessageInfo, 19)
var file_forgepoint_featurestore_v1_featurestore_proto_goTypes = []any{
	(FeatureValueType)(0),                 // 0: forgepoint.featurestore.v1.FeatureValueType
	(*Entity)(nil),                        // 1: forgepoint.featurestore.v1.Entity
	(*FeatureSpec)(nil),                   // 2: forgepoint.featurestore.v1.FeatureSpec
	(*FeatureView)(nil),                   // 3: forgepoint.featurestore.v1.FeatureView
	(*FeatureValues)(nil),                 // 4: forgepoint.featurestore.v1.FeatureValues
	(*FeatureVector)(nil),                 // 5: forgepoint.featurestore.v1.FeatureVector
	(*DefineFeatureViewRequest)(nil),      // 6: forgepoint.featurestore.v1.DefineFeatureViewRequest
	(*DefineFeatureViewResponse)(nil),     // 7: forgepoint.featurestore.v1.DefineFeatureViewResponse
	(*ListFeatureViewsRequest)(nil),       // 8: forgepoint.featurestore.v1.ListFeatureViewsRequest
	(*ListFeatureViewsResponse)(nil),      // 9: forgepoint.featurestore.v1.ListFeatureViewsResponse
	(*WriteFeaturesRequest)(nil),          // 10: forgepoint.featurestore.v1.WriteFeaturesRequest
	(*WriteFeaturesResponse)(nil),         // 11: forgepoint.featurestore.v1.WriteFeaturesResponse
	(*GetOnlineFeaturesRequest)(nil),      // 12: forgepoint.featurestore.v1.GetOnlineFeaturesRequest
	(*GetOnlineFeaturesResponse)(nil),     // 13: forgepoint.featurestore.v1.GetOnlineFeaturesResponse
	(*GetHistoricalFeaturesRequest)(nil),  // 14: forgepoint.featurestore.v1.GetHistoricalFeaturesRequest
	(*GetHistoricalFeaturesResponse)(nil), // 15: forgepoint.featurestore.v1.GetHistoricalFeaturesResponse
	(*FeatureViewDefined)(nil),            // 16: forgepoint.featurestore.v1.FeatureViewDefined
	(*FeaturesWritten)(nil),               // 17: forgepoint.featurestore.v1.FeaturesWritten
	nil,                                   // 18: forgepoint.featurestore.v1.FeatureValues.ValuesEntry
	nil,                                   // 19: forgepoint.featurestore.v1.FeatureVector.ValuesEntry
	(*timestamppb.Timestamp)(nil),         // 20: google.protobuf.Timestamp
	(*v1.PaginationRequest)(nil),          // 21: forgepoint.common.v1.PaginationRequest
	(*v1.PaginationResponse)(nil),         // 22: forgepoint.common.v1.PaginationResponse
	(*structpb.Value)(nil),                // 23: google.protobuf.Value
}
var file_forgepoint_featurestore_v1_featurestore_proto_depIdxs = []int32{
	0,  // 0: forgepoint.featurestore.v1.FeatureSpec.value_type:type_name -> forgepoint.featurestore.v1.FeatureValueType
	1,  // 1: forgepoint.featurestore.v1.FeatureView.entity:type_name -> forgepoint.featurestore.v1.Entity
	2,  // 2: forgepoint.featurestore.v1.FeatureView.features:type_name -> forgepoint.featurestore.v1.FeatureSpec
	20, // 3: forgepoint.featurestore.v1.FeatureView.created_at:type_name -> google.protobuf.Timestamp
	20, // 4: forgepoint.featurestore.v1.FeatureView.updated_at:type_name -> google.protobuf.Timestamp
	18, // 5: forgepoint.featurestore.v1.FeatureValues.values:type_name -> forgepoint.featurestore.v1.FeatureValues.ValuesEntry
	20, // 6: forgepoint.featurestore.v1.FeatureValues.event_time:type_name -> google.protobuf.Timestamp
	19, // 7: forgepoint.featurestore.v1.FeatureVector.values:type_name -> forgepoint.featurestore.v1.FeatureVector.ValuesEntry
	20, // 8: forgepoint.featurestore.v1.FeatureVector.event_time:type_name -> google.protobuf.Timestamp
	1,  // 9: forgepoint.featurestore.v1.DefineFeatureViewRequest.entity:type_name -> forgepoint.featurestore.v1.Entity
	2,  // 10: forgepoint.featurestore.v1.DefineFeatureViewRequest.features:type_name -> forgepoint.featurestore.v1.FeatureSpec
	3,  // 11: forgepoint.featurestore.v1.DefineFeatureViewResponse.feature_view:type_name -> forgepoint.featurestore.v1.FeatureView
	21, // 12: forgepoint.featurestore.v1.ListFeatureViewsRequest.pagination:type_name -> forgepoint.common.v1.PaginationRequest
	3,  // 13: forgepoint.featurestore.v1.ListFeatureViewsResponse.feature_views:type_name -> forgepoint.featurestore.v1.FeatureView
	22, // 14: forgepoint.featurestore.v1.ListFeatureViewsResponse.pagination:type_name -> forgepoint.common.v1.PaginationResponse
	4,  // 15: forgepoint.featurestore.v1.WriteFeaturesRequest.features:type_name -> forgepoint.featurestore.v1.FeatureValues
	5,  // 16: forgepoint.featurestore.v1.GetOnlineFeaturesResponse.vectors:type_name -> forgepoint.featurestore.v1.FeatureVector
	20, // 17: forgepoint.featurestore.v1.GetHistoricalFeaturesRequest.as_of:type_name -> google.protobuf.Timestamp
	21, // 18: forgepoint.featurestore.v1.GetHistoricalFeaturesRequest.pagination:type_name -> forgepoint.common.v1.PaginationRequest
	5,  // 19: forgepoint.featurestore.v1.GetHistoricalFeaturesResponse.vectors:type_name -> forgepoint.featurestore.v1.FeatureVector
	22, // 20: forgepoint.featurestore.v1.GetHistoricalFeaturesResponse.pagination:type_name -> forgepoint.common.v1.PaginationResponse
	20, // 21: forgepoint.featurestore.v1.FeatureViewDefined.occurred_at:type_name -> google.protobuf.Timestamp
	20, // 22: forgepoint.featurestore.v1.FeaturesWritten.occurred_at:type_name -> google.protobuf.Timestamp
	23, // 23: forgepoint.featurestore.v1.FeatureValues.ValuesEntry.value:type_name -> google.protobuf.Value
	23, // 24: forgepoint.featurestore.v1.FeatureVector.ValuesEntry.value:type_name -> google.protobuf.Value
	6,  // 25: forgepoint.featurestore.v1.FeatureStoreService.DefineFeatureView:input_type -> forgepoint.featurestore.v1.DefineFeatureViewRequest
	8,  // 26: forgepoint.featurestore.v1.FeatureStoreService.ListFeatureViews:input_type -> forgepoint.featurestore.v1.ListFeatureViewsRequest
	10, // 27: forgepoint.featurestore.v1.FeatureStoreService.WriteFeatures:input_type -> forgepoint.featurestore.v1.WriteFeaturesRequest
	12, // 28: forgepoint.featurestore.v1.FeatureStoreService.GetOnlineFeatures:input_type -> forgepoint.featurestore.v1.GetOnlineFeaturesRequest
	14, // 29: forgepoint.featurestore.v1.FeatureStoreService.GetHistoricalFeatures:input_type -> forgepoint.featurestore.v1.GetHistoricalFeaturesRequest
	7,  // 30: forgepoint.featurestore.v1.FeatureStoreService.DefineFeatureView:output_type -> forgepoint.featurestore.v1.DefineFeatureViewResponse
	9,  // 31: forgepoint.featurestore.v1.FeatureStoreService.ListFeatureViews:output_type -> forgepoint.featurestore.v1.ListFeatureViewsResponse
	11, // 32: forgepoint.featurestore.v1.FeatureStoreService.WriteFeatures:output_type -> forgepoint.featurestore.v1.WriteFeaturesResponse
	13, // 33: forgepoint.featurestore.v1.FeatureStoreService.GetOnlineFeatures:output_type -> forgepoint.featurestore.v1.GetOnlineFeaturesResponse
	15, // 34: forgepoint.featurestore.v1.FeatureStoreService.GetHistoricalFeatures:output_type -> forgepoint.featurestore.v1.GetHistoricalFeaturesResponse
	30, // [30:35] is the sub-list for method output_type
	25, // [25:30] is the sub-list for method input_type
	25, // [25:25] is the sub-list for extension type_name
	25, // [25:25] is the sub-list for extension extendee
	0,  // [0:25] is the sub-list for field type_name
}

func init() { file_forgepoint_featurestore_v1_featurestore_proto_init() }
func file_forgepoint_featurestore_v1_featurestore_proto_init() {
	if File_forgepoint_featurestore_v1_featurestore_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_forgepoint_featurestore_v1_featurestore_proto_rawDesc), len(file_forgepoint_featurestore_v1_featurestore_proto_rawDesc)),
			NumEnums:      1,
			NumMessages:   19,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_forgepoint_featurestore_v1_featurestore_proto_goTypes,
		DependencyIndexes: file_forgepoint_featurestore_v1_featurestore_proto_depIdxs,
		EnumInfos:         file_forgepoint_featurestore_v1_featurestore_proto_enumTypes,
		MessageInfos:      file_forgepoint_featurestore_v1_featurestore_proto_msgTypes,
	}.Build()
	File_forgepoint_featurestore_v1_featurestore_proto = out.File
	file_forgepoint_featurestore_v1_featurestore_proto_goTypes = nil
	file_forgepoint_featurestore_v1_featurestore_proto_depIdxs = nil
}
