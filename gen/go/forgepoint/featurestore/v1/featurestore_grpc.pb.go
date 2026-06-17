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
//       GetFeatureView       ─► one view's schema (metadata projection)
//       ListFeatureViews     ─► feature_views metadata projection (fleet/catalog)
//
//     ADMIN (event-sourcing lifecycle):
//       DeleteFeatureView    ─► append FeatureViewDeleted (soft retire; log kept)
//       RebuildViews         ─► replay the log → regenerate Redis + Postgres views
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

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.6.2
// - protoc             (unknown)
// source: forgepoint/featurestore/v1/featurestore.proto

package featurestorev1

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
	FeatureStoreService_DefineFeatureView_FullMethodName     = "/forgepoint.featurestore.v1.FeatureStoreService/DefineFeatureView"
	FeatureStoreService_GetFeatureView_FullMethodName        = "/forgepoint.featurestore.v1.FeatureStoreService/GetFeatureView"
	FeatureStoreService_ListFeatureViews_FullMethodName      = "/forgepoint.featurestore.v1.FeatureStoreService/ListFeatureViews"
	FeatureStoreService_WriteFeatures_FullMethodName         = "/forgepoint.featurestore.v1.FeatureStoreService/WriteFeatures"
	FeatureStoreService_GetOnlineFeatures_FullMethodName     = "/forgepoint.featurestore.v1.FeatureStoreService/GetOnlineFeatures"
	FeatureStoreService_GetHistoricalFeatures_FullMethodName = "/forgepoint.featurestore.v1.FeatureStoreService/GetHistoricalFeatures"
	FeatureStoreService_DeleteFeatureView_FullMethodName     = "/forgepoint.featurestore.v1.FeatureStoreService/DeleteFeatureView"
	FeatureStoreService_RebuildViews_FullMethodName          = "/forgepoint.featurestore.v1.FeatureStoreService/RebuildViews"
)

// FeatureStoreServiceClient is the client API for FeatureStoreService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// ============================================================================
// EVENT PAYLOADS — NOW CANONICAL (forgepoint.events.v1), NOT redefined here
// ============================================================================
//
// The Feature Store PRODUCES two events. Their payload messages used to live in
// THIS file; they have been MOVED to the platform's single canonical event
// package and DELETED here. Producers pack the canonical messages into
// common.v1.EventEnvelope.data (a google.protobuf.Any); consumers unpack the
// SAME message by its Any type_url — one contract, both ends of every pipe.
//
//	WHAT THE FEATURE STORE PUBLISHES (producer = "feature-store"):
//	  fp.features.view.defined  → forgepoint.events.v1.FeatureViewDefined
//	  fp.features.written       → forgepoint.events.v1.FeaturesWritten
//
//	WHO CONSUMES (informational — defined by the consumers, not us):
//	  FeatureViewDefined → experiment-tracker (correlate which feature schema
//	                       versions fed a training run), audit subscribers.
//	  FeaturesWritten    → experiment-tracker (which feature versions fed a
//	                       run), model-monitor (scope drift checks / cache
//	                       invalidation to the changed entities).
//
// WHY MOVED (the bug this fixes — conflict #4 in events.proto):
//
//	The platform's async nervous system had DRIFTED. The design doc and the
//	experiment-tracker subscribed to "fp.features.ingested" / "FeaturesIngested",
//	while this service published "FeaturesWritten" on "fp.features.written".
//	Same fact, two names — the subscription would NEVER fire. Centralizing the
//	payloads in forgepoint.events.v1 makes that class of mismatch impossible:
//	producer and consumer literally import the same generated Go type. (The
//	experiment-tracker fix repoints its subscription to fp.features.written.)
//
//	It also DECOUPLES the event schema from this service's API schema. The old
//	local messages were fine until you ask: should the Experiment Tracker
//	compile-depend on featurestore.proto just to read an event? No. The event is
//	a separately-versioned PUBLISHED CONTRACT (schema-registry thinking); it
//	imports only well-known types + common, never a service's API proto.
//
// THIN-BY-DESIGN (unchanged, and codified in the canonical message):
//
//	events.FeaturesWritten carries the affected entity_ids + a count +
//	written_through_version — NOT the feature VALUES. Events are for
//	NOTIFICATION/correlation; a consumer that needs the actual values calls
//	GetOnlineFeatures/GetHistoricalFeatures (the read models below). This keeps
//	NATS small and stops the bus becoming a second source of truth. (This is the
//	"thin event + callback" side of the fat-vs-thin tradeoff — chosen here
//	because feature rows are large and only some consumers want the values.)
//
// PUBLISHER FIELD MAPPING (a few lines in the handler at the publish boundary —
// the price of decoupling, and the only thing the rename touches):
//
//	DefineFeatureView success →  events.FeatureViewDefined{
//	    feature_view_id   = view.id
//	    feature_view_name = view.name        // canonical field is feature_view_name
//	    schema_version    = view.schema_version
//	    owner_team        = view.owner_team
//	    defined_at        = view.updated_at  // RENAMED: old local occurred_at → defined_at
//	}
//	WriteFeatures success →      events.FeaturesWritten{
//	    feature_view_id          = req.feature_view_id
//	    feature_view_name        = view.name
//	    entity_ids               = affected ids (MAY be truncated; count is authoritative)
//	    written_count            = rows appended
//	    written_through_version  = resp.written_through_version
//	    written_at               = append commit time  // RENAMED: old occurred_at → written_at
//	}
//
// The canonical messages live in proto/forgepoint/events/v1/events.proto under
// the "FEATURE-STORE EVENTS" section — read there for field-level docs.
//
// ============================================================================
// FEATURE STORE SERVICE
// ============================================================================
//
// WHY one FeatureStoreService (not split into Catalog/Write/Serve services):
//
//	definition (schema), ingestion (append), and serving (project) all operate
//	on the SAME event log and views and share the same team-ownership boundary.
//	Splitting them would force inter-service calls and a shared event store
//	across service boundaries — violating database-per-service. They live
//	behind one boundary; CQRS happens INSIDE this service (write model = event
//	log; read models = Redis online + Postgres offline), not across services.
//
// RPC CATEGORIES:
//
//	DEFINITION (catalog):  DefineFeatureView, GetFeatureView, ListFeatureViews
//	WRITE (commands):      WriteFeatures                 → appends events
//	READ (projections):    GetOnlineFeatures (latest),
//	                       GetHistoricalFeatures (point-in-time)
//	ADMIN (lifecycle):     DeleteFeatureView (soft retire → FeatureViewDeleted),
//	                       RebuildViews (replay log → regenerate projections)
//
// SERVER-ENFORCED LIMITS (in the CONTRACT, not just prose — see each RPC):
//   - page_size (ListFeatureViews, GetHistoricalFeatures): default 20, hard cap
//     MAX_PAGE_SIZE = 100 (oversized requests are CLAMPED, not rejected).
//   - batch size (entity_ids on GetOnline/GetHistorical; features on Write):
//     hard cap MAX_BATCH_SIZE = 1000 entities/rows; OVER-cap requests are
//     REJECTED with INVALID_ARGUMENT (a too-large write must fail loudly, not be
//     silently truncated — truncation would drop feature rows). proto3 has no
//     native numeric bound, so these caps are documented as named constants and
//     enforced server-side; the constants are the contract.
//
// STREAMING CHOICE (interview-critical):
//
//	DATA-PLANE RPCs are UNARY; the long-running ADMIN replay is SERVER-STREAMING.
//	  - WriteFeatures: producers emit in BATCHES (one RPC = one atomic append
//	    with one idempotency key). Client-streaming would blur the idempotency/
//	    atomicity boundary (when is the append "committed"?). Bounded batches +
//	    idempotency key is simpler and safer.
//	  - GetOnlineFeatures: a single fast key→value projection read; sub-ms
//	    budget on the inference hot path — streaming overhead is unwarranted.
//	  - GetHistoricalFeatures: bounded, PAGINATED point-in-time pulls. Truly
//	    huge training joins are an offline batch job (export to object storage),
//	    not a synchronous server stream — a deliberate later refinement.
//	  - RebuildViews: SERVER-STREAMING — a full log replay runs for minutes, so
//	    we stream RebuildViewsResponse frames for live feedback and use stream-close as the
//	    completion signal (no poll loop). Note this is justified by DURATION, not
//	    data volume — the contrast with the unary reads is the interview point.
//	So the only streaming RPC is the admin replay, deliberately, and every choice
//	is documented so it is defensible.
//
// EVENT-SOURCING RECAP (what an interviewer will probe):
//   - Source of truth = append-only feature_events log. Views are caches.
//   - Reads never replay on the hot path; they read projections.
//   - Reproducibility = GetHistoricalFeatures(as_of) over event_time.
//   - Recovery = RebuildViews replays the log to rebuild Redis + Postgres views.
//   - Soft delete = DeleteFeatureView appends a terminal event; history is kept.
//   - Exactly-once EFFECT under retries = idempotency_key on mutating RPCs.
//   - Eventual consistency online = surfaced via as_of_version in reads.
//   - Async contract = publishes forgepoint.events.v1 FeatureViewDefined /
//     FeaturesWritten (canonical, decoupled from this API proto).
//
// ============================================================================
type FeatureStoreServiceClient interface {
	// DefineFeatureView creates a FeatureView or evolves its schema (append a
	// FeatureViewDefined event; bump schema_version). Owner/team/timestamps are
	// SERVER-assigned. Idempotent via idempotency_key. On commit, publishes
	// forgepoint.events.v1.FeatureViewDefined on fp.features.view.defined.
	// Requires "features:write".
	DefineFeatureView(ctx context.Context, in *DefineFeatureViewRequest, opts ...grpc.CallOption) (*DefineFeatureViewResponse, error)
	// GetFeatureView returns a SINGLE view's current schema/metadata by id or name
	// (a read of the catalog projection), scoped to the caller's team. Requires
	// "features:read".
	GetFeatureView(ctx context.Context, in *GetFeatureViewRequest, opts ...grpc.CallOption) (*GetFeatureViewResponse, error)
	// ListFeatureViews returns a paginated catalog of feature views (a projection
	// over FeatureViewDefined events), scoped to the caller's team. page_size
	// defaults to 20 and is CAPPED at MAX_PAGE_SIZE=100 server-side (clamped).
	// Requires "features:read".
	ListFeatureViews(ctx context.Context, in *ListFeatureViewsRequest, opts ...grpc.CallOption) (*ListFeatureViewsResponse, error)
	// WriteFeatures appends a batch of feature rows to the event log (the core
	// event-sourcing write). Values are validated against the view schema; a type
	// mismatch fails the whole batch. Batch size is capped at MAX_BATCH_SIZE=1000
	// rows (over-cap → INVALID_ARGUMENT). Idempotent via idempotency_key. On
	// commit, publishes forgepoint.events.v1.FeaturesWritten on fp.features.written.
	// Requires "features:write".
	WriteFeatures(ctx context.Context, in *WriteFeaturesRequest, opts ...grpc.CallOption) (*WriteFeaturesResponse, error)
	// GetOnlineFeatures returns the LATEST feature values for entities from the
	// low-latency online projection (the inference hot path). entity_ids batch is
	// capped at MAX_BATCH_SIZE=1000. Eventually consistent with writes; response
	// carries as_of_version for staleness detection. Requires "features:read".
	GetOnlineFeatures(ctx context.Context, in *GetOnlineFeaturesRequest, opts ...grpc.CallOption) (*GetOnlineFeaturesResponse, error)
	// GetHistoricalFeatures returns POINT-IN-TIME ("as-of") feature values from
	// the offline projection — for each entity, the latest event with
	// event_time <= as_of. Powers reproducible training and prevents label
	// leakage. entity_ids batch capped at MAX_BATCH_SIZE=1000; result paginated
	// with page_size capped at MAX_PAGE_SIZE=100. Requires "features:read".
	GetHistoricalFeatures(ctx context.Context, in *GetHistoricalFeaturesRequest, opts ...grpc.CallOption) (*GetHistoricalFeaturesResponse, error)
	// DeleteFeatureView soft-retires a view by appending a terminal
	// FeatureViewDeleted event (the log/history is preserved for reproducibility;
	// the online projection is torn down). Idempotent via idempotency_key.
	// Requires "features:write".
	DeleteFeatureView(ctx context.Context, in *DeleteFeatureViewRequest, opts ...grpc.CallOption) (*DeleteFeatureViewResponse, error)
	// RebuildViews replays the append-only log to regenerate the materialized
	// projections (Redis online + Postgres offline) — the event-sourcing recovery
	// path. SERVER-STREAMING: emits RebuildViewsResponse progress frames until done.
	// This is an ADMIN op requiring the elevated "features:admin" scope (a full
	// replay is expensive and sensitive — not reachable with a routine write token).
	RebuildViews(ctx context.Context, in *RebuildViewsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[RebuildViewsResponse], error)
}

type featureStoreServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewFeatureStoreServiceClient(cc grpc.ClientConnInterface) FeatureStoreServiceClient {
	return &featureStoreServiceClient{cc}
}

func (c *featureStoreServiceClient) DefineFeatureView(ctx context.Context, in *DefineFeatureViewRequest, opts ...grpc.CallOption) (*DefineFeatureViewResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(DefineFeatureViewResponse)
	err := c.cc.Invoke(ctx, FeatureStoreService_DefineFeatureView_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *featureStoreServiceClient) GetFeatureView(ctx context.Context, in *GetFeatureViewRequest, opts ...grpc.CallOption) (*GetFeatureViewResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetFeatureViewResponse)
	err := c.cc.Invoke(ctx, FeatureStoreService_GetFeatureView_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *featureStoreServiceClient) ListFeatureViews(ctx context.Context, in *ListFeatureViewsRequest, opts ...grpc.CallOption) (*ListFeatureViewsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ListFeatureViewsResponse)
	err := c.cc.Invoke(ctx, FeatureStoreService_ListFeatureViews_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *featureStoreServiceClient) WriteFeatures(ctx context.Context, in *WriteFeaturesRequest, opts ...grpc.CallOption) (*WriteFeaturesResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(WriteFeaturesResponse)
	err := c.cc.Invoke(ctx, FeatureStoreService_WriteFeatures_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *featureStoreServiceClient) GetOnlineFeatures(ctx context.Context, in *GetOnlineFeaturesRequest, opts ...grpc.CallOption) (*GetOnlineFeaturesResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetOnlineFeaturesResponse)
	err := c.cc.Invoke(ctx, FeatureStoreService_GetOnlineFeatures_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *featureStoreServiceClient) GetHistoricalFeatures(ctx context.Context, in *GetHistoricalFeaturesRequest, opts ...grpc.CallOption) (*GetHistoricalFeaturesResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetHistoricalFeaturesResponse)
	err := c.cc.Invoke(ctx, FeatureStoreService_GetHistoricalFeatures_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *featureStoreServiceClient) DeleteFeatureView(ctx context.Context, in *DeleteFeatureViewRequest, opts ...grpc.CallOption) (*DeleteFeatureViewResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(DeleteFeatureViewResponse)
	err := c.cc.Invoke(ctx, FeatureStoreService_DeleteFeatureView_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *featureStoreServiceClient) RebuildViews(ctx context.Context, in *RebuildViewsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[RebuildViewsResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &FeatureStoreService_ServiceDesc.Streams[0], FeatureStoreService_RebuildViews_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[RebuildViewsRequest, RebuildViewsResponse]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type FeatureStoreService_RebuildViewsClient = grpc.ServerStreamingClient[RebuildViewsResponse]

// FeatureStoreServiceServer is the server API for FeatureStoreService service.
// All implementations must embed UnimplementedFeatureStoreServiceServer
// for forward compatibility.
//
// ============================================================================
// EVENT PAYLOADS — NOW CANONICAL (forgepoint.events.v1), NOT redefined here
// ============================================================================
//
// The Feature Store PRODUCES two events. Their payload messages used to live in
// THIS file; they have been MOVED to the platform's single canonical event
// package and DELETED here. Producers pack the canonical messages into
// common.v1.EventEnvelope.data (a google.protobuf.Any); consumers unpack the
// SAME message by its Any type_url — one contract, both ends of every pipe.
//
//	WHAT THE FEATURE STORE PUBLISHES (producer = "feature-store"):
//	  fp.features.view.defined  → forgepoint.events.v1.FeatureViewDefined
//	  fp.features.written       → forgepoint.events.v1.FeaturesWritten
//
//	WHO CONSUMES (informational — defined by the consumers, not us):
//	  FeatureViewDefined → experiment-tracker (correlate which feature schema
//	                       versions fed a training run), audit subscribers.
//	  FeaturesWritten    → experiment-tracker (which feature versions fed a
//	                       run), model-monitor (scope drift checks / cache
//	                       invalidation to the changed entities).
//
// WHY MOVED (the bug this fixes — conflict #4 in events.proto):
//
//	The platform's async nervous system had DRIFTED. The design doc and the
//	experiment-tracker subscribed to "fp.features.ingested" / "FeaturesIngested",
//	while this service published "FeaturesWritten" on "fp.features.written".
//	Same fact, two names — the subscription would NEVER fire. Centralizing the
//	payloads in forgepoint.events.v1 makes that class of mismatch impossible:
//	producer and consumer literally import the same generated Go type. (The
//	experiment-tracker fix repoints its subscription to fp.features.written.)
//
//	It also DECOUPLES the event schema from this service's API schema. The old
//	local messages were fine until you ask: should the Experiment Tracker
//	compile-depend on featurestore.proto just to read an event? No. The event is
//	a separately-versioned PUBLISHED CONTRACT (schema-registry thinking); it
//	imports only well-known types + common, never a service's API proto.
//
// THIN-BY-DESIGN (unchanged, and codified in the canonical message):
//
//	events.FeaturesWritten carries the affected entity_ids + a count +
//	written_through_version — NOT the feature VALUES. Events are for
//	NOTIFICATION/correlation; a consumer that needs the actual values calls
//	GetOnlineFeatures/GetHistoricalFeatures (the read models below). This keeps
//	NATS small and stops the bus becoming a second source of truth. (This is the
//	"thin event + callback" side of the fat-vs-thin tradeoff — chosen here
//	because feature rows are large and only some consumers want the values.)
//
// PUBLISHER FIELD MAPPING (a few lines in the handler at the publish boundary —
// the price of decoupling, and the only thing the rename touches):
//
//	DefineFeatureView success →  events.FeatureViewDefined{
//	    feature_view_id   = view.id
//	    feature_view_name = view.name        // canonical field is feature_view_name
//	    schema_version    = view.schema_version
//	    owner_team        = view.owner_team
//	    defined_at        = view.updated_at  // RENAMED: old local occurred_at → defined_at
//	}
//	WriteFeatures success →      events.FeaturesWritten{
//	    feature_view_id          = req.feature_view_id
//	    feature_view_name        = view.name
//	    entity_ids               = affected ids (MAY be truncated; count is authoritative)
//	    written_count            = rows appended
//	    written_through_version  = resp.written_through_version
//	    written_at               = append commit time  // RENAMED: old occurred_at → written_at
//	}
//
// The canonical messages live in proto/forgepoint/events/v1/events.proto under
// the "FEATURE-STORE EVENTS" section — read there for field-level docs.
//
// ============================================================================
// FEATURE STORE SERVICE
// ============================================================================
//
// WHY one FeatureStoreService (not split into Catalog/Write/Serve services):
//
//	definition (schema), ingestion (append), and serving (project) all operate
//	on the SAME event log and views and share the same team-ownership boundary.
//	Splitting them would force inter-service calls and a shared event store
//	across service boundaries — violating database-per-service. They live
//	behind one boundary; CQRS happens INSIDE this service (write model = event
//	log; read models = Redis online + Postgres offline), not across services.
//
// RPC CATEGORIES:
//
//	DEFINITION (catalog):  DefineFeatureView, GetFeatureView, ListFeatureViews
//	WRITE (commands):      WriteFeatures                 → appends events
//	READ (projections):    GetOnlineFeatures (latest),
//	                       GetHistoricalFeatures (point-in-time)
//	ADMIN (lifecycle):     DeleteFeatureView (soft retire → FeatureViewDeleted),
//	                       RebuildViews (replay log → regenerate projections)
//
// SERVER-ENFORCED LIMITS (in the CONTRACT, not just prose — see each RPC):
//   - page_size (ListFeatureViews, GetHistoricalFeatures): default 20, hard cap
//     MAX_PAGE_SIZE = 100 (oversized requests are CLAMPED, not rejected).
//   - batch size (entity_ids on GetOnline/GetHistorical; features on Write):
//     hard cap MAX_BATCH_SIZE = 1000 entities/rows; OVER-cap requests are
//     REJECTED with INVALID_ARGUMENT (a too-large write must fail loudly, not be
//     silently truncated — truncation would drop feature rows). proto3 has no
//     native numeric bound, so these caps are documented as named constants and
//     enforced server-side; the constants are the contract.
//
// STREAMING CHOICE (interview-critical):
//
//	DATA-PLANE RPCs are UNARY; the long-running ADMIN replay is SERVER-STREAMING.
//	  - WriteFeatures: producers emit in BATCHES (one RPC = one atomic append
//	    with one idempotency key). Client-streaming would blur the idempotency/
//	    atomicity boundary (when is the append "committed"?). Bounded batches +
//	    idempotency key is simpler and safer.
//	  - GetOnlineFeatures: a single fast key→value projection read; sub-ms
//	    budget on the inference hot path — streaming overhead is unwarranted.
//	  - GetHistoricalFeatures: bounded, PAGINATED point-in-time pulls. Truly
//	    huge training joins are an offline batch job (export to object storage),
//	    not a synchronous server stream — a deliberate later refinement.
//	  - RebuildViews: SERVER-STREAMING — a full log replay runs for minutes, so
//	    we stream RebuildViewsResponse frames for live feedback and use stream-close as the
//	    completion signal (no poll loop). Note this is justified by DURATION, not
//	    data volume — the contrast with the unary reads is the interview point.
//	So the only streaming RPC is the admin replay, deliberately, and every choice
//	is documented so it is defensible.
//
// EVENT-SOURCING RECAP (what an interviewer will probe):
//   - Source of truth = append-only feature_events log. Views are caches.
//   - Reads never replay on the hot path; they read projections.
//   - Reproducibility = GetHistoricalFeatures(as_of) over event_time.
//   - Recovery = RebuildViews replays the log to rebuild Redis + Postgres views.
//   - Soft delete = DeleteFeatureView appends a terminal event; history is kept.
//   - Exactly-once EFFECT under retries = idempotency_key on mutating RPCs.
//   - Eventual consistency online = surfaced via as_of_version in reads.
//   - Async contract = publishes forgepoint.events.v1 FeatureViewDefined /
//     FeaturesWritten (canonical, decoupled from this API proto).
//
// ============================================================================
type FeatureStoreServiceServer interface {
	// DefineFeatureView creates a FeatureView or evolves its schema (append a
	// FeatureViewDefined event; bump schema_version). Owner/team/timestamps are
	// SERVER-assigned. Idempotent via idempotency_key. On commit, publishes
	// forgepoint.events.v1.FeatureViewDefined on fp.features.view.defined.
	// Requires "features:write".
	DefineFeatureView(context.Context, *DefineFeatureViewRequest) (*DefineFeatureViewResponse, error)
	// GetFeatureView returns a SINGLE view's current schema/metadata by id or name
	// (a read of the catalog projection), scoped to the caller's team. Requires
	// "features:read".
	GetFeatureView(context.Context, *GetFeatureViewRequest) (*GetFeatureViewResponse, error)
	// ListFeatureViews returns a paginated catalog of feature views (a projection
	// over FeatureViewDefined events), scoped to the caller's team. page_size
	// defaults to 20 and is CAPPED at MAX_PAGE_SIZE=100 server-side (clamped).
	// Requires "features:read".
	ListFeatureViews(context.Context, *ListFeatureViewsRequest) (*ListFeatureViewsResponse, error)
	// WriteFeatures appends a batch of feature rows to the event log (the core
	// event-sourcing write). Values are validated against the view schema; a type
	// mismatch fails the whole batch. Batch size is capped at MAX_BATCH_SIZE=1000
	// rows (over-cap → INVALID_ARGUMENT). Idempotent via idempotency_key. On
	// commit, publishes forgepoint.events.v1.FeaturesWritten on fp.features.written.
	// Requires "features:write".
	WriteFeatures(context.Context, *WriteFeaturesRequest) (*WriteFeaturesResponse, error)
	// GetOnlineFeatures returns the LATEST feature values for entities from the
	// low-latency online projection (the inference hot path). entity_ids batch is
	// capped at MAX_BATCH_SIZE=1000. Eventually consistent with writes; response
	// carries as_of_version for staleness detection. Requires "features:read".
	GetOnlineFeatures(context.Context, *GetOnlineFeaturesRequest) (*GetOnlineFeaturesResponse, error)
	// GetHistoricalFeatures returns POINT-IN-TIME ("as-of") feature values from
	// the offline projection — for each entity, the latest event with
	// event_time <= as_of. Powers reproducible training and prevents label
	// leakage. entity_ids batch capped at MAX_BATCH_SIZE=1000; result paginated
	// with page_size capped at MAX_PAGE_SIZE=100. Requires "features:read".
	GetHistoricalFeatures(context.Context, *GetHistoricalFeaturesRequest) (*GetHistoricalFeaturesResponse, error)
	// DeleteFeatureView soft-retires a view by appending a terminal
	// FeatureViewDeleted event (the log/history is preserved for reproducibility;
	// the online projection is torn down). Idempotent via idempotency_key.
	// Requires "features:write".
	DeleteFeatureView(context.Context, *DeleteFeatureViewRequest) (*DeleteFeatureViewResponse, error)
	// RebuildViews replays the append-only log to regenerate the materialized
	// projections (Redis online + Postgres offline) — the event-sourcing recovery
	// path. SERVER-STREAMING: emits RebuildViewsResponse progress frames until done.
	// This is an ADMIN op requiring the elevated "features:admin" scope (a full
	// replay is expensive and sensitive — not reachable with a routine write token).
	RebuildViews(*RebuildViewsRequest, grpc.ServerStreamingServer[RebuildViewsResponse]) error
	mustEmbedUnimplementedFeatureStoreServiceServer()
}

// UnimplementedFeatureStoreServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedFeatureStoreServiceServer struct{}

func (UnimplementedFeatureStoreServiceServer) DefineFeatureView(context.Context, *DefineFeatureViewRequest) (*DefineFeatureViewResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method DefineFeatureView not implemented")
}
func (UnimplementedFeatureStoreServiceServer) GetFeatureView(context.Context, *GetFeatureViewRequest) (*GetFeatureViewResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetFeatureView not implemented")
}
func (UnimplementedFeatureStoreServiceServer) ListFeatureViews(context.Context, *ListFeatureViewsRequest) (*ListFeatureViewsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ListFeatureViews not implemented")
}
func (UnimplementedFeatureStoreServiceServer) WriteFeatures(context.Context, *WriteFeaturesRequest) (*WriteFeaturesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method WriteFeatures not implemented")
}
func (UnimplementedFeatureStoreServiceServer) GetOnlineFeatures(context.Context, *GetOnlineFeaturesRequest) (*GetOnlineFeaturesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetOnlineFeatures not implemented")
}
func (UnimplementedFeatureStoreServiceServer) GetHistoricalFeatures(context.Context, *GetHistoricalFeaturesRequest) (*GetHistoricalFeaturesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetHistoricalFeatures not implemented")
}
func (UnimplementedFeatureStoreServiceServer) DeleteFeatureView(context.Context, *DeleteFeatureViewRequest) (*DeleteFeatureViewResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method DeleteFeatureView not implemented")
}
func (UnimplementedFeatureStoreServiceServer) RebuildViews(*RebuildViewsRequest, grpc.ServerStreamingServer[RebuildViewsResponse]) error {
	return status.Error(codes.Unimplemented, "method RebuildViews not implemented")
}
func (UnimplementedFeatureStoreServiceServer) mustEmbedUnimplementedFeatureStoreServiceServer() {}
func (UnimplementedFeatureStoreServiceServer) testEmbeddedByValue()                             {}

// UnsafeFeatureStoreServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to FeatureStoreServiceServer will
// result in compilation errors.
type UnsafeFeatureStoreServiceServer interface {
	mustEmbedUnimplementedFeatureStoreServiceServer()
}

func RegisterFeatureStoreServiceServer(s grpc.ServiceRegistrar, srv FeatureStoreServiceServer) {
	// If the following call panics, it indicates UnimplementedFeatureStoreServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&FeatureStoreService_ServiceDesc, srv)
}

func _FeatureStoreService_DefineFeatureView_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DefineFeatureViewRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FeatureStoreServiceServer).DefineFeatureView(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FeatureStoreService_DefineFeatureView_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FeatureStoreServiceServer).DefineFeatureView(ctx, req.(*DefineFeatureViewRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FeatureStoreService_GetFeatureView_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetFeatureViewRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FeatureStoreServiceServer).GetFeatureView(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FeatureStoreService_GetFeatureView_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FeatureStoreServiceServer).GetFeatureView(ctx, req.(*GetFeatureViewRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FeatureStoreService_ListFeatureViews_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListFeatureViewsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FeatureStoreServiceServer).ListFeatureViews(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FeatureStoreService_ListFeatureViews_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FeatureStoreServiceServer).ListFeatureViews(ctx, req.(*ListFeatureViewsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FeatureStoreService_WriteFeatures_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(WriteFeaturesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FeatureStoreServiceServer).WriteFeatures(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FeatureStoreService_WriteFeatures_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FeatureStoreServiceServer).WriteFeatures(ctx, req.(*WriteFeaturesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FeatureStoreService_GetOnlineFeatures_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetOnlineFeaturesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FeatureStoreServiceServer).GetOnlineFeatures(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FeatureStoreService_GetOnlineFeatures_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FeatureStoreServiceServer).GetOnlineFeatures(ctx, req.(*GetOnlineFeaturesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FeatureStoreService_GetHistoricalFeatures_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetHistoricalFeaturesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FeatureStoreServiceServer).GetHistoricalFeatures(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FeatureStoreService_GetHistoricalFeatures_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FeatureStoreServiceServer).GetHistoricalFeatures(ctx, req.(*GetHistoricalFeaturesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FeatureStoreService_DeleteFeatureView_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteFeatureViewRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FeatureStoreServiceServer).DeleteFeatureView(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: FeatureStoreService_DeleteFeatureView_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FeatureStoreServiceServer).DeleteFeatureView(ctx, req.(*DeleteFeatureViewRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FeatureStoreService_RebuildViews_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(RebuildViewsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(FeatureStoreServiceServer).RebuildViews(m, &grpc.GenericServerStream[RebuildViewsRequest, RebuildViewsResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type FeatureStoreService_RebuildViewsServer = grpc.ServerStreamingServer[RebuildViewsResponse]

// FeatureStoreService_ServiceDesc is the grpc.ServiceDesc for FeatureStoreService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var FeatureStoreService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "forgepoint.featurestore.v1.FeatureStoreService",
	HandlerType: (*FeatureStoreServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "DefineFeatureView",
			Handler:    _FeatureStoreService_DefineFeatureView_Handler,
		},
		{
			MethodName: "GetFeatureView",
			Handler:    _FeatureStoreService_GetFeatureView_Handler,
		},
		{
			MethodName: "ListFeatureViews",
			Handler:    _FeatureStoreService_ListFeatureViews_Handler,
		},
		{
			MethodName: "WriteFeatures",
			Handler:    _FeatureStoreService_WriteFeatures_Handler,
		},
		{
			MethodName: "GetOnlineFeatures",
			Handler:    _FeatureStoreService_GetOnlineFeatures_Handler,
		},
		{
			MethodName: "GetHistoricalFeatures",
			Handler:    _FeatureStoreService_GetHistoricalFeatures_Handler,
		},
		{
			MethodName: "DeleteFeatureView",
			Handler:    _FeatureStoreService_DeleteFeatureView_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "RebuildViews",
			Handler:       _FeatureStoreService_RebuildViews_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "forgepoint/featurestore/v1/featurestore.proto",
}
