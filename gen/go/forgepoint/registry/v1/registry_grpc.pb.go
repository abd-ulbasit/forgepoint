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
//   WHY CQRS HERE (interview framing):
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
//   auth claims), stage/status (only PromoteVersion may change stage),
//   created_at/updated_at/archived_at, or artifact_path/digest (set by the
//   storage layer on upload). Accepting any of these would let a caller forge
//   ownership, backdate records, or jump a version straight to production —
//   classic mass-assignment escalation. We model only the genuinely
//   client-supplied fields on *Request messages and derive the rest server-side.
//
// VERSIONING: Package path includes v1 (Buf/Google convention). Breaking
// changes require a new forgepoint.registry.v2 package; both coexist during
// migration. buf breaking (FILE level) guards this file.
// ============================================================================

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.6.2
// - protoc             (unknown)
// source: forgepoint/registry/v1/registry.proto

package registryv1

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
	RegistryService_RegisterModel_FullMethodName  = "/forgepoint.registry.v1.RegistryService/RegisterModel"
	RegistryService_CreateVersion_FullMethodName  = "/forgepoint.registry.v1.RegistryService/CreateVersion"
	RegistryService_PromoteVersion_FullMethodName = "/forgepoint.registry.v1.RegistryService/PromoteVersion"
	RegistryService_DeleteModel_FullMethodName    = "/forgepoint.registry.v1.RegistryService/DeleteModel"
	RegistryService_GetModel_FullMethodName       = "/forgepoint.registry.v1.RegistryService/GetModel"
	RegistryService_ListModels_FullMethodName     = "/forgepoint.registry.v1.RegistryService/ListModels"
	RegistryService_SearchByTag_FullMethodName    = "/forgepoint.registry.v1.RegistryService/SearchByTag"
	RegistryService_GetVersion_FullMethodName     = "/forgepoint.registry.v1.RegistryService/GetVersion"
	RegistryService_ListVersions_FullMethodName   = "/forgepoint.registry.v1.RegistryService/ListVersions"
	RegistryService_GetDownloadURL_FullMethodName = "/forgepoint.registry.v1.RegistryService/GetDownloadURL"
)

// RegistryServiceClient is the client API for RegistryService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// ============================================================================
// REGISTRY SERVICE
// ============================================================================
//
// WHY a single RegistryService spanning both command and query RPCs:
//
//	CQRS separates the read/write MODELS and STORES, not necessarily the API
//	surface. Exposing both through one gRPC service keeps the client experience
//	simple (one stub) while the IMPLEMENTATION routes commands to the Postgres
//	write repo and queries to the Redis read repo. If read and write needed
//	independent scaling/deploys we could split them into two services later —
//	the proto is structured (clear COMMAND vs QUERY grouping below) so that
//	split would be mechanical. This is the pragmatic "logical CQRS, physical
//	monolith-of-one-service" choice MLflow's registry also makes.
//
// STREAMING CHOICE — why everything here is UNARY:
//
//	None of these operations is a long-lived stream. Registrations and
//	promotions are discrete commands; lookups return a bounded object; lists are
//	PAGINATED (cursor) rather than server-streamed because pagination gives the
//	client backpressure, resumability (the page_token survives a disconnect),
//	and cacheability that a server stream does not. Contrast with the Pipeline
//	Orchestrator's WatchExecution, which IS server-streaming because execution
//	progress is a genuine open-ended event feed. Picking unary+pagination here
//	is the correct, defensible call (interviewers probe this: "why not stream
//	ListModels?" → backpressure + resumable cursor + simpler caching).
//
// RPC GROUPS:
//
//	COMMANDS (Postgres write, then publish NATS event):
//	  RegisterModel, CreateVersion, PromoteVersion, DeleteModel
//	QUERIES (Redis read projection, eventually consistent):
//	  GetModel, ListModels, SearchByTag, GetVersion, ListVersions
//	STORAGE (object store, presigned URLs — bytes bypass this service):
//	  GetDownloadURL  (upload URL is returned inline by CreateVersion)
//
// ============================================================================
type RegistryServiceClient interface {
	// RegisterModel creates a new model (the identity, no versions yet). Writes to
	// Postgres and publishes ModelRegistered. owner_id/team are taken from the
	// caller's auth claims, never the request. Idempotent via idempotency_key.
	RegisterModel(ctx context.Context, in *RegisterModelRequest, opts ...grpc.CallOption) (*RegisterModelResponse, error)
	// CreateVersion cuts a new immutable version of an existing model. Writes the
	// version row (status=PENDING_UPLOAD, stage=DEV), returns a presigned upload
	// URL for the artifact, and publishes ModelVersionCreated. The artifact bytes
	// never flow through this RPC.
	CreateVersion(ctx context.Context, in *CreateVersionRequest, opts ...grpc.CallOption) (*CreateVersionResponse, error)
	// PromoteVersion advances a version through the stage state machine
	// (DEV→STAGING→PRODUCTION→ARCHIVED), enforcing legal transitions and the
	// single-production invariant in ONE Postgres transaction, then publishes
	// ModelPromoted. This is what drives serving/gateway/monitor/billing reactions.
	PromoteVersion(ctx context.Context, in *PromoteVersionRequest, opts ...grpc.CallOption) (*PromoteVersionResponse, error)
	// DeleteModel soft-deletes (archives) a model and all its versions, removes it
	// from active read lists, and publishes ModelArchived. Rows are retained for
	// lineage/audit — this is not a hard delete.
	DeleteModel(ctx context.Context, in *DeleteModelRequest, opts ...grpc.CallOption) (*DeleteModelResponse, error)
	// GetModel returns a single model by id or name from the Redis read model.
	// May briefly miss a just-registered model (projection lag).
	GetModel(ctx context.Context, in *GetModelRequest, opts ...grpc.CallOption) (*GetModelResponse, error)
	// ListModels returns a paginated, team-scoped page of models from the read
	// model, newest-first, with optional task_type/framework filters. page_size is
	// capped at 100 server-side.
	ListModels(ctx context.Context, in *ListModelsRequest, opts ...grpc.CallOption) (*ListModelsResponse, error)
	// SearchByTag returns models matching a (key,value) tag, served from the
	// projection's per-tag Redis set. Paginated, 100-item cap.
	SearchByTag(ctx context.Context, in *SearchByTagRequest, opts ...grpc.CallOption) (*SearchByTagResponse, error)
	// GetVersion returns a single version by id, or by (model_id, version) label,
	// from the read model.
	GetVersion(ctx context.Context, in *GetVersionRequest, opts ...grpc.CallOption) (*GetVersionResponse, error)
	// ListVersions returns a paginated, newest-first list of a model's versions
	// from the read model, optionally filtered by stage. 100-item cap.
	ListVersions(ctx context.Context, in *ListVersionsRequest, opts ...grpc.CallOption) (*ListVersionsResponse, error)
	// GetDownloadURL returns a short-lived presigned URL to fetch a READY
	// version's artifact directly from MinIO/S3, plus the expected digest for
	// integrity verification. Read-only and safe to retry.
	GetDownloadURL(ctx context.Context, in *GetDownloadURLRequest, opts ...grpc.CallOption) (*GetDownloadURLResponse, error)
}

type registryServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewRegistryServiceClient(cc grpc.ClientConnInterface) RegistryServiceClient {
	return &registryServiceClient{cc}
}

func (c *registryServiceClient) RegisterModel(ctx context.Context, in *RegisterModelRequest, opts ...grpc.CallOption) (*RegisterModelResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(RegisterModelResponse)
	err := c.cc.Invoke(ctx, RegistryService_RegisterModel_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *registryServiceClient) CreateVersion(ctx context.Context, in *CreateVersionRequest, opts ...grpc.CallOption) (*CreateVersionResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(CreateVersionResponse)
	err := c.cc.Invoke(ctx, RegistryService_CreateVersion_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *registryServiceClient) PromoteVersion(ctx context.Context, in *PromoteVersionRequest, opts ...grpc.CallOption) (*PromoteVersionResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(PromoteVersionResponse)
	err := c.cc.Invoke(ctx, RegistryService_PromoteVersion_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *registryServiceClient) DeleteModel(ctx context.Context, in *DeleteModelRequest, opts ...grpc.CallOption) (*DeleteModelResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(DeleteModelResponse)
	err := c.cc.Invoke(ctx, RegistryService_DeleteModel_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *registryServiceClient) GetModel(ctx context.Context, in *GetModelRequest, opts ...grpc.CallOption) (*GetModelResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetModelResponse)
	err := c.cc.Invoke(ctx, RegistryService_GetModel_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *registryServiceClient) ListModels(ctx context.Context, in *ListModelsRequest, opts ...grpc.CallOption) (*ListModelsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ListModelsResponse)
	err := c.cc.Invoke(ctx, RegistryService_ListModels_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *registryServiceClient) SearchByTag(ctx context.Context, in *SearchByTagRequest, opts ...grpc.CallOption) (*SearchByTagResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(SearchByTagResponse)
	err := c.cc.Invoke(ctx, RegistryService_SearchByTag_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *registryServiceClient) GetVersion(ctx context.Context, in *GetVersionRequest, opts ...grpc.CallOption) (*GetVersionResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetVersionResponse)
	err := c.cc.Invoke(ctx, RegistryService_GetVersion_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *registryServiceClient) ListVersions(ctx context.Context, in *ListVersionsRequest, opts ...grpc.CallOption) (*ListVersionsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ListVersionsResponse)
	err := c.cc.Invoke(ctx, RegistryService_ListVersions_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *registryServiceClient) GetDownloadURL(ctx context.Context, in *GetDownloadURLRequest, opts ...grpc.CallOption) (*GetDownloadURLResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetDownloadURLResponse)
	err := c.cc.Invoke(ctx, RegistryService_GetDownloadURL_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RegistryServiceServer is the server API for RegistryService service.
// All implementations must embed UnimplementedRegistryServiceServer
// for forward compatibility.
//
// ============================================================================
// REGISTRY SERVICE
// ============================================================================
//
// WHY a single RegistryService spanning both command and query RPCs:
//
//	CQRS separates the read/write MODELS and STORES, not necessarily the API
//	surface. Exposing both through one gRPC service keeps the client experience
//	simple (one stub) while the IMPLEMENTATION routes commands to the Postgres
//	write repo and queries to the Redis read repo. If read and write needed
//	independent scaling/deploys we could split them into two services later —
//	the proto is structured (clear COMMAND vs QUERY grouping below) so that
//	split would be mechanical. This is the pragmatic "logical CQRS, physical
//	monolith-of-one-service" choice MLflow's registry also makes.
//
// STREAMING CHOICE — why everything here is UNARY:
//
//	None of these operations is a long-lived stream. Registrations and
//	promotions are discrete commands; lookups return a bounded object; lists are
//	PAGINATED (cursor) rather than server-streamed because pagination gives the
//	client backpressure, resumability (the page_token survives a disconnect),
//	and cacheability that a server stream does not. Contrast with the Pipeline
//	Orchestrator's WatchExecution, which IS server-streaming because execution
//	progress is a genuine open-ended event feed. Picking unary+pagination here
//	is the correct, defensible call (interviewers probe this: "why not stream
//	ListModels?" → backpressure + resumable cursor + simpler caching).
//
// RPC GROUPS:
//
//	COMMANDS (Postgres write, then publish NATS event):
//	  RegisterModel, CreateVersion, PromoteVersion, DeleteModel
//	QUERIES (Redis read projection, eventually consistent):
//	  GetModel, ListModels, SearchByTag, GetVersion, ListVersions
//	STORAGE (object store, presigned URLs — bytes bypass this service):
//	  GetDownloadURL  (upload URL is returned inline by CreateVersion)
//
// ============================================================================
type RegistryServiceServer interface {
	// RegisterModel creates a new model (the identity, no versions yet). Writes to
	// Postgres and publishes ModelRegistered. owner_id/team are taken from the
	// caller's auth claims, never the request. Idempotent via idempotency_key.
	RegisterModel(context.Context, *RegisterModelRequest) (*RegisterModelResponse, error)
	// CreateVersion cuts a new immutable version of an existing model. Writes the
	// version row (status=PENDING_UPLOAD, stage=DEV), returns a presigned upload
	// URL for the artifact, and publishes ModelVersionCreated. The artifact bytes
	// never flow through this RPC.
	CreateVersion(context.Context, *CreateVersionRequest) (*CreateVersionResponse, error)
	// PromoteVersion advances a version through the stage state machine
	// (DEV→STAGING→PRODUCTION→ARCHIVED), enforcing legal transitions and the
	// single-production invariant in ONE Postgres transaction, then publishes
	// ModelPromoted. This is what drives serving/gateway/monitor/billing reactions.
	PromoteVersion(context.Context, *PromoteVersionRequest) (*PromoteVersionResponse, error)
	// DeleteModel soft-deletes (archives) a model and all its versions, removes it
	// from active read lists, and publishes ModelArchived. Rows are retained for
	// lineage/audit — this is not a hard delete.
	DeleteModel(context.Context, *DeleteModelRequest) (*DeleteModelResponse, error)
	// GetModel returns a single model by id or name from the Redis read model.
	// May briefly miss a just-registered model (projection lag).
	GetModel(context.Context, *GetModelRequest) (*GetModelResponse, error)
	// ListModels returns a paginated, team-scoped page of models from the read
	// model, newest-first, with optional task_type/framework filters. page_size is
	// capped at 100 server-side.
	ListModels(context.Context, *ListModelsRequest) (*ListModelsResponse, error)
	// SearchByTag returns models matching a (key,value) tag, served from the
	// projection's per-tag Redis set. Paginated, 100-item cap.
	SearchByTag(context.Context, *SearchByTagRequest) (*SearchByTagResponse, error)
	// GetVersion returns a single version by id, or by (model_id, version) label,
	// from the read model.
	GetVersion(context.Context, *GetVersionRequest) (*GetVersionResponse, error)
	// ListVersions returns a paginated, newest-first list of a model's versions
	// from the read model, optionally filtered by stage. 100-item cap.
	ListVersions(context.Context, *ListVersionsRequest) (*ListVersionsResponse, error)
	// GetDownloadURL returns a short-lived presigned URL to fetch a READY
	// version's artifact directly from MinIO/S3, plus the expected digest for
	// integrity verification. Read-only and safe to retry.
	GetDownloadURL(context.Context, *GetDownloadURLRequest) (*GetDownloadURLResponse, error)
	mustEmbedUnimplementedRegistryServiceServer()
}

// UnimplementedRegistryServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedRegistryServiceServer struct{}

func (UnimplementedRegistryServiceServer) RegisterModel(context.Context, *RegisterModelRequest) (*RegisterModelResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method RegisterModel not implemented")
}
func (UnimplementedRegistryServiceServer) CreateVersion(context.Context, *CreateVersionRequest) (*CreateVersionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method CreateVersion not implemented")
}
func (UnimplementedRegistryServiceServer) PromoteVersion(context.Context, *PromoteVersionRequest) (*PromoteVersionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method PromoteVersion not implemented")
}
func (UnimplementedRegistryServiceServer) DeleteModel(context.Context, *DeleteModelRequest) (*DeleteModelResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method DeleteModel not implemented")
}
func (UnimplementedRegistryServiceServer) GetModel(context.Context, *GetModelRequest) (*GetModelResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetModel not implemented")
}
func (UnimplementedRegistryServiceServer) ListModels(context.Context, *ListModelsRequest) (*ListModelsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ListModels not implemented")
}
func (UnimplementedRegistryServiceServer) SearchByTag(context.Context, *SearchByTagRequest) (*SearchByTagResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method SearchByTag not implemented")
}
func (UnimplementedRegistryServiceServer) GetVersion(context.Context, *GetVersionRequest) (*GetVersionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetVersion not implemented")
}
func (UnimplementedRegistryServiceServer) ListVersions(context.Context, *ListVersionsRequest) (*ListVersionsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ListVersions not implemented")
}
func (UnimplementedRegistryServiceServer) GetDownloadURL(context.Context, *GetDownloadURLRequest) (*GetDownloadURLResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetDownloadURL not implemented")
}
func (UnimplementedRegistryServiceServer) mustEmbedUnimplementedRegistryServiceServer() {}
func (UnimplementedRegistryServiceServer) testEmbeddedByValue()                         {}

// UnsafeRegistryServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to RegistryServiceServer will
// result in compilation errors.
type UnsafeRegistryServiceServer interface {
	mustEmbedUnimplementedRegistryServiceServer()
}

func RegisterRegistryServiceServer(s grpc.ServiceRegistrar, srv RegistryServiceServer) {
	// If the following call panics, it indicates UnimplementedRegistryServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&RegistryService_ServiceDesc, srv)
}

func _RegistryService_RegisterModel_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RegisterModelRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RegistryServiceServer).RegisterModel(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RegistryService_RegisterModel_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RegistryServiceServer).RegisterModel(ctx, req.(*RegisterModelRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RegistryService_CreateVersion_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateVersionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RegistryServiceServer).CreateVersion(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RegistryService_CreateVersion_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RegistryServiceServer).CreateVersion(ctx, req.(*CreateVersionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RegistryService_PromoteVersion_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PromoteVersionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RegistryServiceServer).PromoteVersion(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RegistryService_PromoteVersion_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RegistryServiceServer).PromoteVersion(ctx, req.(*PromoteVersionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RegistryService_DeleteModel_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteModelRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RegistryServiceServer).DeleteModel(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RegistryService_DeleteModel_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RegistryServiceServer).DeleteModel(ctx, req.(*DeleteModelRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RegistryService_GetModel_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetModelRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RegistryServiceServer).GetModel(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RegistryService_GetModel_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RegistryServiceServer).GetModel(ctx, req.(*GetModelRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RegistryService_ListModels_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListModelsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RegistryServiceServer).ListModels(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RegistryService_ListModels_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RegistryServiceServer).ListModels(ctx, req.(*ListModelsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RegistryService_SearchByTag_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SearchByTagRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RegistryServiceServer).SearchByTag(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RegistryService_SearchByTag_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RegistryServiceServer).SearchByTag(ctx, req.(*SearchByTagRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RegistryService_GetVersion_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetVersionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RegistryServiceServer).GetVersion(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RegistryService_GetVersion_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RegistryServiceServer).GetVersion(ctx, req.(*GetVersionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RegistryService_ListVersions_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListVersionsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RegistryServiceServer).ListVersions(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RegistryService_ListVersions_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RegistryServiceServer).ListVersions(ctx, req.(*ListVersionsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RegistryService_GetDownloadURL_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetDownloadURLRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RegistryServiceServer).GetDownloadURL(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RegistryService_GetDownloadURL_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RegistryServiceServer).GetDownloadURL(ctx, req.(*GetDownloadURLRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// RegistryService_ServiceDesc is the grpc.ServiceDesc for RegistryService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var RegistryService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "forgepoint.registry.v1.RegistryService",
	HandlerType: (*RegistryServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RegisterModel",
			Handler:    _RegistryService_RegisterModel_Handler,
		},
		{
			MethodName: "CreateVersion",
			Handler:    _RegistryService_CreateVersion_Handler,
		},
		{
			MethodName: "PromoteVersion",
			Handler:    _RegistryService_PromoteVersion_Handler,
		},
		{
			MethodName: "DeleteModel",
			Handler:    _RegistryService_DeleteModel_Handler,
		},
		{
			MethodName: "GetModel",
			Handler:    _RegistryService_GetModel_Handler,
		},
		{
			MethodName: "ListModels",
			Handler:    _RegistryService_ListModels_Handler,
		},
		{
			MethodName: "SearchByTag",
			Handler:    _RegistryService_SearchByTag_Handler,
		},
		{
			MethodName: "GetVersion",
			Handler:    _RegistryService_GetVersion_Handler,
		},
		{
			MethodName: "ListVersions",
			Handler:    _RegistryService_ListVersions_Handler,
		},
		{
			MethodName: "GetDownloadURL",
			Handler:    _RegistryService_GetDownloadURL_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "forgepoint/registry/v1/registry.proto",
}
