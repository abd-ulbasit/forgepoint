// Package handler implements the gRPC server side of the Model Monitor service.
// It is the outermost (adapter) layer in Clean Architecture: it speaks proto
// (wire format) and delegates all business logic to the domain.MonitorService.
//
// ============================================================================
// HANDLER LAYER RESPONSIBILITIES (the anti-corruption layer)
// ============================================================================
//
// Three jobs, no more:
//
//  1. PROTO → DOMAIN: extract fields from the incoming proto Request, validate
//     them, and build domain input types. CRITICAL for this service:
//     - owner_team is taken from the AUTH CLAIMS in the context (set by the
//     auth interceptor), NEVER from a request field — the mass-assignment /
//     cross-tenant guard. The handler passes that claim-derived team into the
//     domain methods (ConfigureMonitor(ctx, ownerTeam, in), etc.).
//     - The writable surface is mapped explicitly (ConfigureMonitorRequest →
//     domain.ConfigureMonitorInput); server-authoritative fields (id, state,
//     baseline_*, timestamps) are NOT read from the request, because the
//     domain owns them.
//
//  2. CALL DOMAIN SERVICE: invoke the MonitorService method. The handler holds a
//     domain.MonitorService interface — it never knows the impl is backed by
//     Postgres + Redis + NATS or by a test fake.
//
//  3. DOMAIN → PROTO: map the domain result to a proto Response, converting the
//     domain enums to the proto enums through an explicit CHECKED switch (not an
//     unchecked numeric cast) so a future enum divergence is caught at the
//     boundary, not silently mis-serialized.
//
// WHAT THE HANDLER DOES NOT DO: drift math, windowing, the retrain policy, SQL,
// or NATS publishing — all of that is the domain/adapters.
//
// ============================================================================
// EMBEDDING UnimplementedMonitorServiceServer — WHY THIS IS CORRECT (NOT A STUB)
// ============================================================================
//
// protoc-gen-go-grpc generates MonitorServiceServer with a private method
// mustEmbedUnimplementedMonitorServiceServer(), forcing every implementation to
// embed UnimplementedMonitorServiceServer. That base implements every RPC to
// return codes.Unimplemented. Embedding it means MonitorHandler:
//
//  1. Satisfies monitorv1.MonitorServiceServer at compile time — the server can be
//     registered and started RIGHT NOW (this scaffold phase).
//  2. Returns codes.Unimplemented for any RPC we haven't written yet — the client
//     gets a real, correct gRPC error, not a panic.
//  3. Is forward-compatible: a new RPC added to the proto is handled by the base
//     (Unimplemented) until we implement it, instead of breaking the build.
//
// This is the IDIOMATIC Go/gRPC scaffold pattern — a production-correct, running
// server. Per-RPC implementations are filled in a later phase by OVERRIDING each
// method on *MonitorHandler (ConfigureMonitor, GetModelHealth, StreamDriftEvents,
// …), at which point each method:
//   - reads owner_team from the context's auth claims,
//   - converts proto → domain input,
//   - calls h.svc.<Method>(...),
//   - converts the domain result (or error sentinel) back to proto / a gRPC status.
//
// INTERVIEW: "How does grpc-go ensure forward compatibility of service servers?"
//
//	The mustEmbedUnimplemented…() private method forces embedding the Unimplemented
//	base. Adding an RPC to the proto can't break a server that embeds the base —
//	it just returns Unimplemented for the new method until implemented. That's
//	gRPC's server-side backward-compatibility story.
package handler

import (
	monitorv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/monitor/v1"
	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
)

// MonitorHandler is the gRPC server implementation for the Model Monitor service.
//
// It embeds monitorv1.UnimplementedMonitorServiceServer (BY VALUE — grpc-go
// requires value embedding so the forward-compat assertions hold) to satisfy the
// full MonitorServiceServer interface immediately; every RPC returns
// codes.Unimplemented until a later phase overrides it.
//
// svc is the domain.MonitorService holding all business logic (windowing config,
// drift reads, ground-truth feedback). It is nil during the scaffold phase
// (main.go passes nil); the embedded Unimplemented methods never dereference svc,
// so that is safe. When the repositories/events adapters land, main.go constructs
// the real service and passes it here, and each overridden RPC method calls
// h.svc.<Method>.
type MonitorHandler struct {
	monitorv1.UnimplementedMonitorServiceServer
	svc domain.MonitorService
}

// NewMonitorHandler creates a MonitorHandler with the given domain service.
//
// svc is nil during the scaffold phase (the repository/Redis/NATS adapters are a
// later phase). The nil is safe because the embedded UnimplementedMonitorServiceServer
// answers every RPC without touching svc.
//
// When the RPCs are implemented, each method will guard a nil service per its
// kind:
//   - UNARY RPCs return (resp, err): if h.svc == nil { return nil,
//     status.Error(codes.Internal, "service not wired") }
//   - The SERVER-STREAMING RPC (StreamDriftEvents) returns only err (responses
//     flow via stream.Send): if h.svc == nil { return status.Error(codes.Internal,
//     "service not wired") }
//
// During this scaffold phase the embedded Unimplemented base answers first, so
// svc is never dereferenced.
func NewMonitorHandler(svc domain.MonitorService) *MonitorHandler {
	return &MonitorHandler{svc: svc}
}
