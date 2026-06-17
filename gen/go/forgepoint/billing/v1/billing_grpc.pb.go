// ============================================================================
// Forgepoint Billing / Usage Service Proto Definitions
// ============================================================================
//
// WHY: The Billing service meters every billable action on the platform —
// primarily inference calls — attributes cost to a team/rate-plan, enforces
// quotas, and rolls metered usage up into invoices. It is the platform's
// "money truth": once an event is recorded here it must NEVER be lost and must
// NEVER be double-counted (both directions are real money errors).
//
// WHAT'S HERE:
//   - Domain messages: Money, RatePlan, UsageRecord, UsageSummary, Invoice,
//     InvoiceLineItem
//   - BillingService RPCs: RecordUsage (meter), GetUsage (aggregate, paged),
//     GetInvoice, ListInvoices (paged)
//   - Event payload messages: UsageRecorded, InvoiceGenerated, QuotaExceeded —
//     the typed payloads carried in common.v1.EventEnvelope.data and published
//     via the OUTBOX poller (see pattern note below)
//
// PATTERN — Outbox (Transactional Outbox / reliable event publishing):
//   The hard problem this service exists to demonstrate is the DUAL-WRITE
//   problem. When we record usage we must do TWO things that look atomic to the
//   rest of the platform:
//     (1) UPDATE the usage counters / INSERT the usage row in Postgres, and
//     (2) PUBLISH an event (UsageRecorded / QuotaExceeded) to NATS.
//   If we write the DB then crash before publishing, downstream (Notification,
//   the Inference Gateway's quota cache) never learns. If we publish then the
//   DB write fails, we've told the world something that isn't true. There is no
//   distributed transaction spanning Postgres and NATS, so we CANNOT make (1)
//   and (2) atomic directly.
//
//   THE OUTBOX FIX:
//     - In ONE local Postgres transaction we write the business rows AND insert
//       a row into an `outbox` table: {id, aggregate_id, event_type, payload,
//       created_at, published_at=NULL}. One transaction, one commit, no dual
//       write — the event is now as durable as the data that justifies it.
//     - A SEPARATE poller goroutine reads `WHERE published_at IS NULL ORDER BY
//       created_at`, publishes each to NATS, then stamps `published_at=NOW()`.
//     - Guarantee: if the DB commit succeeded, the event WILL eventually be
//       published (at-least-once). Consumers are idempotent (EventEnvelope.id)
//       so at-least-once is safe.
//
//   WHY THE API SHAPE REFLECTS THIS:
//     - RecordUsage carries an `idempotency_key` so a REDELIVERED upstream
//       InferenceCompleted (NATS is at-least-once too) does not bill twice. The
//       outbox protects DB→NATS; the idempotency key protects NATS→DB. Both ends
//       of the pipe must be idempotent for the whole thing to be exactly-once
//       *in effect*.
//     - The event payload messages live in THIS proto (not raw JSON) so the
//       outbox `payload` column is a typed, versioned, buf-breaking-checked
//       contract — the same "typed payload in a generic envelope" discipline as
//       the registry service.
//
//   REAL-WORLD COMPARISON:
//     - This is exactly how Stripe, Shopify, and most ledger systems publish
//       events reliably. Debezium + Kafka Connect productize the "tail the
//       outbox/WAL" half. Temporal sidesteps it with durable workflow state.
//     - INTERVIEW NOTE: the classic probe is "why not just publish to NATS
//       inside the same code path after the DB commit?" — answer: the process
//       can die in the gap between commit and publish; the outbox moves the
//       publish intent INTO the committed transaction so a crash loses nothing.
//
// METERING SOURCE — consumes InferenceCompleted:
//   The primary feeder is the async event `fp.inference.completed` emitted by
//   the Inference Gateway. The billing consumer turns each into a RecordUsage
//   (internally) so the same metering path serves both the event consumer and
//   any direct gRPC caller (e.g. a backfill tool). RecordUsage is therefore
//   exposed on the gRPC surface even though, in steady state, most traffic
//   arrives via NATS.
//
// SECURITY / TRUST BOUNDARY (this is a billing service — get this right):
//   EVERY money-bearing or attribution field is SERVER-AUTHORITATIVE. Clients
//   submit only the raw, verifiable METERING FACTS (which model/version, how
//   many requests, how many tokens/compute-units). The server looks up the
//   rate plan and COMPUTES the cost. A client may never assert a price, a
//   discount, a cost, an invoice total, a quota, a status, a timestamp, or
//   whose account to bill. Accepting any of those would be a mass-assignment
//   vulnerability with a direct financial blast radius. See per-field notes.
//
// VERSIONING: Package path includes v1 per Buf/Google convention. Breaking
// changes require a new forgepoint.billing.v2 package.
// ============================================================================

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.6.2
// - protoc             (unknown)
// source: forgepoint/billing/v1/billing.proto

package billingv1

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
	BillingService_RecordUsage_FullMethodName  = "/forgepoint.billing.v1.BillingService/RecordUsage"
	BillingService_GetUsage_FullMethodName     = "/forgepoint.billing.v1.BillingService/GetUsage"
	BillingService_GetInvoice_FullMethodName   = "/forgepoint.billing.v1.BillingService/GetInvoice"
	BillingService_ListInvoices_FullMethodName = "/forgepoint.billing.v1.BillingService/ListInvoices"
)

// BillingServiceClient is the client API for BillingService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// ============================================================================
// BILLING SERVICE
// ============================================================================
//
// WHY a single BillingService (not separate Usage / Invoice services):
//
//	Usage and invoicing are the same bounded context — invoices ARE aggregated
//	usage. Splitting them would force a cross-service call on every period close
//	and fracture the ledger's single source of truth. One service owns the
//	`fp_billing` database (usage_events, quotas, rate_plans, invoices, outbox)
//	per the database-per-service standard.
//
// RPC CATEGORIES:
//
//	METERING (write): RecordUsage — the outbox-protected write path. In steady
//	  state this is driven by the InferenceCompleted NATS consumer; exposed on
//	  gRPC for backfills and direct metering.
//	REPORTING (read): GetUsage (aggregated, paged), GetInvoice, ListInvoices
//	  (paged) — read-only views over the ledger.
//
// WHY ALL UNARY (no streaming):
//   - RecordUsage is a single fact-in / record-out — request/reply.
//   - GetUsage/ListInvoices return BOUNDED, PAGINATED pages; pagination (not a
//     server stream) is the right tool because it gives clients resumability,
//     cacheable page tokens, and a natural total_count — the same choice the
//     registry's list RPCs made. A server stream would suit an UNBOUNDED live
//     feed (e.g. tail usage in real time); that real-time need is served by the
//     async UsageRecorded NATS event instead, keeping the gRPC surface simple.
//
// WHERE CheckQuota / CreateRatePlan WENT (scope note, not a TODO):
//
//	The platform design also lists CheckQuota (gateway pre-flight) and
//	CreateRatePlan (admin). They are deliberately OUT of this proto's scope per
//	the task brief (RecordUsage / GetUsage / GetInvoice / ListInvoices). Quota
//	enforcement is realized here via the QuotaExceeded outbox event that flips
//	the gateway's Redis quota cache (eventual consistency is acceptable for the
//	gateway pre-flight). Adding CheckQuota/CreateRatePlan later is purely
//	additive and buf-breaking-safe.
//
// ASCII DIAGRAM — the outbox metering pipeline (interview-critical):
//
//	Inference Gateway ──fp.inference.completed──► Billing NATS consumer
//	                                                   │ (idempotency_key dedupe)
//	                                                   ▼
//	                                      ┌─ BEGIN tx ──────────────────────┐
//	                                      │  INSERT usage_record (priced)   │
//	                                      │  UPDATE quota counter           │
//	                                      │  INSERT outbox(UsageRecorded)   │
//	                                      │  if over quota:                 │
//	                                      │     INSERT outbox(QuotaExceeded)│
//	                                      └─ COMMIT ────────────────────────┘
//	                                                   │
//	                         outbox poller (separate goroutine, at-least-once)
//	                                                   ▼
//	                         NATS  fp.billing.usage.recorded / .quota.exceeded
//	                                                   ▼
//	                         Notification / Inference Gateway / Experiment Tracker
//
// ============================================================================
type BillingServiceClient interface {
	// RecordUsage meters one billable action and writes it to the ledger via the
	// outbox. The cost is COMPUTED server-side from the team's rate plan — the
	// request carries only meter facts, never a price (see RecordUsageRequest).
	// Idempotent via idempotency_key, so a redelivered InferenceCompleted or a
	// client retry never double-bills. This is the outbox pattern's write half.
	RecordUsage(ctx context.Context, in *RecordUsageRequest, opts ...grpc.CallOption) (*RecordUsageResponse, error)
	// GetUsage returns aggregated, paginated usage summaries (per-meter rollups +
	// grand total) for a team over a time window. Non-admins are scoped to their
	// own team server-side. Read-only.
	GetUsage(ctx context.Context, in *GetUsageRequest, opts ...grpc.CallOption) (*GetUsageResponse, error)
	// GetInvoice fetches a single invoice by id, with its server-computed line
	// items and total. Cross-team reads are denied (tenant isolation). Read-only.
	GetInvoice(ctx context.Context, in *GetInvoiceRequest, opts ...grpc.CallOption) (*GetInvoiceResponse, error)
	// ListInvoices returns a team's invoices newest-first, paginated, optionally
	// filtered by status (e.g. OVERDUE for dunning). Read-only.
	ListInvoices(ctx context.Context, in *ListInvoicesRequest, opts ...grpc.CallOption) (*ListInvoicesResponse, error)
}

type billingServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewBillingServiceClient(cc grpc.ClientConnInterface) BillingServiceClient {
	return &billingServiceClient{cc}
}

func (c *billingServiceClient) RecordUsage(ctx context.Context, in *RecordUsageRequest, opts ...grpc.CallOption) (*RecordUsageResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(RecordUsageResponse)
	err := c.cc.Invoke(ctx, BillingService_RecordUsage_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *billingServiceClient) GetUsage(ctx context.Context, in *GetUsageRequest, opts ...grpc.CallOption) (*GetUsageResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetUsageResponse)
	err := c.cc.Invoke(ctx, BillingService_GetUsage_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *billingServiceClient) GetInvoice(ctx context.Context, in *GetInvoiceRequest, opts ...grpc.CallOption) (*GetInvoiceResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetInvoiceResponse)
	err := c.cc.Invoke(ctx, BillingService_GetInvoice_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *billingServiceClient) ListInvoices(ctx context.Context, in *ListInvoicesRequest, opts ...grpc.CallOption) (*ListInvoicesResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ListInvoicesResponse)
	err := c.cc.Invoke(ctx, BillingService_ListInvoices_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BillingServiceServer is the server API for BillingService service.
// All implementations must embed UnimplementedBillingServiceServer
// for forward compatibility.
//
// ============================================================================
// BILLING SERVICE
// ============================================================================
//
// WHY a single BillingService (not separate Usage / Invoice services):
//
//	Usage and invoicing are the same bounded context — invoices ARE aggregated
//	usage. Splitting them would force a cross-service call on every period close
//	and fracture the ledger's single source of truth. One service owns the
//	`fp_billing` database (usage_events, quotas, rate_plans, invoices, outbox)
//	per the database-per-service standard.
//
// RPC CATEGORIES:
//
//	METERING (write): RecordUsage — the outbox-protected write path. In steady
//	  state this is driven by the InferenceCompleted NATS consumer; exposed on
//	  gRPC for backfills and direct metering.
//	REPORTING (read): GetUsage (aggregated, paged), GetInvoice, ListInvoices
//	  (paged) — read-only views over the ledger.
//
// WHY ALL UNARY (no streaming):
//   - RecordUsage is a single fact-in / record-out — request/reply.
//   - GetUsage/ListInvoices return BOUNDED, PAGINATED pages; pagination (not a
//     server stream) is the right tool because it gives clients resumability,
//     cacheable page tokens, and a natural total_count — the same choice the
//     registry's list RPCs made. A server stream would suit an UNBOUNDED live
//     feed (e.g. tail usage in real time); that real-time need is served by the
//     async UsageRecorded NATS event instead, keeping the gRPC surface simple.
//
// WHERE CheckQuota / CreateRatePlan WENT (scope note, not a TODO):
//
//	The platform design also lists CheckQuota (gateway pre-flight) and
//	CreateRatePlan (admin). They are deliberately OUT of this proto's scope per
//	the task brief (RecordUsage / GetUsage / GetInvoice / ListInvoices). Quota
//	enforcement is realized here via the QuotaExceeded outbox event that flips
//	the gateway's Redis quota cache (eventual consistency is acceptable for the
//	gateway pre-flight). Adding CheckQuota/CreateRatePlan later is purely
//	additive and buf-breaking-safe.
//
// ASCII DIAGRAM — the outbox metering pipeline (interview-critical):
//
//	Inference Gateway ──fp.inference.completed──► Billing NATS consumer
//	                                                   │ (idempotency_key dedupe)
//	                                                   ▼
//	                                      ┌─ BEGIN tx ──────────────────────┐
//	                                      │  INSERT usage_record (priced)   │
//	                                      │  UPDATE quota counter           │
//	                                      │  INSERT outbox(UsageRecorded)   │
//	                                      │  if over quota:                 │
//	                                      │     INSERT outbox(QuotaExceeded)│
//	                                      └─ COMMIT ────────────────────────┘
//	                                                   │
//	                         outbox poller (separate goroutine, at-least-once)
//	                                                   ▼
//	                         NATS  fp.billing.usage.recorded / .quota.exceeded
//	                                                   ▼
//	                         Notification / Inference Gateway / Experiment Tracker
//
// ============================================================================
type BillingServiceServer interface {
	// RecordUsage meters one billable action and writes it to the ledger via the
	// outbox. The cost is COMPUTED server-side from the team's rate plan — the
	// request carries only meter facts, never a price (see RecordUsageRequest).
	// Idempotent via idempotency_key, so a redelivered InferenceCompleted or a
	// client retry never double-bills. This is the outbox pattern's write half.
	RecordUsage(context.Context, *RecordUsageRequest) (*RecordUsageResponse, error)
	// GetUsage returns aggregated, paginated usage summaries (per-meter rollups +
	// grand total) for a team over a time window. Non-admins are scoped to their
	// own team server-side. Read-only.
	GetUsage(context.Context, *GetUsageRequest) (*GetUsageResponse, error)
	// GetInvoice fetches a single invoice by id, with its server-computed line
	// items and total. Cross-team reads are denied (tenant isolation). Read-only.
	GetInvoice(context.Context, *GetInvoiceRequest) (*GetInvoiceResponse, error)
	// ListInvoices returns a team's invoices newest-first, paginated, optionally
	// filtered by status (e.g. OVERDUE for dunning). Read-only.
	ListInvoices(context.Context, *ListInvoicesRequest) (*ListInvoicesResponse, error)
	mustEmbedUnimplementedBillingServiceServer()
}

// UnimplementedBillingServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedBillingServiceServer struct{}

func (UnimplementedBillingServiceServer) RecordUsage(context.Context, *RecordUsageRequest) (*RecordUsageResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method RecordUsage not implemented")
}
func (UnimplementedBillingServiceServer) GetUsage(context.Context, *GetUsageRequest) (*GetUsageResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetUsage not implemented")
}
func (UnimplementedBillingServiceServer) GetInvoice(context.Context, *GetInvoiceRequest) (*GetInvoiceResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetInvoice not implemented")
}
func (UnimplementedBillingServiceServer) ListInvoices(context.Context, *ListInvoicesRequest) (*ListInvoicesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ListInvoices not implemented")
}
func (UnimplementedBillingServiceServer) mustEmbedUnimplementedBillingServiceServer() {}
func (UnimplementedBillingServiceServer) testEmbeddedByValue()                        {}

// UnsafeBillingServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to BillingServiceServer will
// result in compilation errors.
type UnsafeBillingServiceServer interface {
	mustEmbedUnimplementedBillingServiceServer()
}

func RegisterBillingServiceServer(s grpc.ServiceRegistrar, srv BillingServiceServer) {
	// If the following call panics, it indicates UnimplementedBillingServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&BillingService_ServiceDesc, srv)
}

func _BillingService_RecordUsage_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RecordUsageRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BillingServiceServer).RecordUsage(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BillingService_RecordUsage_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BillingServiceServer).RecordUsage(ctx, req.(*RecordUsageRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BillingService_GetUsage_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetUsageRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BillingServiceServer).GetUsage(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BillingService_GetUsage_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BillingServiceServer).GetUsage(ctx, req.(*GetUsageRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BillingService_GetInvoice_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetInvoiceRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BillingServiceServer).GetInvoice(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BillingService_GetInvoice_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BillingServiceServer).GetInvoice(ctx, req.(*GetInvoiceRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BillingService_ListInvoices_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListInvoicesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BillingServiceServer).ListInvoices(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BillingService_ListInvoices_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BillingServiceServer).ListInvoices(ctx, req.(*ListInvoicesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// BillingService_ServiceDesc is the grpc.ServiceDesc for BillingService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var BillingService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "forgepoint.billing.v1.BillingService",
	HandlerType: (*BillingServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RecordUsage",
			Handler:    _BillingService_RecordUsage_Handler,
		},
		{
			MethodName: "GetUsage",
			Handler:    _BillingService_GetUsage_Handler,
		},
		{
			MethodName: "GetInvoice",
			Handler:    _BillingService_GetInvoice_Handler,
		},
		{
			MethodName: "ListInvoices",
			Handler:    _BillingService_ListInvoices_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "forgepoint/billing/v1/billing.proto",
}
