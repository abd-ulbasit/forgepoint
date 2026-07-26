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
//   - BillingService RPCs:
//       METERING:   RecordUsage (outbox-protected write)
//       PRICING:    CreateRatePlan (admin), GetRatePlan
//       QUOTA:      CheckQuota (gateway pre-flight)
//       REPORTING:  GetUsage (aggregate, paged), GetInvoice, ListInvoices (paged)
//
// EVENT PAYLOADS LIVE IN forgepoint.events.v1 (NOT here):
//   This service formerly defined its OWN UsageRecorded/InvoiceGenerated/
//   QuotaExceeded payload messages inline. An adversarial cross-service review
//   found per-service ad-hoc event messages drifting from their consumers, so the
//   platform now has ONE canonical event contract — forgepoint.events.v1 — that
//   both producers and consumers depend on. Those three inline messages have been
//   DELETED from this file; billing publishes the canonical events.UsageRecorded /
//   events.InvoiceGenerated / events.QuotaExceeded instead (see import block and
//   the EVENT CONTRACT section near the service definition). The canonical money-
//   bearing events (UsageRecorded, InvoiceGenerated) FLATTEN Money into
//   cost_micros/total_micros + currency_code — no embedded billing.Money / .Invoice
//   — precisely so a consumer (Experiment Tracker, Notification) need not compile-
//   depend on this billing API proto just to read an event.
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
//     - The event payloads are the canonical forgepoint.events.v1 messages
//       (not raw JSON), so the outbox `payload` column is a typed, versioned,
//       buf-breaking-checked contract — the same "typed payload in a generic
//       envelope" discipline as the registry service. The poller marshals an
//       eventsv1.* message into common.v1.EventEnvelope.data (an Any) and
//       publishes it to the subject below.
//
//   REAL-WORLD COMPARISON:
//     - This is exactly how Stripe, Shopify, and most ledger systems publish
//       events reliably. Debezium + Kafka Connect productize the "tail the
//       outbox/WAL" half. Temporal sidesteps it with durable workflow state.
//     - WHY NOT JUST PUBLISH TO NATS inside the same code path after the DB
//       commit: the process
//       can die in the gap between commit and publish; the outbox moves the
//       publish intent INTO the committed transaction so a crash loses nothing.
//
// METERING SOURCES — the two events billing CONSUMES (canonical eventsv1):
//   1. fp.inference.completed (events.InferenceCompleted) — the PRIMARY feeder,
//      emitted by the Inference Gateway (the single canonical inference event; a
//      serving pod must NOT emit a competing one). Each completed inference yields
//      usage on TWO meters: METER_TYPE_INFERENCE_REQUEST (one per call) and, when
//      token_count > 0, METER_TYPE_INFERENCE_TOKENS. The event carries token_count
//      and api_key_id, so billing meters both axes off ONE event and resolves the
//      billed team from api_key_id (NEVER from a client field).
//      EXACTLY-ONCE-IN-EFFECT (inbound half): NATS is at-least-once, so the
//      consumer DEDUPES on events.InferenceCompleted.request_id — a request_id it
//      already metered is skipped. request_id flows into the UsageRecord as
//      source_request_id (and into RecordUsageRequest.idempotency_key for the
//      direct-call path). This is the NATS→DB half of exactly-once; the outbox is
//      the DB→NATS half.
//   2. fp.models.version.ready (events.ModelVersionReady) — emitted by the
//      Registry when an artifact upload is verified. Billing meters
//      METER_TYPE_STORAGE_BYTES from the event's server-measured size_bytes, so
//      storage cost rides the same outbox pipeline as inference cost.
//   The billing consumer turns each consumed event into a RecordUsage (internally)
//   so the same metering path serves both the event consumers AND any direct gRPC
//   caller (e.g. a backfill tool). RecordUsage is therefore exposed on the gRPC
//   surface even though, in steady state, most traffic arrives via NATS.
//
// ENUM MAPPING — billing.MeterType <-> events.MeterType:
//   The event bus carries a MIRROR enum (events.v1.MeterType) with byte-identical
//   values (UNSPECIFIED=0, INFERENCE_REQUEST=1, INFERENCE_TOKENS=2,
//   COMPUTE_SECONDS=3, STORAGE_BYTES=4). The handler maps billing.MeterType <->
//   events.MeterType at the publish/consume boundary — a few lines of mechanical
//   conversion that keep the API enum free to evolve independently of the wire
//   contract (the "event schema decoupled from API schema" discipline).
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
// INTEGER OVERFLOW — a money service MUST guard its arithmetic:
//   All money is int64 micro-units and all quantities are int64. The metering
//   math (cost = quantity * unit_price_micros) and the invoice rollup
//   (total = Σ line_item.amount) are MULTIPLICATIONS and SUMS over int64 that can
//   silently WRAP on overflow — and a wrapped total is a real, signed money bug
//   (a huge bill becomes a negative credit). The wire types stay int64 (protobuf
//   has no int128, and int64 micro-units comfortably covers any sane bill: 2^63
//   micro-USD ≈ 9.2 trillion USD), but the SERVER MUST:
//     (a) bound each input `quantity` to a sane per-record maximum (see the
//         MAX_QUANTITY_PER_RECORD note on RecordUsageRequest.quantity) and reject
//         out-of-range values with InvalidArgument — never truncate;
//     (b) perform every multiply/sum with CHECKED arithmetic (math/bits or an
//         explicit overflow test) and fail the operation rather than wrap;
//     (c) reject negative `quantity` (only server-issued credits may be negative,
//         and those never come from a client).
//   This guard is a server-side INVARIANT the proto documents but cannot itself
//   enforce; the integration tests assert the rejection at the boundary.
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
	BillingService_RecordUsage_FullMethodName    = "/forgepoint.billing.v1.BillingService/RecordUsage"
	BillingService_CreateRatePlan_FullMethodName = "/forgepoint.billing.v1.BillingService/CreateRatePlan"
	BillingService_GetRatePlan_FullMethodName    = "/forgepoint.billing.v1.BillingService/GetRatePlan"
	BillingService_CheckQuota_FullMethodName     = "/forgepoint.billing.v1.BillingService/CheckQuota"
	BillingService_GetUsage_FullMethodName       = "/forgepoint.billing.v1.BillingService/GetUsage"
	BillingService_GetInvoice_FullMethodName     = "/forgepoint.billing.v1.BillingService/GetInvoice"
	BillingService_ListInvoices_FullMethodName   = "/forgepoint.billing.v1.BillingService/ListInvoices"
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
//	  state this is driven by the events.InferenceCompleted NATS consumer (and
//	  events.ModelVersionReady for storage); exposed on gRPC for backfills and
//	  direct metering.
//	PRICING (admin/read): CreateRatePlan (admin write — sets real prices),
//	  GetRatePlan (read the pricing a team is billed under).
//	QUOTA (read): CheckQuota — the gateway's pre-flight; cheap "how much is left"
//	  answer the gateway caches in Redis (the QuotaExceeded event invalidates it).
//	REPORTING (read): GetUsage (aggregated, paged), GetInvoice, ListInvoices
//	  (paged) — read-only views over the ledger.
//
// WHY ALL UNARY (no streaming):
//   - RecordUsage / CreateRatePlan / CheckQuota are single fact-in / record-out —
//     request/reply.
//   - GetUsage/ListInvoices return BOUNDED, PAGINATED pages; pagination (not a
//     server stream) is the right tool because it gives clients resumability,
//     cacheable page tokens, and a natural total_count — the same choice the
//     registry's list RPCs made. A server stream would suit an UNBOUNDED live
//     feed (e.g. tail usage in real time); that real-time need is served by the
//     async events.UsageRecorded NATS event instead, keeping the gRPC surface
//     simple.
//
// QUOTA ENFORCEMENT — two complementary mechanisms:
//
//	CheckQuota is the PULL/pre-flight (gateway asks before forwarding; cache-fill
//	cold path). The events.QuotaExceeded outbox event is the PUSH/invalidation
//	(the moment a team crosses the cap, the event flips the gateway's Redis cache
//	to "blocked"). Eventual consistency between DB and the gateway cache is
//	acceptable for a pre-flight — the worst case is a handful of over-quota calls
//	slip through before the cache flips, which the next RecordUsage still meters.
//
// ASCII DIAGRAM — the outbox metering pipeline:
//
//	Inference Gateway ──fp.inference.completed──► Billing NATS consumer
//	                                                   │ (dedupe on request_id)
//	                                                   ▼
//	                                      ┌─ BEGIN tx ───────────────────────────┐
//	                                      │  INSERT usage_record (priced)        │
//	                                      │  UPDATE quota counter                │
//	                                      │  INSERT outbox(events.UsageRecorded) │
//	                                      │  if over quota:                      │
//	                                      │     INSERT outbox(events.QuotaExceeded)│
//	                                      └─ COMMIT ─────────────────────────────┘
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
	// Idempotent via idempotency_key, so a redelivered events.InferenceCompleted or
	// a client retry never double-bills. This is the outbox pattern's write half.
	RecordUsage(ctx context.Context, in *RecordUsageRequest, opts ...grpc.CallOption) (*RecordUsageResponse, error)
	// CreateRatePlan defines a new priced plan (unit prices, free allowances, quota
	// caps). ADMIN-ONLY — it sets real money. id/created_at are server-assigned
	// (mass-assignment guard); idempotent via idempotency_key.
	CreateRatePlan(ctx context.Context, in *CreateRatePlanRequest, opts ...grpc.CallOption) (*CreateRatePlanResponse, error)
	// GetRatePlan returns a single rate plan's pricing so a BFF/CLI can show a team
	// what it will be billed. Non-admins may read only their own team's plan.
	// Read-only.
	GetRatePlan(ctx context.Context, in *GetRatePlanRequest, opts ...grpc.CallOption) (*GetRatePlanResponse, error)
	// CheckQuota is the gateway's pre-flight: returns remaining quota + an
	// `exceeded` flag for a team/meter so the gateway can reject/throttle before
	// forwarding. Server-computed; non-admins scoped to their own team. Read-only.
	CheckQuota(ctx context.Context, in *CheckQuotaRequest, opts ...grpc.CallOption) (*CheckQuotaResponse, error)
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

func (c *billingServiceClient) CreateRatePlan(ctx context.Context, in *CreateRatePlanRequest, opts ...grpc.CallOption) (*CreateRatePlanResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(CreateRatePlanResponse)
	err := c.cc.Invoke(ctx, BillingService_CreateRatePlan_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *billingServiceClient) GetRatePlan(ctx context.Context, in *GetRatePlanRequest, opts ...grpc.CallOption) (*GetRatePlanResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetRatePlanResponse)
	err := c.cc.Invoke(ctx, BillingService_GetRatePlan_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *billingServiceClient) CheckQuota(ctx context.Context, in *CheckQuotaRequest, opts ...grpc.CallOption) (*CheckQuotaResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(CheckQuotaResponse)
	err := c.cc.Invoke(ctx, BillingService_CheckQuota_FullMethodName, in, out, cOpts...)
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
//	  state this is driven by the events.InferenceCompleted NATS consumer (and
//	  events.ModelVersionReady for storage); exposed on gRPC for backfills and
//	  direct metering.
//	PRICING (admin/read): CreateRatePlan (admin write — sets real prices),
//	  GetRatePlan (read the pricing a team is billed under).
//	QUOTA (read): CheckQuota — the gateway's pre-flight; cheap "how much is left"
//	  answer the gateway caches in Redis (the QuotaExceeded event invalidates it).
//	REPORTING (read): GetUsage (aggregated, paged), GetInvoice, ListInvoices
//	  (paged) — read-only views over the ledger.
//
// WHY ALL UNARY (no streaming):
//   - RecordUsage / CreateRatePlan / CheckQuota are single fact-in / record-out —
//     request/reply.
//   - GetUsage/ListInvoices return BOUNDED, PAGINATED pages; pagination (not a
//     server stream) is the right tool because it gives clients resumability,
//     cacheable page tokens, and a natural total_count — the same choice the
//     registry's list RPCs made. A server stream would suit an UNBOUNDED live
//     feed (e.g. tail usage in real time); that real-time need is served by the
//     async events.UsageRecorded NATS event instead, keeping the gRPC surface
//     simple.
//
// QUOTA ENFORCEMENT — two complementary mechanisms:
//
//	CheckQuota is the PULL/pre-flight (gateway asks before forwarding; cache-fill
//	cold path). The events.QuotaExceeded outbox event is the PUSH/invalidation
//	(the moment a team crosses the cap, the event flips the gateway's Redis cache
//	to "blocked"). Eventual consistency between DB and the gateway cache is
//	acceptable for a pre-flight — the worst case is a handful of over-quota calls
//	slip through before the cache flips, which the next RecordUsage still meters.
//
// ASCII DIAGRAM — the outbox metering pipeline:
//
//	Inference Gateway ──fp.inference.completed──► Billing NATS consumer
//	                                                   │ (dedupe on request_id)
//	                                                   ▼
//	                                      ┌─ BEGIN tx ───────────────────────────┐
//	                                      │  INSERT usage_record (priced)        │
//	                                      │  UPDATE quota counter                │
//	                                      │  INSERT outbox(events.UsageRecorded) │
//	                                      │  if over quota:                      │
//	                                      │     INSERT outbox(events.QuotaExceeded)│
//	                                      └─ COMMIT ─────────────────────────────┘
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
	// Idempotent via idempotency_key, so a redelivered events.InferenceCompleted or
	// a client retry never double-bills. This is the outbox pattern's write half.
	RecordUsage(context.Context, *RecordUsageRequest) (*RecordUsageResponse, error)
	// CreateRatePlan defines a new priced plan (unit prices, free allowances, quota
	// caps). ADMIN-ONLY — it sets real money. id/created_at are server-assigned
	// (mass-assignment guard); idempotent via idempotency_key.
	CreateRatePlan(context.Context, *CreateRatePlanRequest) (*CreateRatePlanResponse, error)
	// GetRatePlan returns a single rate plan's pricing so a BFF/CLI can show a team
	// what it will be billed. Non-admins may read only their own team's plan.
	// Read-only.
	GetRatePlan(context.Context, *GetRatePlanRequest) (*GetRatePlanResponse, error)
	// CheckQuota is the gateway's pre-flight: returns remaining quota + an
	// `exceeded` flag for a team/meter so the gateway can reject/throttle before
	// forwarding. Server-computed; non-admins scoped to their own team. Read-only.
	CheckQuota(context.Context, *CheckQuotaRequest) (*CheckQuotaResponse, error)
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
func (UnimplementedBillingServiceServer) CreateRatePlan(context.Context, *CreateRatePlanRequest) (*CreateRatePlanResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method CreateRatePlan not implemented")
}
func (UnimplementedBillingServiceServer) GetRatePlan(context.Context, *GetRatePlanRequest) (*GetRatePlanResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetRatePlan not implemented")
}
func (UnimplementedBillingServiceServer) CheckQuota(context.Context, *CheckQuotaRequest) (*CheckQuotaResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method CheckQuota not implemented")
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

func _BillingService_CreateRatePlan_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateRatePlanRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BillingServiceServer).CreateRatePlan(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BillingService_CreateRatePlan_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BillingServiceServer).CreateRatePlan(ctx, req.(*CreateRatePlanRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BillingService_GetRatePlan_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRatePlanRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BillingServiceServer).GetRatePlan(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BillingService_GetRatePlan_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BillingServiceServer).GetRatePlan(ctx, req.(*GetRatePlanRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BillingService_CheckQuota_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CheckQuotaRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BillingServiceServer).CheckQuota(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BillingService_CheckQuota_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BillingServiceServer).CheckQuota(ctx, req.(*CheckQuotaRequest))
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
			MethodName: "CreateRatePlan",
			Handler:    _BillingService_CreateRatePlan_Handler,
		},
		{
			MethodName: "GetRatePlan",
			Handler:    _BillingService_GetRatePlan_Handler,
		},
		{
			MethodName: "CheckQuota",
			Handler:    _BillingService_CheckQuota_Handler,
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
