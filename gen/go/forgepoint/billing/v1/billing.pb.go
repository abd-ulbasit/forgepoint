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

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.11
// 	protoc        (unknown)
// source: forgepoint/billing/v1/billing.proto

package billingv1

import (
	v1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
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
// MeterType — what kind of resource a usage record measures.
// ============================================================================
//
// WHY an enum, not a free string:
//
//	Closed, validated set at the wire boundary. The rate plan prices each meter
//	type differently, so a typo ("infernce") must not silently fall through to
//	a zero price. Buf STANDARD requires the _UNSPECIFIED zero value and the
//	ENUM_NAME prefix on every value.
//
// WHY separate inference-request vs inference-tokens:
//
//	Real ML billing meters on MORE than one axis simultaneously — a single
//	inference call costs a per-request fee AND a per-token (or per-compute-unit)
//	fee. Modeling them as distinct meter types lets one InferenceCompleted
//	produce multiple priced line items, which is how OpenAI/Anthropic-style
//	token billing actually works.
//
// ============================================================================
type MeterType int32

const (
	// Required zero value. Unknown/unset meter — the server rejects RecordUsage
	// with this value rather than guessing a price.
	MeterType_METER_TYPE_UNSPECIFIED MeterType = 0
	// One billable inference REQUEST (the per-call fee), regardless of size.
	MeterType_METER_TYPE_INFERENCE_REQUEST MeterType = 1
	// Tokens processed by an inference call (prompt + completion). Priced
	// per-1k-tokens by the rate plan. The dominant cost axis for LLM serving.
	MeterType_METER_TYPE_INFERENCE_TOKENS MeterType = 2
	// Compute-seconds consumed by serving (for non-token models priced on
	// wall-clock / GPU time rather than tokens).
	MeterType_METER_TYPE_COMPUTE_SECONDS MeterType = 3
	// Artifact bytes stored in the model registry / object store, metered
	// periodically. Lets storage cost ride the same pipeline as inference cost.
	MeterType_METER_TYPE_STORAGE_BYTES MeterType = 4
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
	return file_forgepoint_billing_v1_billing_proto_enumTypes[0].Descriptor()
}

func (MeterType) Type() protoreflect.EnumType {
	return &file_forgepoint_billing_v1_billing_proto_enumTypes[0]
}

func (x MeterType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use MeterType.Descriptor instead.
func (MeterType) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{0}
}

// ============================================================================
// InvoiceStatus — lifecycle of an invoice.
// ============================================================================
//
// WHY an enum: invoice status drives money-moving side effects (you can only
// charge a card on a FINALIZED invoice, you can't re-finalize a PAID one). A
// closed validated set with explicit transitions is mandatory for an auditable
// ledger. Status is SERVER-authoritative — a client can never set it.
// ============================================================================
type InvoiceStatus int32

const (
	// Required zero value. Unknown/unset.
	InvoiceStatus_INVOICE_STATUS_UNSPECIFIED InvoiceStatus = 0
	// Accumulating: the billing period is still open and line items are still
	// being metered onto this invoice. Not yet collectible.
	InvoiceStatus_INVOICE_STATUS_DRAFT InvoiceStatus = 1
	// Period closed and totals locked. The InvoiceGenerated event fires on this
	// transition. Collectible but not yet paid.
	InvoiceStatus_INVOICE_STATUS_FINALIZED InvoiceStatus = 2
	// Payment received and reconciled. Terminal (happy path).
	InvoiceStatus_INVOICE_STATUS_PAID InvoiceStatus = 3
	// Finalized but unpaid past its due date. Drives dunning / Notification.
	InvoiceStatus_INVOICE_STATUS_OVERDUE InvoiceStatus = 4
	// Reversed/written off (e.g. credit issued). Terminal. Kept for audit, never
	// deleted — ledgers are append-only.
	InvoiceStatus_INVOICE_STATUS_VOID InvoiceStatus = 5
)

// Enum value maps for InvoiceStatus.
var (
	InvoiceStatus_name = map[int32]string{
		0: "INVOICE_STATUS_UNSPECIFIED",
		1: "INVOICE_STATUS_DRAFT",
		2: "INVOICE_STATUS_FINALIZED",
		3: "INVOICE_STATUS_PAID",
		4: "INVOICE_STATUS_OVERDUE",
		5: "INVOICE_STATUS_VOID",
	}
	InvoiceStatus_value = map[string]int32{
		"INVOICE_STATUS_UNSPECIFIED": 0,
		"INVOICE_STATUS_DRAFT":       1,
		"INVOICE_STATUS_FINALIZED":   2,
		"INVOICE_STATUS_PAID":        3,
		"INVOICE_STATUS_OVERDUE":     4,
		"INVOICE_STATUS_VOID":        5,
	}
)

func (x InvoiceStatus) Enum() *InvoiceStatus {
	p := new(InvoiceStatus)
	*p = x
	return p
}

func (x InvoiceStatus) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (InvoiceStatus) Descriptor() protoreflect.EnumDescriptor {
	return file_forgepoint_billing_v1_billing_proto_enumTypes[1].Descriptor()
}

func (InvoiceStatus) Type() protoreflect.EnumType {
	return &file_forgepoint_billing_v1_billing_proto_enumTypes[1]
}

func (x InvoiceStatus) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use InvoiceStatus.Descriptor instead.
func (InvoiceStatus) EnumDescriptor() ([]byte, []int) {
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{1}
}

// ============================================================================
// Money — a precise monetary amount.
// ============================================================================
//
// WHY integer minor units, NOT double:
//
//	Floating point cannot represent most decimal money values exactly
//	(0.10 + 0.20 != 0.30 in IEEE-754). Accumulating float errors over millions
//	of metered calls produces real money discrepancies and fails reconciliation.
//	We store the amount as an INTEGER in the currency's minor unit so all
//	arithmetic is exact integer math. This mirrors Stripe's Money model and the
//	google.type.Money guidance.
//
// WHY micro-units (1e-6) rather than cents:
//
//	Per-token / per-request prices are tiny — a single token might cost
//	0.0000004 USD. Cents (1e-2) would round that to zero. We use MICRO units
//	(1/1,000,000 of the major unit, i.e. micro-dollars) so we can price and sum
//	sub-cent meters exactly and only round to cents at invoice finalization.
//	So amount_micros = 1_000_000 means 1.00 USD; 4 means 0.000004 USD.
//
// WHY currency_code is here on every amount:
//
//	Self-describing money prevents the classic "is this dollars or cents? USD
//	or EUR?" ambiguity at every boundary. ISO 4217 code ("USD", "EUR").
//
// SECURITY: Money only ever appears in RESPONSES and in server-built event
// payloads. It is NEVER a field on a client write request — clients submit
// quantities, the server attaches the price.
// ============================================================================
type Money struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Amount in micro-units of the currency's major unit (1e-6). Exact integer.
	// 1_000_000 == 1.00 of the currency. May be negative for credits/refunds.
	AmountMicros int64 `protobuf:"varint,1,opt,name=amount_micros,json=amountMicros,proto3" json:"amount_micros,omitempty"`
	// ISO 4217 currency code, uppercase: "USD", "EUR". Empty is invalid.
	CurrencyCode  string `protobuf:"bytes,2,opt,name=currency_code,json=currencyCode,proto3" json:"currency_code,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Money) Reset() {
	*x = Money{}
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Money) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Money) ProtoMessage() {}

func (x *Money) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Money.ProtoReflect.Descriptor instead.
func (*Money) Descriptor() ([]byte, []int) {
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{0}
}

func (x *Money) GetAmountMicros() int64 {
	if x != nil {
		return x.AmountMicros
	}
	return 0
}

func (x *Money) GetCurrencyCode() string {
	if x != nil {
		return x.CurrencyCode
	}
	return ""
}

// ============================================================================
// RatePlan — the pricing the server applies to metered usage.
// ============================================================================
//
// WHY this lives server-side and is referenced, not supplied:
//
//	The rate plan is the SOURCE of every price. A usage record references a rate
//	plan by id; the server reads the plan's unit prices to compute cost. The
//	client never sees or sends prices on the write path — it can only learn them
//	after the fact via GetUsage/GetInvoice. This is the structural guarantee
//	that "the client cannot set its own price".
//
// NOTE: RatePlan management RPCs (CreateRatePlan, etc.) are intentionally NOT in
// this file's M2 surface — the platform design lists CreateRatePlan as a later
// admin concern. The message is defined now because GetUsage/Invoice responses
// reference the plan that was applied, and because it documents the pricing
// model the metering math depends on. Adding the admin RPCs later is additive
// (buf-breaking-safe).
// ============================================================================
type RatePlan struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// UUID v4. Stable identifier referenced by usage records and invoices.
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// Human-readable plan name: "free-tier", "standard", "enterprise".
	Name string `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	// Per-meter unit prices. Key is the MeterType's string name (e.g.
	// "METER_TYPE_INFERENCE_TOKENS"); value is the price for ONE unit of that
	// meter (per request, per token, per compute-second, per byte) as Money.
	// WHY a map keyed by meter rather than fixed fields: meter types grow over
	// time; a map lets us add a meter's price without a schema change.
	UnitPrices map[string]*Money `protobuf:"bytes,3,rep,name=unit_prices,json=unitPrices,proto3" json:"unit_prices,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	// Included free allowance per billing period, keyed by meter name. Usage
	// beyond the allowance is what gets charged. Empty = no free tier.
	IncludedQuantities map[string]int64 `protobuf:"bytes,4,rep,name=included_quantities,json=includedQuantities,proto3" json:"included_quantities,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"varint,2,opt,name=value"`
	// Monthly quota cap per meter name (hard limit). Exceeding it is what fires
	// the QuotaExceeded event. 0 / absent for a given meter = no hard cap.
	QuotaLimits map[string]int64 `protobuf:"bytes,5,rep,name=quota_limits,json=quotaLimits,proto3" json:"quota_limits,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"varint,2,opt,name=value"`
	// When this plan was created. Server-set, immutable.
	CreatedAt     *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=created_at,json=createdAt,proto3" json:"created_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *RatePlan) Reset() {
	*x = RatePlan{}
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RatePlan) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RatePlan) ProtoMessage() {}

func (x *RatePlan) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RatePlan.ProtoReflect.Descriptor instead.
func (*RatePlan) Descriptor() ([]byte, []int) {
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{1}
}

func (x *RatePlan) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *RatePlan) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *RatePlan) GetUnitPrices() map[string]*Money {
	if x != nil {
		return x.UnitPrices
	}
	return nil
}

func (x *RatePlan) GetIncludedQuantities() map[string]int64 {
	if x != nil {
		return x.IncludedQuantities
	}
	return nil
}

func (x *RatePlan) GetQuotaLimits() map[string]int64 {
	if x != nil {
		return x.QuotaLimits
	}
	return nil
}

func (x *RatePlan) GetCreatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CreatedAt
	}
	return nil
}

// ============================================================================
// UsageRecord — one metered, priced fact (the atom of the ledger).
// ============================================================================
//
// WHY this is the unit of record:
//
//	Every billable action becomes exactly one UsageRecord. Invoices are just
//	aggregations of UsageRecords over a period. Keeping the atom immutable and
//	append-only (we never UPDATE a recorded usage row, we only INSERT) makes the
//	ledger auditable and reconciliation tractable — the same append-only
//	discipline as the Feature Store's event log.
//
// EVERY money/attribution field below is SERVER-COMPUTED. The client side of a
// RecordUsage supplies only meter_type + quantity (+ what was served). See
// RecordUsageRequest for the strict input contract.
// ============================================================================
type UsageRecord struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// UUID v4 assigned by the server. Primary key of the ledger row.
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// The team this usage is billed to. SERVER-derived from the authenticated
	// caller's claims (team) or, for the InferenceCompleted consumer, from the
	// API key that made the inference. NEVER taken from a client write field —
	// letting a caller name the billed team is account-takeover-for-money.
	Team string `protobuf:"bytes,2,opt,name=team,proto3" json:"team,omitempty"`
	// The rate plan that was applied to price this record. SERVER-resolved from
	// the team's current plan at metering time, and pinned here so the price is
	// reproducible even if the team's plan later changes.
	RatePlanId string `protobuf:"bytes,3,opt,name=rate_plan_id,json=ratePlanId,proto3" json:"rate_plan_id,omitempty"`
	// What was metered.
	MeterType MeterType `protobuf:"varint,4,opt,name=meter_type,json=meterType,proto3,enum=forgepoint.billing.v1.MeterType" json:"meter_type,omitempty"`
	// How many units of that meter (request count, token count, compute-seconds,
	// bytes). This is the one quantity the client/event supplies and the server
	// validates (non-negative, sane bounds).
	Quantity int64 `protobuf:"varint,5,opt,name=quantity,proto3" json:"quantity,omitempty"`
	// The price the server computed for this record: quantity * plan unit price
	// (after free-allowance logic). Authoritative; clients cannot supply it.
	Cost *Money `protobuf:"bytes,6,opt,name=cost,proto3" json:"cost,omitempty"`
	// What produced the usage — for attribution and per-model usage reports.
	// SERVER-populated from the inference event / request context.
	ModelId      string `protobuf:"bytes,7,opt,name=model_id,json=modelId,proto3" json:"model_id,omitempty"`
	ModelVersion string `protobuf:"bytes,8,opt,name=model_version,json=modelVersion,proto3" json:"model_version,omitempty"`
	// The inference request id (or upstream event id) this record was derived
	// from. Doubles as the natural idempotency anchor: two records must never
	// share a source_request_id. SERVER-stored from the metering input.
	SourceRequestId string `protobuf:"bytes,9,opt,name=source_request_id,json=sourceRequestId,proto3" json:"source_request_id,omitempty"`
	// When the metered action occurred (event time, not insert time).
	// SERVER-stamped from the source event; clients cannot backdate usage.
	OccurredAt    *timestamppb.Timestamp `protobuf:"bytes,10,opt,name=occurred_at,json=occurredAt,proto3" json:"occurred_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UsageRecord) Reset() {
	*x = UsageRecord{}
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UsageRecord) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UsageRecord) ProtoMessage() {}

func (x *UsageRecord) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UsageRecord.ProtoReflect.Descriptor instead.
func (*UsageRecord) Descriptor() ([]byte, []int) {
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{2}
}

func (x *UsageRecord) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *UsageRecord) GetTeam() string {
	if x != nil {
		return x.Team
	}
	return ""
}

func (x *UsageRecord) GetRatePlanId() string {
	if x != nil {
		return x.RatePlanId
	}
	return ""
}

func (x *UsageRecord) GetMeterType() MeterType {
	if x != nil {
		return x.MeterType
	}
	return MeterType_METER_TYPE_UNSPECIFIED
}

func (x *UsageRecord) GetQuantity() int64 {
	if x != nil {
		return x.Quantity
	}
	return 0
}

func (x *UsageRecord) GetCost() *Money {
	if x != nil {
		return x.Cost
	}
	return nil
}

func (x *UsageRecord) GetModelId() string {
	if x != nil {
		return x.ModelId
	}
	return ""
}

func (x *UsageRecord) GetModelVersion() string {
	if x != nil {
		return x.ModelVersion
	}
	return ""
}

func (x *UsageRecord) GetSourceRequestId() string {
	if x != nil {
		return x.SourceRequestId
	}
	return ""
}

func (x *UsageRecord) GetOccurredAt() *timestamppb.Timestamp {
	if x != nil {
		return x.OccurredAt
	}
	return nil
}

// ============================================================================
// UsageSummary — an aggregated usage rollup (the shape GetUsage returns).
// ============================================================================
//
// WHY a summary type distinct from UsageRecord:
//
//	GetUsage answers "how much has team X used / spent this period?" — callers
//	(dashboards, the BFF) want a ROLLUP, not millions of raw records. Returning
//	raw UsageRecords for a heavy team would be unbounded and useless for a
//	dashboard. The summary buckets quantity and cost per meter type.
//
// ============================================================================
type UsageSummary struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The team this summary covers.
	Team string `protobuf:"bytes,1,opt,name=team,proto3" json:"team,omitempty"`
	// The window this rollup aggregates over (inclusive start, exclusive end).
	PeriodStart *timestamppb.Timestamp `protobuf:"bytes,2,opt,name=period_start,json=periodStart,proto3" json:"period_start,omitempty"`
	PeriodEnd   *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=period_end,json=periodEnd,proto3" json:"period_end,omitempty"`
	// Per-meter aggregation. Key is the MeterType string name; value is the
	// bucketed totals for that meter over the window.
	ByMeter map[string]*MeterUsage `protobuf:"bytes,4,rep,name=by_meter,json=byMeter,proto3" json:"by_meter,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	// Total cost across all meters in the window. Server-computed sum.
	TotalCost     *Money `protobuf:"bytes,5,opt,name=total_cost,json=totalCost,proto3" json:"total_cost,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UsageSummary) Reset() {
	*x = UsageSummary{}
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UsageSummary) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UsageSummary) ProtoMessage() {}

func (x *UsageSummary) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UsageSummary.ProtoReflect.Descriptor instead.
func (*UsageSummary) Descriptor() ([]byte, []int) {
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{3}
}

func (x *UsageSummary) GetTeam() string {
	if x != nil {
		return x.Team
	}
	return ""
}

func (x *UsageSummary) GetPeriodStart() *timestamppb.Timestamp {
	if x != nil {
		return x.PeriodStart
	}
	return nil
}

func (x *UsageSummary) GetPeriodEnd() *timestamppb.Timestamp {
	if x != nil {
		return x.PeriodEnd
	}
	return nil
}

func (x *UsageSummary) GetByMeter() map[string]*MeterUsage {
	if x != nil {
		return x.ByMeter
	}
	return nil
}

func (x *UsageSummary) GetTotalCost() *Money {
	if x != nil {
		return x.TotalCost
	}
	return nil
}

// MeterUsage — one meter's bucket inside a UsageSummary.
type MeterUsage struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Which meter this bucket is for (redundant with the map key, included so the
	// value is self-describing if extracted from the map).
	MeterType MeterType `protobuf:"varint,1,opt,name=meter_type,json=meterType,proto3,enum=forgepoint.billing.v1.MeterType" json:"meter_type,omitempty"`
	// Total units of this meter over the window.
	TotalQuantity int64 `protobuf:"varint,2,opt,name=total_quantity,json=totalQuantity,proto3" json:"total_quantity,omitempty"`
	// Total cost attributed to this meter over the window.
	TotalCost     *Money `protobuf:"bytes,3,opt,name=total_cost,json=totalCost,proto3" json:"total_cost,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *MeterUsage) Reset() {
	*x = MeterUsage{}
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *MeterUsage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MeterUsage) ProtoMessage() {}

func (x *MeterUsage) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MeterUsage.ProtoReflect.Descriptor instead.
func (*MeterUsage) Descriptor() ([]byte, []int) {
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{4}
}

func (x *MeterUsage) GetMeterType() MeterType {
	if x != nil {
		return x.MeterType
	}
	return MeterType_METER_TYPE_UNSPECIFIED
}

func (x *MeterUsage) GetTotalQuantity() int64 {
	if x != nil {
		return x.TotalQuantity
	}
	return 0
}

func (x *MeterUsage) GetTotalCost() *Money {
	if x != nil {
		return x.TotalCost
	}
	return nil
}

// ============================================================================
// Invoice — a finalized (or draft) bill for a team over a billing period.
// ============================================================================
//
// WHY invoices are derived, never client-authored:
//
//	An invoice is a server-built aggregation of UsageRecords plus the plan's
//	pricing over a closed period. There is no "create invoice from client data"
//	path — that would let a customer write their own bill. Invoices are produced
//	by the period-close job, which emits InvoiceGenerated through the outbox.
//
// ============================================================================
type Invoice struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// UUID v4. Stable invoice identifier.
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// Human-facing invoice number (e.g. "INV-2026-000042"). Server-assigned,
	// monotonic, distinct from the UUID so it's safe to print/share.
	InvoiceNumber string `protobuf:"bytes,2,opt,name=invoice_number,json=invoiceNumber,proto3" json:"invoice_number,omitempty"`
	// Team being billed.
	Team string `protobuf:"bytes,3,opt,name=team,proto3" json:"team,omitempty"`
	// The rate plan in effect for this period.
	RatePlanId string `protobuf:"bytes,4,opt,name=rate_plan_id,json=ratePlanId,proto3" json:"rate_plan_id,omitempty"`
	// Lifecycle status. SERVER-authoritative; drives collectibility.
	Status InvoiceStatus `protobuf:"varint,5,opt,name=status,proto3,enum=forgepoint.billing.v1.InvoiceStatus" json:"status,omitempty"`
	// Billing period covered (inclusive start, exclusive end).
	PeriodStart *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=period_start,json=periodStart,proto3" json:"period_start,omitempty"`
	PeriodEnd   *timestamppb.Timestamp `protobuf:"bytes,7,opt,name=period_end,json=periodEnd,proto3" json:"period_end,omitempty"`
	// The priced breakdown that sums to the total. One line per meter (and/or per
	// model), so the customer can see WHERE cost came from.
	LineItems []*InvoiceLineItem `protobuf:"bytes,8,rep,name=line_items,json=lineItems,proto3" json:"line_items,omitempty"`
	// The invoice total. Server-computed sum of line_items, rounded to the
	// currency's minor unit (cents) at finalization. Authoritative.
	Total *Money `protobuf:"bytes,9,opt,name=total,proto3" json:"total,omitempty"`
	// When the invoice was created (period open) and finalized (period close).
	// finalized_at is unset while status == DRAFT.
	CreatedAt   *timestamppb.Timestamp `protobuf:"bytes,10,opt,name=created_at,json=createdAt,proto3" json:"created_at,omitempty"`
	FinalizedAt *timestamppb.Timestamp `protobuf:"bytes,11,opt,name=finalized_at,json=finalizedAt,proto3" json:"finalized_at,omitempty"`
	// When payment is due (set on finalization). Past-due drives OVERDUE.
	DueAt         *timestamppb.Timestamp `protobuf:"bytes,12,opt,name=due_at,json=dueAt,proto3" json:"due_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Invoice) Reset() {
	*x = Invoice{}
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Invoice) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Invoice) ProtoMessage() {}

func (x *Invoice) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Invoice.ProtoReflect.Descriptor instead.
func (*Invoice) Descriptor() ([]byte, []int) {
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{5}
}

func (x *Invoice) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Invoice) GetInvoiceNumber() string {
	if x != nil {
		return x.InvoiceNumber
	}
	return ""
}

func (x *Invoice) GetTeam() string {
	if x != nil {
		return x.Team
	}
	return ""
}

func (x *Invoice) GetRatePlanId() string {
	if x != nil {
		return x.RatePlanId
	}
	return ""
}

func (x *Invoice) GetStatus() InvoiceStatus {
	if x != nil {
		return x.Status
	}
	return InvoiceStatus_INVOICE_STATUS_UNSPECIFIED
}

func (x *Invoice) GetPeriodStart() *timestamppb.Timestamp {
	if x != nil {
		return x.PeriodStart
	}
	return nil
}

func (x *Invoice) GetPeriodEnd() *timestamppb.Timestamp {
	if x != nil {
		return x.PeriodEnd
	}
	return nil
}

func (x *Invoice) GetLineItems() []*InvoiceLineItem {
	if x != nil {
		return x.LineItems
	}
	return nil
}

func (x *Invoice) GetTotal() *Money {
	if x != nil {
		return x.Total
	}
	return nil
}

func (x *Invoice) GetCreatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CreatedAt
	}
	return nil
}

func (x *Invoice) GetFinalizedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.FinalizedAt
	}
	return nil
}

func (x *Invoice) GetDueAt() *timestamppb.Timestamp {
	if x != nil {
		return x.DueAt
	}
	return nil
}

// InvoiceLineItem — one priced row on an invoice.
type InvoiceLineItem struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// What this line meters.
	MeterType MeterType `protobuf:"varint,1,opt,name=meter_type,json=meterType,proto3,enum=forgepoint.billing.v1.MeterType" json:"meter_type,omitempty"`
	// Free-text description for the customer: "Inference tokens (gpt-mini v3)".
	Description string `protobuf:"bytes,2,opt,name=description,proto3" json:"description,omitempty"`
	// Total billable units on this line (after free allowance).
	Quantity int64 `protobuf:"varint,3,opt,name=quantity,proto3" json:"quantity,omitempty"`
	// Per-unit price applied (from the pinned rate plan), for transparency.
	UnitPrice *Money `protobuf:"bytes,4,opt,name=unit_price,json=unitPrice,proto3" json:"unit_price,omitempty"`
	// Line total = quantity * unit_price. Server-computed.
	Amount        *Money `protobuf:"bytes,5,opt,name=amount,proto3" json:"amount,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *InvoiceLineItem) Reset() {
	*x = InvoiceLineItem{}
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *InvoiceLineItem) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InvoiceLineItem) ProtoMessage() {}

func (x *InvoiceLineItem) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use InvoiceLineItem.ProtoReflect.Descriptor instead.
func (*InvoiceLineItem) Descriptor() ([]byte, []int) {
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{6}
}

func (x *InvoiceLineItem) GetMeterType() MeterType {
	if x != nil {
		return x.MeterType
	}
	return MeterType_METER_TYPE_UNSPECIFIED
}

func (x *InvoiceLineItem) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *InvoiceLineItem) GetQuantity() int64 {
	if x != nil {
		return x.Quantity
	}
	return 0
}

func (x *InvoiceLineItem) GetUnitPrice() *Money {
	if x != nil {
		return x.UnitPrice
	}
	return nil
}

func (x *InvoiceLineItem) GetAmount() *Money {
	if x != nil {
		return x.Amount
	}
	return nil
}

// ============================================================================
// RecordUsage
// ============================================================================
//
// RecordUsageRequest is the STRICT metering-input contract. This is the single
// most security-sensitive write in the platform, so the input surface is
// deliberately MINIMAL: the caller supplies only the verifiable raw facts, and
// the server attaches everything that touches money or attribution.
//
// WHAT THE CLIENT MAY SEND (and only this):
//
//	meter_type, quantity, model_id, model_version, source_request_id,
//	occurred_at, idempotency_key.
//
// WHAT THE CLIENT MAY NOT SEND (server-authoritative — absent by design):
//
//	team / billed account  → derived from auth claims or the inference API key
//	rate_plan_id           → resolved from the team's current plan
//	cost / price / Money   → computed by the server from the plan
//	any invoice/status     → never settable
//
// WHY occurred_at is accepted but still constrained:
//
//	The metering source (InferenceCompleted) carries the true event time, which
//	can legitimately be slightly in the past (queue lag). We accept it so usage
//	lands in the correct billing period, but the server CLAMPS it to a sane
//	window (e.g. not in the future, not older than the open period) so a caller
//	can't backdate usage into a closed/paid invoice. Empty = server uses now().
//
// IDEMPOTENCY (critical for an at-least-once meter):
//
//	The upstream InferenceCompleted is delivered at-least-once, and clients
//	retry. A duplicate must NOT bill twice. idempotency_key (client-generated,
//	stable per logical action — typically the inference request id) is stored
//	with the created UsageRecord; a repeat with the same key returns the
//	ORIGINAL record instead of inserting a second one. This is the NATS→DB half
//	of the exactly-once-in-effect guarantee (the outbox is the DB→NATS half).
//
// ============================================================================
type RecordUsageRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// What is being metered. Must be a known, non-UNSPECIFIED meter.
	MeterType MeterType `protobuf:"varint,1,opt,name=meter_type,json=meterType,proto3,enum=forgepoint.billing.v1.MeterType" json:"meter_type,omitempty"`
	// How many units (requests, tokens, compute-seconds, bytes). Server validates
	// non-negative and within sane per-record bounds.
	Quantity int64 `protobuf:"varint,2,opt,name=quantity,proto3" json:"quantity,omitempty"`
	// What produced the usage (for attribution / per-model reports). Optional but
	// strongly recommended; used only for reporting, never for pricing.
	ModelId      string `protobuf:"bytes,3,opt,name=model_id,json=modelId,proto3" json:"model_id,omitempty"`
	ModelVersion string `protobuf:"bytes,4,opt,name=model_version,json=modelVersion,proto3" json:"model_version,omitempty"`
	// The originating inference request id (or upstream event id). Used for
	// lineage and as the natural dedupe anchor. SHOULD be set for inference usage.
	SourceRequestId string `protobuf:"bytes,5,opt,name=source_request_id,json=sourceRequestId,proto3" json:"source_request_id,omitempty"`
	// Event time of the metered action. Server clamps to the valid window; empty
	// means "use server now()". See WHY note above — cannot backdate into a
	// closed period.
	OccurredAt *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=occurred_at,json=occurredAt,proto3" json:"occurred_at,omitempty"`
	// Idempotency key (UUID/stable string). A repeat returns the original record.
	// See IDEMPOTENCY note above — this is what makes redelivery safe.
	IdempotencyKey string `protobuf:"bytes,7,opt,name=idempotency_key,json=idempotencyKey,proto3" json:"idempotency_key,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *RecordUsageRequest) Reset() {
	*x = RecordUsageRequest{}
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RecordUsageRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RecordUsageRequest) ProtoMessage() {}

func (x *RecordUsageRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RecordUsageRequest.ProtoReflect.Descriptor instead.
func (*RecordUsageRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{7}
}

func (x *RecordUsageRequest) GetMeterType() MeterType {
	if x != nil {
		return x.MeterType
	}
	return MeterType_METER_TYPE_UNSPECIFIED
}

func (x *RecordUsageRequest) GetQuantity() int64 {
	if x != nil {
		return x.Quantity
	}
	return 0
}

func (x *RecordUsageRequest) GetModelId() string {
	if x != nil {
		return x.ModelId
	}
	return ""
}

func (x *RecordUsageRequest) GetModelVersion() string {
	if x != nil {
		return x.ModelVersion
	}
	return ""
}

func (x *RecordUsageRequest) GetSourceRequestId() string {
	if x != nil {
		return x.SourceRequestId
	}
	return ""
}

func (x *RecordUsageRequest) GetOccurredAt() *timestamppb.Timestamp {
	if x != nil {
		return x.OccurredAt
	}
	return nil
}

func (x *RecordUsageRequest) GetIdempotencyKey() string {
	if x != nil {
		return x.IdempotencyKey
	}
	return ""
}

// RecordUsageResponse returns the server-built, priced UsageRecord.
// WHY return the full record: the caller (and the integration test) can see the
// authoritative team, rate_plan_id, and computed cost the server attached —
// proving the price was server-derived, not client-supplied.
type RecordUsageResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The committed, priced ledger record (id, team, cost all server-set).
	Record *UsageRecord `protobuf:"bytes,1,opt,name=record,proto3" json:"record,omitempty"`
	// True if this call hit the idempotency path (the key was already recorded)
	// and `record` is the pre-existing one rather than a newly inserted row.
	// Lets retrying clients distinguish "I just created it" from "already there".
	Deduplicated  bool `protobuf:"varint,2,opt,name=deduplicated,proto3" json:"deduplicated,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *RecordUsageResponse) Reset() {
	*x = RecordUsageResponse{}
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[8]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RecordUsageResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RecordUsageResponse) ProtoMessage() {}

func (x *RecordUsageResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[8]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RecordUsageResponse.ProtoReflect.Descriptor instead.
func (*RecordUsageResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{8}
}

func (x *RecordUsageResponse) GetRecord() *UsageRecord {
	if x != nil {
		return x.Record
	}
	return nil
}

func (x *RecordUsageResponse) GetDeduplicated() bool {
	if x != nil {
		return x.Deduplicated
	}
	return false
}

// ============================================================================
// GetUsage
// ============================================================================
//
// GetUsageRequest asks for an AGGREGATED, paginated usage view for a team over
// a time window. Returns UsageSummary rollups, not raw records (see UsageSummary
// WHY note). Pagination applies when the window is broken into sub-period
// buckets (e.g. daily) so a long range stays bounded.
//
// AUTHORIZATION NOTE: a non-admin caller may only query their OWN team. The
// `team` filter is enforced server-side against auth claims — supplying another
// team is rejected unless the caller is an admin. The field exists so admins and
// the BFF can scope queries; it is NOT a way to bypass tenant isolation.
type GetUsageRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Team to report on. For non-admins the server overrides this with the
	// caller's own team (cannot read another team's spend). Required for admins.
	Team string `protobuf:"bytes,1,opt,name=team,proto3" json:"team,omitempty"`
	// Inclusive start / exclusive end of the reporting window. Empty start =
	// current billing period start; empty end = now. Server caps the maximum
	// span to bound query cost.
	PeriodStart *timestamppb.Timestamp `protobuf:"bytes,2,opt,name=period_start,json=periodStart,proto3" json:"period_start,omitempty"`
	PeriodEnd   *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=period_end,json=periodEnd,proto3" json:"period_end,omitempty"`
	// Optional: restrict to a single meter type. UNSPECIFIED = all meters.
	MeterTypeFilter MeterType `protobuf:"varint,4,opt,name=meter_type_filter,json=meterTypeFilter,proto3,enum=forgepoint.billing.v1.MeterType" json:"meter_type_filter,omitempty"`
	// Cursor-based pagination over sub-period buckets. page_size defaults to 20,
	// capped at 100 server-side (see common.proto). See PaginationRequest.
	Pagination    *v1.PaginationRequest `protobuf:"bytes,5,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetUsageRequest) Reset() {
	*x = GetUsageRequest{}
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[9]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetUsageRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetUsageRequest) ProtoMessage() {}

func (x *GetUsageRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[9]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetUsageRequest.ProtoReflect.Descriptor instead.
func (*GetUsageRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{9}
}

func (x *GetUsageRequest) GetTeam() string {
	if x != nil {
		return x.Team
	}
	return ""
}

func (x *GetUsageRequest) GetPeriodStart() *timestamppb.Timestamp {
	if x != nil {
		return x.PeriodStart
	}
	return nil
}

func (x *GetUsageRequest) GetPeriodEnd() *timestamppb.Timestamp {
	if x != nil {
		return x.PeriodEnd
	}
	return nil
}

func (x *GetUsageRequest) GetMeterTypeFilter() MeterType {
	if x != nil {
		return x.MeterTypeFilter
	}
	return MeterType_METER_TYPE_UNSPECIFIED
}

func (x *GetUsageRequest) GetPagination() *v1.PaginationRequest {
	if x != nil {
		return x.Pagination
	}
	return nil
}

// GetUsageResponse returns the page of usage summaries plus a grand total.
type GetUsageResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Per-bucket rollups for this page (e.g. one summary per day in the window).
	Summaries []*UsageSummary `protobuf:"bytes,1,rep,name=summaries,proto3" json:"summaries,omitempty"`
	// The aggregate cost across the ENTIRE requested window (not just this page),
	// so a dashboard can show the period total without paging through everything.
	GrandTotal *Money `protobuf:"bytes,2,opt,name=grand_total,json=grandTotal,proto3" json:"grand_total,omitempty"`
	// Pagination metadata: next_page_token + total_count of buckets.
	Pagination    *v1.PaginationResponse `protobuf:"bytes,3,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetUsageResponse) Reset() {
	*x = GetUsageResponse{}
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[10]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetUsageResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetUsageResponse) ProtoMessage() {}

func (x *GetUsageResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[10]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetUsageResponse.ProtoReflect.Descriptor instead.
func (*GetUsageResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{10}
}

func (x *GetUsageResponse) GetSummaries() []*UsageSummary {
	if x != nil {
		return x.Summaries
	}
	return nil
}

func (x *GetUsageResponse) GetGrandTotal() *Money {
	if x != nil {
		return x.GrandTotal
	}
	return nil
}

func (x *GetUsageResponse) GetPagination() *v1.PaginationResponse {
	if x != nil {
		return x.Pagination
	}
	return nil
}

// GetInvoiceRequest fetches a single invoice by id.
type GetInvoiceRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The invoice UUID (Invoice.id). Server enforces that the caller's team owns
	// this invoice (or the caller is admin) — no cross-team invoice reads.
	InvoiceId     string `protobuf:"bytes,1,opt,name=invoice_id,json=invoiceId,proto3" json:"invoice_id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetInvoiceRequest) Reset() {
	*x = GetInvoiceRequest{}
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[11]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetInvoiceRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetInvoiceRequest) ProtoMessage() {}

func (x *GetInvoiceRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[11]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetInvoiceRequest.ProtoReflect.Descriptor instead.
func (*GetInvoiceRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{11}
}

func (x *GetInvoiceRequest) GetInvoiceId() string {
	if x != nil {
		return x.InvoiceId
	}
	return ""
}

// GetInvoiceResponse wraps the invoice.
// WHY wrap rather than return Invoice directly: Buf RPC_RESPONSE_STANDARD_NAME,
// and forward-compat (we may later add e.g. a payment_url alongside the invoice
// without touching the Invoice domain message).
type GetInvoiceResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The requested invoice with its line items and server-computed totals.
	Invoice       *Invoice `protobuf:"bytes,1,opt,name=invoice,proto3" json:"invoice,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetInvoiceResponse) Reset() {
	*x = GetInvoiceResponse{}
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[12]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetInvoiceResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetInvoiceResponse) ProtoMessage() {}

func (x *GetInvoiceResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[12]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetInvoiceResponse.ProtoReflect.Descriptor instead.
func (*GetInvoiceResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{12}
}

func (x *GetInvoiceResponse) GetInvoice() *Invoice {
	if x != nil {
		return x.Invoice
	}
	return nil
}

// ListInvoicesRequest lists a team's invoices, newest first, paginated.
type ListInvoicesRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Team whose invoices to list. Non-admins are scoped to their own team
	// server-side regardless of this value (tenant isolation).
	Team string `protobuf:"bytes,1,opt,name=team,proto3" json:"team,omitempty"`
	// Optional status filter (e.g. only OVERDUE for a dunning view).
	// UNSPECIFIED = all statuses.
	StatusFilter InvoiceStatus `protobuf:"varint,2,opt,name=status_filter,json=statusFilter,proto3,enum=forgepoint.billing.v1.InvoiceStatus" json:"status_filter,omitempty"`
	// Cursor-based pagination. page_size defaults to 20, capped at 100
	// server-side. See common.proto PaginationRequest for the WHY on cursors.
	Pagination    *v1.PaginationRequest `protobuf:"bytes,3,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListInvoicesRequest) Reset() {
	*x = ListInvoicesRequest{}
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[13]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListInvoicesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListInvoicesRequest) ProtoMessage() {}

func (x *ListInvoicesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[13]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListInvoicesRequest.ProtoReflect.Descriptor instead.
func (*ListInvoicesRequest) Descriptor() ([]byte, []int) {
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{13}
}

func (x *ListInvoicesRequest) GetTeam() string {
	if x != nil {
		return x.Team
	}
	return ""
}

func (x *ListInvoicesRequest) GetStatusFilter() InvoiceStatus {
	if x != nil {
		return x.StatusFilter
	}
	return InvoiceStatus_INVOICE_STATUS_UNSPECIFIED
}

func (x *ListInvoicesRequest) GetPagination() *v1.PaginationRequest {
	if x != nil {
		return x.Pagination
	}
	return nil
}

// ListInvoicesResponse returns the page of invoices.
type ListInvoicesResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The page of invoices (server orders by created_at DESC — newest first).
	Invoices []*Invoice `protobuf:"bytes,1,rep,name=invoices,proto3" json:"invoices,omitempty"`
	// Pagination metadata: next_page_token + total_count.
	Pagination    *v1.PaginationResponse `protobuf:"bytes,2,opt,name=pagination,proto3" json:"pagination,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ListInvoicesResponse) Reset() {
	*x = ListInvoicesResponse{}
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[14]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListInvoicesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListInvoicesResponse) ProtoMessage() {}

func (x *ListInvoicesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[14]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListInvoicesResponse.ProtoReflect.Descriptor instead.
func (*ListInvoicesResponse) Descriptor() ([]byte, []int) {
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{14}
}

func (x *ListInvoicesResponse) GetInvoices() []*Invoice {
	if x != nil {
		return x.Invoices
	}
	return nil
}

func (x *ListInvoicesResponse) GetPagination() *v1.PaginationResponse {
	if x != nil {
		return x.Pagination
	}
	return nil
}

// UsageRecorded — emitted after a UsageRecord commits (via the outbox).
// CONSUMERS: Experiment Tracker (attribute cost to a run/model), and any
// real-time usage dashboard. Carries the full priced record so consumers need
// no callback.
type UsageRecorded struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The committed, priced ledger record. Self-contained: includes team, meter,
	// quantity, and server-computed cost.
	Record        *UsageRecord `protobuf:"bytes,1,opt,name=record,proto3" json:"record,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UsageRecorded) Reset() {
	*x = UsageRecorded{}
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[15]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UsageRecorded) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UsageRecorded) ProtoMessage() {}

func (x *UsageRecorded) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[15]
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
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{15}
}

func (x *UsageRecorded) GetRecord() *UsageRecord {
	if x != nil {
		return x.Record
	}
	return nil
}

// QuotaExceeded — emitted when recorded usage pushes a team OVER its rate plan's
// quota for a meter. Written to the outbox in the SAME transaction as the usage
// update (the canonical outbox example from the implementation plan), so the
// alert is as durable as the usage that triggered it.
// CONSUMERS: Notification (alert the team), Inference Gateway (flip the team's
// quota cache to "blocked" so subsequent inferences are rejected/throttled).
type QuotaExceeded struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The team that exceeded quota.
	Team string `protobuf:"bytes,1,opt,name=team,proto3" json:"team,omitempty"`
	// The rate plan whose quota was hit.
	RatePlanId string `protobuf:"bytes,2,opt,name=rate_plan_id,json=ratePlanId,proto3" json:"rate_plan_id,omitempty"`
	// Which meter's quota was exceeded.
	MeterType MeterType `protobuf:"varint,3,opt,name=meter_type,json=meterType,proto3,enum=forgepoint.billing.v1.MeterType" json:"meter_type,omitempty"`
	// The plan's quota cap for that meter (the limit that was crossed).
	QuotaLimit int64 `protobuf:"varint,4,opt,name=quota_limit,json=quotaLimit,proto3" json:"quota_limit,omitempty"`
	// The team's current usage of that meter (>= quota_limit), so the consumer
	// can show "1,012 / 1,000" without a callback.
	CurrentUsage int64 `protobuf:"varint,5,opt,name=current_usage,json=currentUsage,proto3" json:"current_usage,omitempty"`
	// When the threshold was crossed (event time).
	OccurredAt    *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=occurred_at,json=occurredAt,proto3" json:"occurred_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *QuotaExceeded) Reset() {
	*x = QuotaExceeded{}
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[16]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *QuotaExceeded) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QuotaExceeded) ProtoMessage() {}

func (x *QuotaExceeded) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[16]
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
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{16}
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

// InvoiceGenerated — emitted when a billing period closes and an invoice
// transitions DRAFT → FINALIZED (totals locked). Published via the outbox.
// CONSUMERS: Notification (email/Slack the finalized invoice), and any external
// payment/accounting integration.
type InvoiceGenerated struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The finalized invoice, including line items and the server-computed total.
	// Self-contained so the consumer can render/send it without a callback.
	Invoice       *Invoice `protobuf:"bytes,1,opt,name=invoice,proto3" json:"invoice,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *InvoiceGenerated) Reset() {
	*x = InvoiceGenerated{}
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[17]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *InvoiceGenerated) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InvoiceGenerated) ProtoMessage() {}

func (x *InvoiceGenerated) ProtoReflect() protoreflect.Message {
	mi := &file_forgepoint_billing_v1_billing_proto_msgTypes[17]
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
	return file_forgepoint_billing_v1_billing_proto_rawDescGZIP(), []int{17}
}

func (x *InvoiceGenerated) GetInvoice() *Invoice {
	if x != nil {
		return x.Invoice
	}
	return nil
}

var File_forgepoint_billing_v1_billing_proto protoreflect.FileDescriptor

const file_forgepoint_billing_v1_billing_proto_rawDesc = "" +
	"\n" +
	"#forgepoint/billing/v1/billing.proto\x12\x15forgepoint.billing.v1\x1a\x1fgoogle/protobuf/timestamp.proto\x1a!forgepoint/common/v1/common.proto\"Q\n" +
	"\x05Money\x12#\n" +
	"\ramount_micros\x18\x01 \x01(\x03R\famountMicros\x12#\n" +
	"\rcurrency_code\x18\x02 \x01(\tR\fcurrencyCode\"\xde\x04\n" +
	"\bRatePlan\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12\x12\n" +
	"\x04name\x18\x02 \x01(\tR\x04name\x12P\n" +
	"\vunit_prices\x18\x03 \x03(\v2/.forgepoint.billing.v1.RatePlan.UnitPricesEntryR\n" +
	"unitPrices\x12h\n" +
	"\x13included_quantities\x18\x04 \x03(\v27.forgepoint.billing.v1.RatePlan.IncludedQuantitiesEntryR\x12includedQuantities\x12S\n" +
	"\fquota_limits\x18\x05 \x03(\v20.forgepoint.billing.v1.RatePlan.QuotaLimitsEntryR\vquotaLimits\x129\n" +
	"\n" +
	"created_at\x18\x06 \x01(\v2\x1a.google.protobuf.TimestampR\tcreatedAt\x1a[\n" +
	"\x0fUnitPricesEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x122\n" +
	"\x05value\x18\x02 \x01(\v2\x1c.forgepoint.billing.v1.MoneyR\x05value:\x028\x01\x1aE\n" +
	"\x17IncludedQuantitiesEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\x03R\x05value:\x028\x01\x1a>\n" +
	"\x10QuotaLimitsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\x03R\x05value:\x028\x01\"\x8b\x03\n" +
	"\vUsageRecord\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12\x12\n" +
	"\x04team\x18\x02 \x01(\tR\x04team\x12 \n" +
	"\frate_plan_id\x18\x03 \x01(\tR\n" +
	"ratePlanId\x12?\n" +
	"\n" +
	"meter_type\x18\x04 \x01(\x0e2 .forgepoint.billing.v1.MeterTypeR\tmeterType\x12\x1a\n" +
	"\bquantity\x18\x05 \x01(\x03R\bquantity\x120\n" +
	"\x04cost\x18\x06 \x01(\v2\x1c.forgepoint.billing.v1.MoneyR\x04cost\x12\x19\n" +
	"\bmodel_id\x18\a \x01(\tR\amodelId\x12#\n" +
	"\rmodel_version\x18\b \x01(\tR\fmodelVersion\x12*\n" +
	"\x11source_request_id\x18\t \x01(\tR\x0fsourceRequestId\x12;\n" +
	"\voccurred_at\x18\n" +
	" \x01(\v2\x1a.google.protobuf.TimestampR\n" +
	"occurredAt\"\x85\x03\n" +
	"\fUsageSummary\x12\x12\n" +
	"\x04team\x18\x01 \x01(\tR\x04team\x12=\n" +
	"\fperiod_start\x18\x02 \x01(\v2\x1a.google.protobuf.TimestampR\vperiodStart\x129\n" +
	"\n" +
	"period_end\x18\x03 \x01(\v2\x1a.google.protobuf.TimestampR\tperiodEnd\x12K\n" +
	"\bby_meter\x18\x04 \x03(\v20.forgepoint.billing.v1.UsageSummary.ByMeterEntryR\abyMeter\x12;\n" +
	"\n" +
	"total_cost\x18\x05 \x01(\v2\x1c.forgepoint.billing.v1.MoneyR\ttotalCost\x1a]\n" +
	"\fByMeterEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x127\n" +
	"\x05value\x18\x02 \x01(\v2!.forgepoint.billing.v1.MeterUsageR\x05value:\x028\x01\"\xb1\x01\n" +
	"\n" +
	"MeterUsage\x12?\n" +
	"\n" +
	"meter_type\x18\x01 \x01(\x0e2 .forgepoint.billing.v1.MeterTypeR\tmeterType\x12%\n" +
	"\x0etotal_quantity\x18\x02 \x01(\x03R\rtotalQuantity\x12;\n" +
	"\n" +
	"total_cost\x18\x03 \x01(\v2\x1c.forgepoint.billing.v1.MoneyR\ttotalCost\"\xd6\x04\n" +
	"\aInvoice\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12%\n" +
	"\x0einvoice_number\x18\x02 \x01(\tR\rinvoiceNumber\x12\x12\n" +
	"\x04team\x18\x03 \x01(\tR\x04team\x12 \n" +
	"\frate_plan_id\x18\x04 \x01(\tR\n" +
	"ratePlanId\x12<\n" +
	"\x06status\x18\x05 \x01(\x0e2$.forgepoint.billing.v1.InvoiceStatusR\x06status\x12=\n" +
	"\fperiod_start\x18\x06 \x01(\v2\x1a.google.protobuf.TimestampR\vperiodStart\x129\n" +
	"\n" +
	"period_end\x18\a \x01(\v2\x1a.google.protobuf.TimestampR\tperiodEnd\x12E\n" +
	"\n" +
	"line_items\x18\b \x03(\v2&.forgepoint.billing.v1.InvoiceLineItemR\tlineItems\x122\n" +
	"\x05total\x18\t \x01(\v2\x1c.forgepoint.billing.v1.MoneyR\x05total\x129\n" +
	"\n" +
	"created_at\x18\n" +
	" \x01(\v2\x1a.google.protobuf.TimestampR\tcreatedAt\x12=\n" +
	"\ffinalized_at\x18\v \x01(\v2\x1a.google.protobuf.TimestampR\vfinalizedAt\x121\n" +
	"\x06due_at\x18\f \x01(\v2\x1a.google.protobuf.TimestampR\x05dueAt\"\x83\x02\n" +
	"\x0fInvoiceLineItem\x12?\n" +
	"\n" +
	"meter_type\x18\x01 \x01(\x0e2 .forgepoint.billing.v1.MeterTypeR\tmeterType\x12 \n" +
	"\vdescription\x18\x02 \x01(\tR\vdescription\x12\x1a\n" +
	"\bquantity\x18\x03 \x01(\x03R\bquantity\x12;\n" +
	"\n" +
	"unit_price\x18\x04 \x01(\v2\x1c.forgepoint.billing.v1.MoneyR\tunitPrice\x124\n" +
	"\x06amount\x18\x05 \x01(\v2\x1c.forgepoint.billing.v1.MoneyR\x06amount\"\xc3\x02\n" +
	"\x12RecordUsageRequest\x12?\n" +
	"\n" +
	"meter_type\x18\x01 \x01(\x0e2 .forgepoint.billing.v1.MeterTypeR\tmeterType\x12\x1a\n" +
	"\bquantity\x18\x02 \x01(\x03R\bquantity\x12\x19\n" +
	"\bmodel_id\x18\x03 \x01(\tR\amodelId\x12#\n" +
	"\rmodel_version\x18\x04 \x01(\tR\fmodelVersion\x12*\n" +
	"\x11source_request_id\x18\x05 \x01(\tR\x0fsourceRequestId\x12;\n" +
	"\voccurred_at\x18\x06 \x01(\v2\x1a.google.protobuf.TimestampR\n" +
	"occurredAt\x12'\n" +
	"\x0fidempotency_key\x18\a \x01(\tR\x0eidempotencyKey\"u\n" +
	"\x13RecordUsageResponse\x12:\n" +
	"\x06record\x18\x01 \x01(\v2\".forgepoint.billing.v1.UsageRecordR\x06record\x12\"\n" +
	"\fdeduplicated\x18\x02 \x01(\bR\fdeduplicated\"\xb6\x02\n" +
	"\x0fGetUsageRequest\x12\x12\n" +
	"\x04team\x18\x01 \x01(\tR\x04team\x12=\n" +
	"\fperiod_start\x18\x02 \x01(\v2\x1a.google.protobuf.TimestampR\vperiodStart\x129\n" +
	"\n" +
	"period_end\x18\x03 \x01(\v2\x1a.google.protobuf.TimestampR\tperiodEnd\x12L\n" +
	"\x11meter_type_filter\x18\x04 \x01(\x0e2 .forgepoint.billing.v1.MeterTypeR\x0fmeterTypeFilter\x12G\n" +
	"\n" +
	"pagination\x18\x05 \x01(\v2'.forgepoint.common.v1.PaginationRequestR\n" +
	"pagination\"\xde\x01\n" +
	"\x10GetUsageResponse\x12A\n" +
	"\tsummaries\x18\x01 \x03(\v2#.forgepoint.billing.v1.UsageSummaryR\tsummaries\x12=\n" +
	"\vgrand_total\x18\x02 \x01(\v2\x1c.forgepoint.billing.v1.MoneyR\n" +
	"grandTotal\x12H\n" +
	"\n" +
	"pagination\x18\x03 \x01(\v2(.forgepoint.common.v1.PaginationResponseR\n" +
	"pagination\"2\n" +
	"\x11GetInvoiceRequest\x12\x1d\n" +
	"\n" +
	"invoice_id\x18\x01 \x01(\tR\tinvoiceId\"N\n" +
	"\x12GetInvoiceResponse\x128\n" +
	"\ainvoice\x18\x01 \x01(\v2\x1e.forgepoint.billing.v1.InvoiceR\ainvoice\"\xbd\x01\n" +
	"\x13ListInvoicesRequest\x12\x12\n" +
	"\x04team\x18\x01 \x01(\tR\x04team\x12I\n" +
	"\rstatus_filter\x18\x02 \x01(\x0e2$.forgepoint.billing.v1.InvoiceStatusR\fstatusFilter\x12G\n" +
	"\n" +
	"pagination\x18\x03 \x01(\v2'.forgepoint.common.v1.PaginationRequestR\n" +
	"pagination\"\x9c\x01\n" +
	"\x14ListInvoicesResponse\x12:\n" +
	"\binvoices\x18\x01 \x03(\v2\x1e.forgepoint.billing.v1.InvoiceR\binvoices\x12H\n" +
	"\n" +
	"pagination\x18\x02 \x01(\v2(.forgepoint.common.v1.PaginationResponseR\n" +
	"pagination\"K\n" +
	"\rUsageRecorded\x12:\n" +
	"\x06record\x18\x01 \x01(\v2\".forgepoint.billing.v1.UsageRecordR\x06record\"\x89\x02\n" +
	"\rQuotaExceeded\x12\x12\n" +
	"\x04team\x18\x01 \x01(\tR\x04team\x12 \n" +
	"\frate_plan_id\x18\x02 \x01(\tR\n" +
	"ratePlanId\x12?\n" +
	"\n" +
	"meter_type\x18\x03 \x01(\x0e2 .forgepoint.billing.v1.MeterTypeR\tmeterType\x12\x1f\n" +
	"\vquota_limit\x18\x04 \x01(\x03R\n" +
	"quotaLimit\x12#\n" +
	"\rcurrent_usage\x18\x05 \x01(\x03R\fcurrentUsage\x12;\n" +
	"\voccurred_at\x18\x06 \x01(\v2\x1a.google.protobuf.TimestampR\n" +
	"occurredAt\"L\n" +
	"\x10InvoiceGenerated\x128\n" +
	"\ainvoice\x18\x01 \x01(\v2\x1e.forgepoint.billing.v1.InvoiceR\ainvoice*\xa8\x01\n" +
	"\tMeterType\x12\x1a\n" +
	"\x16METER_TYPE_UNSPECIFIED\x10\x00\x12 \n" +
	"\x1cMETER_TYPE_INFERENCE_REQUEST\x10\x01\x12\x1f\n" +
	"\x1bMETER_TYPE_INFERENCE_TOKENS\x10\x02\x12\x1e\n" +
	"\x1aMETER_TYPE_COMPUTE_SECONDS\x10\x03\x12\x1c\n" +
	"\x18METER_TYPE_STORAGE_BYTES\x10\x04*\xb5\x01\n" +
	"\rInvoiceStatus\x12\x1e\n" +
	"\x1aINVOICE_STATUS_UNSPECIFIED\x10\x00\x12\x18\n" +
	"\x14INVOICE_STATUS_DRAFT\x10\x01\x12\x1c\n" +
	"\x18INVOICE_STATUS_FINALIZED\x10\x02\x12\x17\n" +
	"\x13INVOICE_STATUS_PAID\x10\x03\x12\x1a\n" +
	"\x16INVOICE_STATUS_OVERDUE\x10\x04\x12\x17\n" +
	"\x13INVOICE_STATUS_VOID\x10\x052\x9f\x03\n" +
	"\x0eBillingService\x12d\n" +
	"\vRecordUsage\x12).forgepoint.billing.v1.RecordUsageRequest\x1a*.forgepoint.billing.v1.RecordUsageResponse\x12[\n" +
	"\bGetUsage\x12&.forgepoint.billing.v1.GetUsageRequest\x1a'.forgepoint.billing.v1.GetUsageResponse\x12a\n" +
	"\n" +
	"GetInvoice\x12(.forgepoint.billing.v1.GetInvoiceRequest\x1a).forgepoint.billing.v1.GetInvoiceResponse\x12g\n" +
	"\fListInvoices\x12*.forgepoint.billing.v1.ListInvoicesRequest\x1a+.forgepoint.billing.v1.ListInvoicesResponseBJZHgithub.com/abd-ulbasit/forgepoint/gen/go/forgepoint/billing/v1;billingv1b\x06proto3"

var (
	file_forgepoint_billing_v1_billing_proto_rawDescOnce sync.Once
	file_forgepoint_billing_v1_billing_proto_rawDescData []byte
)

func file_forgepoint_billing_v1_billing_proto_rawDescGZIP() []byte {
	file_forgepoint_billing_v1_billing_proto_rawDescOnce.Do(func() {
		file_forgepoint_billing_v1_billing_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_forgepoint_billing_v1_billing_proto_rawDesc), len(file_forgepoint_billing_v1_billing_proto_rawDesc)))
	})
	return file_forgepoint_billing_v1_billing_proto_rawDescData
}

var file_forgepoint_billing_v1_billing_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_forgepoint_billing_v1_billing_proto_msgTypes = make([]protoimpl.MessageInfo, 22)
var file_forgepoint_billing_v1_billing_proto_goTypes = []any{
	(MeterType)(0),                // 0: forgepoint.billing.v1.MeterType
	(InvoiceStatus)(0),            // 1: forgepoint.billing.v1.InvoiceStatus
	(*Money)(nil),                 // 2: forgepoint.billing.v1.Money
	(*RatePlan)(nil),              // 3: forgepoint.billing.v1.RatePlan
	(*UsageRecord)(nil),           // 4: forgepoint.billing.v1.UsageRecord
	(*UsageSummary)(nil),          // 5: forgepoint.billing.v1.UsageSummary
	(*MeterUsage)(nil),            // 6: forgepoint.billing.v1.MeterUsage
	(*Invoice)(nil),               // 7: forgepoint.billing.v1.Invoice
	(*InvoiceLineItem)(nil),       // 8: forgepoint.billing.v1.InvoiceLineItem
	(*RecordUsageRequest)(nil),    // 9: forgepoint.billing.v1.RecordUsageRequest
	(*RecordUsageResponse)(nil),   // 10: forgepoint.billing.v1.RecordUsageResponse
	(*GetUsageRequest)(nil),       // 11: forgepoint.billing.v1.GetUsageRequest
	(*GetUsageResponse)(nil),      // 12: forgepoint.billing.v1.GetUsageResponse
	(*GetInvoiceRequest)(nil),     // 13: forgepoint.billing.v1.GetInvoiceRequest
	(*GetInvoiceResponse)(nil),    // 14: forgepoint.billing.v1.GetInvoiceResponse
	(*ListInvoicesRequest)(nil),   // 15: forgepoint.billing.v1.ListInvoicesRequest
	(*ListInvoicesResponse)(nil),  // 16: forgepoint.billing.v1.ListInvoicesResponse
	(*UsageRecorded)(nil),         // 17: forgepoint.billing.v1.UsageRecorded
	(*QuotaExceeded)(nil),         // 18: forgepoint.billing.v1.QuotaExceeded
	(*InvoiceGenerated)(nil),      // 19: forgepoint.billing.v1.InvoiceGenerated
	nil,                           // 20: forgepoint.billing.v1.RatePlan.UnitPricesEntry
	nil,                           // 21: forgepoint.billing.v1.RatePlan.IncludedQuantitiesEntry
	nil,                           // 22: forgepoint.billing.v1.RatePlan.QuotaLimitsEntry
	nil,                           // 23: forgepoint.billing.v1.UsageSummary.ByMeterEntry
	(*timestamppb.Timestamp)(nil), // 24: google.protobuf.Timestamp
	(*v1.PaginationRequest)(nil),  // 25: forgepoint.common.v1.PaginationRequest
	(*v1.PaginationResponse)(nil), // 26: forgepoint.common.v1.PaginationResponse
}
var file_forgepoint_billing_v1_billing_proto_depIdxs = []int32{
	20, // 0: forgepoint.billing.v1.RatePlan.unit_prices:type_name -> forgepoint.billing.v1.RatePlan.UnitPricesEntry
	21, // 1: forgepoint.billing.v1.RatePlan.included_quantities:type_name -> forgepoint.billing.v1.RatePlan.IncludedQuantitiesEntry
	22, // 2: forgepoint.billing.v1.RatePlan.quota_limits:type_name -> forgepoint.billing.v1.RatePlan.QuotaLimitsEntry
	24, // 3: forgepoint.billing.v1.RatePlan.created_at:type_name -> google.protobuf.Timestamp
	0,  // 4: forgepoint.billing.v1.UsageRecord.meter_type:type_name -> forgepoint.billing.v1.MeterType
	2,  // 5: forgepoint.billing.v1.UsageRecord.cost:type_name -> forgepoint.billing.v1.Money
	24, // 6: forgepoint.billing.v1.UsageRecord.occurred_at:type_name -> google.protobuf.Timestamp
	24, // 7: forgepoint.billing.v1.UsageSummary.period_start:type_name -> google.protobuf.Timestamp
	24, // 8: forgepoint.billing.v1.UsageSummary.period_end:type_name -> google.protobuf.Timestamp
	23, // 9: forgepoint.billing.v1.UsageSummary.by_meter:type_name -> forgepoint.billing.v1.UsageSummary.ByMeterEntry
	2,  // 10: forgepoint.billing.v1.UsageSummary.total_cost:type_name -> forgepoint.billing.v1.Money
	0,  // 11: forgepoint.billing.v1.MeterUsage.meter_type:type_name -> forgepoint.billing.v1.MeterType
	2,  // 12: forgepoint.billing.v1.MeterUsage.total_cost:type_name -> forgepoint.billing.v1.Money
	1,  // 13: forgepoint.billing.v1.Invoice.status:type_name -> forgepoint.billing.v1.InvoiceStatus
	24, // 14: forgepoint.billing.v1.Invoice.period_start:type_name -> google.protobuf.Timestamp
	24, // 15: forgepoint.billing.v1.Invoice.period_end:type_name -> google.protobuf.Timestamp
	8,  // 16: forgepoint.billing.v1.Invoice.line_items:type_name -> forgepoint.billing.v1.InvoiceLineItem
	2,  // 17: forgepoint.billing.v1.Invoice.total:type_name -> forgepoint.billing.v1.Money
	24, // 18: forgepoint.billing.v1.Invoice.created_at:type_name -> google.protobuf.Timestamp
	24, // 19: forgepoint.billing.v1.Invoice.finalized_at:type_name -> google.protobuf.Timestamp
	24, // 20: forgepoint.billing.v1.Invoice.due_at:type_name -> google.protobuf.Timestamp
	0,  // 21: forgepoint.billing.v1.InvoiceLineItem.meter_type:type_name -> forgepoint.billing.v1.MeterType
	2,  // 22: forgepoint.billing.v1.InvoiceLineItem.unit_price:type_name -> forgepoint.billing.v1.Money
	2,  // 23: forgepoint.billing.v1.InvoiceLineItem.amount:type_name -> forgepoint.billing.v1.Money
	0,  // 24: forgepoint.billing.v1.RecordUsageRequest.meter_type:type_name -> forgepoint.billing.v1.MeterType
	24, // 25: forgepoint.billing.v1.RecordUsageRequest.occurred_at:type_name -> google.protobuf.Timestamp
	4,  // 26: forgepoint.billing.v1.RecordUsageResponse.record:type_name -> forgepoint.billing.v1.UsageRecord
	24, // 27: forgepoint.billing.v1.GetUsageRequest.period_start:type_name -> google.protobuf.Timestamp
	24, // 28: forgepoint.billing.v1.GetUsageRequest.period_end:type_name -> google.protobuf.Timestamp
	0,  // 29: forgepoint.billing.v1.GetUsageRequest.meter_type_filter:type_name -> forgepoint.billing.v1.MeterType
	25, // 30: forgepoint.billing.v1.GetUsageRequest.pagination:type_name -> forgepoint.common.v1.PaginationRequest
	5,  // 31: forgepoint.billing.v1.GetUsageResponse.summaries:type_name -> forgepoint.billing.v1.UsageSummary
	2,  // 32: forgepoint.billing.v1.GetUsageResponse.grand_total:type_name -> forgepoint.billing.v1.Money
	26, // 33: forgepoint.billing.v1.GetUsageResponse.pagination:type_name -> forgepoint.common.v1.PaginationResponse
	7,  // 34: forgepoint.billing.v1.GetInvoiceResponse.invoice:type_name -> forgepoint.billing.v1.Invoice
	1,  // 35: forgepoint.billing.v1.ListInvoicesRequest.status_filter:type_name -> forgepoint.billing.v1.InvoiceStatus
	25, // 36: forgepoint.billing.v1.ListInvoicesRequest.pagination:type_name -> forgepoint.common.v1.PaginationRequest
	7,  // 37: forgepoint.billing.v1.ListInvoicesResponse.invoices:type_name -> forgepoint.billing.v1.Invoice
	26, // 38: forgepoint.billing.v1.ListInvoicesResponse.pagination:type_name -> forgepoint.common.v1.PaginationResponse
	4,  // 39: forgepoint.billing.v1.UsageRecorded.record:type_name -> forgepoint.billing.v1.UsageRecord
	0,  // 40: forgepoint.billing.v1.QuotaExceeded.meter_type:type_name -> forgepoint.billing.v1.MeterType
	24, // 41: forgepoint.billing.v1.QuotaExceeded.occurred_at:type_name -> google.protobuf.Timestamp
	7,  // 42: forgepoint.billing.v1.InvoiceGenerated.invoice:type_name -> forgepoint.billing.v1.Invoice
	2,  // 43: forgepoint.billing.v1.RatePlan.UnitPricesEntry.value:type_name -> forgepoint.billing.v1.Money
	6,  // 44: forgepoint.billing.v1.UsageSummary.ByMeterEntry.value:type_name -> forgepoint.billing.v1.MeterUsage
	9,  // 45: forgepoint.billing.v1.BillingService.RecordUsage:input_type -> forgepoint.billing.v1.RecordUsageRequest
	11, // 46: forgepoint.billing.v1.BillingService.GetUsage:input_type -> forgepoint.billing.v1.GetUsageRequest
	13, // 47: forgepoint.billing.v1.BillingService.GetInvoice:input_type -> forgepoint.billing.v1.GetInvoiceRequest
	15, // 48: forgepoint.billing.v1.BillingService.ListInvoices:input_type -> forgepoint.billing.v1.ListInvoicesRequest
	10, // 49: forgepoint.billing.v1.BillingService.RecordUsage:output_type -> forgepoint.billing.v1.RecordUsageResponse
	12, // 50: forgepoint.billing.v1.BillingService.GetUsage:output_type -> forgepoint.billing.v1.GetUsageResponse
	14, // 51: forgepoint.billing.v1.BillingService.GetInvoice:output_type -> forgepoint.billing.v1.GetInvoiceResponse
	16, // 52: forgepoint.billing.v1.BillingService.ListInvoices:output_type -> forgepoint.billing.v1.ListInvoicesResponse
	49, // [49:53] is the sub-list for method output_type
	45, // [45:49] is the sub-list for method input_type
	45, // [45:45] is the sub-list for extension type_name
	45, // [45:45] is the sub-list for extension extendee
	0,  // [0:45] is the sub-list for field type_name
}

func init() { file_forgepoint_billing_v1_billing_proto_init() }
func file_forgepoint_billing_v1_billing_proto_init() {
	if File_forgepoint_billing_v1_billing_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_forgepoint_billing_v1_billing_proto_rawDesc), len(file_forgepoint_billing_v1_billing_proto_rawDesc)),
			NumEnums:      2,
			NumMessages:   22,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_forgepoint_billing_v1_billing_proto_goTypes,
		DependencyIndexes: file_forgepoint_billing_v1_billing_proto_depIdxs,
		EnumInfos:         file_forgepoint_billing_v1_billing_proto_enumTypes,
		MessageInfos:      file_forgepoint_billing_v1_billing_proto_msgTypes,
	}.Build()
	File_forgepoint_billing_v1_billing_proto = out.File
	file_forgepoint_billing_v1_billing_proto_goTypes = nil
	file_forgepoint_billing_v1_billing_proto_depIdxs = nil
}
