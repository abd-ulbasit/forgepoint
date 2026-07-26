// Package domain is the innermost ring of the billing service's Clean
// Architecture onion. It holds the canonical business types (Money, RatePlan,
// UsageRecord, Invoice, OutboxEvent), the overflow-safe money arithmetic, and
// the metering/aggregation rules — with ZERO imports of gRPC, NATS, SQL, or
// generated proto. It depends only on the standard library and
// github.com/google/uuid.
//
// ============================================================================
// WHY A PURE DOMAIN (and why a BILLING domain especially must be pure)
// ============================================================================
//
// Clean Architecture says business rules must not depend on delivery mechanisms
// or infrastructure. For a billing service the payoff is concrete: the money
// math (the part that, if wrong, charges a real customer a wrong amount) is unit
// testable WITHOUT a database, a gRPC server, or a NATS broker. A test can prove
// "quantity * price overflows → we reject, never wrap" in microseconds against
// pure functions. That is exactly the property a money system needs, and Clean
// Architecture is what makes it cheap to demonstrate.
//
// The handler converts proto (billing.v1.*) ↔ these domain types; the
// repository converts these ↔ Postgres rows; the events layer converts these ↔
// the canonical forgepoint.events.v1 payloads. The domain itself never knows
// any of those exist.
//
// ============================================================================
// THE OUTBOX PATTERN — where it lives in the domain (the centerpiece)
// ============================================================================
//
// The dual-write problem: RecordUsage must (1) persist the priced UsageRecord
// AND (2) publish an events.UsageRecorded so downstream (Notification, the
// gateway's quota cache, Experiment Tracker) learns. There is no distributed
// transaction across Postgres and NATS, so we cannot make (1) and (2) atomic.
//
// THE FIX, modeled here: the domain produces the business write AND its
// OutboxEvent(s) as ONE unit of work and hands BOTH to a single port method
// (UsageStore.RecordUsageTx — see ports.go), whose CONTRACT is to commit them in
// one Postgres transaction. A separate relay goroutine then publishes unpublished
// outbox rows to NATS and stamps them published. If the commit succeeds, the
// event WILL eventually publish (at-least-once); consumers dedupe on the envelope
// id, so at-least-once is safe.
//
// The domain's job is to BUILD the correct OutboxEvent alongside each business
// write and hand the pair to the store port atomically. It does NOT talk to
// NATS, does NOT poll, does NOT marshal proto — those are adapter concerns.
// Keeping the outbox RECORD-BUILDING in the domain is what makes the pattern
// testable here (we assert "a usage write yields exactly one matching outbox
// event") without any infrastructure.
//
// WHERE THE OUTBOX PATTERN'S TRANSACTIONAL BOUNDARY LIVES: in the PORT METHOD.
// RecordUsageTx(record, []OutboxEvent) is one call = one atomic unit; the adapter wraps it in a tx. The domain owns the
// CONTENT of the event and the invariant that it always accompanies the write;
// the adapter owns the ATOMICITY mechanism (the tx) and the relay owns delivery.
// ============================================================================
package domain

import (
	"math/bits"
	"strings"
	"time"
)

// ============================================================================
// MONEY — exact integer money in micro-units, with overflow-safe arithmetic.
// ============================================================================
//
// WHY int64 micro-units, NOT float64:
//
//	IEEE-754 cannot represent most decimal money exactly (0.10 + 0.20 != 0.30).
//	Summed over millions of metered calls those errors become real
//	reconciliation failures. Integer minor-unit math is EXACT. We use MICRO
//	units (1e-6 of the major unit) rather than cents because per-token prices
//	are sub-cent (a token might cost 0.0000004 USD); cents would round it to
//	zero. AmountMicros == 1_000_000 means 1.00 of the currency. This mirrors
//	Stripe's Money and google.type.Money.
//
// WHY currency travels WITH every amount:
//
//	Self-describing money kills the "dollars or cents? USD or EUR?" ambiguity at
//	every boundary, and lets Add/arithmetic REFUSE to combine mismatched
//	currencies (ErrCurrencyMismatch) instead of producing a nonsense sum.
type Money struct {
	// AmountMicros is the amount in micro-units (1e-6) of CurrencyCode's major
	// unit. Exact integer. May be negative ONLY for server-issued credits.
	AmountMicros int64
	// CurrencyCode is the uppercase ISO-4217 code ("USD","EUR"). Empty is invalid.
	CurrencyCode string
}

// MicrosPerUnit is the number of micro-units in one major currency unit.
// 1_000_000 micro-USD == 1.00 USD. Named (not a magic literal) so the rounding
// math at invoice finalization reads as intent.
const MicrosPerUnit = 1_000_000

// MicrosPerCent is the number of micro-units in one minor unit (cent) of a
// 2-decimal currency. Used to ROUND a micro-precise running total down/half-up
// to the currency's smallest real denomination at invoice finalization — you
// can sum sub-cent meters internally, but you bill in cents.
const MicrosPerCent = MicrosPerUnit / 100 // 10_000

// MaxQuantityPerRecord bounds a single RecordUsage quantity. THIS IS THE
// LINCHPIN OF THE OVERFLOW PROOF.
//
//	The cost multiply is  cost_micros = quantity * unit_price_micros.
//	int64 max ≈ 9.22e18. If we cap quantity at 1e12 and we (separately) cap a
//	sane unit price at, say, 1e6 micro-units (1.00 of the currency per unit —
//	already absurd for a per-token price), the worst-case product is
//	1e12 * 1e6 = 1e18 < 9.22e18 — provably no overflow. Even at a 1e9 price the
//	product (1e21) WOULD overflow, which is exactly why we ALSO use checked
//	arithmetic (mulInt64Checked) rather than trusting the bound alone: the bound
//	makes the common case provably safe; the checked multiply is the belt-and-
//	suspenders that turns any residual overflow into an explicit error, never a
//	silent wrap.
//
// WHY 1e12 specifically: it comfortably exceeds any real per-record meter
// (a single inference is < ~1e6 tokens; storage bytes per record < ~1e15 only
// for absurdly large artifacts, which the storage feeder chunks). 1e12 leaves
// >6 orders of magnitude of headroom under int64 for the price multiplier.
const MaxQuantityPerRecord int64 = 1_000_000_000_000 // 1e12

// IsZero reports whether the amount is exactly zero. Currency-agnostic: a zero
// amount is zero in any currency (used to skip emitting $0 line items/invoices).
func (m Money) IsZero() bool { return m.AmountMicros == 0 }

// Add returns m + other, REFUSING to combine different currencies and REFUSING
// to overflow. This is the safe primitive every invoice/summary rollup uses.
//
// WHY return (Money, error) and not just Money:
//
//	A money sum has two failure modes that must NOT be swept under the rug:
//	(1) mismatched currencies (meaningless result) and (2) int64 overflow
//	(a wrapped, sign-flipped total — the worst money bug there is). Both must
//	be surfaceable so the caller aborts the operation rather than emit a wrong
//	number. A panic would be wrong (a hostile-but-valid input shouldn't crash
//	the service); a silent wrap would be catastrophic. So we return an error.
//
// EMPTY-CURRENCY ZERO is the additive identity: adding a zero-value Money{} (no
// currency) is allowed and yields other unchanged, so callers can start a sum
// from a zero accumulator without first knowing the currency. Two NON-empty
// currencies that differ → ErrCurrencyMismatch.
func (m Money) Add(other Money) (Money, error) {
	// Treat a zero-value accumulator (no currency yet) as the identity element so
	// a fold like `total := Money{}; for ... { total = total.Add(item) }` works
	// without the caller seeding the currency. The FIRST real addend defines the
	// currency; subsequent ones must match.
	if m.CurrencyCode == "" {
		return other, nil
	}
	if other.CurrencyCode == "" {
		// Adding a pure-zero (identity) keeps m, including when other is the zero
		// value Money{}. If other has a non-zero amount but no currency, that's a
		// malformed amount — but we still treat "" as identity-only when amount is
		// zero; a non-zero amount with empty currency is a construction bug the
		// validation layer prevents upstream.
		if other.AmountMicros == 0 {
			return m, nil
		}
	}
	if m.CurrencyCode != other.CurrencyCode {
		return Money{}, ErrCurrencyMismatch
	}
	sum, err := addInt64Checked(m.AmountMicros, other.AmountMicros)
	if err != nil {
		return Money{}, err
	}
	return Money{AmountMicros: sum, CurrencyCode: m.CurrencyCode}, nil
}

// RoundToCents rounds a micro-precise amount to the currency's minor unit (cent)
// using round-half-up on the magnitude (symmetric for negatives). WHY:
//
//	We meter and sum in micro-units so sub-cent prices accumulate exactly, but a
//	customer is BILLED in cents — an invoice total must be a whole number of
//	cents. We round ONCE, at finalization, not per line, to avoid compounding
//	rounding error across thousands of lines (round-then-sum != sum-then-round;
//	sum-then-round is correct and is what we do).
//
// Round-half-up (away from zero on .5) is the conventional consumer-billing
// rounding (a half-cent rounds up to the customer's nearest cent). We keep the
// result in micro-units (a whole number of cents expressed as micros) so the
// type stays Money and downstream math is uniform.
func (m Money) RoundToCents() Money {
	micros := m.AmountMicros
	half := int64(MicrosPerCent / 2) // 5_000 micros == half a cent
	var rounded int64
	if micros >= 0 {
		// Add half a cent then truncate toward zero → round-half-up.
		rounded = ((micros + half) / MicrosPerCent) * MicrosPerCent
	} else {
		// Mirror for negatives so -0.005 rounds to -0.01 (away from zero).
		rounded = ((micros - half) / MicrosPerCent) * MicrosPerCent
	}
	return Money{AmountMicros: rounded, CurrencyCode: m.CurrencyCode}
}

// ============================================================================
// CHECKED INTEGER ARITHMETIC — the overflow guards, isolated and tested.
// ============================================================================
//
// These two functions are the entire "never wrap" guarantee, factored out so
// they are independently unit-testable and reused by every money computation.
// They use math/bits, the stdlib primitive that performs the operation AND
// reports carry/overflow in one shot — faster and clearer than re-deriving the
// overflow condition by hand.

// mulInt64Checked returns a*b, or ErrAmountOverflow if the true product does not
// fit in int64. WHY this is non-trivial for SIGNED ints:
//
//	bits.Mul64 multiplies UNSIGNED 64-bit values and returns hi:lo. For signed
//	operands we compute on absolute values and reconstruct the sign. A product
//	fits in int64 iff its magnitude is <= math.MaxInt64 (for a positive result)
//	or <= math.MaxInt64+1 == 2^63 (for the single negative edge case
//	MinInt64 = -2^63). We handle the zero and sign cases explicitly, then use
//	bits.Mul64 on the magnitudes: hi must be 0 and lo must be within the signed
//	bound. This rejects, e.g., 1e12 * 1e9 (= 1e21, overflow) instead of wrapping
//	it to a small negative — the exact bug that turns a huge bill into a credit.
func mulInt64Checked(a, b int64) (int64, error) {
	if a == 0 || b == 0 {
		return 0, nil
	}
	// Determine the sign of the result; work with unsigned magnitudes.
	negative := (a < 0) != (b < 0)
	ua := abs64(a)
	ub := abs64(b)

	hi, lo := bits.Mul64(ua, ub)
	if hi != 0 {
		// Magnitude needs more than 64 bits → cannot possibly fit in int64.
		return 0, ErrAmountOverflow
	}
	// lo is the full magnitude (hi==0). The largest magnitude representable in a
	// signed int64 is 2^63 - 1 for positives, and 2^63 for the single value
	// MinInt64. Use those bounds to accept the edge and reject the rest.
	const maxPosMagnitude = uint64(1)<<63 - 1 // 2^63 - 1  (math.MaxInt64)
	const maxNegMagnitude = uint64(1) << 63   // 2^63      (|math.MinInt64|)
	if negative {
		if lo > maxNegMagnitude {
			return 0, ErrAmountOverflow
		}
		// -lo without overflowing the negation: handle |MinInt64| specially.
		if lo == maxNegMagnitude {
			return -1 << 63, nil // math.MinInt64
		}
		return -int64(lo), nil
	}
	if lo > maxPosMagnitude {
		return 0, ErrAmountOverflow
	}
	return int64(lo), nil
}

// addInt64Checked returns a+b, or ErrAmountOverflow on signed overflow.
// bits.Add64 gives the unsigned sum + carry; we map the SIGNED overflow rule
// (overflow iff both addends share a sign and the result's sign differs) onto
// it. Used by Money.Add and the invoice/summary rollups where summing many line
// items could, in pathological data, exceed int64 even though each line is in
// range — ErrAmountOverflow rather than a wrapped negative total.
func addInt64Checked(a, b int64) (int64, error) {
	sum := a + b
	// Signed overflow detection: if a and b have the same sign but sum has the
	// opposite sign, the add wrapped. (Two values of opposite signs can never
	// overflow.) This is the canonical branch-free-ish overflow test.
	if (a > 0 && b > 0 && sum < 0) || (a < 0 && b < 0 && sum >= 0) {
		return 0, ErrAmountOverflow
	}
	return sum, nil
}

// abs64 returns the unsigned magnitude of a signed int64, correct even for
// math.MinInt64 (whose positive magnitude 2^63 does not fit in int64). WHY a
// helper: -MinInt64 overflows int64, so we compute the magnitude in uint64.
func abs64(v int64) uint64 {
	if v < 0 {
		// Negate in uint64 space: ^uint64(v) + 1 == two's-complement magnitude,
		// which is correct for MinInt64 (yields 2^63) where -v would overflow.
		return ^uint64(v) + 1
	}
	return uint64(v)
}

// ============================================================================
// METER TYPE — the closed set of billable resources (mirrors the proto enum).
// ============================================================================
//
// WHY a domain enum distinct from billingv1.MeterType:
//
//	The domain must not import generated proto. We mirror the closed set as a
//	Go string-backed type. The handler maps billingv1.MeterType <-> this, and
//	the events layer maps this <-> eventsv1.MeterType. Strings (not ints) make
//	logs and the rate-plan price map keys human-readable and stable under
//	re-numbering of the proto enum.
type MeterType string

const (
	// MeterTypeUnspecified is the zero value — an unset/unknown meter. RecordUsage
	// REJECTS it (ErrUnknownMeter): we never guess a price for an unknown meter,
	// because guessing $0 is a silent revenue leak.
	MeterTypeUnspecified MeterType = ""
	// MeterTypeInferenceRequest is the per-call fee (one unit per inference).
	MeterTypeInferenceRequest MeterType = "INFERENCE_REQUEST"
	// MeterTypeInferenceTokens is tokens processed (prompt+completion) — the
	// dominant LLM cost axis, priced per token.
	MeterTypeInferenceTokens MeterType = "INFERENCE_TOKENS"
	// MeterTypeComputeSeconds is wall-clock/GPU seconds for non-token models.
	MeterTypeComputeSeconds MeterType = "COMPUTE_SECONDS"
	// MeterTypeStorageBytes is artifact bytes stored, metered periodically.
	MeterTypeStorageBytes MeterType = "STORAGE_BYTES"
)

// IsBillable reports whether the meter is a known, priceable type (i.e. not the
// unspecified zero value). Pure domain rule used to reject RecordUsage early.
func (mt MeterType) IsBillable() bool {
	switch mt {
	case MeterTypeInferenceRequest, MeterTypeInferenceTokens,
		MeterTypeComputeSeconds, MeterTypeStorageBytes:
		return true
	default:
		return false
	}
}

// ============================================================================
// RATE PLAN — the server-side source of every price (clients never supply one).
// ============================================================================
//
// SECURITY: the rate plan is THE structural guarantee that a client cannot set
// its own price. A UsageRecord references a plan by id; the server reads the
// plan's UnitPrices to compute cost. The client write path never carries a
// price — it can only learn prices after the fact via GetUsage/GetInvoice.
type RatePlan struct {
	ID   string // UUID v4, server-assigned
	Name string // "free-tier","standard","enterprise"

	// UnitPrices maps a MeterType to the price of ONE unit of that meter. A map
	// (not fixed fields) so a new meter's price is added without a schema change.
	// INVARIANT (enforced at creation): all entries share one CurrencyCode.
	UnitPrices map[MeterType]Money

	// IncludedQuantities is the free allowance per meter per billing period.
	// Usage up to this is not charged; only the excess is priced. Absent = 0.
	IncludedQuantities map[MeterType]int64

	// QuotaLimits is the hard cap per meter per period. Crossing it is what fires
	// the QuotaExceeded outbox event. 0/absent = no cap for that meter.
	QuotaLimits map[MeterType]int64

	CreatedAt time.Time // server-set, immutable
}

// Currency returns the plan's single currency code (every UnitPrices entry shares
// it by invariant). Returns "" for an empty plan. Used to stamp computed costs
// and to validate that a sum stays single-currency.
func (p RatePlan) Currency() string {
	for _, price := range p.UnitPrices {
		return price.CurrencyCode
	}
	return ""
}

// PriceFor returns the unit price for a meter and whether the plan prices it.
// A meter the plan has no entry for is NOT free — it is UNPRICED, and metering
// it is an error (ErrUnknownMeter), never a silent $0. The bool lets the caller
// distinguish "priced at zero" (a real, intentional zero price) from "no price".
func (p RatePlan) PriceFor(mt MeterType) (Money, bool) {
	price, ok := p.UnitPrices[mt]
	return price, ok
}

// ============================================================================
// USAGE RECORD — one metered, priced fact (the immutable atom of the ledger).
// ============================================================================
//
// Every billable action becomes exactly one UsageRecord, append-only (we INSERT,
// never UPDATE). Invoices are aggregations of UsageRecords over a period.
// EVERY money/attribution field is SERVER-COMPUTED — see RecordUsageInput for
// the strict client input contract.
type UsageRecord struct {
	ID         string    // UUID v4, server-assigned (ledger primary key)
	Team       string    // SERVER-derived (auth claims / inference api key) — never client-set
	RatePlanID string    // SERVER-resolved + pinned (reproducible pricing)
	MeterType  MeterType // what was metered
	Quantity   int64     // billable units AFTER free-allowance subtraction
	Cost       Money     // SERVER-computed = billableQuantity * unit price

	ModelID      string // attribution (reporting only, never pricing)
	ModelVersion string

	// SourceRequestID is the originating inference request id / upstream event id.
	// Doubles as the natural idempotency anchor: two records must never share one.
	SourceRequestID string

	// IdempotencyKey is the client/consumer-supplied stable key (typically equal
	// to SourceRequestID). A repeat with the same key returns the ORIGINAL record
	// instead of inserting a second — the NATS→DB half of exactly-once-in-effect.
	IdempotencyKey string

	OccurredAt time.Time // event time (server-clamped), not insert time
	CreatedAt  time.Time // insert time (server-stamped)
}

// ============================================================================
// INVOICE — a server-built bill for a team over a period (never client-authored).
// ============================================================================
type Invoice struct {
	ID            string
	InvoiceNumber string // human-facing "INV-2026-000042", server-assigned
	Team          string
	RatePlanID    string
	Status        InvoiceStatus

	PeriodStart time.Time // inclusive
	PeriodEnd   time.Time // exclusive

	LineItems []InvoiceLineItem
	Total     Money // server-computed Σ line items, rounded to cents at finalize

	CreatedAt   time.Time
	FinalizedAt *time.Time // nil while DRAFT
	DueAt       *time.Time // set on finalization
}

// InvoiceLineItem is one priced row on an invoice (one per meter).
type InvoiceLineItem struct {
	MeterType   MeterType
	Description string
	Quantity    int64 // billable units on this line (after free allowance)
	UnitPrice   Money // per-unit price from the pinned plan (transparency)
	Amount      Money // server-computed = Quantity * UnitPrice
}

// InvoiceStatus is the lifecycle of an invoice. Server-authoritative; a client
// can never set it. Mirrors the proto enum (string-backed for readable logs).
type InvoiceStatus string

const (
	InvoiceStatusUnspecified InvoiceStatus = ""
	InvoiceStatusDraft       InvoiceStatus = "DRAFT"     // period open, accumulating
	InvoiceStatusFinalized   InvoiceStatus = "FINALIZED" // locked; InvoiceGenerated fires here
	InvoiceStatusPaid        InvoiceStatus = "PAID"      // terminal happy path
	InvoiceStatusOverdue     InvoiceStatus = "OVERDUE"   // finalized + past due
	InvoiceStatusVoid        InvoiceStatus = "VOID"      // reversed/written off; terminal
)

// ============================================================================
// USAGE SUMMARY — an aggregated rollup (the shape GetUsage returns).
// ============================================================================
type UsageSummary struct {
	Team        string
	PeriodStart time.Time
	PeriodEnd   time.Time
	ByMeter     map[MeterType]MeterUsage
	TotalCost   Money
}

// MeterUsage is one meter's bucket inside a UsageSummary.
type MeterUsage struct {
	MeterType     MeterType
	TotalQuantity int64
	TotalCost     Money
}

// ============================================================================
// OUTBOX EVENT — the transactional-outbox record (the pattern's core type).
// ============================================================================
//
// An OutboxEvent is the PUBLISH INTENT, captured as a row to be committed in the
// SAME Postgres transaction as the business write it describes. The relay
// goroutine (events phase) reads unpublished rows, publishes them to NATS, and
// stamps PublishedAt. Modeling it HERE (in the domain) lets the service build
// the correct event payload alongside each write and lets tests assert the
// outbox invariant ("every usage write produces exactly one UsageRecorded
// outbox event with the matching team/cost") with no infrastructure.
//
// WHY Payload is a typed domain struct, not bytes/JSON here:
//
//	The domain owns the CONTENT and the invariant; serialization (to the
//	eventsv1 proto carried in the EventEnvelope.data Any) is an ADAPTER concern.
//	Keeping Payload as a domain interface means a domain test can assert on the
//	actual fields (team, cost) rather than on opaque bytes. The repository will
//	marshal Payload → proto → the outbox `payload` column at write time.
type OutboxEvent struct {
	// ID is the outbox row id (UUID). It becomes the EventEnvelope.id at publish
	// time, which is the IDEMPOTENCY KEY consumers dedupe on — this is what makes
	// the at-least-once relay safe end to end.
	ID string

	// AggregateID ties the event to the business entity it describes (the
	// UsageRecord id / Invoice id). Lets the relay order/partition by aggregate
	// and lets an operator trace an event back to its row.
	AggregateID string

	// EventType is the canonical event/subject discriminator, e.g.
	// "fp.billing.usage.recorded". The relay uses it to pick the NATS subject and
	// the concrete eventsv1 message type to marshal Payload into.
	EventType string

	// Payload is the typed domain payload (UsageRecordedPayload, etc.). The
	// adapter marshals it to the matching eventsv1 proto. Kept as an interface so
	// the single OutboxEvent type carries any of billing's three event shapes.
	Payload OutboxPayload

	// CreatedAt is when the intent was recorded (== the business write time).
	CreatedAt time.Time

	// PublishedAt is nil until the relay successfully publishes to NATS. The relay
	// query is WHERE published_at IS NULL ORDER BY created_at. Nil here in the
	// domain because the domain only ever CREATES outbox events (unpublished); the
	// relay (adapter) stamps it.
	PublishedAt *time.Time
}

// OutboxPayload marks the domain payload types that can ride the outbox. It is a
// sealed-ish marker interface (one unexported method) so only payloads declared
// in this package satisfy it — preventing an arbitrary type from being smuggled
// into an outbox event. The events adapter type-switches on the concrete type to
// pick the right eventsv1 message.
type OutboxPayload interface {
	isOutboxPayload()
	// EventType returns the canonical subject/type string for this payload, so
	// the service can build an OutboxEvent without a separate lookup table.
	EventType() string
}

// Canonical event subjects (the contract documented in billing.proto and
// event-contract.md). Centralized as consts so the service and the future relay
// agree on one spelling.
const (
	EventTypeUsageRecorded    = "fp.billing.usage.recorded"
	EventTypeQuotaExceeded    = "fp.billing.quota.exceeded"
	EventTypeInvoiceGenerated = "fp.billing.invoice.generated"
)

// UsageRecordedPayload mirrors events.UsageRecorded (FLATTENED money: micros +
// currency, not a Money struct — so a consumer needn't depend on this API). The
// events adapter maps this 1:1 to eventsv1.UsageRecorded.
type UsageRecordedPayload struct {
	RecordID        string
	Team            string
	RatePlanID      string
	MeterType       MeterType
	Quantity        int64
	CostMicros      int64
	CurrencyCode    string
	ModelID         string
	ModelVersion    string
	SourceRequestID string
	OccurredAt      time.Time
}

func (UsageRecordedPayload) isOutboxPayload()  {}
func (UsageRecordedPayload) EventType() string { return EventTypeUsageRecorded }

// QuotaExceededPayload mirrors events.QuotaExceeded. Built and enqueued in the
// SAME outbox batch as the usage write when that write pushes a team over its
// plan quota — so the gateway's cache-invalidation event is as durable as the
// usage that triggered it.
type QuotaExceededPayload struct {
	Team         string
	RatePlanID   string
	MeterType    MeterType
	QuotaLimit   int64
	CurrentUsage int64
	OccurredAt   time.Time
}

func (QuotaExceededPayload) isOutboxPayload()  {}
func (QuotaExceededPayload) EventType() string { return EventTypeQuotaExceeded }

// InvoiceGeneratedPayload mirrors events.InvoiceGenerated (flattened total).
// Enqueued in the same tx as the DRAFT→FINALIZED invoice transition.
type InvoiceGeneratedPayload struct {
	InvoiceID     string
	InvoiceNumber string
	Team          string
	RatePlanID    string
	PeriodStart   time.Time
	PeriodEnd     time.Time
	TotalMicros   int64
	CurrencyCode  string
	FinalizedAt   time.Time
}

func (InvoiceGeneratedPayload) isOutboxPayload()  {}
func (InvoiceGeneratedPayload) EventType() string { return EventTypeInvoiceGenerated }

// ============================================================================
// CURRENCY VALIDATION — the ISO-4217 SHAPE guard (pure, used by the service).
// ============================================================================

// IsValidCurrencyCode reports whether s is a well-formed ISO-4217 code: exactly
// three UPPERCASE ASCII letters. WHY shape-only (not a membership check against
// the full ISO table): a closed allow-list is a deployment policy concern; the
// shape guard catches the programming errors ("usd","US$","") that would
// otherwise reach a stored price or an emitted event and break a downstream
// payment integration. Kept pure here so it is unit-testable and reused by both
// CreateRatePlan validation and Money construction.
func IsValidCurrencyCode(s string) bool {
	if len(s) != 3 {
		return false
	}
	// strings.ToUpper round-trip would allow lowercase; we require already-upper.
	if s != strings.ToUpper(s) {
		return false
	}
	for _, r := range s {
		if r < 'A' || r > 'Z' {
			return false
		}
	}
	return true
}
