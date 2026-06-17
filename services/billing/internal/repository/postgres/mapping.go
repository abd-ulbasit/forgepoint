// mapping.go — the domain ⇄ Postgres value translations (JSONB shapes + nullables).
//
// ============================================================================
// WHY A DEDICATED MAPPING LAYER
// ============================================================================
//
// The domain types (Money, RatePlan's price maps, InvoiceLineItem, the OutboxEvent
// payloads) are not 1:1 with SQL scalar columns — several are maps/slices/structs
// we persist as JSONB. Keeping ALL of those conversions in one file (a) makes the
// stored shape auditable in one place, and (b) decouples the stored JSON keys from
// the Go field names: we re-describe each shape in a small wire struct with
// explicit `json` tags, so renaming a domain field later cannot silently break
// already-stored rows. The JSON keys are short/stable snake-ish names, the
// platform's stored convention.
//
// Money is stored as TWO columns (micros BIGINT + currency CHAR(3)) on the
// business tables, but INSIDE a JSONB map/array it is stored as a small object
// {"micros":N,"currency":"USD"} — same exact-integer money, no float ever.
// ============================================================================
package postgres

import (
	"encoding/json"
	"fmt"

	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
)

// ----------------------------------------------------------------------------
// nullable string helper
// ----------------------------------------------------------------------------

// nullIfEmpty maps "" → SQL NULL and a non-empty string → itself. WHY for the
// idempotency_key column specifically: the partial UNIQUE index is
// `WHERE idempotency_key IS NOT NULL`, so a KEYLESS usage record must store NULL
// (not "") to be EXEMPT from the uniqueness constraint — otherwise many keyless
// records would all collide on the empty-string key. Returning `any` lets pgx bind
// either a string or a nil (→ NULL) through the same parameter slot.
func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// ============================================================================
// MONEY ⇄ JSONB object  (used inside price maps and line items)
// ============================================================================

// moneyJSON is the stored shape of a Money inside a JSONB document. Explicit tags
// keep the keys stable and decoupled from the domain field names.
type moneyJSON struct {
	Micros   int64  `json:"micros"`
	Currency string `json:"currency"`
}

func toMoneyJSON(m domain.Money) moneyJSON {
	return moneyJSON{Micros: m.AmountMicros, Currency: m.CurrencyCode}
}

func (mj moneyJSON) toDomain() domain.Money {
	return domain.Money{AmountMicros: mj.Micros, CurrencyCode: mj.Currency}
}

// ============================================================================
// RATE PLAN MAPS ⇄ JSONB
// ============================================================================
//
// A plan carries three meter→value maps. We marshal them with the MeterType
// string as the JSON object key. WHY a string key (not an int): it keeps the
// stored JSON human-readable ("INFERENCE_TOKENS": {...}) and stable under any
// re-numbering of the proto enum, matching the domain's string-backed MeterType.

// marshalUnitPrices encodes UnitPrices (meter → Money) to JSONB bytes:
// {"INFERENCE_TOKENS":{"micros":400,"currency":"USD"}, ...}.
func marshalUnitPrices(prices map[domain.MeterType]domain.Money) ([]byte, error) {
	out := make(map[string]moneyJSON, len(prices))
	for meter, price := range prices {
		out[string(meter)] = toMoneyJSON(price)
	}
	return json.Marshal(out)
}

// unmarshalUnitPrices is the inverse. A nil/empty document decodes to a nil map
// (a plan with no priced meters — which the domain rejects upstream, but the
// adapter round-trips faithfully rather than inventing entries).
func unmarshalUnitPrices(b []byte) (map[domain.MeterType]domain.Money, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var in map[string]moneyJSON
	if err := json.Unmarshal(b, &in); err != nil {
		return nil, fmt.Errorf("unmarshal unit_prices: %w", err)
	}
	out := make(map[domain.MeterType]domain.Money, len(in))
	for meter, mj := range in {
		out[domain.MeterType(meter)] = mj.toDomain()
	}
	return out, nil
}

// marshalQtyMap encodes a meter→int64 map (IncludedQuantities or QuotaLimits) to
// JSONB. An empty/nil map encodes to "{}" so the column's NOT NULL DEFAULT '{}'
// invariant holds and reads come back as an empty (non-nil) map.
func marshalQtyMap(m map[domain.MeterType]int64) ([]byte, error) {
	out := make(map[string]int64, len(m))
	for meter, v := range m {
		out[string(meter)] = v
	}
	return json.Marshal(out)
}

// unmarshalQtyMap is the inverse. Always returns a non-nil map (possibly empty) so
// the domain's `plan.IncludedQuantities[meter]` lookups never nil-panic — a map
// read on a nil map is fine in Go, but a non-nil empty map keeps the round-trip
// shape uniform with what CreateRatePlan stored.
func unmarshalQtyMap(b []byte) (map[domain.MeterType]int64, error) {
	out := map[domain.MeterType]int64{}
	if len(b) == 0 {
		return out, nil
	}
	var in map[string]int64
	if err := json.Unmarshal(b, &in); err != nil {
		return nil, fmt.Errorf("unmarshal quantity map: %w", err)
	}
	for meter, v := range in {
		out[domain.MeterType(meter)] = v
	}
	return out, nil
}

// ============================================================================
// INVOICE LINE ITEMS ⇄ JSONB array
// ============================================================================

// lineItemJSON is the stored shape of one invoice line. Money fields nest the
// moneyJSON object so a line is fully self-describing (qty, unit price, amount).
type lineItemJSON struct {
	MeterType   string    `json:"meter_type"`
	Description string    `json:"description"`
	Quantity    int64     `json:"quantity"`
	UnitPrice   moneyJSON `json:"unit_price"`
	Amount      moneyJSON `json:"amount"`
}

// marshalLineItems encodes the line-item slice to a JSONB array. A nil slice
// encodes to "[]" (matching the column's NOT NULL DEFAULT '[]') so an invoice row
// always carries a well-formed array.
func marshalLineItems(items []domain.InvoiceLineItem) ([]byte, error) {
	out := make([]lineItemJSON, 0, len(items))
	for _, li := range items {
		out = append(out, lineItemJSON{
			MeterType:   string(li.MeterType),
			Description: li.Description,
			Quantity:    li.Quantity,
			UnitPrice:   toMoneyJSON(li.UnitPrice),
			Amount:      toMoneyJSON(li.Amount),
		})
	}
	return json.Marshal(out)
}

// unmarshalLineItems is the inverse. Empty bytes → nil slice (the inverse of a nil
// slice marshaling to "[]" then back — both render as "no line items").
func unmarshalLineItems(b []byte) ([]domain.InvoiceLineItem, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var in []lineItemJSON
	if err := json.Unmarshal(b, &in); err != nil {
		return nil, fmt.Errorf("unmarshal line_items: %w", err)
	}
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]domain.InvoiceLineItem, 0, len(in))
	for _, li := range in {
		out = append(out, domain.InvoiceLineItem{
			MeterType:   domain.MeterType(li.MeterType),
			Description: li.Description,
			Quantity:    li.Quantity,
			UnitPrice:   li.UnitPrice.toDomain(),
			Amount:      li.Amount.toDomain(),
		})
	}
	return out, nil
}

// ============================================================================
// OUTBOX PAYLOAD ⇄ JSONB  (the publish intent's body)
// ============================================================================
//
// The domain's OutboxEvent.Payload is a sealed interface (OutboxPayload) with
// three concrete shapes (UsageRecordedPayload, QuotaExceededPayload,
// InvoiceGeneratedPayload). The relay (events phase) will marshal it to the
// canonical eventsv1 proto at publish time; HERE, at write time, we persist it as
// JSONB so the row is self-contained and the relay can reconstruct the typed
// payload. EventType (already stored in its own column) is the DISCRIMINATOR the
// relay/decoder uses to pick the concrete type — so we don't need a second type
// tag inside the JSON. We marshal the concrete struct directly; Go's json package
// emits its exported fields. (The interface is sealed, so the type switch is total.)
func marshalOutboxPayload(p domain.OutboxPayload) ([]byte, error) {
	// A type switch documents the closed set and gives a precise error if a new
	// payload shape is added without updating the adapter — better than silently
	// marshaling an unknown type whose JSON the relay can't decode.
	switch p.(type) {
	case domain.UsageRecordedPayload,
		domain.QuotaExceededPayload,
		domain.InvoiceGeneratedPayload:
		b, err := json.Marshal(p)
		if err != nil {
			return nil, fmt.Errorf("marshal outbox payload (%s): %w", p.EventType(), err)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("unknown outbox payload type %T (event %s)", p, p.EventType())
	}
}
