// errors.go — sentinel errors owned by the billing domain layer.
//
// ============================================================================
// WHY SENTINEL ERRORS (errors.Is-friendly) INSTEAD OF FREE-FORM STRINGS
// ============================================================================
//
// The handler maps each domain failure to a precise gRPC status code
// (InvalidArgument, NotFound, FailedPrecondition, Internal). For that mapping
// to be robust it must NOT string-match error text — it must errors.Is against
// a stable sentinel. So every business outcome the handler needs to distinguish
// gets a named var here, and the service wraps it with %w when it needs to add
// a human-readable cause ("...: quantity 5_000_000_000 exceeds per-record cap").
//
// ============================================================================
// THE TWO ERROR VOCABULARIES (storage vs business) — same split as Auth
// ============================================================================
//
// Storage sentinels (ErrRepoNotFound, ErrRepoDuplicate) live in ports.go and
// describe what the PERSISTENCE port observed ("no row", "unique violation").
// The errors below are BUSINESS outcomes the handler turns into status codes.
// The service is the single translation point: it catches a storage sentinel
// and re-expresses it as the right business error so the handler never imports
// the repository package.
package domain

import "errors"

var (
	// ErrValidation is the catch-all for malformed input the service rejects
	// BEFORE touching any port (unknown meter, empty currency, etc.). Wrapped
	// with %w so callers get a specific message AND can errors.Is(err,
	// ErrValidation) → codes.InvalidArgument.
	ErrValidation = errors.New("billing: validation failed")

	// ErrNegativeQuantity is returned by RecordUsage when a CLIENT submits a
	// negative quantity. WHY a dedicated sentinel and not just ErrValidation:
	//   This is a security invariant, not a typo. Only the SERVER may issue a
	//   negative quantity (a credit/refund). A negative quantity arriving on the
	//   client write path is either a bug or an attempt to manufacture a credit
	//   ("bill me minus a million tokens" → a negative cost → free money). We
	//   name it so the rejection is greppable in audit logs and provable in a
	//   test. Maps to codes.InvalidArgument.
	ErrNegativeQuantity = errors.New("billing: quantity must be non-negative (only server-issued credits may be negative)")

	// ErrQuantityTooLarge is returned when quantity exceeds MaxQuantityPerRecord.
	// WHY this is an OVERFLOW guard, not just a sanity bound:
	//   cost = quantity * unit_price_micros is an int64 multiply. If quantity is
	//   unbounded, a large quantity times a large price WRAPS int64 — and a
	//   wrapped product is a SIGNED money bug (a 9-trillion-dollar bill silently
	//   becomes a negative credit). MaxQuantityPerRecord is chosen so that even
	//   the largest sane unit price cannot overflow the product (see models.go).
	//   We REJECT out-of-range input with this error rather than truncate or
	//   wrap — "never wrap" is the headline rule of a money service. Maps to
	//   codes.InvalidArgument.
	ErrQuantityTooLarge = errors.New("billing: quantity exceeds per-record maximum")

	// ErrAmountOverflow is returned when a money computation (a multiply or a
	// sum) would exceed the int64 micro-unit range even though each individual
	// input passed its bound. WHY a SEPARATE error from ErrQuantityTooLarge:
	//   ErrQuantityTooLarge is a per-INPUT rejection at the boundary. ErrAmount-
	//   Overflow is the result of an ACCUMULATION — e.g. summing thousands of
	//   line items into an invoice total, where no single item is too large but
	//   the running total would overflow. The checked-arithmetic helpers
	//   (addInt64Checked, mulInt64Checked) return this when bits.Add/Mul reports
	//   a carry. It exists so an interviewer-grade audit can tell "one input was
	//   absurd" from "the sum blew the budget". Maps to codes.Internal (we
	//   refuse to emit a wrong number) — it should be unreachable given the input
	//   bounds, but a money service asserts it anyway (defense in depth).
	ErrAmountOverflow = errors.New("billing: monetary amount overflowed int64 micro-units")

	// ErrCurrencyMismatch is returned when an operation tries to combine Money in
	// different currencies (e.g. summing a USD line and a EUR line into one
	// total). Adding 100 USD-micros to 100 EUR-micros is meaningless — the int64
	// fields are commensurable but the VALUES are not. We refuse rather than
	// produce a nonsense total. A rate plan is also single-currency by invariant
	// (see CreateRatePlan validation). Maps to codes.InvalidArgument (on plan
	// creation) or codes.Internal (if it ever surfaces during aggregation, which
	// the single-currency-plan invariant should prevent).
	ErrCurrencyMismatch = errors.New("billing: cannot combine amounts of different currencies")

	// ErrInvalidCurrency is returned when a currency code is not a well-formed
	// ISO-4217 alphabetic code (3 uppercase letters). WHY validate the shape:
	//   "usd", "US$", "" are all rejected so a malformed code can never reach a
	//   stored price or an emitted event where a downstream payment integration
	//   would choke on it. We validate the SHAPE (3 A–Z), not membership in the
	//   full ISO table — a closed allow-list is a deployment concern, the shape
	//   guard catches the programming errors. Maps to codes.InvalidArgument.
	ErrInvalidCurrency = errors.New("billing: currency code must be a 3-letter uppercase ISO-4217 code")

	// ErrUnknownMeter is returned when a RecordUsage carries MeterTypeUnspecified
	// (the proto zero value) or a meter the rate plan has no price for. WHY reject
	// rather than default to a zero price:
	//   A zero price means "free", and silently billing a metered action as free
	//   is a revenue leak. An unknown/unset meter is a programming error upstream;
	//   we fail loudly so it's caught, never absorbed as $0. Maps to
	//   codes.InvalidArgument (unspecified meter) / codes.FailedPrecondition
	//   (meter has no price in the plan).
	ErrUnknownMeter = errors.New("billing: unknown or unpriced meter type")

	// ErrRatePlanNotFound is the business error when a referenced rate plan does
	// not exist (GetRatePlan miss, or a team mapped to a non-existent plan). It is
	// the translation of the storage ErrRepoNotFound on the rate-plan port. Maps
	// to codes.NotFound (direct lookup) or codes.FailedPrecondition (metering a
	// team with no resolvable plan — we cannot price without a plan).
	ErrRatePlanNotFound = errors.New("billing: rate plan not found")

	// ErrInvoiceNotFound is the business error when GetInvoice targets a missing
	// invoice. Translation of ErrRepoNotFound on the invoice port. Maps to
	// codes.NotFound.
	ErrInvoiceNotFound = errors.New("billing: invoice not found")

	// ErrNoUsage is returned by GenerateInvoice when a team has zero usage records
	// in the requested period. WHY this is a distinct, non-fatal signal:
	//   "No usage" is not an error in the failure sense — it is a legitimate
	//   answer ("this team owes nothing this period"). The period-close job uses
	//   it to SKIP emitting a zero-total invoice (and the InvoiceGenerated event)
	//   rather than spamming customers with $0.00 bills. Callers errors.Is against
	//   it to branch, not to log a failure.
	ErrNoUsage = errors.New("billing: no usage records in period")

	// ErrInvalidPeriod is returned when a billing/reporting window is malformed
	// (end <= start, or a span wider than MaxUsageWindow). WHY cap the span:
	//   GetUsage scans the ledger over [start, end). An unbounded window lets a
	//   caller scan the entire table (a denial-of-service / cost-blowup). We
	//   reject a too-wide or inverted window rather than silently truncate it, so
	//   the caller learns their query was rejected. Maps to codes.InvalidArgument.
	ErrInvalidPeriod = errors.New("billing: invalid or too-wide reporting period")
)
