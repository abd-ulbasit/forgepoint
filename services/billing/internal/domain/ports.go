// ports.go — the persistence/external PORTS the billing domain depends on.
//
// ============================================================================
// WHY THE PORTS LIVE IN THE DOMAIN (Hexagonal "consumer-owned ports")
// ============================================================================
//
// Same hard-won lesson as the Auth service (see docs/design/service-architecture.md
// "Ports live in the DOMAIN"): the domain's service impl (billing_service_impl.go)
// references these interfaces, and the interface signatures reference domain
// types. If the interfaces lived in `internal/repository`, then `repository`
// would import `domain` (for the types) AND `domain` would import `repository`
// (for the interfaces) → an import CYCLE Go rejects outright.
//
// The hexagonal resolution: the CONSUMER owns the port. The domain is the
// consumer of persistence and of the outbox, so the PORT interfaces live here.
// The Postgres adapter (repo phase) imports `domain` and IMPLEMENTS them — a
// single inward arrow, no cycle:
//
//	       domain (defines ports + service + models)   ← stdlib + uuid only
//	          ▲ implements                 ▲ implements
//	postgres adapter (UsageStore, ...)    (OutboxStore via same tx)
//
// ============================================================================
// THE OUTBOX TRANSACTIONAL BOUNDARY — expressed as a PORT, not a leak
// ============================================================================
//
// The outbox pattern's whole point is that the business write AND the outbox
// event commit in ONE database transaction. The domain must express "do these
// two writes atomically" WITHOUT importing database/sql (purity rule). We do
// that by giving the persistence port a single method that takes BOTH the
// business rows and the outbox events and is CONTRACTUALLY required to commit
// them in one tx (see UsageStore.RecordUsageTx). The atomicity MECHANISM (BEGIN/
// COMMIT, pgx) is the adapter's secret; the domain only states the requirement.
//
// KEEPING THE DOMAIN PURE WHILE STILL DEMANDING A TRANSACTION:
//
//	The port METHOD is the transaction boundary: one call = one atomic unit. The
//	domain hands the adapter everything that must commit together; the adapter
//	wraps it in a tx. The domain never sees a *sql.Tx — it sees a method whose
//	CONTRACT is atomicity.
package domain

import (
	"context"
	"errors"
	"time"
)

// ============================================================================
// STORAGE SENTINELS (what the ports return; the service translates to business
// errors in errors.go — same two-vocabulary split as Auth).
// ============================================================================

// ErrRepoNotFound is the storage sentinel for "no such row". The service maps it
// to the right business error (ErrRatePlanNotFound, ErrInvoiceNotFound, ...).
var ErrRepoNotFound = errors.New("repository: not found")

// ErrRepoDuplicate is the storage sentinel for a unique-constraint violation
// (e.g. the idempotency-key unique index rejected a second insert). The service
// uses it on the RecordUsage path to detect a RACE: two concurrent calls with
// the same idempotency key — one wins the insert, the loser gets this and must
// re-read the winner's record (so both callers see the same single record).
var ErrRepoDuplicate = errors.New("repository: duplicate key")

// ============================================================================
// UsageStore — the ledger persistence port (business rows + outbox, atomically).
// ============================================================================

// UsageStore persists usage records and the period-usage rollups the metering
// math needs, AND is the transactional boundary for the outbox. The Postgres
// adapter implements it; tests inject a hand-written mock.
type UsageStore interface {
	// FindByIdempotencyKey returns a previously-recorded UsageRecord for this
	// (team, idempotencyKey), or ErrRepoNotFound if none exists. This is the
	// FIRST step of the idempotent RecordUsage: a hit short-circuits to returning
	// the original record (deduplicated=true) WITHOUT a second insert. WHY scoped
	// by team as well as key: idempotency keys are client-generated and only
	// unique within a tenant; the team comes from server-authoritative claims.
	FindByIdempotencyKey(ctx context.Context, team, idempotencyKey string) (UsageRecord, error)

	// CurrentPeriodUsage returns the team's total quantity ALREADY recorded for a
	// meter in the current billing period, BEFORE this write. The service needs it
	// to (a) apply the free allowance correctly across records and (b) decide
	// whether THIS write crosses the plan's quota (and must therefore enqueue a
	// QuotaExceeded outbox event). Returns 0 when there is no prior usage.
	CurrentPeriodUsage(ctx context.Context, team string, meter MeterType, periodStart time.Time) (int64, error)

	// RecordUsageTx atomically persists the UsageRecord AND inserts the given
	// OutboxEvents in ONE transaction (the outbox pattern's commit boundary).
	//
	// CONTRACT (the adapter MUST honor):
	//   - INSERT the usage record and ALL outbox events in a single tx; either all
	//     commit or none do. A crash after this returns nil means BOTH the row and
	//     the publish-intent are durable — the relay will publish the events.
	//   - The usage record carries a UNIQUE (team, idempotency_key) constraint. If
	//     a concurrent writer already inserted that key, return ErrRepoDuplicate so
	//     the service can re-read the winner's record (idempotency under races).
	//   - Return the stored record (with any DB-populated fields).
	//
	// WHY events are a SLICE: a single RecordUsage may emit ONE UsageRecorded and,
	// when it crosses quota, ALSO a QuotaExceeded — both must commit with the row.
	RecordUsageTx(ctx context.Context, record UsageRecord, events []OutboxEvent) (UsageRecord, error)
}

// ============================================================================
// RatePlanStore — pricing persistence + team→plan resolution.
// ============================================================================

// RatePlanStore persists rate plans and resolves which plan prices a given team.
type RatePlanStore interface {
	// CreatePlan persists a new RatePlan (id/created_at already set by the
	// service). Returns ErrRepoDuplicate if the idempotency key (or unique name)
	// was already used, so CreateRatePlan can return the original plan.
	CreatePlan(ctx context.Context, plan RatePlan, idempotencyKey string) (RatePlan, error)

	// FindPlanByIdempotencyKey returns the plan previously created under this key,
	// or ErrRepoNotFound. First step of idempotent CreateRatePlan.
	FindPlanByIdempotencyKey(ctx context.Context, idempotencyKey string) (RatePlan, error)

	// GetPlan returns a plan by id, or ErrRepoNotFound.
	GetPlan(ctx context.Context, id string) (RatePlan, error)

	// ResolvePlanForTeam returns the rate plan currently assigned to a team. This
	// is the SERVER-AUTHORITATIVE price source on the metering path: RecordUsage
	// calls it to find the plan whose prices it applies (the client never names a
	// plan). Returns ErrRepoNotFound if the team has no plan (the service maps
	// that to ErrRatePlanNotFound → FailedPrecondition: we cannot price without a
	// plan).
	ResolvePlanForTeam(ctx context.Context, team string) (RatePlan, error)
}

// ============================================================================
// InvoiceStore — invoice persistence (also a transactional outbox boundary).
// ============================================================================

// InvoiceStore persists invoices and the line-item rollup, and is the outbox
// boundary for the InvoiceGenerated event.
type InvoiceStore interface {
	// AggregateUsageForPeriod returns the per-meter rollup of a team's usage over
	// [periodStart, periodEnd). The service turns these buckets into invoice line
	// items. Returns an empty slice (not an error) when the team had no usage —
	// the service maps "empty" to ErrNoUsage so the period-close job skips a $0
	// invoice. Keeping the aggregation in the store (a GROUP BY in Postgres) and
	// the LINE-ITEM/total math in the domain is the right split: the DB does set
	// math, the domain does money math (overflow-safe).
	AggregateUsageForPeriod(ctx context.Context, team string, periodStart, periodEnd time.Time) ([]MeterUsage, error)

	// SaveInvoiceTx atomically persists the finalized Invoice AND the
	// InvoiceGenerated outbox event in ONE transaction (same outbox commit
	// boundary as RecordUsageTx). The DRAFT→FINALIZED transition and its event are
	// durable together.
	SaveInvoiceTx(ctx context.Context, invoice Invoice, event OutboxEvent) (Invoice, error)

	// GetInvoice returns an invoice by id, or ErrRepoNotFound.
	GetInvoice(ctx context.Context, id string) (Invoice, error)

	// ListInvoices returns a team's invoices (newest first), paginated.
	ListInvoices(ctx context.Context, opts ListInvoicesOptions) (invoices []Invoice, nextToken string, err error)
}

// ListInvoicesOptions carries the (server-validated) filters/pagination for
// ListInvoices. Cursor pagination (opaque token) over LIMIT/OFFSET for stability
// under concurrent inserts — same rationale as the Auth/registry list ports.
type ListInvoicesOptions struct {
	Team         string
	StatusFilter InvoiceStatus // "" = all statuses
	PageSize     int           // 0 → service default; service caps at MaxPageSize
	PageToken    string        // opaque cursor; "" = first page
}

// ============================================================================
// IDProvider & Clock — injectable purity seams (deterministic tests).
// ============================================================================

// IDProvider generates unique ids (UUIDs in production). WHY a port and not a
// direct uuid.NewString() call in the service: injecting it lets a test supply
// DETERMINISTIC ids so it can assert "the outbox event's AggregateID equals the
// usage record's ID" by exact value, not by "some uuid". The production adapter
// is a one-liner over github.com/google/uuid.
type IDProvider interface {
	NewID() string
}

// Clock returns the current time. Injected for the same reason: the service
// CLAMPS occurred_at against "now" (no backdating into a closed period) and
// stamps CreatedAt; a fixed clock makes those rules testable to the nanosecond.
// Production uses a real-time clock; tests use a frozen one.
type Clock interface {
	Now() time.Time
}
