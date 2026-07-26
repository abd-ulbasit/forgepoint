// invoice_store.go — the Postgres adapter for domain.InvoiceStore.
//
// ============================================================================
// TWO RESPONSIBILITIES: the SET MATH (here) and the OUTBOX (here too)
// ============================================================================
//
// AggregateUsageForPeriod is the SET MATH the domain delegates to Postgres: a
// GROUP BY over the ledger that returns one bucket per meter (SUM quantity, SUM
// cost). The domain keeps the MONEY MATH (line items, overflow-safe totals,
// round-to-cents) — the DB does set aggregation, the domain does money. That split
// is deliberate: Postgres SUM over BIGINT is exact and fast, but rounding/currency
// rules belong in the overflow-safe domain code where they're unit-tested.
//
// SaveInvoiceTx is the SECOND outbox boundary (mirrors UsageStore.RecordUsageTx):
// the finalized invoice row AND its InvoiceGenerated outbox row commit in ONE tx,
// so the DRAFT→FINALIZED transition and the event that announces it are durable
// together. Same pattern, same guarantee, different aggregate.
// ============================================================================
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
)

// Compile-time proof the adapter satisfies the domain port.
var _ domain.InvoiceStore = (*InvoiceStore)(nil)

// AggregateUsageForPeriod returns the per-meter rollup of a team's usage over
// [periodStart, periodEnd) — a GROUP BY in Postgres. Returns an EMPTY slice (not
// an error) when the team had no usage; the service maps empty → ErrNoUsage so the
// period-close job skips a $0 invoice.
//
// WHY the half-open window [start, end): occurred_at >= start AND < end is the
// standard period convention — a record exactly at the next period's start belongs
// to that next period, never double-counted. We GROUP BY currency too so a
// (pathological) mixed-currency team yields one bucket per (meter, currency)
// rather than silently summing incommensurable micros — the domain then refuses to
// total mixed currencies (ErrCurrencyMismatch) instead of emitting a nonsense sum.
func (s *InvoiceStore) AggregateUsageForPeriod(ctx context.Context, team string, periodStart, periodEnd time.Time) ([]domain.MeterUsage, error) {
	const q = `
		SELECT meter_type, currency_code,
		       COALESCE(SUM(quantity), 0)    AS total_quantity,
		       COALESCE(SUM(cost_micros), 0) AS total_cost_micros
		FROM usage_records
		WHERE team = $1 AND occurred_at >= $2 AND occurred_at < $3
		GROUP BY meter_type, currency_code
		ORDER BY meter_type`
	rows, err := s.pool.Query(ctx, q, team, periodStart, periodEnd)
	if err != nil {
		return nil, fmt.Errorf("aggregate usage: %w", err)
	}
	defer rows.Close()

	var buckets []domain.MeterUsage
	for rows.Next() {
		var (
			meter    string
			currency string
			qty      int64
			costMic  int64
		)
		if err := rows.Scan(&meter, &currency, &qty, &costMic); err != nil {
			return nil, fmt.Errorf("scan usage bucket: %w", err)
		}
		buckets = append(buckets, domain.MeterUsage{
			MeterType:     domain.MeterType(meter),
			TotalQuantity: qty,
			TotalCost:     domain.Money{AmountMicros: costMic, CurrencyCode: currency},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate usage buckets: %w", err)
	}
	return buckets, nil
}

// SaveInvoiceTx atomically persists the finalized Invoice AND its InvoiceGenerated
// outbox event in ONE tx (the outbox commit boundary). Returns the stored invoice.
func (s *InvoiceStore) SaveInvoiceTx(ctx context.Context, invoice domain.Invoice, event domain.OutboxEvent) (domain.Invoice, error) {
	lineItemsJSON, err := marshalLineItems(invoice.LineItems)
	if err != nil {
		return domain.Invoice{}, err
	}
	payloadJSON, err := marshalOutboxPayload(event.Payload)
	if err != nil {
		return domain.Invoice{}, err
	}

	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		const insertInvoice = `
			INSERT INTO invoices
				(id, invoice_number, team, rate_plan_id, status,
				 period_start, period_end, line_items,
				 total_micros, currency_code, created_at, finalized_at, due_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`
		if _, err := tx.Exec(ctx, insertInvoice,
			invoice.ID, invoice.InvoiceNumber, invoice.Team, invoice.RatePlanID, string(invoice.Status),
			invoice.PeriodStart, invoice.PeriodEnd, lineItemsJSON,
			invoice.Total.AmountMicros, invoice.Total.CurrencyCode, invoice.CreatedAt,
			invoice.FinalizedAt, invoice.DueAt,
		); err != nil {
			return err
		}

		// The InvoiceGenerated event commits with the invoice. published_at NULL —
		// the relay stamps it after publishing.
		const insertOutbox = `
			INSERT INTO outbox (id, aggregate_id, event_type, payload, created_at)
			VALUES ($1, $2, $3, $4, $5)`
		if _, err := tx.Exec(ctx, insertOutbox,
			event.ID, event.AggregateID, event.EventType, payloadJSON, event.CreatedAt,
		); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		// invoice_number carries a UNIQUE constraint; a 23505 means a duplicate
		// invoice number (a generation bug or a retried finalize). Surface it as the
		// duplicate sentinel so the caller can decide; other errors are infra.
		if isUniqueViolation(err) {
			return domain.Invoice{}, domain.ErrRepoDuplicate
		}
		return domain.Invoice{}, fmt.Errorf("save invoice tx: %w", err)
	}

	return invoice, nil
}

// GetInvoice returns an invoice by id, or domain.ErrRepoNotFound.
func (s *InvoiceStore) GetInvoice(ctx context.Context, id string) (domain.Invoice, error) {
	const q = `
		SELECT id, invoice_number, team, rate_plan_id, status,
		       period_start, period_end, line_items,
		       total_micros, currency_code, created_at, finalized_at, due_at
		FROM invoices
		WHERE id = $1`
	return scanInvoice(s.pool.QueryRow(ctx, q, id))
}

// ListInvoices returns a team's invoices newest-first, keyset-paginated on
// (created_at, id). The optional status filter and the cursor predicate are
// assembled dynamically, but EVERY value is a bound parameter ($N) — never
// interpolated text — so there is no injection surface even though the SQL string
// is built up. We fetch pageSize+1 rows to detect whether a next page exists.
func (s *InvoiceStore) ListInvoices(ctx context.Context, opts domain.ListInvoicesOptions) ([]domain.Invoice, string, error) {
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}

	cur, err := decodeCursor(opts.PageToken)
	if err != nil {
		return nil, "", fmt.Errorf("list invoices: %w", err)
	}

	args := []any{opts.Team}
	where := "WHERE team = $1"
	if opts.StatusFilter != domain.InvoiceStatusUnspecified {
		args = append(args, string(opts.StatusFilter))
		where += fmt.Sprintf(" AND status = $%d", len(args))
	}
	if cur != nil {
		// Strictly BEFORE the cursor in DESC (created_at, id) order. The row-value
		// comparison expresses the composite "< (created_at, id)" cleanly so ties on
		// created_at break deterministically by id.
		args = append(args, cur.CreatedAt, cur.ID)
		where += fmt.Sprintf(" AND (created_at, id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, pageSize+1) // one extra row to detect a next page
	q := fmt.Sprintf(`
		SELECT id, invoice_number, team, rate_plan_id, status,
		       period_start, period_end, line_items,
		       total_micros, currency_code, created_at, finalized_at, due_at
		FROM invoices %s
		ORDER BY created_at DESC, id DESC
		LIMIT $%d`, where, len(args))

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list invoices: %w", err)
	}
	defer rows.Close()

	invoices := make([]domain.Invoice, 0, pageSize)
	for rows.Next() {
		inv, err := scanInvoice(rows)
		if err != nil {
			return nil, "", fmt.Errorf("scan invoice: %w", err)
		}
		invoices = append(invoices, inv)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate invoices: %w", err)
	}

	// If we got the extra (pageSize+1)th row, there IS a next page: trim the extra
	// and emit a cursor from the LAST kept row. Otherwise this is the final page and
	// the token is empty.
	nextToken := ""
	if len(invoices) > pageSize {
		invoices = invoices[:pageSize]
		last := invoices[len(invoices)-1]
		nextToken = encodeCursor(cursor{CreatedAt: last.CreatedAt, ID: last.ID})
	}
	return invoices, nextToken, nil
}

// scanInvoice decodes one invoices row (in the SELECT column order used by
// GetInvoice and ListInvoices) into a domain.Invoice, hydrating the line-item
// JSONB array and the nullable finalized_at/due_at timestamps. Accepts a
// rowScanner so both QueryRow (single) and Rows (list iteration) share one scan +
// JSONB-decode site.
func scanInvoice(row rowScanner) (domain.Invoice, error) {
	var (
		inv        domain.Invoice
		status     string
		lineItemsB []byte
		totalMic   int64
		currency   string
	)
	if err := row.Scan(
		&inv.ID, &inv.InvoiceNumber, &inv.Team, &inv.RatePlanID, &status,
		&inv.PeriodStart, &inv.PeriodEnd, &lineItemsB,
		&totalMic, &currency, &inv.CreatedAt, &inv.FinalizedAt, &inv.DueAt,
	); err != nil {
		if isNoRows(err) {
			return domain.Invoice{}, domain.ErrRepoNotFound
		}
		return domain.Invoice{}, fmt.Errorf("scan invoice: %w", err)
	}
	inv.Status = domain.InvoiceStatus(status)
	inv.Total = domain.Money{AmountMicros: totalMic, CurrencyCode: currency}

	items, err := unmarshalLineItems(lineItemsB)
	if err != nil {
		return domain.Invoice{}, err
	}
	inv.LineItems = items
	return inv, nil
}

// rowScanner is the minimal surface both pgx.Row (single-row QueryRow result) and
// pgx.Rows (iterated Query result) share: a Scan method. Abstracting it lets one
// scanInvoice serve BOTH the single-row reads and the list iteration, so the
// column order and JSONB decoding are written exactly once.
type rowScanner interface {
	Scan(dest ...any) error
}
