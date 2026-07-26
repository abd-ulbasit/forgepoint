// usage_store.go — the Postgres adapter for domain.UsageStore.
//
// ============================================================================
// THE OUTBOX TRANSACTIONAL BOUNDARY (the centerpiece of this service)
// ============================================================================
//
// RecordUsageTx is THE outbox commit point: it INSERTs the usage_records row AND
// every outbox row in ONE Postgres transaction. The domain (ports.go) states the
// CONTRACT ("commit them atomically"); this method supplies the MECHANISM
// (pgx.BeginFunc → BEGIN/COMMIT/ROLLBACK). Either all rows commit or none do, so a
// crash never leaves a usage record without its publish intent (a lost event) or a
// publish intent without its record (a phantom event). There is NO distributed
// transaction across Postgres and NATS — and we don't need one: we commit the
// INTENT to a table in the same DB tx, and a separate relay publishes unpublished
// rows to NATS afterward (at-least-once; consumers dedupe on the outbox id).
//
//	┌─ RecordUsageTx (ONE tx) ──────────────────────────────┐
//	│  INSERT usage_records (... )           ← the ledger row│
//	│  INSERT outbox (UsageRecorded)         ← publish intent│
//	│  INSERT outbox (QuotaExceeded)?        ← only on cross.│
//	│  COMMIT  ──────────────────────────────────────────────┼─► durable together
//	└────────────────────────────────────────────────────────┘
//	          later: relay SELECT … WHERE published_at IS NULL → NATS → UPDATE published_at
//
// IF THE RELAY CRASHES AFTER PUBLISHING BUT BEFORE STAMPING published_at: the
// row stays unpublished, the relay re-publishes on restart, and the consumer dedupes on the outbox id (== EventEnvelope.id). The
// failure mode is at-least-once, never at-most-once — we never silently drop an
// event. The atomicity test below proves the WRITE side of that guarantee.
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
var _ domain.UsageStore = (*UsageStore)(nil)

// FindByIdempotencyKey returns a previously-recorded UsageRecord for this
// (team, key), or domain.ErrRepoNotFound. Scoped by team because client keys are
// only unique within a tenant. This is the FIRST step of idempotent RecordUsage
// (a hit short-circuits to the original record) AND the race-recovery re-read when
// RecordUsageTx reports a duplicate.
func (s *UsageStore) FindByIdempotencyKey(ctx context.Context, team, idempotencyKey string) (domain.UsageRecord, error) {
	const q = `
		SELECT id, team, rate_plan_id, meter_type, quantity,
		       cost_micros, currency_code, model_id, model_version,
		       source_request_id, idempotency_key, occurred_at, created_at
		FROM usage_records
		WHERE team = $1 AND idempotency_key = $2`
	return scanUsageRecord(s.pool.QueryRow(ctx, q, team, idempotencyKey))
}

// CurrentPeriodUsage returns the team's total quantity already recorded for a
// meter since periodStart (the gross usage BEFORE this write). The service uses it
// to apply the free allowance across cumulative usage and to detect quota
// crossings. COALESCE turns "no rows" into 0 so the absence of prior usage reads
// as 0, not NULL.
//
// WHY occurred_at (not created_at) is the period filter: the billing period is
// keyed on the EVENT time (when the usage happened), which the service clamps; a
// late-arriving record for this period still counts toward it.
func (s *UsageStore) CurrentPeriodUsage(ctx context.Context, team string, meter domain.MeterType, periodStart time.Time) (int64, error) {
	const q = `
		SELECT COALESCE(SUM(quantity), 0)
		FROM usage_records
		WHERE team = $1 AND meter_type = $2 AND occurred_at >= $3`
	var total int64
	if err := s.pool.QueryRow(ctx, q, team, string(meter), periodStart).Scan(&total); err != nil {
		return 0, fmt.Errorf("current period usage: %w", err)
	}
	return total, nil
}

// RecordUsageTx atomically persists the UsageRecord AND all OutboxEvents in ONE
// transaction (the outbox commit boundary — see the file header). Returns the
// stored record on success, or domain.ErrRepoDuplicate when a concurrent writer
// already inserted the same (team, idempotency_key) — the partial UNIQUE index
// rejected this insert, and the service re-reads the winner.
func (s *UsageStore) RecordUsageTx(ctx context.Context, record domain.UsageRecord, events []domain.OutboxEvent) (domain.UsageRecord, error) {
	// Marshal outbox payloads BEFORE opening the tx: a marshal failure is a
	// programmer error (an unknown payload type), and we'd rather fail fast without
	// holding a transaction/connection than discover it mid-tx.
	payloads := make([][]byte, len(events))
	for i, ev := range events {
		b, err := marshalOutboxPayload(ev.Payload)
		if err != nil {
			return domain.UsageRecord{}, err
		}
		payloads[i] = b
	}

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		const insertUsage = `
			INSERT INTO usage_records
				(id, team, rate_plan_id, meter_type, quantity,
				 cost_micros, currency_code, model_id, model_version,
				 source_request_id, idempotency_key, occurred_at, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`
		if _, err := tx.Exec(ctx, insertUsage,
			record.ID, record.Team, record.RatePlanID, string(record.MeterType), record.Quantity,
			record.Cost.AmountMicros, record.Cost.CurrencyCode, record.ModelID, record.ModelVersion,
			record.SourceRequestID, nullIfEmpty(record.IdempotencyKey), record.OccurredAt, record.CreatedAt,
		); err != nil {
			return err
		}

		// Insert every outbox row IN THE SAME TX. published_at is left NULL — the
		// relay stamps it after a successful NATS publish; the writer only ever
		// creates UNPUBLISHED intents.
		const insertOutbox = `
			INSERT INTO outbox (id, aggregate_id, event_type, payload, created_at)
			VALUES ($1, $2, $3, $4, $5)`
		for i, ev := range events {
			if _, err := tx.Exec(ctx, insertOutbox,
				ev.ID, ev.AggregateID, ev.EventType, payloads[i], ev.CreatedAt,
			); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		// The (team, idempotency_key) partial UNIQUE index is the only business
		// unique constraint touched here. A 23505 means a concurrent writer won;
		// report the domain duplicate sentinel so the service re-reads the winner.
		if isUniqueViolation(err) {
			return domain.UsageRecord{}, domain.ErrRepoDuplicate
		}
		return domain.UsageRecord{}, fmt.Errorf("record usage tx: %w", err)
	}

	return record, nil
}

// scanUsageRecord decodes one usage_records row (in the SELECT column order used
// by FindByIdempotencyKey and the test reads) into a domain.UsageRecord. The
// nullable idempotency_key column scans into a *string (NULL → empty string in the
// domain). pgx.Row is the single-row result type.
func scanUsageRecord(row pgx.Row) (domain.UsageRecord, error) {
	var (
		rec     domain.UsageRecord
		meter   string
		idemKey *string // NULL-able column → pointer; nil means keyless
	)
	if err := row.Scan(
		&rec.ID, &rec.Team, &rec.RatePlanID, &meter, &rec.Quantity,
		&rec.Cost.AmountMicros, &rec.Cost.CurrencyCode, &rec.ModelID, &rec.ModelVersion,
		&rec.SourceRequestID, &idemKey, &rec.OccurredAt, &rec.CreatedAt,
	); err != nil {
		if isNoRows(err) {
			return domain.UsageRecord{}, domain.ErrRepoNotFound
		}
		return domain.UsageRecord{}, fmt.Errorf("scan usage record: %w", err)
	}
	rec.MeterType = domain.MeterType(meter)
	if idemKey != nil {
		rec.IdempotencyKey = *idemKey
	}
	return rec, nil
}
