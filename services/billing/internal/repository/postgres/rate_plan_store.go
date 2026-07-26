// rate_plan_store.go — the Postgres adapter for domain.RatePlanStore.
//
// ============================================================================
// THE SERVER-AUTHORITATIVE PRICE SOURCE (security-critical persistence)
// ============================================================================
//
// Every price the billing engine applies comes from a rate plan READ FROM HERE —
// a client never supplies a price. So this adapter's correctness is a security
// property, not just a data property: if ResolvePlanForTeam returned the wrong
// plan, a tenant would be mispriced. The reads are plain parameterized SELECTs;
// the price maps round-trip through the JSONB codec in mapping.go.
//
// IDEMPOTENT CREATE: CreatePlan persists the plan AND (when a key is supplied) an
// idempotency mapping row in ONE tx, so a retry resolves to the same single plan.
// The UNIQUE(idempotency_key) on rate_plan_idempotency is the DB-side race winner:
// the loser of a concurrent double-create hits 23505 → ErrRepoDuplicate, and the
// service re-reads the winner via FindPlanByIdempotencyKey.
// ============================================================================
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
)

// Compile-time proof the adapter satisfies the domain port. Signature drift fails
// the BUILD here, not a call site later.
var _ domain.RatePlanStore = (*RatePlanStore)(nil)

// CreatePlan inserts a new RatePlan (id/created_at already set by the service) and,
// when idempotencyKey != "", a rate_plan_idempotency mapping — both in ONE tx so a
// crash never leaves a plan without its key mapping (or vice versa). Returns
// domain.ErrRepoDuplicate if the idempotency key was already used (the unique
// index rejected the mapping insert) so CreateRatePlan can return the original.
func (s *RatePlanStore) CreatePlan(ctx context.Context, plan domain.RatePlan, idempotencyKey string) (domain.RatePlan, error) {
	unitPricesJSON, err := marshalUnitPrices(plan.UnitPrices)
	if err != nil {
		return domain.RatePlan{}, err
	}
	includedJSON, err := marshalQtyMap(plan.IncludedQuantities)
	if err != nil {
		return domain.RatePlan{}, err
	}
	quotaJSON, err := marshalQtyMap(plan.QuotaLimits)
	if err != nil {
		return domain.RatePlan{}, err
	}

	// One tx for the plan row + the optional idempotency mapping. pgx.BeginFunc
	// runs the closure inside BEGIN…COMMIT and ROLLBACKs automatically if it (or
	// the commit) returns an error — so a failed key insert undoes the plan insert.
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		const insertPlan = `
			INSERT INTO rate_plans
				(id, name, unit_prices, included_quantities, quota_limits, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)`
		if _, err := tx.Exec(ctx, insertPlan,
			plan.ID, plan.Name, unitPricesJSON, includedJSON, quotaJSON, plan.CreatedAt,
		); err != nil {
			return err
		}

		if idempotencyKey != "" {
			const insertKey = `
				INSERT INTO rate_plan_idempotency (idempotency_key, rate_plan_id)
				VALUES ($1, $2)`
			if _, err := tx.Exec(ctx, insertKey, idempotencyKey, plan.ID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		// A unique_violation here is the idempotency-key collision (the only unique
		// constraint in this tx besides the PK). Map it to the domain duplicate
		// sentinel so the service re-reads the winner; the PK collision (a UUID
		// clash) is astronomically unlikely and also surfaces as a duplicate.
		if isUniqueViolation(err) {
			return domain.RatePlan{}, domain.ErrRepoDuplicate
		}
		return domain.RatePlan{}, fmt.Errorf("create rate plan: %w", err)
	}

	return plan, nil
}

// FindPlanByIdempotencyKey returns the plan previously created under this key, or
// domain.ErrRepoNotFound. It JOINs the mapping to the plan so a single round-trip
// both resolves the key and hydrates the plan.
func (s *RatePlanStore) FindPlanByIdempotencyKey(ctx context.Context, idempotencyKey string) (domain.RatePlan, error) {
	const q = `
		SELECT p.id, p.name, p.unit_prices, p.included_quantities, p.quota_limits, p.created_at
		FROM rate_plan_idempotency k
		JOIN rate_plans p ON p.id = k.rate_plan_id
		WHERE k.idempotency_key = $1`
	return s.scanPlan(ctx, s.pool.QueryRow(ctx, q, idempotencyKey))
}

// GetPlan returns a plan by id, or domain.ErrRepoNotFound.
func (s *RatePlanStore) GetPlan(ctx context.Context, id string) (domain.RatePlan, error) {
	const q = `
		SELECT id, name, unit_prices, included_quantities, quota_limits, created_at
		FROM rate_plans
		WHERE id = $1`
	return s.scanPlan(ctx, s.pool.QueryRow(ctx, q, id))
}

// ResolvePlanForTeam returns the plan currently assigned to a team — the price
// source the metering path reads. JOINs team_rate_plans → rate_plans so the
// assignment lookup and the plan hydration are one query. Returns
// domain.ErrRepoNotFound when the team has no plan (the service maps that to
// ErrRatePlanNotFound → FailedPrecondition: we cannot price without a plan).
func (s *RatePlanStore) ResolvePlanForTeam(ctx context.Context, team string) (domain.RatePlan, error) {
	const q = `
		SELECT p.id, p.name, p.unit_prices, p.included_quantities, p.quota_limits, p.created_at
		FROM team_rate_plans t
		JOIN rate_plans p ON p.id = t.rate_plan_id
		WHERE t.team = $1`
	return s.scanPlan(ctx, s.pool.QueryRow(ctx, q, team))
}

// AssignPlanToTeam UPSERTs the team→plan binding. This is NOT a domain.RatePlanStore
// port method — the binding is created by an admin/onboarding flow, not the metering
// path — but the adapter exposes it so the composition root (and the integration
// tests) can establish a team's plan before metering. UPSERT (ON CONFLICT) makes
// reassigning a team's plan a single idempotent statement.
func (s *RatePlanStore) AssignPlanToTeam(ctx context.Context, team, ratePlanID string) error {
	const q = `
		INSERT INTO team_rate_plans (team, rate_plan_id)
		VALUES ($1, $2)
		ON CONFLICT (team) DO UPDATE SET rate_plan_id = EXCLUDED.rate_plan_id, assigned_at = now()`
	if _, err := s.pool.Exec(ctx, q, team, ratePlanID); err != nil {
		return fmt.Errorf("assign plan to team: %w", err)
	}
	return nil
}

// scanPlan decodes one rate_plans row (in the SELECT column order used by every
// query above) into a domain.RatePlan, hydrating the three JSONB maps. It accepts
// a pgx.Row (the single-row QueryRow result) so all four read paths share one
// scan + JSONB-decode site — column order and map decoding are written once.
func (s *RatePlanStore) scanPlan(_ context.Context, row pgx.Row) (domain.RatePlan, error) {
	var (
		plan        domain.RatePlan
		unitPricesB []byte
		includedB   []byte
		quotaB      []byte
	)
	if err := row.Scan(&plan.ID, &plan.Name, &unitPricesB, &includedB, &quotaB, &plan.CreatedAt); err != nil {
		if isNoRows(err) {
			return domain.RatePlan{}, domain.ErrRepoNotFound
		}
		return domain.RatePlan{}, fmt.Errorf("scan rate plan: %w", err)
	}

	var err error
	if plan.UnitPrices, err = unmarshalUnitPrices(unitPricesB); err != nil {
		return domain.RatePlan{}, err
	}
	if plan.IncludedQuantities, err = unmarshalQtyMap(includedB); err != nil {
		return domain.RatePlan{}, err
	}
	if plan.QuotaLimits, err = unmarshalQtyMap(quotaB); err != nil {
		return domain.RatePlan{}, err
	}
	return plan, nil
}
