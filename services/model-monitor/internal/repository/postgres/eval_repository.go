// eval_repository.go — the Postgres adapter for domain.EvalStore (the M7/L4 LLM
// quality-evaluation score log, eval_scores table).
//
// ============================================================================
// THE TWO MECHANICS (mirroring drift_report_repository's discipline)
// ============================================================================
//
//  1. IDEMPOTENT Record on request_id — a redelivered fp.ai.completion.served event
//     must not record the same judged completion twice (which would double-count it
//     in the rolling quality average and could nudge a false drift). Record does
//     INSERT … ON CONFLICT (request_id) DO NOTHING, so a second Record of the same
//     request_id is a silent no-op. request_id is the gateway-minted server-
//     authoritative id (never a user id), so it is a safe natural dedup key.
//
//  2. TENANCY on the read — the rolling-average read (RecentOverall) filters on
//     (team, model) TOGETHER, never model alone. A model name is not globally unique
//     across teams (team-a/"chatbot" ≠ team-b/"chatbot"), so an unscoped read would
//     average two teams' quality together and could trip a cross-tenant false drift.
//     Every value is a BOUND parameter ($1,$2,…) — no string concatenation of caller
//     input, so SQL injection is structurally impossible.
package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EvalRepository is the Postgres adapter for domain.EvalStore. It holds the shared
// pool (the same pool the monitor/report adapters use — one Postgres database per
// service).
type EvalRepository struct{ pool *pgxpool.Pool }

// Evals returns the adapter implementing domain.EvalStore.
func (s *Store) Evals() *EvalRepository { return &EvalRepository{pool: s.pool} }

// Compile-time proof the adapter satisfies the domain port. A signature drift fails
// the build here — the cheapest contract test.
var _ domain.EvalStore = (*EvalRepository)(nil)

// Record persists one eval row (scored or unscored) IDEMPOTENTLY on request_id.
//
// We persist UNSCORED rows too (scored=FALSE, 0 axes) so the VOLUME of un-judgeable
// traffic is observable — a rising unscored ratio means the judge model is failing
// or the gateway is sending no text. The CHECK constraint on the table allows 0s
// only when scored=false, so a scored row is guaranteed to carry sane 1–5 axes.
//
// ON CONFLICT (request_id) DO NOTHING makes a redelivered completion a no-op: the
// row already exists, the write is suppressed, and we return nil (success) — exactly
// the consumer-idempotency contract.
func (r *EvalRepository) Record(ctx context.Context, e domain.Eval) error {
	const q = `
		INSERT INTO eval_scores (
			request_id, team, model,
			relevance, coherence, safety, overall, scored,
			created_at
		) VALUES (
			$1, $2, $3,
			$4, $5, $6, $7, $8,
			$9
		)
		ON CONFLICT (request_id) DO NOTHING`

	_, err := r.pool.Exec(ctx, q,
		e.RequestID, e.Team, e.Model,
		e.Scores.Relevance, e.Scores.Coherence, e.Scores.Safety, e.Scores.Overall, e.Scores.Scored,
		e.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("record eval (request_id=%s): %w", e.RequestID, err)
	}
	return nil
}

// RecentOverall returns the OVERALL scores of the most recent SCORED evals for a
// (team, model), newest first, up to `limit`. UNSCORED rows are excluded (WHERE
// scored = TRUE) — they carry no meaningful overall and must not skew the rolling
// average. This is the input to domain.EvaluateQualityDrift.
//
// The eval_scores_team_model_created_idx (partial on scored=TRUE) backs the
// (team, model) filter + ORDER BY created_at DESC + LIMIT as an index range scan.
func (r *EvalRepository) RecentOverall(ctx context.Context, team, model string, limit int) ([]float64, error) {
	if limit <= 0 {
		return nil, nil
	}
	const q = `
		SELECT overall
		FROM eval_scores
		WHERE team = $1 AND model = $2 AND scored = TRUE
		ORDER BY created_at DESC
		LIMIT $3`

	rows, err := r.pool.Query(ctx, q, team, model, limit)
	if err != nil {
		return nil, fmt.Errorf("recent overall (team=%s, model=%s): %w", team, model, err)
	}
	defer rows.Close()

	out := make([]float64, 0, limit)
	for rows.Next() {
		var v float64
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("scan eval overall: %w", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate eval overall: %w", err)
	}
	return out, nil
}

// evalColumns is the canonical SELECT column list for an eval ROW, scanned through
// scanEval in this exact order by ListScores. Note there is NO `id` column —
// request_id is the table's PRIMARY KEY, so it doubles as the keyset tiebreaker.
const evalColumns = `team, model, request_id, relevance, coherence, safety, overall, scored, created_at`

// scanEval reconstructs a domain.Eval from a row in evalColumns order. The three axes +
// overall are stored DOUBLE PRECISION (1–5); scored distinguishes a judged row from an
// unscored one (axes all 0).
func scanEval(row rowScanner) (domain.Eval, error) {
	var e domain.Eval
	if err := row.Scan(
		&e.Team, &e.Model, &e.RequestID,
		&e.Scores.Relevance, &e.Scores.Coherence, &e.Scores.Safety, &e.Scores.Overall, &e.Scores.Scored,
		&e.CreatedAt,
	); err != nil {
		return domain.Eval{}, err
	}
	return e, nil
}

// ListScores returns a page of a team's eval ROWS (scored AND unscored), newest first,
// under the filter, plus an opaque next-page cursor — the read model behind the L4 eval
// dashboard.
//
// TENANCY (mirrors drift_report_repository.List): team is the MANDATORY scope (set by the
// service from auth claims), always bound first. model is an OPTIONAL filter — set narrows
// to one model, empty spans ALL of the team's models (the default cross-model dashboard
// view). since is an OPTIONAL created_at lower bound. EVERY value is a BOUND parameter
// ($1,$2,…); only placeholder TEXT is assembled into the WHERE, so SQL injection is
// structurally impossible even with the dynamic predicate list.
//
// KEYSET PAGINATION: the cursor rides (created_at, request_id). created_at alone is not
// unique (two completions judged the same instant), so we append request_id — the table's
// PRIMARY KEY — to make the sort key TOTAL and the "strictly before the cursor" boundary
// exact (no skipped/duplicated rows at a tie). DESC on both gives newest-first. Unlike
// RecentOverall, we DO NOT filter scored = TRUE: the dashboard wants un-judgeable traffic
// visible too, so unscored rows are included.
func (r *EvalRepository) ListScores(ctx context.Context, f domain.EvalFilter, opts domain.ListOptions) ([]domain.Eval, string, error) {
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	cur, err := decodeListCursor(opts.PageToken)
	if err != nil {
		return nil, "", err
	}

	// team is the mandatory tenant scope — always $1. model/since are optional; the
	// keyset predicate (when paging) is appended last. next() binds a value and returns
	// its positional placeholder, so caller input is NEVER concatenated into the SQL.
	args := []any{f.Team}
	conds := []string{"team = $1"}
	next := func(v any) string { args = append(args, v); return fmt.Sprintf("$%d", len(args)) }

	if f.Model != "" {
		conds = append(conds, "model = "+next(f.Model))
	}
	if !f.Since.IsZero() {
		conds = append(conds, "created_at >= "+next(f.Since))
	}
	if cur != nil {
		tsPH := next(cur.TS)
		idPH := next(cur.ID) // the keyset tiebreaker is request_id (the PK), carried in cursor.ID
		conds = append(conds, fmt.Sprintf("(created_at, request_id) < (%s, %s)", tsPH, idPH))
	}
	limitPH := next(pageSize + 1) // +1 sentinel to detect a next page without a COUNT

	q := `SELECT ` + evalColumns + `
		FROM eval_scores
		WHERE ` + strings.Join(conds, " AND ") + `
		ORDER BY created_at DESC, request_id DESC
		LIMIT ` + limitPH

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list eval scores: %w", err)
	}
	defer rows.Close()

	out := make([]domain.Eval, 0, pageSize)
	for rows.Next() {
		e, err := scanEval(rows)
		if err != nil {
			return nil, "", fmt.Errorf("scan eval score row: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate eval scores: %w", err)
	}

	var nextToken string
	if len(out) > pageSize {
		out = out[:pageSize]
		last := out[len(out)-1]
		// The cursor carries the last row's (created_at, request_id) — the exact keyset
		// position the next page resumes strictly before.
		nextToken = encodeListCursor(listCursor{TS: last.CreatedAt, ID: last.RequestID})
	}
	return out, nextToken, nil
}
