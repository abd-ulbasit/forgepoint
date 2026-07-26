// drift_report_repository.go — the Postgres adapter for
// domain.DriftReportRepository (the durable drift-verdict history).
//
// ============================================================================
// THE TWO DEFINING MECHANICS
// ============================================================================
//
// 1) IDEMPOTENT Save on window_id — the persistence half of exactly-once.
//    Each closed window has a stable UUID (Window.ID). Save does
//    INSERT … ON CONFLICT (window_id) DO NOTHING RETURNING *. A FRESH window
//    inserts and RETURNING yields the row (inserted=true). A REDELIVERED /
//    double-scored window hits the conflict, DO NOTHING suppresses the write, and
//    RETURNING yields ZERO rows — so we refetch the EXISTING row by window_id and
//    return it with inserted=false. The caller emits the ModelDriftDetected event
//    ONLY on inserted=true, so the event fires exactly once per window even under
//    stream redelivery. This is the same idempotency-key discipline every consumer
//    on the platform follows.
//
// 2) TENANCY on EVERY read/purge — owner_team is part of the key, never optional.
//    Because monitors are keyed by (owner_team, model_name), a model name is not
//    globally unique. So List/LatestByModel filter on (owner_team, model_name)
//    TOGETHER, GetByID filters on (id, owner_team) TOGETHER, and PurgeByModel
//    deletes on (owner_team, model_name) TOGETHER. A guessed/leaked report id from
//    another tenant returns ErrRepoNotFound — INDISTINGUISHABLE from "no such id"
//    — so an attacker gets no enumeration oracle (anti-IDOR), and a purge can never
//    destroy another team's same-named model's history.
// ============================================================================
package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
)

// Compile-time proof the adapter satisfies the domain port.
var _ domain.DriftReportRepository = (*DriftReportRepository)(nil)

// reportColumns is the canonical SELECT column list for a report, scanned through
// scanReport in this exact order by every read path.
const reportColumns = `
	id, monitor_id, owner_team, model_name, model_version,
	drift_type, severity, metrics,
	window_id, sample_count, window_start, window_end, created_at`

// scanReport reconstructs a domain.DriftReport from a row in reportColumns order.
// Works for both QueryRow and Rows iteration via the rowScanner abstraction.
func scanReport(row rowScanner) (domain.DriftReport, error) {
	var (
		rep         domain.DriftReport
		metricsJSON []byte
	)
	if err := row.Scan(
		&rep.ID, &rep.MonitorID, &rep.OwnerTeam, &rep.ModelName, &rep.ModelVersion,
		&rep.DriftType, &rep.Severity, &metricsJSON,
		&rep.WindowID, &rep.SampleCount, &rep.WindowStart, &rep.WindowEnd, &rep.CreatedAt,
	); err != nil {
		return domain.DriftReport{}, err
	}
	metrics, err := decodeMetrics(metricsJSON)
	if err != nil {
		return domain.DriftReport{}, err
	}
	rep.Metrics = metrics
	return rep, nil
}

// Save persists a report IDEMPOTENTLY on its WindowID. Returns the stored report and
// `inserted` (true on a fresh insert, false if a report for that window already
// existed). See the package-doc mechanic #1 for the ON CONFLICT … RETURNING flow.
//
// The FK on monitor_id means a report for a monitor that doesn't exist (or was
// hard-deleted out from under a slow scorer) fails with a foreign_key_violation,
// which we map to ErrRepoNotFound so the service reports the missing monitor rather
// than a raw DB error.
func (r *DriftReportRepository) Save(ctx context.Context, rep domain.DriftReport) (domain.DriftReport, bool, error) {
	metricsJSON, err := encodeMetrics(rep.Metrics)
	if err != nil {
		return domain.DriftReport{}, false, err
	}

	const insertQ = `
		INSERT INTO drift_reports (
			id, monitor_id, owner_team, model_name, model_version,
			drift_type, severity, metrics,
			window_id, sample_count, window_start, window_end, created_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8,
			$9, $10, $11, $12, $13
		)
		ON CONFLICT (window_id) DO NOTHING
		RETURNING ` + reportColumns

	stored, err := scanReport(r.pool.QueryRow(ctx, insertQ,
		rep.ID, rep.MonitorID, rep.OwnerTeam, rep.ModelName, rep.ModelVersion,
		int16(rep.DriftType), int16(rep.Severity), metricsJSON,
		rep.WindowID, rep.SampleCount, rep.WindowStart, rep.WindowEnd, rep.CreatedAt,
	))
	if err == nil {
		// RETURNING produced a row ⇒ this was a fresh INSERT.
		return stored, true, nil
	}
	if isForeignKeyViolation(err) {
		// monitor_id references a monitor that doesn't exist (or was hard-deleted).
		return domain.DriftReport{}, false, domain.ErrRepoNotFound
	}
	if !isNoRows(err) {
		return domain.DriftReport{}, false, fmt.Errorf("insert drift report: %w", err)
	}

	// isNoRows: the ON CONFLICT DO NOTHING suppressed the write (a report for this
	// window_id already exists). Refetch the EXISTING row by its idempotency key and
	// return inserted=false — the caller must NOT re-emit the drift event.
	existing, err := r.byWindowID(ctx, rep.WindowID)
	if err != nil {
		return domain.DriftReport{}, false, fmt.Errorf("refetch existing report on conflict: %w", err)
	}
	return existing, false, nil
}

// byWindowID is the conflict-path refetch. window_id is UNIQUE, so this resolves to
// exactly the row the INSERT collided with. NOT team-scoped: the caller (Save) is on
// the scoring path with a report it built from an already-resolved monitor, so the
// window_id is its own — there is no cross-tenant exposure (a window_id is never
// supplied by a client to this method).
func (r *DriftReportRepository) byWindowID(ctx context.Context, windowID string) (domain.DriftReport, error) {
	const q = `SELECT ` + reportColumns + ` FROM drift_reports WHERE window_id = $1`
	rep, err := scanReport(r.pool.QueryRow(ctx, q, windowID))
	if err != nil {
		if isNoRows(err) {
			return domain.DriftReport{}, domain.ErrRepoNotFound
		}
		return domain.DriftReport{}, err
	}
	return rep, nil
}

// GetByID returns a report by id SCOPED to ownerTeam, or ErrRepoNotFound. The
// (id, owner_team) predicate is the anti-IDOR control: a report id is not secret (it
// rides on events, retrain requests, deep links, logs), so id ALONE is not
// authorization. A tenant mismatch returns ErrRepoNotFound — indistinguishable from
// a non-existent id — so a caller cannot probe which ids exist in other tenants.
func (r *DriftReportRepository) GetByID(ctx context.Context, ownerTeam, id string) (domain.DriftReport, error) {
	const q = `SELECT ` + reportColumns + `
		FROM drift_reports
		WHERE id = $1 AND owner_team = $2`
	rep, err := scanReport(r.pool.QueryRow(ctx, q, id, ownerTeam))
	if err != nil {
		if isNoRows(err) {
			return domain.DriftReport{}, domain.ErrRepoNotFound
		}
		return domain.DriftReport{}, fmt.Errorf("get drift report by id: %w", err)
	}
	return rep, nil
}

// LatestByModel returns the most recent report (by window_end) for a team's model,
// or ErrRepoNotFound if none yet. Scoped by (owner_team, model_name) so a same-named
// model in another team cannot bleed its latest verdict into this team's health
// tile. The drift_reports_team_model_window_idx makes this an index-backed LIMIT 1.
func (r *DriftReportRepository) LatestByModel(ctx context.Context, ownerTeam, modelName string) (domain.DriftReport, error) {
	const q = `SELECT ` + reportColumns + `
		FROM drift_reports
		WHERE owner_team = $1 AND model_name = $2
		ORDER BY window_end DESC, id DESC
		LIMIT 1`
	rep, err := scanReport(r.pool.QueryRow(ctx, q, ownerTeam, modelName))
	if err != nil {
		if isNoRows(err) {
			return domain.DriftReport{}, domain.ErrRepoNotFound
		}
		return domain.DriftReport{}, fmt.Errorf("latest report by model: %w", err)
	}
	return rep, nil
}

// List returns a page of a (team, model)'s reports (newest window_end first) under
// the filter, plus a next-page cursor.
//
// The mandatory scope is owner_team (set by the service from auth claims). model_name
// is an OPTIONAL filter — set narrows to one model, empty spans all of the team's
// models (the fleet "recent drift" view). Other optional filters: MinSeverity (>=),
// Since (window_end >= ), Until (window_end <= ). Every value is a BOUND parameter;
// the dynamic part is only the placeholder text, so SQL injection is impossible even
// with the assembled WHERE. Keyset cursor rides (window_end, id) — the index's total
// order — so pages are stable under the continuous insert of new reports.
func (r *DriftReportRepository) List(ctx context.Context, f domain.ReportFilter, opts domain.ListOptions) ([]domain.DriftReport, string, error) {
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	cur, err := decodeListCursor(opts.PageToken)
	if err != nil {
		return nil, "", err
	}

	// owner_team is the MANDATORY tenant scope — always bound, always first. model_name
	// is now OPTIONAL: when set it narrows to one model; when empty the predicate is
	// omitted so the page spans ALL of the team's models (the fleet "recent drift" view).
	// Tenancy is unaffected — owner_team is still the keyed scope, so an empty model_name
	// can never widen past this team. The keyset cursor still rides (window_end, id), so
	// the cross-model page is ordered newest-first and paginates stably.
	args := []any{f.OwnerTeam}
	conds := []string{"owner_team = $1"}
	next := func(v any) string { args = append(args, v); return fmt.Sprintf("$%d", len(args)) }

	if f.ModelName != "" {
		conds = append(conds, "model_name = "+next(f.ModelName))
	}
	if f.MinSeverity != domain.DriftSeverityUnspecified {
		conds = append(conds, "severity >= "+next(int16(f.MinSeverity)))
	}
	if !f.Since.IsZero() {
		conds = append(conds, "window_end >= "+next(f.Since))
	}
	if !f.Until.IsZero() {
		conds = append(conds, "window_end <= "+next(f.Until))
	}
	if cur != nil {
		tsPH := next(cur.TS)
		idPH := next(cur.ID)
		conds = append(conds, fmt.Sprintf("(window_end, id) < (%s, %s)", tsPH, idPH))
	}
	limitPH := next(pageSize + 1) // +1 sentinel to detect a next page without COUNT

	q := `SELECT ` + reportColumns + `
		FROM drift_reports
		WHERE ` + strings.Join(conds, " AND ") + `
		ORDER BY window_end DESC, id DESC
		LIMIT ` + limitPH

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list drift reports: %w", err)
	}
	defer rows.Close()

	out := make([]domain.DriftReport, 0, pageSize)
	for rows.Next() {
		rep, err := scanReport(rows)
		if err != nil {
			return nil, "", fmt.Errorf("scan drift report row: %w", err)
		}
		out = append(out, rep)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate drift reports: %w", err)
	}

	var nextToken string
	if len(out) > pageSize {
		out = out[:pageSize]
		last := out[len(out)-1]
		nextToken = encodeListCursor(listCursor{TS: last.WindowEnd, ID: last.ID})
	}
	return out, nextToken, nil
}

// PurgeByModel hard-deletes a team's reports for a model and returns how many were
// removed. Used by DeleteMonitor(purge_reports=true) for GDPR / test cleanup.
//
// Scoped by (owner_team, model_name): deleting team-a's "fraud" reports MUST NOT
// touch team-b's "fraud" history. pgx's CommandTag.RowsAffected gives the exact
// purge count the service surfaces back to the caller. A purge of a model with no
// reports returns 0, nil — a benign no-op, not an error.
func (r *DriftReportRepository) PurgeByModel(ctx context.Context, ownerTeam, modelName string) (int, error) {
	const q = `DELETE FROM drift_reports WHERE owner_team = $1 AND model_name = $2`
	tag, err := r.pool.Exec(ctx, q, ownerTeam, modelName)
	if err != nil {
		return 0, fmt.Errorf("purge reports by model: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
