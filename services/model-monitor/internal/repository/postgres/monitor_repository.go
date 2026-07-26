// monitor_repository.go — the Postgres adapter for domain.MonitorRepository (the
// monitor CONFIG / write model).
//
// ============================================================================
// WHAT THIS ADAPTER OWNS
// ============================================================================
//
// The `monitors` table: one row per (owner_team, model_name), soft-deleted. The
// service has already set every server-authoritative field (id, owner_team, state,
// timestamps, baseline) before calling — the adapter just persists and reads back.
// The interesting mechanics here are:
//
//   - Upsert keyed by the PARTIAL unique index (owner_team, model_name) WHERE
//     deleted_at IS NULL, with a server-reported `created` flag derived from the
//     row's system xmax (the standard pgx "was this an insert?" trick).
//   - Delete is a SOFT delete that is IDEMPOTENT: deleting an already-deleted (or
//     never-existing) monitor returns nil, so a retried DeleteMonitor is safe.
//   - List is team-scoped keyset pagination with optional state/severity filters,
//     built with PARAMETERIZED placeholders only (no string-concatenated values).
//
// ============================================================================
package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
)

// Compile-time proof the adapter satisfies the domain port. Drift fails the build
// here, not at a call site — the cheapest place to catch a signature mismatch.
var _ domain.MonitorRepository = (*MonitorRepository)(nil)

// monitorColumns is the canonical SELECT column list, written once so every read
// (GetByModel, GetByID, List) scans the SAME order through scanMonitor.
const monitorColumns = `
	id, model_name, owner_team,
	window_duration_ns, window_size, min_samples,
	thresholds,
	auto_retrain, retrain_pipeline_id,
	state, baseline_version, baseline_captured_at,
	created_at, updated_at`

// monitorColumnsM is the same list table-qualified with the `m.` alias, for the
// List query which joins/correlates against drift_reports and must disambiguate
// columns. Kept in lock-step with monitorColumns above (same order ⇒ scanMonitor
// works for both). Written explicitly rather than string-munging the constant so a
// future column add is an obvious two-line edit, not a fragile find-replace.
const monitorColumnsM = `
	m.id, m.model_name, m.owner_team,
	m.window_duration_ns, m.window_size, m.min_samples,
	m.thresholds,
	m.auto_retrain, m.retrain_pipeline_id,
	m.state, m.baseline_version, m.baseline_captured_at,
	m.created_at, m.updated_at`

// scanMonitor reconstructs a domain.Monitor from a row in monitorColumns order. It
// works for BOTH a single-row QueryRow and a List iteration (rowScanner abstracts
// pgx.Row / pgx.Rows). baseline_captured_at is nullable (NULL until a baseline
// loads), so we scan into a *time.Time and map NULL → the zero time the domain uses.
func scanMonitor(row rowScanner) (domain.Monitor, error) {
	var (
		m              domain.Monitor
		thresholdsJSON []byte
		baselineAt     *time.Time
	)
	if err := row.Scan(
		&m.ID, &m.ModelName, &m.OwnerTeam,
		&m.WindowDuration, &m.WindowSize, &m.MinSamples,
		&thresholdsJSON,
		&m.AutoRetrain, &m.RetrainPipelineID,
		&m.State, &m.BaselineVersion, &baselineAt,
		&m.CreatedAt, &m.UpdatedAt,
	); err != nil {
		return domain.Monitor{}, err
	}
	// window_duration_ns is stored as a BIGINT nanosecond count; pgx scans it into
	// the time.Duration field directly (Duration is an int64 under the hood), so no
	// manual conversion is needed.
	if baselineAt != nil {
		m.BaselineCapturedAt = *baselineAt
	}
	thresholds, err := decodeThresholds(thresholdsJSON)
	if err != nil {
		return domain.Monitor{}, err
	}
	m.Thresholds = thresholds
	return m, nil
}

// Upsert inserts or updates a monitor, keyed by (owner_team, model_name) among LIVE
// rows. Returns the stored monitor and `created` true on a fresh insert.
//
// ============================================================================
// THE UPSERT MECHANICS
// ============================================================================
//
//   - ON CONFLICT targets the PARTIAL unique index by repeating its predicate
//     (owner_team, model_name) WHERE deleted_at IS NULL. Postgres requires the
//     index predicate on the conflict clause to UNAMBIGUOUSLY pick the partial
//     index (a plain `(owner_team, model_name)` would not match a partial index).
//
//   - DO UPDATE re-asserts every mutable field from the EXCLUDED (proposed) row, so
//     a re-configure overwrites window shape / thresholds / auto-retrain / the
//     server-set state+baseline+updated_at. created_at is NOT touched on update
//     (it keeps the original) — we only set it on insert.
//
//   - `created` is derived from the system column xmax: for a freshly INSERTED row
//     xmax = 0; for an UPDATED row xmax is the updating transaction's id (non-zero).
//     `(xmax = 0) AS created` is the canonical, race-free way to learn which branch
//     ON CONFLICT took WITHIN the single statement — no second round-trip, no
//     read-then-write window. The whole upsert is therefore ONE atomic statement.
//
//   - Re-inserting over a SOFT-DELETED monitor: the partial index excludes deleted
//     rows, so a (team, model) that was soft-deleted does NOT conflict — the INSERT
//     branch runs and a brand-new live row is created (`created` true), leaving the
//     tombstone for audit. That is the intended "re-configure a previously deleted
//     monitor" behavior.
//
// ============================================================================
func (r *MonitorRepository) Upsert(ctx context.Context, m domain.Monitor) (domain.Monitor, bool, error) {
	thresholdsJSON, err := encodeThresholds(m.Thresholds)
	if err != nil {
		return domain.Monitor{}, false, err
	}
	// baseline_captured_at: pass NULL for the zero time so a not-yet-resolved
	// baseline stores as NULL (and round-trips back to the zero time), not epoch.
	var baselineAt *time.Time
	if !m.BaselineCapturedAt.IsZero() {
		baselineAt = &m.BaselineCapturedAt
	}

	const q = `
		INSERT INTO monitors (
			id, model_name, owner_team,
			window_duration_ns, window_size, min_samples,
			thresholds,
			auto_retrain, retrain_pipeline_id,
			state, baseline_version, baseline_captured_at,
			created_at, updated_at
		) VALUES (
			$1, $2, $3,
			$4, $5, $6,
			$7,
			$8, $9,
			$10, $11, $12,
			$13, $14
		)
		ON CONFLICT (owner_team, model_name) WHERE deleted_at IS NULL
		DO UPDATE SET
			window_duration_ns  = EXCLUDED.window_duration_ns,
			window_size         = EXCLUDED.window_size,
			min_samples         = EXCLUDED.min_samples,
			thresholds          = EXCLUDED.thresholds,
			auto_retrain        = EXCLUDED.auto_retrain,
			retrain_pipeline_id = EXCLUDED.retrain_pipeline_id,
			state               = EXCLUDED.state,
			baseline_version    = EXCLUDED.baseline_version,
			baseline_captured_at= EXCLUDED.baseline_captured_at,
			updated_at          = EXCLUDED.updated_at
		RETURNING ` + monitorColumns + `, (xmax = 0) AS created`

	var (
		stored         domain.Monitor
		thresholdsRead []byte
		baselineRead   *time.Time
		created        bool
	)
	err = r.pool.QueryRow(ctx, q,
		m.ID, m.ModelName, m.OwnerTeam,
		int64(m.WindowDuration), m.WindowSize, m.MinSamples,
		thresholdsJSON,
		m.AutoRetrain, m.RetrainPipelineID,
		int16(m.State), m.BaselineVersion, baselineAt,
		m.CreatedAt, m.UpdatedAt,
	).Scan(
		&stored.ID, &stored.ModelName, &stored.OwnerTeam,
		&stored.WindowDuration, &stored.WindowSize, &stored.MinSamples,
		&thresholdsRead,
		&stored.AutoRetrain, &stored.RetrainPipelineID,
		&stored.State, &stored.BaselineVersion, &baselineRead,
		&stored.CreatedAt, &stored.UpdatedAt,
		&created,
	)
	if err != nil {
		return domain.Monitor{}, false, fmt.Errorf("upsert monitor: %w", err)
	}
	if baselineRead != nil {
		stored.BaselineCapturedAt = *baselineRead
	}
	thresholds, err := decodeThresholds(thresholdsRead)
	if err != nil {
		return domain.Monitor{}, false, err
	}
	stored.Thresholds = thresholds
	return stored, created, nil
}

// GetByModel returns a team's LIVE monitor for a model, or ErrRepoNotFound.
// Team-scoped by construction: (owner_team, model_name) is the lookup key, so a
// same-named model in another team is invisible here.
func (r *MonitorRepository) GetByModel(ctx context.Context, ownerTeam, modelName string) (domain.Monitor, error) {
	const q = `SELECT ` + monitorColumns + `
		FROM monitors
		WHERE owner_team = $1 AND model_name = $2 AND deleted_at IS NULL`
	m, err := scanMonitor(r.pool.QueryRow(ctx, q, ownerTeam, modelName))
	if err != nil {
		if isNoRows(err) {
			return domain.Monitor{}, domain.ErrRepoNotFound
		}
		return domain.Monitor{}, fmt.Errorf("get monitor by model: %w", err)
	}
	return m, nil
}

// GetByID returns a LIVE monitor by its server-assigned id, or ErrRepoNotFound.
//
// WHY NOT team-scoped here (unlike the report GetByID): this is a DATA-PLANE lookup.
// The events adapter has ALREADY resolved model→owning-team→monitor-id before the
// scorer loads the config by id, so the tenancy decision was made upstream; the id
// is an internal handle, not a client-supplied value at this point. (The monitor's
// owner_team is on the returned row, so any caller that needs to re-assert tenancy
// still can.) The control-plane reads (GetMonitorStatus etc.) go through GetByModel,
// which IS team-scoped.
func (r *MonitorRepository) GetByID(ctx context.Context, id string) (domain.Monitor, error) {
	const q = `SELECT ` + monitorColumns + `
		FROM monitors
		WHERE id = $1 AND deleted_at IS NULL`
	m, err := scanMonitor(r.pool.QueryRow(ctx, q, id))
	if err != nil {
		if isNoRows(err) {
			return domain.Monitor{}, domain.ErrRepoNotFound
		}
		return domain.Monitor{}, fmt.Errorf("get monitor by id: %w", err)
	}
	return m, nil
}

// Delete SOFT-deletes a team's monitor for a model (sets deleted_at = now). Drift
// history is retained (the caller purges reports separately via PurgeByModel).
//
// IDEMPOTENT: the UPDATE only matches LIVE rows (deleted_at IS NULL). A second
// delete (or a delete of a never-existed monitor) matches zero rows and returns nil
// — so a retried DeleteMonitor is a safe no-op, not an error. WHY return nil rather
// than ErrRepoNotFound on zero rows: the desired end state ("this monitor is gone")
// already holds, and surfacing not-found would make a benign retry look like a
// failure (and could trigger a harmful client retry loop). Idempotent delete is the
// contract the domain documents.
func (r *MonitorRepository) Delete(ctx context.Context, ownerTeam, modelName string) error {
	const q = `
		UPDATE monitors
		SET deleted_at = now(), updated_at = now()
		WHERE owner_team = $1 AND model_name = $2 AND deleted_at IS NULL`
	if _, err := r.pool.Exec(ctx, q, ownerTeam, modelName); err != nil {
		return fmt.Errorf("delete monitor: %w", err)
	}
	return nil
}

// List returns a page of the team's LIVE monitors (newest-first) under optional
// state / min-severity filters, plus a next-page cursor.
//
// ============================================================================
// TEAM SCOPING + SAFE DYNAMIC SQL
// ============================================================================
//
// owner_team is ALWAYS the first predicate (the service set it from auth claims), so
// the list can never read another tenant's fleet. The optional filters and the
// keyset cursor are appended by building only PLACEHOLDER text ($N) with an
// incrementing index — the VALUES are always bound parameters in `args`, never
// concatenated into the query string. So even though the WHERE clause is assembled
// dynamically, SQL injection is structurally impossible.
//
// MIN-SEVERITY ON A CONFIG ROW: a monitor row has no "severity" of its own (severity
// is a property of a REPORT). The FleetFilter.MinSeverity is interpreted as "only
// monitors whose LATEST report is at least this severe". WHY a correlated subquery
// rather than a stored column: a monitor's worst-current severity changes on every
// window close; denormalizing it onto the config row would be a write on the hot
// scoring path. The fleet list is a low-frequency control-plane read, so we compute
// it on read via a LATERAL-style scalar subquery against drift_reports (team-scoped,
// newest window). MinSeverity == Unspecified (0) skips the filter entirely.
// ============================================================================
func (r *MonitorRepository) List(ctx context.Context, f domain.FleetFilter, opts domain.ListOptions) ([]domain.Monitor, string, error) {
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	cur, err := decodeListCursor(opts.PageToken)
	if err != nil {
		return nil, "", err
	}

	// args accumulates the bound parameters; conds the placeholder predicates.
	args := []any{f.OwnerTeam}
	conds := []string{"m.owner_team = $1", "m.deleted_at IS NULL"}
	next := func(v any) string { args = append(args, v); return fmt.Sprintf("$%d", len(args)) }

	if f.State != domain.MonitorStateUnspecified {
		conds = append(conds, "m.state = "+next(int16(f.State)))
	}
	if f.MinSeverity != domain.DriftSeverityUnspecified {
		// "latest report's severity >= MinSeverity", computed per-row. The subquery
		// is team+model scoped so it can't read another tenant's verdicts.
		ph := next(int16(f.MinSeverity))
		conds = append(conds, `COALESCE((
			SELECT dr.severity FROM drift_reports dr
			WHERE dr.owner_team = m.owner_team AND dr.model_name = m.model_name
			ORDER BY dr.window_end DESC, dr.id DESC
			LIMIT 1
		), 0) >= `+ph)
	}
	// Keyset cursor: rows strictly BEFORE (created_at, id) of the last page (DESC).
	// The (created_at, id) tuple is the table's total order, so the row comparison
	// `(created_at, id) < ($ts, $id)` is a single, index-friendly predicate.
	if cur != nil {
		tsPH := next(cur.TS)
		idPH := next(cur.ID)
		conds = append(conds, fmt.Sprintf("(m.created_at, m.id) < (%s, %s)", tsPH, idPH))
	}
	// LIMIT pageSize+1: we fetch ONE extra row to know whether a next page exists
	// WITHOUT a second COUNT query. If we get pageSize+1 back, there's more — we drop
	// the extra and emit a cursor from the last KEPT row.
	limitPH := next(pageSize + 1)

	q := `SELECT ` + monitorColumnsM + `
		FROM monitors m
		WHERE ` + strings.Join(conds, " AND ") + `
		ORDER BY m.created_at DESC, m.id DESC
		LIMIT ` + limitPH

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list monitors: %w", err)
	}
	defer rows.Close()

	out := make([]domain.Monitor, 0, pageSize)
	for rows.Next() {
		m, err := scanMonitor(rows)
		if err != nil {
			return nil, "", fmt.Errorf("scan monitor row: %w", err)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate monitors: %w", err)
	}

	// Trim the sentinel extra row and, if it existed, emit a cursor anchored on the
	// LAST returned row so the next page continues strictly before it.
	var nextToken string
	if len(out) > pageSize {
		out = out[:pageSize]
		last := out[len(out)-1]
		nextToken = encodeListCursor(listCursor{TS: last.CreatedAt, ID: last.ID})
	}
	return out, nextToken, nil
}
