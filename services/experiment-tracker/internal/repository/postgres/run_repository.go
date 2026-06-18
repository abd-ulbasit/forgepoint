// run_repository.go — the Postgres adapter for domain.RunRepository.
//
// ============================================================================
// THE RUN AGGREGATE STORE (run + params + metric time-series)
// ============================================================================
//
// A Run, its params, and its metric points form one aggregate. This adapter owns
// all three tables (runs, run_params, run_metrics) plus the read paths the
// analysis RPCs need. The interesting persistence semantics:
//
//   AppendMetrics   — the HIGH-THROUGHPUT write the whole event-driven pattern
//                     optimizes. ONE multi-row INSERT per batch with
//                     ON CONFLICT (run_id,key,step) DO NOTHING: a retried/
//                     redelivered batch double-writes NOTHING, and the returned
//                     row count is exactly how many points were NEW
//                     (accepted_count). One INSERT, not one-per-point.
//
//   UpdateRunStatus — the terminal transition + the CQRS projection in ONE write:
//                     status, ended_at, and the denormalized final_metrics JSONB
//                     are stamped together so a reader never sees a FINISHED run
//                     without its finals (no torn state).
//
//   AppendParams    — write-once params. The service has already rejected changed
//                     values; the adapter inserts the new keys with ON CONFLICT
//                     (run_id,key) DO NOTHING as the storage backstop against a
//                     racing double-log.
//
//   GetMetricHistory — keyset-paginated, ordered (key, step, ts) scan over the
//                     time-series, with optional key/step-range filters. This is
//                     what GetMetricHistory/CompareRuns and FinishRun's
//                     finals-drain read.
// ============================================================================
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/domain"
)

// Compile-time proof the adapter satisfies the domain port.
var _ domain.RunRepository = (*RunRepository)(nil)

// runColumns is the canonical run SELECT list — one definition shared by GetRun,
// ListRuns, and the RETURNING clauses so column order is defined exactly once.
const runColumns = `id, experiment_id, display_name, status, source, model_version_id, owner_id, final_metrics, artifacts, started_at, ended_at`

// ============================================================================
// RUN LIFECYCLE
// ============================================================================

// CreateRun persists a new Run together with its initial params, in ONE
// transaction. ATOMICITY: a run and the params supplied at StartRun must be
// all-or-nothing — a crash between the run insert and a param insert would leave a
// run whose recorded configuration is incomplete (a silent corruption of "what
// config produced this model"). pgx.BeginFunc commits on a nil return and rolls
// back on any error, so we can't forget a rollback on an early return.
//
// final_metrics starts as the empty array (a fresh run has no finals); artifacts
// starts NULL (none attached yet). A dangling experiment_id surfaces as an FK
// violation → ErrRepoNotFound (the service already verified the parent, so this is
// a backstop).
func (r *RunRepository) CreateRun(ctx context.Context, run domain.Run) (domain.Run, error) {
	// Delegate to the tx-aware path with an EMPTY idempotency key: no key means the
	// run is inserted (with its params) in one tx and no idempotency row is written.
	// CreateRun and CreateRunIdem therefore share ONE insert path — they can't drift.
	return r.CreateRunIdem(ctx, run, "", "")
}

// CreateRunIdem persists a new Run together with its initial params AND, when
// idempotencyKey is non-empty, the (operation, key) → run.ID idempotency row — ALL
// in ONE transaction.
//
// ============================================================================
// WHY THE IDEMPOTENCY KEY MUST COMMIT IN THE SAME TX AS THE RUN (the fix)
// ============================================================================
//
// The Stripe idempotency pattern only holds if "the effect happened" and "the key
// is recorded" are the SAME durable fact. The previous shape committed the run in
// its own tx and THEN recorded the key in a separate pool.Exec. A crash, dropped
// connection, or cancelled ctx in the gap left the run committed but the key
// missing — so the next retry of the same key got Lookup found=false and created a
// SECOND run. That defeats the entire guarantee.
//
// This method closes the gap: one pgx.BeginFunc inserts the run, its params, and
// the idempotency row. The DB commits all three or none — there is no in-between
// state a retry can observe. On replay the service's Lookup now ALWAYS finds the
// recorded run (it could not have been committed without its key), so no duplicate
// run is ever created. INTERVIEW: "How do you make create-then-record atomic
// without 2PC?" Put both writes in the same local transaction against the same DB;
// the key lives in the same database as the entity, so one commit covers both.
//
// FK NOTE: a dangling experiment_id surfaces as a 23503 → ErrRepoNotFound (the
// service already verified the parent; this is the storage backstop).
func (r *RunRepository) CreateRunIdem(ctx context.Context, run domain.Run, operation, idempotencyKey string) (domain.Run, error) {
	finalsJSON, err := marshalFinalMetrics(run.FinalMetrics) // "[]" for a fresh run
	if err != nil {
		return domain.Run{}, fmt.Errorf("marshal final metrics: %w", err)
	}
	artifactsJSON, err := marshalArtifacts(run.Artifacts)
	if err != nil {
		return domain.Run{}, fmt.Errorf("marshal artifacts: %w", err)
	}

	// Stamp the natural-key backstop columns ONLY when an idempotency key is
	// present. Empty key → NULL columns, which the partial UNIQUE index ignores, so
	// keyless runs are never constrained against each other. A non-empty key writes
	// (operation, key) onto the row so the runs_idempotency_uniq index physically
	// forbids a second run with the same key — the StartRun backstop the finding asks
	// for. We pass *string (nil = SQL NULL) rather than "" so a keyless run stores
	// NULL, not an empty string that would all-collide under the index.
	var idemOp, idemKey *string
	if idempotencyKey != "" {
		idemOp, idemKey = &operation, &idempotencyKey
	}

	err = pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		const insertRun = `
			INSERT INTO runs
				(id, experiment_id, display_name, status, source, model_version_id,
				 owner_id, final_metrics, artifacts, started_at, ended_at,
				 idempotency_operation, idempotency_key)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`
		if _, err := tx.Exec(ctx, insertRun,
			run.ID, run.ExperimentID, run.DisplayName, string(run.Status), string(run.Source),
			run.ModelVersionID, run.OwnerID, finalsJSON, artifactsJSON, run.StartedAt, run.EndedAt,
			idemOp, idemKey,
		); err != nil {
			return err
		}
		// Initial params (if any) in the SAME tx. write-once index protects against a
		// duplicate key WITHIN the batch (the service deduped, but the index is the
		// backstop); we DO NOTHING on conflict so a benign duplicate doesn't abort.
		if err := insertParams(ctx, tx, run.ID, run.Params); err != nil {
			return err
		}
		// The idempotency row, in the SAME tx — commit-together-or-neither. No-op when
		// idempotencyKey == "" (CreateRun's path / no idempotency requested).
		return recordIdempotencyKeyTx(ctx, tx, operation, idempotencyKey, run.ID)
	})
	if err != nil {
		if isForeignKeyViolation(err) {
			return domain.Run{}, domain.ErrRepoNotFound // parent experiment gone
		}
		// A unique_violation on the natural-key backstop (runs_idempotency_uniq)
		// means a CONCURRENT creator already committed a run with this exact
		// (operation, key). That is not a server fault — it is the backstop doing its
		// job. We surface ErrRepoConflict so the service can resolve the race by
		// looking the key up and returning the winner's run (the idempotency row was
		// written in the winner's tx, so the Lookup will find it).
		if constraint, ok := isUniqueViolation(err); ok && constraint == "runs_idempotency_uniq" {
			return domain.Run{}, domain.ErrRepoConflict
		}
		return domain.Run{}, fmt.Errorf("insert run: %w", err)
	}
	return run, nil
}

// GetRun returns the Run WITH its params, final metrics, and artifacts (NOT the
// full metric series — that is GetMetricHistory's job), or ErrRepoNotFound. Two
// reads (the run row, then its params) rather than a JOIN: a JOIN would fan the
// run columns across every param row and complicate the scan; two clean reads are
// simpler and the params read is a tiny indexed lookup.
func (r *RunRepository) GetRun(ctx context.Context, id string) (domain.Run, error) {
	q := `SELECT ` + runColumns + ` FROM runs WHERE id = $1`
	run, err := scanRun(r.pool.QueryRow(ctx, q, id))
	if err != nil {
		return domain.Run{}, err // ErrRepoNotFound-mapped in scanRun
	}
	params, err := r.loadParams(ctx, id)
	if err != nil {
		return domain.Run{}, err
	}
	run.Params = params
	return run, nil
}

// ListRuns returns a page of runs within an experiment (newest-first), optionally
// filtered by status (RunStatusUnspecified = no filter), plus a nextToken cursor.
// Keyset-paginated on (started_at, id) exactly like the experiment list.
//
// LIST CONTRACT: list items carry the run row (incl. denormalized final_metrics —
// cheap, it's already on the row) but OMIT params (Params left nil). A caller
// wanting params calls GetRun. This keeps the list a single indexed scan instead
// of N+1 param loads.
func (r *RunRepository) ListRuns(ctx context.Context, experimentID string, statusFilter domain.RunStatus, opts domain.ListOptions) ([]domain.Run, string, error) {
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	cur, err := decodeListCursor(opts.PageToken)
	if err != nil {
		return nil, "", fmt.Errorf("list runs: %w", err)
	}

	args := []any{experimentID}
	where := "WHERE experiment_id = $1"
	if statusFilter != domain.RunStatusUnspecified {
		args = append(args, string(statusFilter))
		where += fmt.Sprintf(" AND status = $%d", len(args))
	}
	if cur != nil {
		args = append(args, cur.TS, cur.ID)
		where += fmt.Sprintf(" AND (started_at, id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, pageSize+1)
	q := fmt.Sprintf(`
		SELECT %s FROM runs
		%s
		ORDER BY started_at DESC, id DESC
		LIMIT $%d`, runColumns, where, len(args))

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list runs: %w", err)
	}
	defer rows.Close()

	runs := make([]domain.Run, 0, pageSize)
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, "", err
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate runs: %w", err)
	}

	nextToken := ""
	if len(runs) > pageSize {
		runs = runs[:pageSize]
		last := runs[len(runs)-1]
		nextToken = encodeListCursor(listCursor{TS: last.StartedAt, ID: last.ID})
	}
	return runs, nextToken, nil
}

// ListRunsByTeam returns ALL runs owned by a team across every experiment,
// newest-first, optionally status-filtered (the unscoped runs view).
//
// TENANCY: runs carry no team of their own — team lives on the parent
// experiment — so we JOIN runs to experiments and filter on experiments.team.
// That join predicate IS the tenancy boundary: a team can never page another
// team's runs. Same keyset cursor as ListRuns, keyed on the runs row
// (rn.started_at, rn.id). The SELECT list is runColumns with the `rn` alias so
// the bare `id` is unambiguous against experiments.id; column ORDER mirrors
// runColumns/scanRun exactly.
func (r *RunRepository) ListRunsByTeam(ctx context.Context, team string, statusFilter domain.RunStatus, opts domain.ListOptions) ([]domain.Run, string, error) {
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	cur, err := decodeListCursor(opts.PageToken)
	if err != nil {
		return nil, "", fmt.Errorf("list runs by team: %w", err)
	}

	args := []any{team}
	where := "WHERE e.team = $1"
	if statusFilter != domain.RunStatusUnspecified {
		args = append(args, string(statusFilter))
		where += fmt.Sprintf(" AND rn.status = $%d", len(args))
	}
	if cur != nil {
		args = append(args, cur.TS, cur.ID)
		where += fmt.Sprintf(" AND (rn.started_at, rn.id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, pageSize+1)
	q := fmt.Sprintf(`
		SELECT rn.id, rn.experiment_id, rn.display_name, rn.status, rn.source, rn.model_version_id, rn.owner_id, rn.final_metrics, rn.artifacts, rn.started_at, rn.ended_at
		FROM runs rn
		JOIN experiments e ON e.id = rn.experiment_id
		%s
		ORDER BY rn.started_at DESC, rn.id DESC
		LIMIT $%d`, where, len(args))

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list runs by team: %w", err)
	}
	defer rows.Close()

	runs := make([]domain.Run, 0, pageSize)
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, "", err
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate runs: %w", err)
	}

	nextToken := ""
	if len(runs) > pageSize {
		runs = runs[:pageSize]
		last := runs[len(runs)-1]
		nextToken = encodeListCursor(listCursor{TS: last.StartedAt, ID: last.ID})
	}
	return runs, nextToken, nil
}

// UpdateRunStatus stamps the terminal transition: status, ended_at, and the
// computed final_metrics in ONE write. The service has already validated the
// transition (CanTransitionTo) and computed the finals. Persisting all three in a
// single UPDATE means a concurrent reader never sees a FINISHED run that still
// shows RUNNING-era ended_at=NULL or empty finals — no torn terminal state.
//
// We RETURN the updated row so the service gets the stored truth (and the
// re-decoded finals) without a second read. ErrRepoNotFound if the row vanished.
func (r *RunRepository) UpdateRunStatus(ctx context.Context, runID string, status domain.RunStatus, endedAt time.Time, finalMetrics []domain.MetricPoint) (domain.Run, error) {
	finalsJSON, err := marshalFinalMetrics(finalMetrics)
	if err != nil {
		return domain.Run{}, fmt.Errorf("marshal final metrics: %w", err)
	}
	q := `
		UPDATE runs
		SET status = $2, ended_at = $3, final_metrics = $4
		WHERE id = $1
		RETURNING ` + runColumns
	run, err := scanRun(r.pool.QueryRow(ctx, q, runID, string(status), endedAt, finalsJSON))
	if err != nil {
		return domain.Run{}, err // ErrRepoNotFound-mapped in scanRun
	}
	// Re-attach params so the returned run is complete (the service publishes /
	// returns it). A small extra read; FinishRun is not a hot path.
	params, err := r.loadParams(ctx, runID)
	if err != nil {
		return domain.Run{}, err
	}
	run.Params = params
	return run, nil
}

// DeleteRun hard-deletes a run; the ON DELETE CASCADE FKs on run_params and
// run_metrics remove its children in the same statement (one DELETE, the DB
// handles the cascade). Returns ErrRepoNotFound if the run is already gone — the
// service may treat that as idempotent success when an idempotency key matches.
//
// WHY hard delete here (vs the experiment's soft delete): the service has already
// enforced the lineage guard (no live model references this run). A run with no
// lineage left is safe to physically remove, reclaiming the metric rows — runs can
// be high-cardinality, so we don't want tombstones accumulating. Experiments stay
// soft-deleted because they anchor tenancy and are few.
func (r *RunRepository) DeleteRun(ctx context.Context, runID string) error {
	// Delegate to the tx-aware path with an EMPTY key: the delete runs in its own
	// tx and no idempotency row is written. One delete path, no drift.
	return r.DeleteRunIdem(ctx, runID, "", "")
}

// DeleteRunIdem hard-deletes a run (children cascade) AND, when idempotencyKey is
// non-empty, records the (operation, key) → runID row — in ONE transaction.
//
// WHY atomic here too: without it, a crash between the committed DELETE and a
// separate Record leaves the run gone but no key recorded. The next retry of the
// same key sees Lookup found=false, loads the run, finds it ALREADY gone, and the
// service maps that to ErrRunNotFound — turning a successful idempotent delete into
// an error on replay. Committing the key in the same tx as the DELETE makes the
// replay find the key and short-circuit to success, which is the contract DeleteRun
// promises ("a retried delete after the run is already gone returns nil").
//
// ORDERING: we DELETE first (and require it to have removed a row) and ONLY THEN
// record the key, so the key is written for a delete that actually happened. If the
// run was already gone (RowsAffected 0) we return ErrRepoNotFound WITHOUT recording
// — there was no successful delete on THIS call to make idempotent.
func (r *RunRepository) DeleteRunIdem(ctx context.Context, runID, operation, idempotencyKey string) error {
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM runs WHERE id = $1`, runID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return domain.ErrRepoNotFound // nothing deleted → no key to record
		}
		return recordIdempotencyKeyTx(ctx, tx, operation, idempotencyKey, runID)
	})
	if err != nil {
		if errors.Is(err, domain.ErrRepoNotFound) {
			return domain.ErrRepoNotFound // surface the sentinel unwrapped
		}
		return fmt.Errorf("delete run: %w", err)
	}
	return nil
}

// ============================================================================
// METRIC INGESTION — the high-throughput batch write
// ============================================================================

// AppendMetrics writes a batch of metric points for a run in ONE transactional,
// idempotent multi-row INSERT, returning how many were NEWLY written.
//
// ============================================================================
// WHY ONE INSERT, NOT ONE-PER-POINT (the pattern's whole optimization)
// ============================================================================
//
// The service hands us an already-deduped, timestamp-stamped batch. We build a
// SINGLE INSERT with a multi-row VALUES list ($1..$N parameterized — never
// concatenated) and append ON CONFLICT (run_id, key, step) DO NOTHING. That gives:
//
//   * THROUGHPUT: one network round-trip and one transaction per batch instead of
//     N. For a training flush of hundreds of points per step this is the
//     difference between a snappy LogMetrics and a chatty one.
//   * CROSS-BATCH IDEMPOTENCY: a retried batch (same client retry, a redelivered
//     NATS message) hits the unique index and DO NOTHING drops the duplicates —
//     double-writing nothing. This is the DB half of the idempotency guarantee
//     the domain's DedupMetricPoints starts in memory.
//   * ACCURATE accepted_count: pgx's CommandTag.RowsAffected() after an
//     INSERT ... ON CONFLICT DO NOTHING reports ONLY the rows actually inserted
//     (conflicting rows are not counted), which is precisely the "how many NEW
//     points landed" the service surfaces.
//
// FK NOTE: a run that was deleted between the service's load and this write would
// surface as a 23503 → ErrRepoNotFound. In practice the service holds the run as
// RUNNING, so this is a defensive mapping.
//
// We wrap the INSERT in an explicit transaction (BeginFunc) even though it is a
// single statement: it makes the "one atomic batch" boundary explicit and matches
// the port's "one transactional write" contract — and leaves room to add a
// per-batch side-write later (e.g. a run-activity timestamp) without changing the
// shape.
func (r *RunRepository) AppendMetrics(ctx context.Context, runID string, points []domain.MetricPoint) (int, error) {
	// Delegate to the tx-aware path with an EMPTY key: the batch INSERT runs in its
	// own tx and no idempotency row is written. One insert path, no drift.
	return r.AppendMetricsIdem(ctx, runID, points, "", "")
}

// AppendMetricsIdem writes the metric batch AND, when idempotencyKey is non-empty,
// the (operation, key) → runID idempotency row — in ONE transaction.
//
// LogMetrics already has a strong backstop: the run_metrics (run_id,key,step)
// UNIQUE index means a replayed batch double-writes NOTHING even if its idempotency
// row was lost. So unlike StartRun, a lost LogMetrics key cannot corrupt data — but
// it CAN cause a replayed batch to re-do the (cheap, idempotent) INSERT instead of
// short-circuiting on the key. Recording the key in the SAME tx as the batch makes
// the short-circuit reliable: the replay's Lookup finds the key and returns
// accepted=0 without touching the table. This brings LogMetrics in line with the
// other mutations and means "the key is recorded iff the batch committed."
func (r *RunRepository) AppendMetricsIdem(ctx context.Context, runID string, points []domain.MetricPoint, operation, idempotencyKey string) (int, error) {
	if len(points) == 0 {
		return 0, nil // nothing to write (the service guards this, but be safe)
	}

	// Build the multi-row VALUES list with bound placeholders. Each row carries 5
	// columns (run_id, key, step, value, ts), so point i occupies parameter slots
	// [base+1 .. base+5] where base = i*5. The query text contains ONLY the
	// placeholder numbers "($1,$2,$3,$4,$5),($6,...)" — every actual value goes in
	// args and is never interpolated. This is the safe way to do a dynamic-arity
	// multi-row INSERT. We include run_id per row so each VALUES tuple is complete
	// and matches the ON CONFLICT target (the (run_id,key,step) PK).
	const colsPerRow = 5
	args := make([]any, 0, len(points)*colsPerRow)
	valuesSQL := make([]byte, 0, len(points)*20)
	for i, p := range points {
		base := i * colsPerRow
		if i > 0 {
			valuesSQL = append(valuesSQL, ',')
		}
		valuesSQL = append(valuesSQL, []byte(fmt.Sprintf("($%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5))...)
		args = append(args, runID, p.Key, p.Step, p.Value, p.Timestamp)
	}

	q := `INSERT INTO run_metrics (run_id, key, step, value, ts) VALUES ` +
		string(valuesSQL) +
		` ON CONFLICT (run_id, key, step) DO NOTHING`

	var written int
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, q, args...)
		if err != nil {
			return err
		}
		written = int(tag.RowsAffected()) // ON CONFLICT DO NOTHING counts only inserts
		// Idempotency row in the SAME tx — no-op when the key is empty.
		return recordIdempotencyKeyTx(ctx, tx, operation, idempotencyKey, runID)
	})
	if err != nil {
		if isForeignKeyViolation(err) {
			return 0, domain.ErrRepoNotFound // run vanished mid-flight
		}
		return 0, fmt.Errorf("append metrics: %w", err)
	}
	return written, nil
}

// ============================================================================
// PARAMS — write-once inputs
// ============================================================================

// AppendParams writes the (already conflict-resolved) new params and returns how
// many were newly written. The service has read existing params and rejected
// changed values, so the keys here are expected to be new; we still use
// ON CONFLICT (run_id,key) DO NOTHING as the storage backstop so a racing
// double-log can't create a second row or abort the insert. RowsAffected() is the
// newly-written count.
func (r *RunRepository) AppendParams(ctx context.Context, runID string, params []domain.Param) (int, error) {
	if len(params) == 0 {
		return 0, nil
	}
	var written int
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		n, err := insertParamsCounting(ctx, tx, runID, params)
		written = n
		return err
	})
	if err != nil {
		if isForeignKeyViolation(err) {
			return 0, domain.ErrRepoNotFound
		}
		return 0, fmt.Errorf("append params: %w", err)
	}
	return written, nil
}

// GetRunParams returns a run's existing params — the service reads these to
// enforce write-once semantics before AppendParams. Ordered by key for stable,
// deterministic output.
func (r *RunRepository) GetRunParams(ctx context.Context, runID string) ([]domain.Param, error) {
	return r.loadParams(ctx, runID)
}

// loadParams reads all params for a run, ordered by key. A shared helper used by
// GetRun, GetRunParams, and UpdateRunStatus.
func (r *RunRepository) loadParams(ctx context.Context, runID string) ([]domain.Param, error) {
	const q = `SELECT key, value FROM run_params WHERE run_id = $1 ORDER BY key ASC`
	rows, err := r.pool.Query(ctx, q, runID)
	if err != nil {
		return nil, fmt.Errorf("load params: %w", err)
	}
	defer rows.Close()

	var params []domain.Param
	for rows.Next() {
		var p domain.Param
		if err := rows.Scan(&p.Key, &p.Value); err != nil {
			return nil, fmt.Errorf("scan param: %w", err)
		}
		params = append(params, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate params: %w", err)
	}
	return params, nil
}

// insertParams inserts params within a caller-provided tx (used by CreateRun),
// DO NOTHING on conflict. Discards the count — CreateRun doesn't report it.
func insertParams(ctx context.Context, tx pgx.Tx, runID string, params []domain.Param) error {
	_, err := insertParamsCounting(ctx, tx, runID, params)
	return err
}

// insertParamsCounting inserts params within a tx and returns the newly-written
// count (RowsAffected over the multi-row insert). Single INSERT with a dynamic
// parameterized VALUES list — same safe construction as AppendMetrics.
func insertParamsCounting(ctx context.Context, tx pgx.Tx, runID string, params []domain.Param) (int, error) {
	if len(params) == 0 {
		return 0, nil
	}
	args := make([]any, 0, len(params)*3)
	valuesSQL := make([]byte, 0, len(params)*12)
	for i, p := range params {
		base := i * 3
		if i > 0 {
			valuesSQL = append(valuesSQL, ',')
		}
		valuesSQL = append(valuesSQL, []byte(fmt.Sprintf("($%d,$%d,$%d)", base+1, base+2, base+3))...)
		args = append(args, runID, p.Key, p.Value)
	}
	q := `INSERT INTO run_params (run_id, key, value) VALUES ` +
		string(valuesSQL) +
		` ON CONFLICT (run_id, key) DO NOTHING`
	tag, err := tx.Exec(ctx, q, args...)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// ============================================================================
// METRIC HISTORY — paginated time-series read
// ============================================================================

// GetMetricHistory returns the metric points matching the query (optional key and
// step-range filters), keyset-paginated, ordered by (key, step, ts) — the exact
// order the run_metrics PRIMARY KEY (run_id, key, step) provides, so this is an
// in-order index scan with no sort.
//
// FILTERS (all parameterized):
//   - Keys: empty = all keys. Non-empty = key = ANY($n) (a single bound array
//     param, NOT N concatenated literals — ANY(array) keeps it one safe parameter
//     regardless of how many keys).
//   - MinStep/MaxStep: applied only when HasMinStep/HasMaxStep is set, so "min
//     step 0" (a real bound) is distinct from "no bound".
//   - cursor: (key, step, ts) strictly AFTER the previous page's last row
//     (row-value comparison gives the exact keyset boundary in ASC order).
//
// PAGE SIZE: the service caps it (DefaultMetricPageSize/MaxMetricPageSize) before
// calling; defaultPageSize is the adapter backstop. We fetch pageSize+1 to detect
// a next page.
func (r *RunRepository) GetMetricHistory(ctx context.Context, qy domain.MetricHistoryQuery) ([]domain.MetricPoint, string, error) {
	pageSize := qy.Pagination.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	cur, err := decodeMetricCursor(qy.Pagination.PageToken)
	if err != nil {
		return nil, "", fmt.Errorf("metric history: %w", err)
	}

	args := []any{qy.RunID}
	where := "WHERE run_id = $1"
	if len(qy.Keys) > 0 {
		args = append(args, qy.Keys) // one bound array param
		where += fmt.Sprintf(" AND key = ANY($%d)", len(args))
	}
	if qy.HasMinStep {
		args = append(args, qy.MinStep)
		where += fmt.Sprintf(" AND step >= $%d", len(args))
	}
	if qy.HasMaxStep {
		args = append(args, qy.MaxStep)
		where += fmt.Sprintf(" AND step <= $%d", len(args))
	}
	if cur != nil {
		// Strictly after (key, step, ts) in ASC order — row-value comparison.
		args = append(args, cur.Key, cur.Step, cur.TS)
		where += fmt.Sprintf(" AND (key, step, ts) > ($%d, $%d, $%d)", len(args)-2, len(args)-1, len(args))
	}
	args = append(args, pageSize+1)
	q := fmt.Sprintf(`
		SELECT key, step, value, ts FROM run_metrics
		%s
		ORDER BY key ASC, step ASC, ts ASC
		LIMIT $%d`, where, len(args))

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", fmt.Errorf("metric history: %w", err)
	}
	defer rows.Close()

	points := make([]domain.MetricPoint, 0, pageSize)
	for rows.Next() {
		var p domain.MetricPoint
		if err := rows.Scan(&p.Key, &p.Step, &p.Value, &p.Timestamp); err != nil {
			return nil, "", fmt.Errorf("scan metric point: %w", err)
		}
		points = append(points, p)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate metric points: %w", err)
	}

	nextToken := ""
	if len(points) > pageSize {
		points = points[:pageSize]
		last := points[len(points)-1]
		nextToken = encodeMetricCursor(metricCursor{Key: last.Key, Step: last.Step, TS: last.Timestamp})
	}
	return points, nextToken, nil
}

// ============================================================================
// ARTIFACTS
// ============================================================================

// SetArtifacts replaces the run's free-form artifacts (already size-validated by
// the service) and returns the updated run. A nil/empty map writes SQL NULL
// (clearing artifacts). RETURNING gives back the stored row so the caller sees
// exactly what was persisted; we re-attach params for a complete run.
func (r *RunRepository) SetArtifacts(ctx context.Context, runID string, artifacts map[string]any) (domain.Run, error) {
	// Delegate to the tx-aware path with an EMPTY key: the UPDATE runs in its own
	// tx and no idempotency row is written. One update path, no drift.
	return r.SetArtifactsIdem(ctx, runID, artifacts, "", "")
}

// SetArtifactsIdem replaces the run's artifacts AND, when idempotencyKey is
// non-empty, records the (operation, key) → runID row — in ONE transaction.
//
// SetArtifacts is an overwrite, so a lost key only causes a harmless re-overwrite
// with the same payload on replay (not corruption). Still, recording the key in the
// SAME tx as the UPDATE keeps the guarantee uniform across all four mutating RPCs
// and lets the service's replay short-circuit to "return the stored run" reliably.
//
// We do the UPDATE ... RETURNING inside the tx so the row we hand back is the one
// we just committed; the param re-attach (a second read) happens AFTER the tx since
// it does not need to be in the same atomic unit (params don't change here).
func (r *RunRepository) SetArtifactsIdem(ctx context.Context, runID string, artifacts map[string]any, operation, idempotencyKey string) (domain.Run, error) {
	artifactsJSON, err := marshalArtifacts(artifacts)
	if err != nil {
		return domain.Run{}, fmt.Errorf("marshal artifacts: %w", err)
	}

	var run domain.Run
	err = pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		q := `UPDATE runs SET artifacts = $2 WHERE id = $1 RETURNING ` + runColumns
		updated, scanErr := scanRun(tx.QueryRow(ctx, q, runID, artifactsJSON))
		if scanErr != nil {
			return scanErr // ErrRepoNotFound-mapped in scanRun (no row to update)
		}
		run = updated
		// Idempotency row in the SAME tx — no-op when the key is empty.
		return recordIdempotencyKeyTx(ctx, tx, operation, idempotencyKey, runID)
	})
	if err != nil {
		return domain.Run{}, err // ErrRepoNotFound passes through unwrapped from scanRun
	}

	params, err := r.loadParams(ctx, runID)
	if err != nil {
		return domain.Run{}, err
	}
	run.Params = params
	return run, nil
}

// ============================================================================
// SCAN HELPER
// ============================================================================

// scanRun materializes one run row (WITHOUT params) into the domain type,
// casting the TEXT enums back to their domain types and decoding the
// final_metrics + artifacts JSONB. One helper for GetRun, ListRuns, and the
// RETURNING paths so the column order lives in exactly one place (runColumns).
func scanRun(row rowScanner) (domain.Run, error) {
	var (
		run           domain.Run
		status        string
		source        string
		finalsJSON    []byte
		artifactsJSON []byte
	)
	if err := row.Scan(
		&run.ID, &run.ExperimentID, &run.DisplayName, &status, &source,
		&run.ModelVersionID, &run.OwnerID, &finalsJSON, &artifactsJSON,
		&run.StartedAt, &run.EndedAt, // *time.Time scans NULL as nil
	); err != nil {
		if isNoRows(err) {
			return domain.Run{}, domain.ErrRepoNotFound
		}
		return domain.Run{}, fmt.Errorf("scan run: %w", err)
	}
	run.Status = domain.RunStatus(status)
	run.Source = domain.RunSource(source)

	finals, err := unmarshalFinalMetrics(finalsJSON)
	if err != nil {
		return domain.Run{}, fmt.Errorf("decode final metrics: %w", err)
	}
	run.FinalMetrics = finals

	artifacts, err := unmarshalArtifacts(artifactsJSON)
	if err != nil {
		return domain.Run{}, fmt.Errorf("decode artifacts: %w", err)
	}
	run.Artifacts = artifacts
	return run, nil
}
