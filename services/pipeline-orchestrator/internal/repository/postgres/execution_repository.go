// execution_repository.go — the Postgres adapter for domain.ExecutionRepository.
//
// ============================================================================
// THE DURABILITY BACKBONE OF THE SAGA (the most important adapter in this svc)
// ============================================================================
//
// An Execution is one RUN of a pipeline; its StepExecutions are the write-ahead
// CHECKPOINTS the engine relies on for crash recovery. This adapter persists
// both, mapping the domain's two-granularity port to two write paths:
//
//	Create   → INSERT the execution + ALL its PENDING step rows in ONE tx
//	           (atomic: a run is never half-created; its checkpoints exist the
//	           instant the run does). Idempotent on the trigger key.
//	Save     → UPDATE the COARSE execution-level state (status, current_step,
//	           completed_at, error) — the saga state-machine transition.
//	SaveStep → UPSERT one step checkpoint (the FINE-grained transition the
//	           recovery path reads). Called before a step runs and after it
//	           settles.
//	GetByID  → SELECT the execution + its steps (ordered) — what recovery and
//	           GetExecution both read.
//	List     → JOIN pipelines for the TEAM scope (executions have no team column;
//	           tenancy is a property of the parent template).
//
// WHY Create's atomicity matters: the PENDING step rows ARE the
// recovery map. If the execution row committed but a step row didn't, recovery
// would see a run with fewer checkpoints than steps and either re-run or strand
// a step. One tx for "the execution and all its checkpoints" closes that gap —
// the saga's durability contract starts here.
// ============================================================================
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/domain"
)

// Compile-time proof the adapter satisfies the domain port.
var _ domain.ExecutionRepository = (*ExecutionRepository)(nil)

// Create persists a new Execution together with its PENDING StepExecutions, in a
// single transaction, idempotently on idempotencyKey.
//
// The execution's tenancy is resolved from its parent pipeline at write time:
// executions carry no team column (tenancy is the template's property), so the
// idempotency table is keyed by the pipeline's team. We look that team up inside
// the same logical operation so a retry's dedup is correctly team-scoped.
//
// IDEMPOTENCY: identical to the pipeline path — fast-path lookup, then an atomic
// insert, then a race-recovery re-read on a key unique_violation. A deduped
// trigger returns the ORIGINAL execution (exactly-once trigger): the service
// detects the dedup by seeing stored.ID != the one it generated (or status !=
// PENDING) and SKIPS re-running the saga — never double-applying a deployment.
func (s *ExecutionRepository) Create(ctx context.Context, e domain.Execution, idempotencyKey string) (domain.Execution, error) {
	// Resolve the owning team from the parent pipeline (for team-scoped dedup).
	team, err := s.pipelineTeam(ctx, e.PipelineID)
	if err != nil {
		return domain.Execution{}, err // ErrRepoNotFound if the pipeline is gone
	}

	// Fast-path dedup: a retried trigger with a known key returns the original run.
	if idempotencyKey != "" {
		if existing, ok, lerr := s.executionByKey(ctx, team, idempotencyKey); lerr != nil {
			return domain.Execution{}, lerr
		} else if ok {
			return existing, nil
		}
	}

	inputJSON, err := marshalMap(e.Input)
	if err != nil {
		return domain.Execution{}, fmt.Errorf("marshal input: %w", err)
	}

	// Atomic write: the execution row, every PENDING step row, and the key mapping.
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		const insertExec = `
			INSERT INTO executions
				(id, pipeline_id, status, current_step, triggered_by, started_at, completed_at, input, error)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
		if _, err := tx.Exec(ctx, insertExec,
			e.ID, e.PipelineID, int16(e.Status), nullIfEmpty(e.CurrentStep), e.TriggeredBy,
			e.StartedAt, e.CompletedAt, inputJSON, nullIfEmpty(e.Error),
		); err != nil {
			return err
		}

		// Insert every step checkpoint. These PENDING rows are the recovery map;
		// they MUST land in the same tx as the execution. We insert them
		// individually (a saga has a handful of steps; a DAG tens) — simple and
		// clear; pgx.Batch would micro-optimize but obscure the intent.
		const insertStep = `
			INSERT INTO step_executions
				(id, execution_id, step_id, step_type, status, started_at, completed_at, output, error, attempt)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
		for _, st := range e.Steps {
			outJSON, mErr := marshalMap(st.Output)
			if mErr != nil {
				return fmt.Errorf("marshal step output: %w", mErr)
			}
			if _, err := tx.Exec(ctx, insertStep,
				st.ID, e.ID, st.StepID, int16(st.StepType), int16(st.Status),
				st.StartedAt, st.CompletedAt, outJSON, nullIfEmpty(st.Error), st.Attempt,
			); err != nil {
				return err
			}
		}

		if idempotencyKey != "" {
			const insertKey = `
				INSERT INTO execution_idempotency (team, idempotency_key, execution_id)
				VALUES ($1, $2, $3)`
			if _, err := tx.Exec(ctx, insertKey, team, idempotencyKey, e.ID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		// Race recovery: a concurrent trigger with the same key committed between
		// our lookup and our insert. Re-read and return the winner's execution.
		if isUniqueViolation(err) && idempotencyKey != "" {
			if existing, found, lerr := s.executionByKey(ctx, team, idempotencyKey); lerr == nil && found {
				return existing, nil
			}
		}
		// A dangling pipeline_id would surface as an FK violation — but we already
		// resolved the team above (which proves the pipeline exists), so this is a
		// genuine write error.
		return domain.Execution{}, fmt.Errorf("insert execution: %w", err)
	}

	return e, nil
}

// Save persists the COARSE execution-level transition: status, current_step,
// completed_at, error. This is the saga state-machine checkpoint
// (RUNNING→COMPENSATING→FAILED, etc.). It does NOT touch step rows (SaveStep owns
// those) — the two granularities are persisted independently, which is exactly
// why a crash loses at most the in-flight step, not the whole run.
//
// Returns ErrRepoNotFound if the execution row vanished (it never should once
// created, but we surface it rather than silently no-op so a logic bug shows).
func (s *ExecutionRepository) Save(ctx context.Context, e domain.Execution) error {
	inputJSON, err := marshalMap(e.Input)
	if err != nil {
		return fmt.Errorf("marshal input: %w", err)
	}
	const q = `
		UPDATE executions
		SET status = $2, current_step = $3, completed_at = $4, input = $5, error = $6
		WHERE id = $1`
	tag, err := s.pool.Exec(ctx, q,
		e.ID, int16(e.Status), nullIfEmpty(e.CurrentStep), e.CompletedAt, inputJSON, nullIfEmpty(e.Error),
	)
	if err != nil {
		return fmt.Errorf("save execution: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrRepoNotFound
	}
	return nil
}

// SaveStep persists a SINGLE StepExecution transition — the write-ahead
// checkpoint. UPSERT (INSERT ... ON CONFLICT ... DO UPDATE) keyed on
// (execution_id, step_id) so it works whether the engine is creating a checkpoint
// the rare path didn't pre-create or (the common case) updating the PENDING row
// Create already inserted. Idempotent by construction: re-saving the same
// transition lands the same row state.
//
// WHY UPSERT and not a plain UPDATE: although Create pre-inserts a PENDING row per
// step, modeling SaveStep as an idempotent UPSERT makes it robust to any path
// that produces a step row out of band (and makes crash-recovery replays safe —
// replaying a checkpoint write never errors on "already exists"). The conflict
// target is the (execution_id, step_id) unique index, which guarantees one
// runtime row per template step per run.
func (s *ExecutionRepository) SaveStep(ctx context.Context, step domain.StepExecution) error {
	outJSON, err := marshalMap(step.Output)
	if err != nil {
		return fmt.Errorf("marshal step output: %w", err)
	}
	const q = `
		INSERT INTO step_executions
			(id, execution_id, step_id, step_type, status, started_at, completed_at, output, error, attempt)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (execution_id, step_id) DO UPDATE SET
			status       = EXCLUDED.status,
			started_at   = EXCLUDED.started_at,
			completed_at = EXCLUDED.completed_at,
			output       = EXCLUDED.output,
			error        = EXCLUDED.error,
			attempt      = EXCLUDED.attempt`
	if _, err := s.pool.Exec(ctx, q,
		step.ID, step.ExecutionID, step.StepID, int16(step.StepType), int16(step.Status),
		step.StartedAt, step.CompletedAt, outJSON, nullIfEmpty(step.Error), step.Attempt,
	); err != nil {
		// A step for a nonexistent execution is an FK violation — surface as
		// not-found for the parent (a logic bug if it ever happens).
		if isForeignKeyViolation(err) {
			return domain.ErrRepoNotFound
		}
		return fmt.Errorf("save step: %w", err)
	}
	return nil
}

// GetByID returns the Execution WITH its StepExecutions (the full live timeline),
// or ErrRepoNotFound. Two queries (the execution row, then its steps) rather than
// a JOIN: a JOIN would fan the execution columns across every step row (wasteful)
// and complicate the scan; two clean reads are simpler and the steps query is a
// single indexed range-scan. The steps are ordered by completed_at then id so the
// timeline is stable and the compensator's "completion order" is reproducible.
func (s *ExecutionRepository) GetByID(ctx context.Context, id string) (domain.Execution, error) {
	const execQ = `
		SELECT id, pipeline_id, status, current_step, triggered_by, started_at, completed_at, input, error
		FROM executions WHERE id = $1`
	e, err := s.scanExecution(s.pool.QueryRow(ctx, execQ, id))
	if err != nil {
		return domain.Execution{}, err // already ErrRepoNotFound-mapped in scanExecution
	}

	steps, err := s.loadSteps(ctx, id)
	if err != nil {
		return domain.Execution{}, err
	}
	e.Steps = steps
	return e, nil
}

// loadSteps reads all StepExecutions for an execution, ordered by completion time
// then id. NULLS LAST keeps not-yet-completed steps (NULL completed_at) after the
// completed ones, so the slice reads as "what finished, in order; then what's
// still pending" — a natural timeline.
func (s *ExecutionRepository) loadSteps(ctx context.Context, executionID string) ([]domain.StepExecution, error) {
	const q = `
		SELECT id, execution_id, step_id, step_type, status, started_at, completed_at, output, error, attempt
		FROM step_executions
		WHERE execution_id = $1
		ORDER BY completed_at ASC NULLS LAST, id ASC`
	rows, err := s.pool.Query(ctx, q, executionID)
	if err != nil {
		return nil, fmt.Errorf("load steps: %w", err)
	}
	defer rows.Close()

	var steps []domain.StepExecution
	for rows.Next() {
		st, err := scanStep(rows)
		if err != nil {
			return nil, err
		}
		steps = append(steps, st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate steps: %w", err)
	}
	return steps, nil
}

// List returns a tenancy-scoped page of executions (newest first) + cursor.
//
// TEAM SCOPE VIA JOIN: executions have no team column — tenancy is the parent
// pipeline's property — so we JOIN pipelines and filter on p.team. The service
// sets f.Team from auth claims (never client input), so this JOIN is the anti-
// IDOR boundary: a caller can only ever page executions of pipelines their team
// owns. Optional pipeline_id and status filters narrow further. Keyset-paginated
// on (started_at, id) exactly like the pipeline list.
//
// LIST CONTRACT: list items OMIT per-step detail (Steps is left nil). The port
// documents this — a caller wanting the full timeline calls GetExecution. This
// keeps the list query a single indexed scan instead of N+1 step loads.
func (s *ExecutionRepository) List(ctx context.Context, f domain.ListExecutionsFilter) ([]domain.Execution, string, error) {
	pageSize := f.List.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	cur, err := decodeCursor(f.List.PageToken)
	if err != nil {
		return nil, "", fmt.Errorf("list executions: %w", err)
	}

	// Dynamic-but-parameterized WHERE: team is required, pipeline_id/status/cursor
	// are optional. Every value is a bound $N parameter — no interpolation.
	args := []any{f.Team}
	where := "WHERE p.team = $1"
	if f.PipelineID != "" {
		args = append(args, f.PipelineID)
		where += fmt.Sprintf(" AND e.pipeline_id = $%d", len(args))
	}
	if f.Status != domain.ExecutionStatusUnspecified {
		args = append(args, int16(f.Status))
		where += fmt.Sprintf(" AND e.status = $%d", len(args))
	}
	if cur != nil {
		args = append(args, cur.CreatedAt, cur.ID)
		where += fmt.Sprintf(" AND (e.started_at, e.id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, pageSize+1)
	q := fmt.Sprintf(`
		SELECT e.id, e.pipeline_id, e.status, e.current_step, e.triggered_by,
		       e.started_at, e.completed_at, e.input, e.error
		FROM executions e
		JOIN pipelines p ON p.id = e.pipeline_id
		%s
		ORDER BY e.started_at DESC, e.id DESC
		LIMIT $%d`, where, len(args))

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list executions: %w", err)
	}
	defer rows.Close()

	executions := make([]domain.Execution, 0, pageSize)
	for rows.Next() {
		e, err := s.scanExecution(rows)
		if err != nil {
			return nil, "", err
		}
		executions = append(executions, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate executions: %w", err)
	}

	nextToken := ""
	if len(executions) > pageSize {
		executions = executions[:pageSize]
		last := executions[len(executions)-1]
		nextToken = encodeCursor(cursor{CreatedAt: last.StartedAt, ID: last.ID})
	}
	return executions, nextToken, nil
}

// ============================================================================
// SCAN + LOOKUP HELPERS
// ============================================================================

// scanExecution materializes one execution row (WITHOUT its steps) into the
// domain type, decoding the JSONB input, casting the SMALLINT status, and turning
// nullable text columns into Go strings. One helper for both GetByID and List so
// the column order is defined once.
func (s *ExecutionRepository) scanExecution(row rowScanner) (domain.Execution, error) {
	var (
		e           domain.Execution
		status      int16
		currentStep *string
		inputJSON   []byte
		errText     *string
	)
	if err := row.Scan(
		&e.ID, &e.PipelineID, &status, &currentStep, &e.TriggeredBy,
		&e.StartedAt, &e.CompletedAt, &inputJSON, &errText,
	); err != nil {
		if isNoRows(err) {
			return domain.Execution{}, domain.ErrRepoNotFound
		}
		return domain.Execution{}, fmt.Errorf("scan execution: %w", err)
	}
	e.Status = domain.ExecutionStatus(status)
	e.CurrentStep = deref(currentStep)
	e.Error = deref(errText)
	input, err := unmarshalMap(inputJSON)
	if err != nil {
		return domain.Execution{}, fmt.Errorf("decode input: %w", err)
	}
	e.Input = input
	return e, nil
}

// scanStep materializes one step_executions row into the domain type. A free
// function (not a method) because it needs no Store state — it just decodes a row.
func scanStep(row rowScanner) (domain.StepExecution, error) {
	var (
		st       domain.StepExecution
		stepType int16
		status   int16
		outJSON  []byte
		errText  *string
	)
	if err := row.Scan(
		&st.ID, &st.ExecutionID, &st.StepID, &stepType, &status,
		&st.StartedAt, &st.CompletedAt, &outJSON, &errText, &st.Attempt,
	); err != nil {
		return domain.StepExecution{}, fmt.Errorf("scan step: %w", err)
	}
	st.StepType = domain.StepType(stepType)
	st.Status = domain.StepStatus(status)
	st.Error = deref(errText)
	out, err := unmarshalMap(outJSON)
	if err != nil {
		return domain.StepExecution{}, fmt.Errorf("decode step output: %w", err)
	}
	st.Output = out
	return st, nil
}

// pipelineTeam resolves the owning team of a pipeline (for team-scoped execution
// dedup). Returns ErrRepoNotFound if the pipeline doesn't exist — which the
// service maps to ErrPipelineNotFound (you can't trigger a run of a missing
// template). A lightweight single-column read, not a full GetByID.
func (s *ExecutionRepository) pipelineTeam(ctx context.Context, pipelineID string) (string, error) {
	const q = `SELECT team FROM pipelines WHERE id = $1`
	var team string
	if err := s.pool.QueryRow(ctx, q, pipelineID).Scan(&team); err != nil {
		if isNoRows(err) {
			return "", domain.ErrRepoNotFound
		}
		return "", fmt.Errorf("lookup pipeline team: %w", err)
	}
	return team, nil
}

// executionByKey resolves a trigger idempotency key (team-scoped) to its full
// execution, if any. Loads the execution WITH its steps (GetByID) so a deduped
// trigger returns the complete original run.
func (s *ExecutionRepository) executionByKey(ctx context.Context, team, key string) (domain.Execution, bool, error) {
	const q = `SELECT execution_id FROM execution_idempotency WHERE team = $1 AND idempotency_key = $2`
	var execID string
	if err := s.pool.QueryRow(ctx, q, team, key).Scan(&execID); err != nil {
		if isNoRows(err) {
			return domain.Execution{}, false, nil
		}
		return domain.Execution{}, false, fmt.Errorf("lookup execution idempotency key: %w", err)
	}
	e, err := s.GetByID(ctx, execID)
	if err != nil {
		return domain.Execution{}, false, fmt.Errorf("load deduped execution: %w", err)
	}
	return e, true, nil
}

// ============================================================================
// NULL HELPERS — the empty-string ⇄ SQL NULL boundary
// ============================================================================

// nullIfEmpty maps "" → nil (SQL NULL) and a non-empty string → itself. WHY:
// several columns (current_step, error) are semantically "absent" when empty; a
// NULL is the honest durable representation (and keeps text indexes/space tidy).
// The domain carries these as plain strings, so this is the one-way bridge on
// write; deref is the bridge on read.
func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// deref turns a scanned *string (which is nil for a SQL NULL) back into a plain
// string ("" for NULL) — the inverse of nullIfEmpty on the read path.
func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
