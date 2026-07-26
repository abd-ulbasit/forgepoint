// pipeline_repository.go — the Postgres adapter for domain.PipelineRepository.
//
// ============================================================================
// THE TEMPLATE REPOSITORY (the "program" store)
// ============================================================================
//
// Implements CRUD + soft-delete + keyset-paginated list for PipelineDefinition
// (the reusable workflow template). The interesting parts:
//
//   - Create is IDEMPOTENT on idempotencyKey (Stripe pattern), done ATOMICALLY:
//     the pipeline row and the key→id mapping are written in ONE transaction so a
//     crash can't leave a key pointing at a half-written pipeline (or a pipeline
//     with no key recorded). On a repeat key we return the ORIGINAL.
//
//   - Archive is a SOFT delete (sets archived=true) — past executions reference
//     this id for lineage, so we never physically remove the row.
//
//   - List is KEYSET (cursor) paginated, not LIMIT/OFFSET, so a page is stable
//     under concurrent inserts (OFFSET can skip/duplicate rows when the set
//     shifts). The cursor is (created_at, id) — the same tuple the index orders.
// ============================================================================
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/domain"
)

// Compile-time proof the adapter satisfies the domain port. If a signature ever
// drifts, the build breaks HERE (at the assertion) with a precise message,
// rather than mysteriously at the wiring site in main.
var _ domain.PipelineRepository = (*PipelineRepository)(nil)

// Create persists a new PipelineDefinition, idempotently on idempotencyKey.
//
// ATOMICITY (why a transaction): we insert the pipeline AND, when a key is given,
// the idempotency mapping. Those two writes must be all-or-nothing: a crash
// between them would either (a) leave a pipeline with no recorded key (a retry
// would create a DUPLICATE) or (b) leave a key pointing at nothing. A single tx
// makes the pair atomic — exactly the discipline the task calls out for
// "outbox write+event, saga state" style paired writes.
//
// IDEMPOTENCY FLOW:
//  1. If key != "" look it up (team-scoped). HIT → return the original pipeline
//     (no insert, no duplicate). This is the fast path for a retry.
//  2. MISS → in a tx, INSERT the pipeline, then INSERT the key mapping.
//  3. A concurrent racer that slipped between our lookup and insert trips the
//     idempotency PK's unique_violation; we catch it, roll back, and re-read the
//     winner's pipeline — so even a true race returns the SAME pipeline to both
//     callers (the key's whole promise).
func (s *PipelineRepository) Create(ctx context.Context, p domain.PipelineDefinition, idempotencyKey string) (domain.PipelineDefinition, error) {
	// Step 1: fast-path dedup lookup (only when a key was supplied).
	if idempotencyKey != "" {
		if existing, ok, err := s.pipelineByKey(ctx, p.Team, idempotencyKey); err != nil {
			return domain.PipelineDefinition{}, err
		} else if ok {
			return existing, nil // retry → original, no duplicate
		}
	}

	stepsJSON, err := marshalSteps(p.Steps)
	if err != nil {
		return domain.PipelineDefinition{}, fmt.Errorf("marshal steps: %w", err)
	}

	// Step 2: the atomic write. pgx.BeginFunc runs the closure in a tx and
	// commits if it returns nil, rolls back if it returns an error — so we can't
	// forget a rollback on an early return (the classic tx-leak bug).
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		const insertPipeline = `
			INSERT INTO pipelines (id, name, type, steps, created_by, team, created_at, archived)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
		if _, err := tx.Exec(ctx, insertPipeline,
			p.ID, p.Name, int16(p.Type), stepsJSON, p.CreatedBy, p.Team, p.CreatedAt, p.Archived,
		); err != nil {
			return err
		}
		// Record the idempotency mapping in the SAME tx (only if a key was given).
		if idempotencyKey != "" {
			const insertKey = `
				INSERT INTO pipeline_idempotency (team, idempotency_key, pipeline_id)
				VALUES ($1, $2, $3)`
			if _, err := tx.Exec(ctx, insertKey, p.Team, idempotencyKey, p.ID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		// Step 3: a racer beat us to the key between our lookup and our insert.
		// The unique_violation is on the idempotency PK (team, idempotency_key);
		// the winner's pipeline is now committed, so re-read and return IT.
		if constraint, ok := isUniqueViolation(err); ok && idempotencyKey != "" {
			if existing, found, lookupErr := s.pipelineByKey(ctx, p.Team, idempotencyKey); lookupErr == nil && found {
				return existing, nil
			}
			// Fall through to a generic error if the re-read also failed.
			_ = constraint
		}
		return domain.PipelineDefinition{}, fmt.Errorf("insert pipeline: %w", err)
	}

	return p, nil
}

// pipelineByKey resolves an idempotency key (team-scoped) to its pipeline, if any.
// Returns (pipeline, true, nil) on a hit, (zero, false, nil) on a miss, and a
// non-nil error only for an unexpected failure. Used by Create's dedup path.
func (s *PipelineRepository) pipelineByKey(ctx context.Context, team, key string) (domain.PipelineDefinition, bool, error) {
	const q = `SELECT pipeline_id FROM pipeline_idempotency WHERE team = $1 AND idempotency_key = $2`
	var pipelineID string
	if err := s.pool.QueryRow(ctx, q, team, key).Scan(&pipelineID); err != nil {
		if isNoRows(err) {
			return domain.PipelineDefinition{}, false, nil
		}
		return domain.PipelineDefinition{}, false, fmt.Errorf("lookup idempotency key: %w", err)
	}
	p, err := s.GetByID(ctx, pipelineID)
	if err != nil {
		return domain.PipelineDefinition{}, false, fmt.Errorf("load deduped pipeline: %w", err)
	}
	return p, true, nil
}

// GetByID returns the pipeline with that id (INCLUDING archived ones — the
// service decides whether an archived template is usable for the operation at
// hand, e.g. it blocks Trigger but still resolves lineage). Returns
// domain.ErrRepoNotFound when absent — the storage sentinel the service maps to
// the business ErrPipelineNotFound.
func (s *PipelineRepository) GetByID(ctx context.Context, id string) (domain.PipelineDefinition, error) {
	const q = `
		SELECT id, name, type, steps, created_by, team, created_at, archived
		FROM pipelines WHERE id = $1`
	return s.scanPipeline(s.pool.QueryRow(ctx, q, id))
}

// Update replaces the EDITABLE fields (name, steps) of an existing pipeline,
// preserving id/created_by/created_at/team (the immutable, server-authoritative
// provenance). Returns ErrRepoNotFound if the id doesn't exist. We use UPDATE ...
// RETURNING so the method returns the freshly-stored row in one round-trip and a
// zero-rows result cleanly signals "not found".
func (s *PipelineRepository) Update(ctx context.Context, p domain.PipelineDefinition) (domain.PipelineDefinition, error) {
	stepsJSON, err := marshalSteps(p.Steps)
	if err != nil {
		return domain.PipelineDefinition{}, fmt.Errorf("marshal steps: %w", err)
	}
	const q = `
		UPDATE pipelines SET name = $2, steps = $3
		WHERE id = $1
		RETURNING id, name, type, steps, created_by, team, created_at, archived`
	updated, err := s.scanPipeline(s.pool.QueryRow(ctx, q, p.ID, p.Name, stepsJSON))
	if err != nil {
		if errors.Is(err, domain.ErrRepoNotFound) {
			return domain.PipelineDefinition{}, domain.ErrRepoNotFound
		}
		// A duplicate live name (the partial unique index) surfaces here as a
		// unique_violation; map it to a generic error — the service validates
		// names before reaching us, so this is a defensive backstop.
		if _, ok := isUniqueViolation(err); ok {
			return domain.PipelineDefinition{}, fmt.Errorf("update pipeline: name conflict: %w", err)
		}
		return domain.PipelineDefinition{}, fmt.Errorf("update pipeline: %w", err)
	}
	return updated, nil
}

// Archive SOFT-deletes a pipeline (sets archived=true). Idempotent in effect:
// archiving an already-archived row succeeds (still one row matched). Returns
// ErrRepoNotFound only when the id doesn't exist at all. WHY soft-delete:
// executions reference this id for audit/lineage; a hard delete would dangle them
// (and the schema's RESTRICT FK would block it anyway).
func (s *PipelineRepository) Archive(ctx context.Context, id string) error {
	const q = `UPDATE pipelines SET archived = TRUE WHERE id = $1`
	tag, err := s.pool.Exec(ctx, q, id)
	if err != nil {
		return fmt.Errorf("archive pipeline: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrRepoNotFound
	}
	return nil
}

// List returns a tenancy-scoped page of LIVE pipelines (newest first) plus a
// nextToken cursor (empty when the last page is reached).
//
// KEYSET PAGINATION (the WHY): instead of OFFSET n (which scans
// and discards n rows AND can skip/duplicate under concurrent inserts), we carry
// a cursor = the (created_at, id) of the last row of the previous page and ask
// for "rows ordered DESC that sort strictly AFTER this cursor". A composite
// comparison ((created_at, id) < (c.created_at, c.id)) over the matching index
// makes each page an index range-scan — O(pageSize), stable, and total-ordered
// because id breaks created_at ties. We fetch pageSize+1 rows: the extra row, if
// present, tells us another page exists and supplies its cursor; we then trim it.
func (s *PipelineRepository) List(ctx context.Context, f domain.ListPipelinesFilter) ([]domain.PipelineDefinition, string, error) {
	pageSize := f.List.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}

	// Decode the opaque cursor (empty on the first page).
	cur, err := decodeCursor(f.List.PageToken)
	if err != nil {
		return nil, "", fmt.Errorf("list pipelines: %w", err)
	}

	// Build the query with positional args. The type filter and the cursor
	// predicate are OPTIONAL, so we assemble args + predicates dynamically — but
	// EVERY value is still a bound parameter ($N), never interpolated text, so
	// there is no injection surface even though the SQL string is built up.
	args := []any{f.Team}
	where := "WHERE team = $1 AND NOT archived"
	if f.Type != domain.PipelineTypeUnspecified {
		args = append(args, int16(f.Type))
		where += fmt.Sprintf(" AND type = $%d", len(args))
	}
	if cur != nil {
		// Strictly-after the cursor in DESC (created_at, id) order. Row-value
		// comparison expresses the composite "< (created_at, id)" cleanly.
		args = append(args, cur.CreatedAt, cur.ID)
		where += fmt.Sprintf(" AND (created_at, id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, pageSize+1) // fetch one extra to detect a next page
	q := fmt.Sprintf(`
		SELECT id, name, type, steps, created_by, team, created_at, archived
		FROM pipelines %s
		ORDER BY created_at DESC, id DESC
		LIMIT $%d`, where, len(args))

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list pipelines: %w", err)
	}
	defer rows.Close()

	pipelines := make([]domain.PipelineDefinition, 0, pageSize)
	for rows.Next() {
		p, err := s.scanPipeline(rows)
		if err != nil {
			return nil, "", fmt.Errorf("scan pipeline: %w", err)
		}
		pipelines = append(pipelines, p)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate pipelines: %w", err)
	}

	// If we got the extra (pageSize+1)th row, there IS a next page: trim the
	// extra and emit a cursor from the LAST kept row. Otherwise this is the final
	// page and the token is empty.
	nextToken := ""
	if len(pipelines) > pageSize {
		pipelines = pipelines[:pageSize]
		last := pipelines[len(pipelines)-1]
		nextToken = encodeCursor(cursor{CreatedAt: last.CreatedAt, ID: last.ID})
	}
	return pipelines, nextToken, nil
}

// scanPipeline materializes one pipeline row (from QueryRow or a Rows cursor)
// into the domain type, decoding the JSONB steps and casting the SMALLINT enum.
// Centralized so every read path (GetByID, Update RETURNING, List) decodes a row
// identically — one place to get the column order and JSONB handling right.
//
// rowScanner abstracts pgx.Row (single) and pgx.Rows (iterated); both expose
// Scan, so one helper serves both.
func (s *PipelineRepository) scanPipeline(row rowScanner) (domain.PipelineDefinition, error) {
	var (
		p         domain.PipelineDefinition
		typ       int16
		stepsJSON []byte
	)
	if err := row.Scan(&p.ID, &p.Name, &typ, &stepsJSON, &p.CreatedBy, &p.Team, &p.CreatedAt, &p.Archived); err != nil {
		if isNoRows(err) {
			return domain.PipelineDefinition{}, domain.ErrRepoNotFound
		}
		return domain.PipelineDefinition{}, fmt.Errorf("scan pipeline: %w", err)
	}
	p.Type = domain.PipelineType(typ)
	steps, err := unmarshalSteps(stepsJSON)
	if err != nil {
		return domain.PipelineDefinition{}, fmt.Errorf("decode steps: %w", err)
	}
	p.Steps = steps
	return p, nil
}
