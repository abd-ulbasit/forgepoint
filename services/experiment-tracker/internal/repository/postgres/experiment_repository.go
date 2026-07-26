// experiment_repository.go — the Postgres adapter for domain.ExperimentRepository.
//
// ============================================================================
// THE EXPERIMENT STORE (the named grouping + tenancy anchor)
// ============================================================================
//
// Implements Create / GetByID / Update / List for Experiment. The notable parts:
//
//   - Create maps a (team, name) ACTIVE-uniqueness violation (SQLSTATE 23505 on
//     the partial index) to domain.ErrRepoConflict — the storage sentinel the
//     service turns into ErrExperimentNameExists. Every other column is set by the
//     service (server-authoritative ids/owner/team/timestamps), so the adapter is
//     a faithful writer, not a policy maker.
//
//   - Update is the SINGLE write path for name/description/tags edits AND for the
//     soft-delete (the service stamps archived_at on the struct and calls Update).
//     So Update writes archived_at too — there is no separate Archive method on
//     the port; archiving IS an update with archived_at set.
//
//   - List is KEYSET (cursor) paginated on (created_at, id), newest-first, scoped
//     by team (the anti-IDOR boundary the service fills from auth claims).
//
// ============================================================================
package postgres

import (
	"context"
	"fmt"

	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/domain"
)

// Compile-time proof the adapter satisfies the domain port. If a signature ever
// drifts, the build breaks HERE with a precise message, not mysteriously at the
// wiring site in main.
var _ domain.ExperimentRepository = (*ExperimentRepository)(nil)

// experimentColumns is the canonical SELECT column list, defined once so GetByID
// and List scan IDENTICAL column order through the same scanExperiment helper.
const experimentColumns = `id, name, description, tags, owner_id, team, created_at, updated_at, archived_at`

// Create persists a new Experiment. The service has already set the
// server-authoritative fields (id, owner, team, timestamps); the adapter just
// writes them and translates a unique-name collision.
//
// CONFLICT MAPPING: a duplicate ACTIVE (team, name) trips the partial unique
// index experiments_team_name_active_uniq (23505). We return ErrRepoConflict so
// the service can surface ErrExperimentNameExists. We do NOT try to distinguish
// the constraint by name beyond "it was a unique violation" — this table has only
// one unique index that user input can trip, so any 23505 here is the name clash.
func (r *ExperimentRepository) Create(ctx context.Context, exp domain.Experiment) (domain.Experiment, error) {
	tagsJSON, err := marshalTags(exp.Tags)
	if err != nil {
		return domain.Experiment{}, fmt.Errorf("marshal tags: %w", err)
	}
	const q = `
		INSERT INTO experiments (id, name, description, tags, owner_id, team, created_at, updated_at, archived_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	if _, err := r.pool.Exec(ctx, q,
		exp.ID, exp.Name, exp.Description, tagsJSON, exp.OwnerID, exp.Team,
		exp.CreatedAt, exp.UpdatedAt, exp.ArchivedAt,
	); err != nil {
		if _, ok := isUniqueViolation(err); ok {
			return domain.Experiment{}, domain.ErrRepoConflict
		}
		return domain.Experiment{}, fmt.Errorf("insert experiment: %w", err)
	}
	return exp, nil
}

// GetByID returns the Experiment or domain.ErrRepoNotFound. It does NOT filter by
// team — the SERVICE checks the returned Team against the caller's claims (defense
// in depth). Returning the row regardless of team lets the service give a uniform
// "not found" for both missing and cross-tenant ids (no existence leak), which is
// its job, not the adapter's.
func (r *ExperimentRepository) GetByID(ctx context.Context, id string) (domain.Experiment, error) {
	q := `SELECT ` + experimentColumns + ` FROM experiments WHERE id = $1`
	return scanExperiment(r.pool.QueryRow(ctx, q, id))
}

// Update persists the mutable fields (name/description/tags) AND archived_at and
// updated_at. The service has already applied the field mask / archive stamp and
// refreshed updated_at, so the adapter writes the whole mutable row. A
// name-collision (renaming onto another active name) trips the same partial
// unique index → ErrRepoConflict.
//
// We RETURN the post-update row columns so the caller gets exactly what was
// stored (Postgres-truncated timestamps, normalized JSONB) without a second read.
func (r *ExperimentRepository) Update(ctx context.Context, exp domain.Experiment) (domain.Experiment, error) {
	tagsJSON, err := marshalTags(exp.Tags)
	if err != nil {
		return domain.Experiment{}, fmt.Errorf("marshal tags: %w", err)
	}
	q := `
		UPDATE experiments
		SET name = $2, description = $3, tags = $4, updated_at = $5, archived_at = $6
		WHERE id = $1
		RETURNING ` + experimentColumns
	updated, err := scanExperiment(r.pool.QueryRow(ctx, q,
		exp.ID, exp.Name, exp.Description, tagsJSON, exp.UpdatedAt, exp.ArchivedAt,
	))
	if err != nil {
		if _, ok := isUniqueViolation(err); ok {
			return domain.Experiment{}, domain.ErrRepoConflict
		}
		// scanExperiment already mapped no-rows → ErrRepoNotFound (the row vanished).
		return domain.Experiment{}, err
	}
	return updated, nil
}

// List returns a team-scoped page of experiments (newest-first) plus a nextToken
// cursor. includeArchived toggles whether soft-deleted rows are returned.
//
// KEYSET PAGINATION: ordered by (created_at DESC, id DESC) — the exact tuple the
// experiments_team_created_idx serves — so the page is a clean index scan and the
// cursor is (created_at, id). We fetch pageSize+1 rows: if we get the extra one,
// there IS a next page and we emit a token built from the LAST kept row.
//
// SQL SAFETY: team and the cursor values are bound parameters ($1, $2, $3); the
// only thing we ever build into the query text is the WHERE-clause PLACEHOLDER
// numbers — no user value is ever concatenated.
func (r *ExperimentRepository) List(ctx context.Context, team string, includeArchived bool, opts domain.ListOptions) ([]domain.Experiment, string, error) {
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	cur, err := decodeListCursor(opts.PageToken)
	if err != nil {
		return nil, "", fmt.Errorf("list experiments: %w", err)
	}

	args := []any{team}
	where := "WHERE team = $1"
	if !includeArchived {
		where += " AND archived_at IS NULL"
	}
	if cur != nil {
		// "strictly before" the cursor in DESC order = the (created_at, id) tuple is
		// row-wise LESS THAN the cursor. Row-value comparison gives the exact keyset
		// boundary in one predicate.
		args = append(args, cur.TS, cur.ID)
		where += fmt.Sprintf(" AND (created_at, id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, pageSize+1) // +1 sentinel row to detect a next page
	q := fmt.Sprintf(`
		SELECT %s FROM experiments
		%s
		ORDER BY created_at DESC, id DESC
		LIMIT $%d`, experimentColumns, where, len(args))

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list experiments: %w", err)
	}
	defer rows.Close()

	exps := make([]domain.Experiment, 0, pageSize)
	for rows.Next() {
		exp, err := scanExperiment(rows)
		if err != nil {
			return nil, "", err
		}
		exps = append(exps, exp)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate experiments: %w", err)
	}

	nextToken := ""
	if len(exps) > pageSize {
		exps = exps[:pageSize] // drop the sentinel
		last := exps[len(exps)-1]
		nextToken = encodeListCursor(listCursor{TS: last.CreatedAt, ID: last.ID})
	}
	return exps, nextToken, nil
}

// scanExperiment materializes one experiment row into the domain type, decoding
// the tags JSONB and turning the nullable archived_at into the domain's
// *time.Time. One helper for GetByID, Update (RETURNING), and List so the column
// order lives in exactly one place (experimentColumns).
func scanExperiment(row rowScanner) (domain.Experiment, error) {
	var (
		exp      domain.Experiment
		tagsJSON []byte
	)
	if err := row.Scan(
		&exp.ID, &exp.Name, &exp.Description, &tagsJSON, &exp.OwnerID, &exp.Team,
		&exp.CreatedAt, &exp.UpdatedAt, &exp.ArchivedAt, // *time.Time scans a NULL as nil
	); err != nil {
		if isNoRows(err) {
			return domain.Experiment{}, domain.ErrRepoNotFound
		}
		return domain.Experiment{}, fmt.Errorf("scan experiment: %w", err)
	}
	tags, err := unmarshalTags(tagsJSON)
	if err != nil {
		return domain.Experiment{}, fmt.Errorf("decode tags: %w", err)
	}
	exp.Tags = tags
	return exp, nil
}
