// Package postgres is the WRITE-side adapter of the registry's CQRS split: it
// implements domain.WriteStore against PostgreSQL using jackc/pgx/v5.
//
// ============================================================================
// THIS IS THE "SOURCE OF TRUTH" ADAPTER (the WRITE side)
// ============================================================================
//
// The domain (services/registry/internal/domain) defines the WriteStore PORT and
// stays pure — no SQL, no pgx. THIS package is the ADAPTER that satisfies that port
// against a real database. The dependency points inward: postgres imports domain
// (for the types + the ErrRecordNotFound/ErrWriteConflict sentinels it must return),
// never the reverse.
//
// THE FOUR THINGS THIS ADAPTER IS RESPONSIBLE FOR (the persistence contract):
//
//  1. PARAMETERIZED SQL ONLY. Every value a caller controls goes through a $1/$2
//     bind parameter — NEVER string concatenation. This is the structural defense
//     against SQL injection: pgx sends the query text and the arguments separately,
//     so a value like  "'; DROP TABLE models; --"  is data, never executable SQL.
//
//  2. ERROR TRANSLATION. A unique-constraint violation (SQLSTATE 23505) becomes
//     domain.ErrWriteConflict; a no-rows result (pgx.ErrNoRows) becomes
//     domain.ErrRecordNotFound. The service then maps those storage sentinels to
//     business errors. We translate at THIS boundary so the domain never sees a
//     driver-specific error string.
//
//  3. TRANSACTIONS for atomic invariants. PromoteVersionTx (the single-production
//     swap) and ArchiveModelTx (model + all versions) each run as ONE transaction —
//     the aggregate is the consistency boundary, so a partial write is impossible.
//
//  4. POOL LIFECYCLE + ctx. We own a *pgxpool.Pool, honor the caller's ctx on every
//     query (cancellation/deadline propagation), and Close() the pool on shutdown.
//
// WHY pgxpool (not a single *pgx.Conn, not database/sql): a connection POOL is
// required for a concurrent gRPC server — each in-flight RPC borrows a conn and
// returns it. pgx's native pool (pgxpool) is faster than database/sql's generic pool
// for Postgres (binary protocol, no driver.Valuer round-tripping) and exposes pgx's
// richer types (pgconn.PgError for SQLSTATE inspection). database/sql would hide the
// SQLSTATE behind a generic error, defeating point (2).
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abd-ulbasit/forgepoint/services/registry/internal/domain"
)

// WriteStore is the Postgres-backed implementation of domain.WriteStore.
//
// It holds the pool, not individual connections — every method borrows a conn from
// the pool for the duration of its query/transaction and returns it. The pool is
// safe for concurrent use by many goroutines (each RPC handler).
type WriteStore struct {
	pool *pgxpool.Pool
}

// Compile-time proof the adapter satisfies the port. If a domain method signature
// drifts, this line fails to BUILD — the cheapest place to catch interface skew,
// exactly as the service impl does with its own `var _` assertion.
var _ domain.WriteStore = (*WriteStore)(nil)

// NewWriteStore builds a WriteStore from a DSN, creating and pinging a pgx pool.
//
// WHY ping here: a pool created by pgxpool.New is LAZY — it does not connect until
// the first query. Pinging in the constructor surfaces a bad DSN / unreachable DB at
// startup (fail fast) instead of on the first RPC. The caller (main.go) treats a
// constructor error as a fatal boot failure.
//
// The returned cleanup-via-Close is the pool lifecycle: the caller defers store.Close().
func NewWriteStore(ctx context.Context, dsn string) (*WriteStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("registry/postgres: create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("registry/postgres: ping: %w", err)
	}
	return &WriteStore{pool: pool}, nil
}

// NewWriteStoreFromPool wraps an already-constructed pool. Tests use this so they can
// share ONE pool (the same one that applied migrations) with the adapter, avoiding a
// second connection and making the migration + the adapter act on the same database.
func NewWriteStoreFromPool(pool *pgxpool.Pool) *WriteStore {
	return &WriteStore{pool: pool}
}

// Close releases the pool. Idempotent-safe to call once at shutdown.
func (s *WriteStore) Close() { s.pool.Close() }

// ============================================================================
// SQL ERROR TRANSLATION — the storage→domain sentinel boundary
// ============================================================================

// isUniqueViolation reports whether err is a Postgres unique_violation (SQLSTATE
// 23505). We inspect the typed *pgconn.PgError rather than string-matching the
// message (which is locale- and version-dependent and would be brittle). 23505 is
// the ANSI/Postgres code for "you violated a UNIQUE constraint" — the signal that a
// (team,name), (model_id,version), single-production, or idempotency index rejected
// the row. Every such case maps to domain.ErrWriteConflict.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// ============================================================================
// MODELS — CreateModel / LookupModelByIdempotencyKey / GetModel / UpdateModel
// ============================================================================

// modelColumns is the canonical SELECT list for a model row, in the exact order
// scanModel reads them. Defining it once keeps every SELECT and the scanner in lockstep
// — adding a column means editing two adjacent spots, not hunting call sites.
const modelColumns = `id, name, description, owner_id, team, framework, task_type,
	tags, production_version, latest_version, created_at, updated_at, archived_at`

// CreateModel inserts a new model, recording the optional CREATE-idempotency key
// inline on the row. Returns domain.ErrWriteConflict on a (team,name) OR
// (team,idempotency_key) collision — both are unique-index violations the service
// distinguishes by having checked the key FIRST.
//
// nullableKey turns an empty key into SQL NULL so the partial unique index (which
// only covers NON-NULL keys) lets unlimited no-key creates through while still
// enforcing one-row-per-key for real keys.
func (s *WriteStore) CreateModel(ctx context.Context, m domain.Model, idempotencyKey string) (domain.Model, error) {
	tagsJSON, err := marshalTags(m.Tags)
	if err != nil {
		return domain.Model{}, err
	}
	const q = `
		INSERT INTO models
			(id, name, description, owner_id, team, framework, task_type, tags,
			 production_version, latest_version, idempotency_key, created_at, updated_at, archived_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`
	_, err = s.pool.Exec(ctx, q,
		m.ID, m.Name, m.Description, m.OwnerID, m.Team, m.Framework, m.TaskType, tagsJSON,
		m.ProductionVersion, m.LatestVersion, nullableString(idempotencyKey),
		m.CreatedAt, m.UpdatedAt, nullableTime(m.ArchivedAt),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return domain.Model{}, domain.ErrWriteConflict
		}
		return domain.Model{}, fmt.Errorf("registry/postgres: insert model: %w", err)
	}
	return m, nil
}

// LookupModelByIdempotencyKey returns the model previously created with this key in
// this team, or domain.ErrRecordNotFound if none. An EMPTY key never matches (the
// caller opted out) — we short-circuit without a query so an empty key can't
// accidentally match the partial index's absence-of-rows.
func (s *WriteStore) LookupModelByIdempotencyKey(ctx context.Context, team, key string) (domain.Model, error) {
	if key == "" {
		return domain.Model{}, domain.ErrRecordNotFound
	}
	q := `SELECT ` + modelColumns + ` FROM models WHERE team = $1 AND idempotency_key = $2`
	row := s.pool.QueryRow(ctx, q, team, key)
	return scanModel(row)
}

// GetModel loads a model by id for command-side validation (read the TRUTH, never the
// projection). Returns domain.ErrRecordNotFound when absent.
func (s *WriteStore) GetModel(ctx context.Context, id string) (domain.Model, error) {
	q := `SELECT ` + modelColumns + ` FROM models WHERE id = $1`
	row := s.pool.QueryRow(ctx, q, id)
	return scanModel(row)
}

// UpdateModel persists a mutated model: the editable fields (description/tags) and the
// denormalized pointers + archived_at + updated_at. We do NOT update immutable fields
// (id/name/team/owner/created_at) — they are never part of an edit. Returns the model
// as written. ErrRecordNotFound if the id no longer exists (e.g. a concurrent delete),
// detected via the command tag's RowsAffected.
func (s *WriteStore) UpdateModel(ctx context.Context, m domain.Model) (domain.Model, error) {
	tagsJSON, err := marshalTags(m.Tags)
	if err != nil {
		return domain.Model{}, err
	}
	const q = `
		UPDATE models
		   SET description = $2,
		       framework = $3,
		       task_type = $4,
		       tags = $5,
		       production_version = $6,
		       latest_version = $7,
		       archived_at = $8,
		       updated_at = $9
		 WHERE id = $1`
	tag, err := s.pool.Exec(ctx, q,
		m.ID, m.Description, m.Framework, m.TaskType, tagsJSON,
		m.ProductionVersion, m.LatestVersion, nullableTime(m.ArchivedAt), m.UpdatedAt,
	)
	if err != nil {
		return domain.Model{}, fmt.Errorf("registry/postgres: update model: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.Model{}, domain.ErrRecordNotFound
	}
	return m, nil
}

// ============================================================================
// VERSIONS — Create / Lookup / Get / Count / UpdateStatus / Promote / FindProd
// ============================================================================

// versionColumns is the canonical SELECT list for a version row, matching scanVersion.
const versionColumns = `id, model_id, version, description, metrics, artifact_path,
	artifact_digest, size_bytes, stage, status, created_by, created_at`

// CreateVersion inserts a new version row (always Stage=DEV / Status=PENDING_UPLOAD as
// stamped by the service). Returns domain.ErrWriteConflict on a (model_id, version)
// collision — the signal the service's collision-safe loop races against — OR on a
// (model_id, idempotency_key) collision.
func (s *WriteStore) CreateVersion(ctx context.Context, v domain.ModelVersion, idempotencyKey string) (domain.ModelVersion, error) {
	metricsJSON, err := marshalMetrics(v.Metrics)
	if err != nil {
		return domain.ModelVersion{}, err
	}
	const q = `
		INSERT INTO model_versions
			(id, model_id, version, description, metrics, artifact_path, artifact_digest,
			 size_bytes, stage, status, idempotency_key, created_by, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`
	_, err = s.pool.Exec(ctx, q,
		v.ID, v.ModelID, v.Version, v.Description, metricsJSON, v.ArtifactPath, v.ArtifactDigest,
		v.SizeBytes, int16(v.Stage), int16(v.Status), nullableString(idempotencyKey),
		v.CreatedBy, v.CreatedAt,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return domain.ModelVersion{}, domain.ErrWriteConflict
		}
		return domain.ModelVersion{}, fmt.Errorf("registry/postgres: insert version: %w", err)
	}
	return v, nil
}

// LookupVersionByIdempotencyKey mirrors the model lookup, scoped per model. Empty key
// never matches.
func (s *WriteStore) LookupVersionByIdempotencyKey(ctx context.Context, modelID, key string) (domain.ModelVersion, error) {
	if key == "" {
		return domain.ModelVersion{}, domain.ErrRecordNotFound
	}
	q := `SELECT ` + versionColumns + ` FROM model_versions WHERE model_id = $1 AND idempotency_key = $2`
	row := s.pool.QueryRow(ctx, q, modelID, key)
	return scanVersion(row)
}

// GetVersion loads a version by id for command-side validation. ErrRecordNotFound if absent.
func (s *WriteStore) GetVersion(ctx context.Context, id string) (domain.ModelVersion, error) {
	q := `SELECT ` + versionColumns + ` FROM model_versions WHERE id = $1`
	row := s.pool.QueryRow(ctx, q, id)
	return scanVersion(row)
}

// CountVersions returns how many versions a model has — the seed for the service's
// auto-assigned next label (count+1). A COUNT(*) is exact here (no race-free guarantee
// is needed: the service's insert loop RETRIES on the resulting collision, so a stale
// count is self-correcting — see the CreateVersion loop's TOCTOU note in the service).
func (s *WriteStore) CountVersions(ctx context.Context, modelID string) (int, error) {
	const q = `SELECT count(*) FROM model_versions WHERE model_id = $1`
	var n int
	if err := s.pool.QueryRow(ctx, q, modelID).Scan(&n); err != nil {
		return 0, fmt.Errorf("registry/postgres: count versions: %w", err)
	}
	return n, nil
}

// UpdateVersionStatus persists a status transition plus the server-measured artifact
// facts (path/digest/size) — written on the PENDING_UPLOAD→READY edge. We update stage
// too (it is part of the row's current truth, though MarkVersionReady doesn't change it)
// so the method is a faithful "persist this version's mutable state". ErrRecordNotFound
// if the id vanished.
func (s *WriteStore) UpdateVersionStatus(ctx context.Context, v domain.ModelVersion) (domain.ModelVersion, error) {
	const q = `
		UPDATE model_versions
		   SET status = $2,
		       stage = $3,
		       artifact_path = $4,
		       artifact_digest = $5,
		       size_bytes = $6
		 WHERE id = $1`
	tag, err := s.pool.Exec(ctx, q,
		v.ID, int16(v.Status), int16(v.Stage), v.ArtifactPath, v.ArtifactDigest, v.SizeBytes,
	)
	if err != nil {
		return domain.ModelVersion{}, fmt.Errorf("registry/postgres: update version status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ModelVersion{}, domain.ErrRecordNotFound
	}
	return v, nil
}

// FindProductionVersion returns the model's current PRODUCTION version, or
// domain.ErrRecordNotFound if it has none. There is AT MOST one such row (the partial
// unique index guarantees it), so LIMIT 1 is belt-and-suspenders, not a tiebreaker.
func (s *WriteStore) FindProductionVersion(ctx context.Context, modelID string) (domain.ModelVersion, error) {
	q := `SELECT ` + versionColumns + `
	        FROM model_versions
	       WHERE model_id = $1 AND stage = $2
	       LIMIT 1`
	row := s.pool.QueryRow(ctx, q, modelID, int16(domain.StageProduction))
	return scanVersion(row)
}

// PromoteVersionTx is the SINGLE-PRODUCTION INVARIANT made atomic. It writes BOTH the
// promoted version's new stage AND (if present) the demoted prior-production version's
// ARCHIVED stage inside ONE transaction, so there is never an instant with two
// PRODUCTION rows for the model.
//
// THE ORDERING SUBTLETY — DEMOTE BEFORE PROMOTE:
//
//	The model_versions_one_production_per_model partial unique index forbids two rows
//	with stage=PRODUCTION for the same model. If we UPDATE the new version to PRODUCTION
//	FIRST, while the incumbent is still PRODUCTION, that second PRODUCTION row violates
//	the index and the statement fails — even though the END state is valid. So we
//	ARCHIVE the incumbent first, THEN promote the new one. Within a transaction the
//	index is checked per-statement (it is not DEFERRABLE here), so the intermediate
//	state must itself be legal. Ordering the writes demote→promote keeps every
//	intermediate state ≤1 production row.
//
//	(An alternative is a DEFERRABLE unique constraint checked at COMMIT, which would
//	let the two UPDATEs run in any order; we choose explicit ordering because a partial
//	index cannot be deferred in Postgres and the ordering is a clear, auditable rule.)
//
// The service has already computed both ends and validated the transition; the adapter
// only persists them transactionally. A zero-value `demoted` (ID == "") means "no
// incumbent to demote" (first promotion to production, or a non-production target).
func (s *WriteStore) PromoteVersionTx(ctx context.Context, promoted domain.ModelVersion, demoted domain.ModelVersion) (domain.ModelVersion, domain.ModelVersion, error) {
	// runInTx wraps the two updates so a failure rolls BOTH back — the atomicity that
	// makes the swap safe. pgx.BeginFunc commits on a nil return and rolls back on any
	// error (or panic), so we cannot leak a half-applied swap.
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// DEMOTE FIRST (see the ordering note). Only when there is an incumbent.
		if demoted.ID != "" {
			if err := updateVersionStageTx(ctx, tx, demoted.ID, demoted.Stage); err != nil {
				return fmt.Errorf("demote prior production: %w", err)
			}
		}
		// THEN PROMOTE. By now the model has zero PRODUCTION rows, so setting this one
		// to PRODUCTION cannot violate the partial unique index.
		if err := updateVersionStageTx(ctx, tx, promoted.ID, promoted.Stage); err != nil {
			return fmt.Errorf("promote version: %w", err)
		}
		return nil
	})
	if err != nil {
		// A unique violation here means a CONCURRENT promotion beat us to PRODUCTION
		// (the index backstop fired). Surface it as a write conflict so the service can
		// treat it like any other lost race rather than a corrupt state.
		if isUniqueViolation(err) {
			return domain.ModelVersion{}, domain.ModelVersion{}, domain.ErrWriteConflict
		}
		return domain.ModelVersion{}, domain.ModelVersion{}, fmt.Errorf("registry/postgres: promote tx: %w", err)
	}
	return promoted, demoted, nil
}

// updateVersionStageTx updates a single version's stage within a transaction. Factored
// out so PromoteVersionTx reads as "demote, then promote" with the SQL detail hidden.
func updateVersionStageTx(ctx context.Context, tx pgx.Tx, id string, stage domain.ModelStage) error {
	const q = `UPDATE model_versions SET stage = $2 WHERE id = $1`
	tag, err := tx.Exec(ctx, q, id, int16(stage))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrRecordNotFound
	}
	return nil
}

// ArchiveModelTx soft-deletes a model AND archives ALL its versions in one transaction
// — the aggregate is the consistency boundary, so either everything archives or nothing
// does. The service has already stamped the model's ArchivedAt/UpdatedAt.
//
// WHY archive versions too (not just the model): a "deleted" model whose versions still
// claimed PRODUCTION would be a dangling servable pointer. Archiving the children keeps
// the lifecycle honest — nothing under an archived model is servable. The single-prod
// index is unaffected (PRODUCTION→ARCHIVED removes the row from the partial index).
func (s *WriteStore) ArchiveModelTx(ctx context.Context, m domain.Model) (domain.Model, error) {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// 1. Stamp the model's soft-delete (archived_at) + updated_at. Also clear the
		//    denormalized production_version pointer — an archived model serves nothing.
		const mq = `
			UPDATE models
			   SET archived_at = $2, updated_at = $3, production_version = ''
			 WHERE id = $1`
		tag, err := tx.Exec(ctx, mq, m.ID, m.ArchivedAt, m.UpdatedAt)
		if err != nil {
			return fmt.Errorf("archive model row: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return domain.ErrRecordNotFound
		}
		// 2. Archive every NON-already-archived version of this model. The
		//    `stage <> ARCHIVED` predicate keeps the statement idempotent-friendly and
		//    avoids needless writes. This is the aggregate cascade in one statement.
		const vq = `UPDATE model_versions SET stage = $2 WHERE model_id = $1 AND stage <> $2`
		if _, err := tx.Exec(ctx, vq, m.ID, int16(domain.StageArchived)); err != nil {
			return fmt.Errorf("archive versions: %w", err)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, domain.ErrRecordNotFound) {
			return domain.Model{}, domain.ErrRecordNotFound
		}
		return domain.Model{}, fmt.Errorf("registry/postgres: archive tx: %w", err)
	}
	return m, nil
}

// ============================================================================
// MUTATION-IDEMPOTENCY LEDGER — LookupCommandIdempotency / RecordCommandIdempotency
// ============================================================================

// LookupCommandIdempotency returns the entity id a prior (team, command, key)
// invocation recorded, or domain.ErrRecordNotFound if unseen. Empty key never matches
// (caller opted out). This is the READ half of the generic mutation-idempotency ledger.
func (s *WriteStore) LookupCommandIdempotency(ctx context.Context, team string, command domain.Command, key string) (string, error) {
	if key == "" {
		return "", domain.ErrRecordNotFound
	}
	const q = `SELECT entity_id FROM command_idempotency WHERE team = $1 AND command = $2 AND key = $3`
	var entityID string
	err := s.pool.QueryRow(ctx, q, team, int16(command), key).Scan(&entityID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", domain.ErrRecordNotFound
		}
		return "", fmt.Errorf("registry/postgres: lookup command idempotency: %w", err)
	}
	return entityID, nil
}

// RecordCommandIdempotency records (team, command, key) → entityID. Empty key is a no-op.
//
// THE CONFLICT SEMANTICS (matching the port's contract precisely):
//
//	We use INSERT ... ON CONFLICT (team, command, key) DO NOTHING so the FIRST writer
//	for a key wins a concurrent race and the loser's insert is a silent no-op — NOT an
//	error — as long as the loser is recording the SAME entity_id (a benign duplicate).
//	But if a concurrent writer already recorded a DIFFERENT entity for the same key,
//	that is a genuine KEY-REUSE race the port says to surface as ErrWriteConflict. We
//	detect it by RETURNING the row we tried to insert: on a real insert the row comes
//	back; on a DO NOTHING conflict nothing comes back, so we then read the existing row
//	and compare its entity_id. Same → benign, return nil. Different → ErrWriteConflict.
//
//	WHY not a bare unique-violation catch: ON CONFLICT DO NOTHING SUPPRESSES the 23505,
//	so there is no error to catch — we must explicitly read-back to tell "same key, same
//	entity (fine)" from "same key, different entity (conflict)".
func (s *WriteStore) RecordCommandIdempotency(ctx context.Context, team string, command domain.Command, key, entityID string) error {
	if key == "" {
		return nil
	}
	// Try to insert; DO NOTHING on conflict. RETURNING entity_id yields a row ONLY when
	// our insert actually happened (no conflict).
	const insertQ = `
		INSERT INTO command_idempotency (team, command, key, entity_id)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (team, command, key) DO NOTHING
		RETURNING entity_id`
	var inserted string
	err := s.pool.QueryRow(ctx, insertQ, team, int16(command), key, entityID).Scan(&inserted)
	if err == nil {
		// Our insert won — the ledger now points at entityID.
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("registry/postgres: record command idempotency: %w", err)
	}
	// ErrNoRows ⇒ the ON CONFLICT path fired (a row already existed). Read it back and
	// compare: same entity is a benign duplicate; different entity is a key-reuse race.
	const readQ = `SELECT entity_id FROM command_idempotency WHERE team = $1 AND command = $2 AND key = $3`
	var existing string
	if err := s.pool.QueryRow(ctx, readQ, team, int16(command), key).Scan(&existing); err != nil {
		return fmt.Errorf("registry/postgres: read-back command idempotency: %w", err)
	}
	if existing != entityID {
		return domain.ErrWriteConflict
	}
	return nil
}

// ============================================================================
// SCANNERS + (DE)SERIALIZATION HELPERS
// ============================================================================

// rowScanner abstracts pgx.Row and pgx.Rows for the scan helpers, so the same
// scanModel/scanVersion works whether the caller used QueryRow (single) or Query (loop).
type rowScanner interface {
	Scan(dest ...any) error
}

// scanModel reads one model row in modelColumns order, translating pgx.ErrNoRows to the
// domain's ErrRecordNotFound and decoding JSONB tags + the nullable archived_at.
func scanModel(row rowScanner) (domain.Model, error) {
	var (
		m          domain.Model
		tagsJSON   []byte
		archivedAt *time.Time // NULL when active
	)
	err := row.Scan(
		&m.ID, &m.Name, &m.Description, &m.OwnerID, &m.Team, &m.Framework, &m.TaskType,
		&tagsJSON, &m.ProductionVersion, &m.LatestVersion, &m.CreatedAt, &m.UpdatedAt, &archivedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Model{}, domain.ErrRecordNotFound
		}
		return domain.Model{}, fmt.Errorf("registry/postgres: scan model: %w", err)
	}
	if archivedAt != nil {
		m.ArchivedAt = *archivedAt
	}
	tags, err := unmarshalTags(tagsJSON)
	if err != nil {
		return domain.Model{}, err
	}
	m.Tags = tags
	return m, nil
}

// scanVersion reads one version row in versionColumns order, decoding the SMALLINT
// stage/status back into the domain enums and the JSONB metrics map.
func scanVersion(row rowScanner) (domain.ModelVersion, error) {
	var (
		v           domain.ModelVersion
		metricsJSON []byte
		stage       int16
		status      int16
	)
	err := row.Scan(
		&v.ID, &v.ModelID, &v.Version, &v.Description, &metricsJSON, &v.ArtifactPath,
		&v.ArtifactDigest, &v.SizeBytes, &stage, &status, &v.CreatedBy, &v.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ModelVersion{}, domain.ErrRecordNotFound
		}
		return domain.ModelVersion{}, fmt.Errorf("registry/postgres: scan version: %w", err)
	}
	v.Stage = domain.ModelStage(stage)
	v.Status = domain.VersionStatus(status)
	metrics, err := unmarshalMetrics(metricsJSON)
	if err != nil {
		return domain.ModelVersion{}, err
	}
	v.Metrics = metrics
	return v, nil
}

// marshalTags encodes a string→string tag map to JSONB bytes. A nil/empty map becomes
// the literal `{}` (not SQL NULL) so the column's NOT NULL DEFAULT '{}' invariant holds
// and the read side always gets a non-null object to decode.
func marshalTags(tags map[string]string) ([]byte, error) {
	if len(tags) == 0 {
		return []byte(`{}`), nil
	}
	b, err := json.Marshal(tags)
	if err != nil {
		return nil, fmt.Errorf("registry/postgres: marshal tags: %w", err)
	}
	return b, nil
}

// unmarshalTags decodes JSONB tag bytes back to a map. An empty object decodes to nil
// (a clean zero-value model), matching the domain's cloneTags convention.
func unmarshalTags(b []byte) (map[string]string, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var tags map[string]string
	if err := json.Unmarshal(b, &tags); err != nil {
		return nil, fmt.Errorf("registry/postgres: unmarshal tags: %w", err)
	}
	if len(tags) == 0 {
		return nil, nil
	}
	return tags, nil
}

// marshalMetrics / unmarshalMetrics are the float64 analog for version metrics.
func marshalMetrics(metrics map[string]float64) ([]byte, error) {
	if len(metrics) == 0 {
		return []byte(`{}`), nil
	}
	b, err := json.Marshal(metrics)
	if err != nil {
		return nil, fmt.Errorf("registry/postgres: marshal metrics: %w", err)
	}
	return b, nil
}

func unmarshalMetrics(b []byte) (map[string]float64, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var metrics map[string]float64
	if err := json.Unmarshal(b, &metrics); err != nil {
		return nil, fmt.Errorf("registry/postgres: unmarshal metrics: %w", err)
	}
	if len(metrics) == 0 {
		return nil, nil
	}
	return metrics, nil
}

// nullableString maps "" → SQL NULL (via a *string) so the partial unique idempotency
// index treats no-key rows as absent. pgx encodes a nil *string as NULL.
func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// nullableTime maps the zero time → SQL NULL (active model). A non-zero ArchivedAt is
// passed through. This is the (archived_at IS NULL) == active mapping at the boundary.
func nullableTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
