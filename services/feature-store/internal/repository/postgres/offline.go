// offline.go — the Postgres ADAPTER implementing domain.OfflineViewStore: the
// durable, point-in-time / baseline READ MODEL (the CQRS read side).
//
// ============================================================================
// PATTERN: CQRS read model materialized in Postgres
// ============================================================================
//
// Event sourcing's natural companion is CQRS: the write model (the event log) and
// the read models have DIFFERENT shapes. The offline read model is a flat
// (view, entity) -> vector table, materialized from a replay of the log by
// RebuildViews(offline). GetAsOf serves point-in-time reads from it.
//
// IMPORTANT — relationship to the domain fold: the domain's ProjectAsOf is the
// REFERENCE semantics. The service's GetHistoricalFeatures currently folds the log
// directly (so behavior is identical regardless of this store's maturity); this
// offline_view table is the durable BASELINE that Rebuild materializes and that a
// future scaled GetAsOf reads from. This adapter must AGREE with the fold: Rebuild
// stores exactly the vectors the service hands it (already projected), and GetAsOf
// returns them filtered to the requested entities and to event_time ≤ asOf. We do
// not recompute the projection in SQL here — the service owns the projection; this
// store persists and serves it. (A production evolution could push the
// DISTINCT ON (entity_id) … ORDER BY version DESC scan into SQL; the schema's
// idx_feature_events_entity_time index already supports it. We keep the
// materialized-baseline approach so this store stays a faithful cache of the domain
// fold the tests pin.)
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/domain"
)

// OfflineViewStore is the Postgres-backed implementation of domain.OfflineViewStore.
type OfflineViewStore struct {
	pool *pgxpool.Pool
}

// NewOfflineViewStore constructs the adapter. Matches main.go's documented
// constructor: postgres.NewOfflineViewStore(pool).
func NewOfflineViewStore(pool *pgxpool.Pool) *OfflineViewStore {
	return &OfflineViewStore{pool: pool}
}

var _ domain.OfflineViewStore = (*OfflineViewStore)(nil)

// ============================================================================
// GetAsOf — point-in-time read, paginated over entities
// ============================================================================
//
// Returns the materialized vectors for the requested entities whose producing
// event_time is ≤ asOf, paginated by entity_id cursor. Entities with no qualifying
// row are simply absent from the map (the service reports them missing). nextToken
// is empty on the last page.
//
// WHY also filter event_time ≤ asOf here (the baseline stores "latest"): a caller
// may ask for an as-of BEFORE the baseline's event_time. The materialized row then
// does not qualify and we omit it — keeping GetAsOf honest about point-in-time even
// against a "latest" baseline, and never returning a value newer than the requested
// instant.
func (s *OfflineViewStore) GetAsOf(ctx context.Context, featureViewID string, entityIDs []string, asOf time.Time, opts domain.ListOptions) (map[string]domain.FeatureVector, string, error) {
	out := make(map[string]domain.FeatureVector)
	if len(entityIDs) == 0 {
		return out, "", nil
	}

	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = domain.DefaultPageSize
	}
	if pageSize > domain.MaxPageSize {
		pageSize = domain.MaxPageSize
	}

	// Query is fully parameterized: the entity id set is bound as a single array
	// parameter ($2 = ANY), the time as $3, the cursor as $4. Passing the id slice as
	// ANY($2) (not building an N-placeholder IN list by string concat) is both
	// injection-safe and avoids query-plan churn for different batch sizes.
	//
	// We fetch pageSize+1 to detect a next page without a COUNT, then trim.
	const q = `
		SELECT entity_id, feature_values, as_of_version, event_time, schema_version
		FROM offline_view
		WHERE feature_view_id = $1
		  AND entity_id = ANY($2)
		  AND event_time <= $3
		  AND entity_id > $4
		ORDER BY entity_id ASC
		LIMIT $5`

	rows, err := s.pool.Query(ctx, q, featureViewID, entityIDs, asOf, opts.PageToken, pageSize+1)
	if err != nil {
		return nil, "", fmt.Errorf("get as-of: %w", err)
	}
	defer rows.Close()

	// Track the ordered ids so we can compute the keyset cursor (the last kept
	// entity_id) and trim the +1 sentinel row.
	var orderedIDs []string
	for rows.Next() {
		var (
			entityID   string
			valuesJSON []byte
			vec        domain.FeatureVector
		)
		if scanErr := rows.Scan(&entityID, &valuesJSON, &vec.AsOfVersion, &vec.EventTime, &vec.SchemaVersion); scanErr != nil {
			return nil, "", fmt.Errorf("scan offline row: %w", scanErr)
		}
		values, uerr := unmarshalValues(valuesJSON)
		if uerr != nil {
			return nil, "", uerr
		}
		vec.EntityID = entityID
		vec.Values = values
		out[entityID] = vec
		orderedIDs = append(orderedIDs, entityID)
	}
	if rows.Err() != nil {
		return nil, "", fmt.Errorf("iterate offline rows: %w", rows.Err())
	}

	nextToken := ""
	if len(orderedIDs) > pageSize {
		// Drop the sentinel overflow row from BOTH the map and the cursor calc.
		overflowID := orderedIDs[pageSize]
		delete(out, overflowID)
		orderedIDs = orderedIDs[:pageSize]
		nextToken = orderedIDs[len(orderedIDs)-1]
	}
	return out, nextToken, nil
}

// ============================================================================
// Rebuild — replace the offline projection for a view (idempotent)
// ============================================================================
//
// Called by RebuildViews(offline). It atomically REPLACES the view's materialized
// rows with the freshly-computed set: DELETE the view's old rows, then INSERT the
// new vectors — all in one transaction so a reader never sees a half-rebuilt view
// (atomicity), and a re-run produces the same result (idempotent).
//
// WHY delete-then-insert rather than UPSERT-and-prune: a Rebuild may REMOVE entities
// that no longer have a qualifying value (e.g. after a corrective replay). A plain
// UPSERT would leave stale rows for entities absent from the new set; the explicit
// DELETE of the whole view guarantees the table reflects exactly the new projection.
func (s *OfflineViewStore) Rebuild(ctx context.Context, featureViewID string, vectors map[string]domain.FeatureVector) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin rebuild tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Clear the view's existing materialization. Scoped to the one view so a rebuild
	// of view A never touches view B.
	if _, err = tx.Exec(ctx, `DELETE FROM offline_view WHERE feature_view_id = $1`, featureViewID); err != nil {
		return fmt.Errorf("clear offline view: %w", err)
	}

	// Insert the new vectors. We use a batch so N inserts are one network round-trip
	// (pgx pipelines the batch), keeping a large rebuild efficient. All values are
	// parameterized.
	if len(vectors) > 0 {
		batch := &pgx.Batch{}
		const ins = `
			INSERT INTO offline_view
				(feature_view_id, entity_id, feature_values, as_of_version, event_time, schema_version)
			VALUES ($1, $2, $3, $4, $5, $6)`
		for entityID, vec := range vectors {
			valuesJSON, merr := marshalVector(vec.Values)
			if merr != nil {
				return merr
			}
			batch.Queue(ins, featureViewID, entityID, valuesJSON, vec.AsOfVersion, vec.EventTime, vec.SchemaVersion)
		}
		br := tx.SendBatch(ctx, batch)
		// Drain the batch results: each Queue'd insert must Exec cleanly. We must read
		// every result before Close, or pgx reports the batch incomplete.
		for range vectors {
			if _, execErr := br.Exec(); execErr != nil {
				_ = br.Close()
				return fmt.Errorf("insert offline vector: %w", execErr)
			}
		}
		if closeErr := br.Close(); closeErr != nil {
			return fmt.Errorf("close offline batch: %w", closeErr)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rebuild tx: %w", err)
	}
	return nil
}
