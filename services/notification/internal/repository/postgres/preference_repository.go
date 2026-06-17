// preference_repository.go — the Postgres adapter for domain.PreferenceRepository:
// the per-user ROUTING RULES the choreography reactor consults on every event.
//
// ============================================================================
// THE AGGREGATE-IN-TWO-TABLES, WRITTEN ATOMICALLY
// ============================================================================
//
// NotificationPreferences is ONE domain document (PUT semantics on Upsert) but
// lives in TWO tables: the root (notification_preferences: muted patterns +
// updated_at) and its children (notification_channel_prefs: one row per channel).
// The whole point of an aggregate is that it is read and written as a unit, so the
// Upsert MUST be atomic: a reader must never observe the root updated with stale
// child rows, or new child rows under an old root. We get that with a single
// pgx.Tx:
//
//	BEGIN
//	  UPSERT root (muted_patterns, updated_at)
//	  DELETE all child rows for the user        ┐ replace-the-set: PUT semantics
//	  INSERT the new child rows                 ┘ (absent channel = removed)
//	COMMIT  (or ROLLBACK on any error → no partial write)
//
// WHY delete-then-insert rather than per-row upsert: Upsert is PUT (replace the
// whole document), not PATCH. A channel the client omitted this time must DISAPPEAR
// — delete-all + insert-the-new-set expresses that exactly, whereas a per-row
// upsert would leave stale rows for omitted channels. The ON DELETE CASCADE on the
// child FK is a backstop; here we delete explicitly inside the tx so the timing is
// ours to control.
//
// SECURITY: UserID is server-set from auth claims by the service before this is
// called (never from a request body — anti mass-assignment), and every channel
// Target was already SSRF-validated by the service. The adapter trusts that
// contract and just persists.
// ============================================================================
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/abd-ulbasit/forgepoint/services/notification/internal/domain"
)

// Compile-time proof the adapter satisfies the domain port.
var _ domain.PreferenceRepository = (*PreferenceRepository)(nil)

// GetByUser returns the user's stored preferences, reassembling the aggregate from
// the root row + its channel rows. Returns domain.ErrRepoNotFound if the user has
// NO root row (never configured anything) — the service then substitutes sane
// defaults so the settings UI always has something to render. A user with a root
// row but zero channel rows is a VALID empty-channels document (not a not-found).
//
// Two queries (root, then children) rather than one join: the join would multiply
// the root's array columns across child rows (and a user with no channels would
// need an awkward LEFT JOIN with NULL-handling). Two small indexed reads are
// simpler and clearer; preferences are read-once-per-event and cacheable upstream,
// so the extra round-trip is immaterial.
func (r *PreferenceRepository) GetByUser(ctx context.Context, userID string) (domain.NotificationPreferences, error) {
	const rootQ = `SELECT user_id, muted_event_patterns, updated_at FROM notification_preferences WHERE user_id = $1`
	var prefs domain.NotificationPreferences
	if err := r.pool.QueryRow(ctx, rootQ, userID).Scan(
		&prefs.UserID, &prefs.MutedEventPatterns, &prefs.UpdatedAt,
	); err != nil {
		if isNoRows(err) {
			return domain.NotificationPreferences{}, domain.ErrRepoNotFound
		}
		return domain.NotificationPreferences{}, fmt.Errorf("get preferences root: %w", err)
	}

	const childQ = `
		SELECT channel, enabled, min_severity, target
		FROM notification_channel_prefs
		WHERE user_id = $1
		ORDER BY channel ASC`
	rows, err := r.pool.Query(ctx, childQ, userID)
	if err != nil {
		return domain.NotificationPreferences{}, fmt.Errorf("get channel prefs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cp          domain.ChannelPreference
			channel     int16
			minSeverity int16
		)
		if err := rows.Scan(&channel, &cp.Enabled, &minSeverity, &cp.Target); err != nil {
			return domain.NotificationPreferences{}, fmt.Errorf("scan channel pref: %w", err)
		}
		cp.Channel = domain.NotificationChannel(channel)
		cp.MinSeverity = domain.Severity(minSeverity)
		prefs.Channels = append(prefs.Channels, cp)
	}
	if err := rows.Err(); err != nil {
		return domain.NotificationPreferences{}, fmt.Errorf("iterate channel prefs: %w", err)
	}
	return prefs, nil
}

// Upsert replaces the user's preferences wholesale (PUT) inside ONE transaction and
// returns the stored result. The atomic root-upsert + children-replace is the heart
// of this method (see the file header). We return the input prefs (the caller's
// already-server-stamped document) on success — the write is authoritative, so
// there is nothing the DB changed that the caller doesn't already hold; a re-read
// would be a redundant round-trip.
func (r *PreferenceRepository) Upsert(ctx context.Context, prefs domain.NotificationPreferences) (domain.NotificationPreferences, error) {
	// pgx.BeginFunc runs the closure inside a transaction, COMMITting if it returns
	// nil and ROLLING BACK (and returning the error) otherwise. This is the
	// idiomatic, leak-proof tx wrapper: we cannot forget to Rollback on an early
	// return, and a panic still rolls back. The ATOMICITY the aggregate needs is
	// exactly this all-or-nothing boundary — a failed insert leaves NO partial write
	// (the root upsert and the child deletes roll back together).
	// The muted_event_patterns column is NOT NULL DEFAULT '{}'. A nil Go slice would
	// be encoded by pgx as SQL NULL (violating the NOT NULL), so we coalesce nil to a
	// non-nil empty slice → pgx writes '{}'. (The domain typically passes a non-nil
	// slice, but the adapter must not depend on that; "no patterns" is a valid,
	// non-NULL empty array.)
	muted := prefs.MutedEventPatterns
	if muted == nil {
		muted = []string{}
	}

	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		// 1) UPSERT the aggregate root. ON CONFLICT (user_id) DO UPDATE makes this a
		//    create-or-replace: first write inserts, subsequent writes overwrite the
		//    muted list + updated_at. user_id is the PK, so the conflict target is the
		//    user identity.
		const rootQ = `
			INSERT INTO notification_preferences (user_id, muted_event_patterns, updated_at)
			VALUES ($1, $2, $3)
			ON CONFLICT (user_id)
			DO UPDATE SET muted_event_patterns = EXCLUDED.muted_event_patterns,
			              updated_at           = EXCLUDED.updated_at`
		if _, err := tx.Exec(ctx, rootQ, prefs.UserID, muted, prefs.UpdatedAt); err != nil {
			return fmt.Errorf("upsert preferences root: %w", err)
		}

		// 2) DELETE the existing child rows — PUT semantics: the new set fully
		//    replaces the old, so omitted channels disappear.
		if _, err := tx.Exec(ctx, `DELETE FROM notification_channel_prefs WHERE user_id = $1`, prefs.UserID); err != nil {
			return fmt.Errorf("clear channel prefs: %w", err)
		}

		// 3) INSERT the new child rows. We use pgx.Batch so all the inserts go out in
		//    one network round-trip within the tx (fewer syscalls than N separate
		//    Execs), and any single failure aborts the whole tx via the BeginFunc
		//    error path. Each value is a bound parameter — no array/string building.
		if len(prefs.Channels) > 0 {
			batch := &pgx.Batch{}
			const childQ = `
				INSERT INTO notification_channel_prefs (user_id, channel, enabled, min_severity, target)
				VALUES ($1, $2, $3, $4, $5)`
			for _, cp := range prefs.Channels {
				batch.Queue(childQ, prefs.UserID, int16(cp.Channel), cp.Enabled, int16(cp.MinSeverity), cp.Target)
			}
			br := tx.SendBatch(ctx, batch)
			// We MUST drain the batch results: SendBatch returns a BatchResults we have
			// to Exec()/Close() so each queued statement's outcome is checked. A failing
			// insert (e.g. a duplicate channel for the user → 23505 on the PK) surfaces
			// here and aborts the tx. Closing before the function returns releases the
			// batch's hold on the connection.
			for range prefs.Channels {
				if _, err := br.Exec(); err != nil {
					_ = br.Close()
					return fmt.Errorf("insert channel pref: %w", err)
				}
			}
			if err := br.Close(); err != nil {
				return fmt.Errorf("close channel pref batch: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return domain.NotificationPreferences{}, err
	}
	return prefs, nil
}
