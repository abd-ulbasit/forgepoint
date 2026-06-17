// notification_repository.go — the Postgres adapter for
// domain.NotificationRepository: the inbox READ MODEL + the delivery_log AUDIT
// TRAIL of the choreography reactor.
//
// ============================================================================
// WHAT THIS ADAPTER GUARANTEES (the security/behaviour the ports demand)
// ============================================================================
//
//   - ANTI-IDOR BY CONSTRUCTION: every read/mutate is scoped by
//     recipient_user_id in the WHERE clause. There is NO query that fetches a
//     notification by id alone — GetByIDForUser takes the user as a bound
//     parameter, so a cross-user read returns zero rows (→ ErrRepoNotFound), which
//     the service surfaces as the existence-hiding ErrNotFound. The DB enforces
//     the authority the port promises.
//
//   - IDEMPOTENT CREATE: a duplicate (event_id, recipient_user_id) trips the
//     UNIQUE index (23505) and we return ErrRepoAlreadyExists — the durable half
//     of the at-least-once-NATS dedup guarantee.
//
//   - IDEMPOTENT MarkRead: the UPDATE only flips rows that are currently unread
//     (read = FALSE) AND belong to the user, and returns the affected-row count.
//     Re-marking an already-read id, or a foreign/unknown id, transitions nothing
//     and is not counted — exactly the port's "MarkRead is idempotent" contract.
//
//   - KEYSET PAGINATION: both list methods are cursor-paginated (see cursor.go),
//     stable under the concurrent writes the reactor produces.
//
// ============================================================================
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/notification/internal/domain"
)

// Compile-time proof the adapter satisfies the domain port. If a signature ever
// drifts, the build breaks HERE with a precise message, not mysteriously at the
// wiring site in main.
var _ domain.NotificationRepository = (*NotificationRepository)(nil)

// notificationColumns is the canonical SELECT column list, defined once so
// GetByIDForUser and ListForUser scan IDENTICAL column order through the same
// scanNotification helper.
const notificationColumns = `id, recipient_user_id, title, body, severity, channels, read, event_id, event_type, source_service, created_at, read_at`

// ============================================================================
// Create — persist a new inbox row (idempotent on (event_id, recipient)).
// ============================================================================

// Create writes a new Notification. The service has already set every
// server-authoritative field (id, recipient, severity, channels, timestamps); the
// adapter just writes the row and translates the dedup collision.
//
// channels is passed as a []int16 (the domain ints widened) — pgx encodes a Go
// slice straight into the SMALLINT[] column via its array codec, no manual array
// literal building (and therefore no string-concatenation of values). read_at is
// the zero time.Time while unread; we store it as NULL (not the zero instant) via
// nullTime so a round-trip preserves "unread" as a genuine NULL, matching the
// scanNotification side.
func (r *NotificationRepository) Create(ctx context.Context, n domain.Notification) (domain.Notification, error) {
	const q = `
		INSERT INTO notifications
			(id, recipient_user_id, title, body, severity, channels, read,
			 event_id, event_type, source_service, created_at, read_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`
	if _, err := r.pool.Exec(ctx, q,
		n.ID, n.RecipientUserID, n.Title, n.Body,
		int16(n.Severity), channelsToInt16(n.Channels), n.Read,
		n.EventID, n.EventType, n.SourceService,
		n.CreatedAt, nullTime(n.ReadAt),
	); err != nil {
		if isUniqueViolation(err) {
			// Redelivered event (or a racing replica) for the same recipient — the
			// inbox row already exists. The consumer adapter treats this as "already
			// materialized; skip", NOT as a failure. (Idempotent consumer.)
			return domain.Notification{}, ErrRepoAlreadyExists
		}
		return domain.Notification{}, fmt.Errorf("insert notification: %w", err)
	}
	return n, nil
}

// ============================================================================
// GetByIDForUser — anti-IDOR single read.
// ============================================================================

// GetByIDForUser returns the notification IFF it belongs to recipientUserID.
// recipientUserID is a bound parameter in the WHERE clause, so a foreign or
// unknown id simply matches no row → ErrRepoNotFound. The service maps that to the
// existence-hiding ErrNotFound: "exists but not yours" and "doesn't exist" are
// indistinguishable to the caller, closing the IDOR/enumeration side channel.
func (r *NotificationRepository) GetByIDForUser(ctx context.Context, id, recipientUserID string) (domain.Notification, error) {
	q := `SELECT ` + notificationColumns + ` FROM notifications WHERE id = $1 AND recipient_user_id = $2`
	return scanNotification(r.pool.QueryRow(ctx, q, id, recipientUserID))
}

// ============================================================================
// ListForUser — keyset-paginated inbox (newest-first) with filters.
// ============================================================================

// ListForUser returns a page of the user's inbox plus a nextToken. Ordered by
// (created_at DESC, id DESC) — the exact tuple notifications_recipient_created_idx
// serves — so the page is a clean backwards index scan and the cursor is
// (created_at, id). We fetch pageSize+1 rows: if the extra one comes back there IS
// a next page, and the token is built from the LAST kept row.
//
// FILTERS (all optional, all bound parameters): UnreadOnly, MinSeverity (numeric
// floor via the stored int), EventTypeFilter. SQL SAFETY: recipient_user_id, the
// cursor values, and every filter value are bound parameters ($1, $2, ...); the
// only thing built into the query TEXT is the WHERE-clause PLACEHOLDER numbers —
// no user value is ever concatenated.
func (r *NotificationRepository) ListForUser(ctx context.Context, recipientUserID string, opts domain.ListOptions) ([]domain.Notification, string, error) {
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	cur, err := decodeNotifCursor(opts.PageToken)
	if err != nil {
		return nil, "", fmt.Errorf("list notifications: %w", err)
	}

	args := []any{recipientUserID}
	where := "WHERE recipient_user_id = $1"

	if opts.UnreadOnly {
		where += " AND read = FALSE"
	}
	// MinSeverity: a non-zero floor means "severity >= floor". SeverityUnspecified
	// (0) is "no floor" per the domain's MeetsFloor semantics, so we add no predicate
	// for it — matching the floor rule exactly (a zero floor must NOT exclude an
	// unspecified-severity row).
	if opts.MinSeverity != domain.SeverityUnspecified {
		args = append(args, int16(opts.MinSeverity))
		where += fmt.Sprintf(" AND severity >= $%d", len(args))
	}
	if opts.EventTypeFilter != "" {
		args = append(args, opts.EventTypeFilter)
		where += fmt.Sprintf(" AND event_type = $%d", len(args))
	}
	if cur != nil {
		// "strictly before" the cursor in DESC order = the (created_at, id) tuple is
		// row-wise LESS THAN the cursor. The row-value comparison gives the exact
		// keyset boundary in one predicate (and is index-friendly).
		args = append(args, cur.TS, cur.ID)
		where += fmt.Sprintf(" AND (created_at, id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, pageSize+1) // +1 sentinel row to detect a next page
	q := fmt.Sprintf(`
		SELECT %s FROM notifications
		%s
		ORDER BY created_at DESC, id DESC
		LIMIT $%d`, notificationColumns, where, len(args))

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list notifications: %w", err)
	}
	defer rows.Close()

	out := make([]domain.Notification, 0, pageSize)
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate notifications: %w", err)
	}

	nextToken := ""
	if len(out) > pageSize {
		out = out[:pageSize] // drop the sentinel
		last := out[len(out)-1]
		nextToken = encodeNotifCursor(notifCursor{TS: last.CreatedAt, ID: last.ID})
	}
	return out, nextToken, nil
}

// ============================================================================
// CountUnread — cheap badge count off the partial index.
// ============================================================================

// CountUnread returns the user's total unread count, independent of any page. The
// WHERE read = FALSE predicate is served by the partial index
// notifications_recipient_unread_idx, so this is a cheap indexed COUNT rather than
// a full per-user scan.
func (r *NotificationRepository) CountUnread(ctx context.Context, recipientUserID string) (int, error) {
	const q = `SELECT COUNT(*) FROM notifications WHERE recipient_user_id = $1 AND read = FALSE`
	var n int
	if err := r.pool.QueryRow(ctx, q, recipientUserID).Scan(&n); err != nil {
		return 0, fmt.Errorf("count unread: %w", err)
	}
	return n, nil
}

// ============================================================================
// MarkRead / MarkAllRead — idempotent read transitions.
// ============================================================================

// MarkRead flips the given ids to read for recipientUserID and returns how many
// actually TRANSITIONED. The UPDATE's predicate (read = FALSE AND
// recipient_user_id = $user AND id = ANY($ids)) is the whole idempotency story:
//   - already-read ids fail `read = FALSE` → not updated, not counted.
//   - foreign/unknown ids fail the user scope or the id set → not updated.
//
// So a retry, or a mix of valid/foreign/already-read ids, is safe and the count is
// exactly the number of genuine unread→read transitions. We pass ids as a single
// $ids array parameter (= ANY($n)) rather than building an IN (...) list — one
// bound array, no per-id placeholder concatenation, no injection surface, and it
// handles an empty slice gracefully (ANY of an empty array matches nothing → 0).
//
// `now` is injected (the domain's Clock) so the read_at timestamp is deterministic
// in tests.
func (r *NotificationRepository) MarkRead(ctx context.Context, recipientUserID string, ids []string, now time.Time) (int, error) {
	if len(ids) == 0 {
		return 0, nil // nothing asked → nothing transitioned (avoid a pointless round-trip)
	}
	const q = `
		UPDATE notifications
		SET read = TRUE, read_at = $1
		WHERE recipient_user_id = $2 AND read = FALSE AND id = ANY($3)`
	tag, err := r.pool.Exec(ctx, q, now, recipientUserID, ids)
	if err != nil {
		return 0, fmt.Errorf("mark read: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// MarkAllRead flips ALL of the user's unread notifications to read in one indexed
// UPDATE and returns the count transitioned. Separate from MarkRead so the adapter
// does it as a single statement (served by the partial unread index) rather than
// first enumerating ids client-side. Idempotent: a second call finds no unread rows
// and returns 0.
func (r *NotificationRepository) MarkAllRead(ctx context.Context, recipientUserID string, now time.Time) (int, error) {
	const q = `
		UPDATE notifications
		SET read = TRUE, read_at = $1
		WHERE recipient_user_id = $2 AND read = FALSE`
	tag, err := r.pool.Exec(ctx, q, now, recipientUserID)
	if err != nil {
		return 0, fmt.Errorf("mark all read: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// ============================================================================
// delivery_log — append + read the audit trail.
// ============================================================================

// AppendDeliveryAttempt records one delivery_log row. The adapter denormalizes the
// recipient onto the row (recipient_user_id is carried on the domain
// DeliveryAttempt? — no: the DeliveryAttempt type does NOT carry the recipient, so
// the consumer adapter must look it up). To keep this port faithful to its
// signature (it takes only a DeliveryAttempt), we resolve the owner from the parent
// notification in the SAME statement via a sub-SELECT, so the denormalized
// recipient is always consistent with the notification and never client-supplied.
//
// WHY the sub-SELECT instead of a join-on-read: the fleet list filters by user a
// LOT and we want it index-served without joining notifications every page; storing
// the owner once at append time (read from the authoritative parent row) is the
// classic denormalize-for-read tradeoff. If the parent notification doesn't exist,
// the sub-SELECT yields NULL → the NOT NULL column rejects it, OR the FK rejects
// the notification_id; either way we surface ErrRepoNotFound (no orphan attempts).
func (r *NotificationRepository) AppendDeliveryAttempt(ctx context.Context, a domain.DeliveryAttempt) error {
	const q = `
		INSERT INTO delivery_log
			(notification_id, recipient_user_id, channel, status, attempt, response_code, error_message, attempted_at)
		SELECT $1, n.recipient_user_id, $2, $3, $4, $5, $6, $7
		FROM notifications n
		WHERE n.id = $1`
	tag, err := r.pool.Exec(ctx, q,
		a.NotificationID, int16(a.Channel), int16(a.Status), a.Attempt,
		a.ResponseCode, a.ErrorMessage, a.AttemptedAt,
	)
	if err != nil {
		if isForeignKeyViolation(err) {
			return domain.ErrRepoNotFound
		}
		return fmt.Errorf("append delivery attempt: %w", err)
	}
	// Zero rows inserted means the SELECT found no parent notification — the FK can't
	// fire because there was no value to check, so we detect the missing parent here.
	if tag.RowsAffected() == 0 {
		return domain.ErrRepoNotFound
	}
	return nil
}

// deliveryColumns is the canonical SELECT list for delivery_log reads, scanned by
// scanDeliveryAttempt. Note `id` leads it for the cursor (the fleet list) even
// though the domain DeliveryAttempt doesn't expose it — the scan helper reads it
// into a local for the cursor and discards it for the per-notification view.
const deliveryColumns = `id, notification_id, channel, status, attempt, response_code, error_message, attempted_at`

// GetAttemptsForNotification returns the per-channel attempts for ONE of the user's
// notifications, OLDEST-FIRST (the natural reading order of an attempt timeline).
// Scoped by recipient_user_id for the same anti-IDOR reason as GetByIDForUser: a
// caller can only read attempts for a notification they own. Returns an empty slice
// (not an error) when the notification exists with no attempts OR is not theirs —
// the service has already authorized the parent via GetByIDForUser on that path.
func (r *NotificationRepository) GetAttemptsForNotification(ctx context.Context, recipientUserID, notificationID string) ([]domain.DeliveryAttempt, error) {
	q := `
		SELECT ` + deliveryColumns + `
		FROM delivery_log
		WHERE notification_id = $1 AND recipient_user_id = $2
		ORDER BY attempted_at ASC, id ASC`
	rows, err := r.pool.Query(ctx, q, notificationID, recipientUserID)
	if err != nil {
		return nil, fmt.Errorf("get attempts: %w", err)
	}
	defer rows.Close()

	out := make([]domain.DeliveryAttempt, 0, 4)
	for rows.Next() {
		a, _, err := scanDeliveryAttempt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate attempts: %w", err)
	}
	return out, nil
}

// ListDeliveryAttemptsForUser returns a keyset page of the user's delivery log,
// NEWEST-FIRST, with optional channel/status/notification filters. Ordered by
// (attempted_at DESC, id DESC) — served by delivery_log_recipient_attempted_idx —
// with the BIGINT id as the unique tiebreaker in the cursor (logCursor). Same +1
// sentinel + bound-parameter discipline as ListForUser.
func (r *NotificationRepository) ListDeliveryAttemptsForUser(ctx context.Context, recipientUserID string, opts domain.ListOptions) ([]domain.DeliveryAttempt, string, error) {
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	cur, err := decodeLogCursor(opts.PageToken)
	if err != nil {
		return nil, "", fmt.Errorf("list delivery attempts: %w", err)
	}

	args := []any{recipientUserID}
	where := "WHERE recipient_user_id = $1"

	if opts.Channel != domain.ChannelUnspecified {
		args = append(args, int16(opts.Channel))
		where += fmt.Sprintf(" AND channel = $%d", len(args))
	}
	if opts.Status != domain.StatusUnspecified {
		args = append(args, int16(opts.Status))
		where += fmt.Sprintf(" AND status = $%d", len(args))
	}
	if opts.NotificationID != "" {
		args = append(args, opts.NotificationID)
		where += fmt.Sprintf(" AND notification_id = $%d", len(args))
	}
	if cur != nil {
		args = append(args, cur.TS, cur.ID)
		where += fmt.Sprintf(" AND (attempted_at, id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, pageSize+1)
	q := fmt.Sprintf(`
		SELECT %s FROM delivery_log
		%s
		ORDER BY attempted_at DESC, id DESC
		LIMIT $%d`, deliveryColumns, where, len(args))

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list delivery attempts: %w", err)
	}
	defer rows.Close()

	// We track the surrogate row ids in a parallel slice because the delivery-log
	// cursor's tiebreaker is the BIGINT id, which the domain DeliveryAttempt does NOT
	// carry (the surrogate key is an adapter detail). Keeping ids[] alongside out[]
	// lets us build the cursor from the last KEPT row without re-querying.
	out := make([]domain.DeliveryAttempt, 0, pageSize)
	ids := make([]int64, 0, pageSize)
	for rows.Next() {
		a, id, err := scanDeliveryAttempt(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, a)
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate delivery attempts: %w", err)
	}

	nextToken := ""
	if len(out) > pageSize {
		out = out[:pageSize] // drop the sentinel
		ids = ids[:pageSize]
		last := out[len(out)-1]
		nextToken = encodeLogCursor(logCursor{TS: last.AttemptedAt, ID: ids[len(ids)-1]})
	}
	return out, nextToken, nil
}

// scanNotification materializes one notifications row into the domain type,
// decoding the SMALLINT[] channels into []domain.NotificationChannel and turning
// the nullable read_at into the domain's zero-or-set time.Time. One helper for
// GetByIDForUser and ListForUser so the column order lives in exactly one place.
func scanNotification(row rowScanner) (domain.Notification, error) {
	var (
		n        domain.Notification
		severity int16
		channels []int16
		readAt   *time.Time
	)
	if err := row.Scan(
		&n.ID, &n.RecipientUserID, &n.Title, &n.Body,
		&severity, &channels, &n.Read,
		&n.EventID, &n.EventType, &n.SourceService,
		&n.CreatedAt, &readAt,
	); err != nil {
		if isNoRows(err) {
			return domain.Notification{}, domain.ErrRepoNotFound
		}
		return domain.Notification{}, fmt.Errorf("scan notification: %w", err)
	}
	n.Severity = domain.Severity(severity)
	n.Channels = channelsFromInt16(channels)
	if readAt != nil {
		n.ReadAt = *readAt
	}
	return n, nil
}

// scanDeliveryAttempt materializes one delivery_log row. It returns the domain
// DeliveryAttempt AND the row's BIGINT id separately: the id is the cursor
// tiebreaker the fleet list needs but the domain type deliberately doesn't expose
// (the surrogate key is an adapter detail). Channel/status are widened back from the
// stored int16s.
func scanDeliveryAttempt(row rowScanner) (domain.DeliveryAttempt, int64, error) {
	var (
		a       domain.DeliveryAttempt
		id      int64
		channel int16
		status  int16
	)
	if err := row.Scan(
		&id, &a.NotificationID, &channel, &status,
		&a.Attempt, &a.ResponseCode, &a.ErrorMessage, &a.AttemptedAt,
	); err != nil {
		if isNoRows(err) {
			return domain.DeliveryAttempt{}, 0, domain.ErrRepoNotFound
		}
		return domain.DeliveryAttempt{}, 0, fmt.Errorf("scan delivery attempt: %w", err)
	}
	a.Channel = domain.NotificationChannel(channel)
	a.Status = domain.DeliveryStatus(status)
	return a, id, nil
}

// ============================================================================
// small encoding helpers (domain ints ⇄ storage int16; zero-time ⇄ NULL)
// ============================================================================

// channelsToInt16 widens the domain channel ints to the []int16 pgx encodes into
// SMALLINT[]. nil/empty → an empty (non-nil) slice so the column gets '{}' not NULL
// (the column is NOT NULL DEFAULT '{}').
func channelsToInt16(chs []domain.NotificationChannel) []int16 {
	out := make([]int16, len(chs))
	for i, c := range chs {
		out[i] = int16(c)
	}
	return out
}

// channelsFromInt16 narrows the scanned SMALLINT[] back to domain channels.
func channelsFromInt16(vals []int16) []domain.NotificationChannel {
	if len(vals) == 0 {
		return nil // a zero-channel notification reads back as a nil slice (the zero value)
	}
	out := make([]domain.NotificationChannel, len(vals))
	for i, v := range vals {
		out[i] = domain.NotificationChannel(v)
	}
	return out
}

// nullTime maps the domain's zero time.Time (the "unread"/"never" sentinel) to a
// SQL NULL, and a set time to itself. WHY: the schema uses NULL for "not yet"
// (read_at), and a zero time.Time round-tripped as a real timestamp would be a
// surprising year-1 date. Returning *time.Time (nil for zero) lets pgx write NULL.
func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
