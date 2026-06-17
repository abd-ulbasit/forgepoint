// cursor.go — keyset (cursor) pagination tokens + the shared row-scan abstraction.
//
// ============================================================================
// THE OPAQUE CURSORS (keyset pagination's "page token")
// ============================================================================
//
// ListForUser (inbox) and ListDeliveryAttemptsForUser (delivery log) each return
// an opaque nextToken the client echoes back for the next page. "Opaque" is a
// deliberate contract: the client must NOT parse or construct it — it is our
// internal sort tuple, base64url-encoded so it is URL/gRPC-metadata-safe and
// obviously not meant to be hand-edited. Encoding it (rather than exposing raw
// columns) means we can change the cursor's internal shape later without breaking
// clients, and the recipient_user_id filter is applied SEPARATELY (server-side,
// from auth claims) so a forged cursor can never scan outside its user — it only
// positions WITHIN the already-scoped query. (Anti-IDOR: the cursor is a position,
// not an authority.)
//
// WHY a COMPOSITE key, never a single column: the leading sort column is not
// unique (two notifications created the same instant; two attempts logged the same
// instant), so a cursor on it alone could skip or repeat rows at a tie boundary.
// Appending a unique tiebreaker (the row id) makes the sort key TOTAL — every row
// has a distinct tuple, so "strictly after the cursor" is unambiguous and page
// boundaries are exact.
//
// WHY keyset and not LIMIT/OFFSET: the inbox and delivery log are written
// CONCURRENTLY by the event reactor. OFFSET re-counts from the top and skips or
// duplicates rows when rows are inserted between page fetches. Keyset is stable: it
// always says "rows after THIS position", independent of inserts elsewhere — the
// exact property the ports.go doc demands for the concurrently-written inbox.
// ============================================================================
package postgres

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

// notifCursor is the decoded keyset position for the inbox list: the
// (created_at, id) of the last row of the previous page. The list is newest-first
// (DESC), so the next page is "rows strictly before this".
type notifCursor struct {
	TS time.Time `json:"t"`
	ID string    `json:"i"`
}

// encodeNotifCursor serializes a notifCursor to the opaque base64url token. JSON
// (not a hand-rolled format) keeps it trivial to evolve; base64url WITHOUT padding
// keeps it safe in URLs and gRPC metadata.
func encodeNotifCursor(c notifCursor) string {
	b, _ := json.Marshal(c) // time+string never fails to marshal
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodeNotifCursor parses a client page token back into a notifCursor. Empty
// token => first page (nil, nil). A malformed token is a CLIENT error we surface
// (rather than silently treating it as page 1, which would hide a bug and could
// quietly re-serve the whole list).
func decodeNotifCursor(token string) (*notifCursor, error) {
	if token == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("invalid page token (bad base64): %w", err)
	}
	var c notifCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("invalid page token (bad payload): %w", err)
	}
	return &c, nil
}

// logCursor is the decoded keyset position for the delivery-log fleet list: the
// (attempted_at, id) of the last row of the previous page. delivery_log.id is the
// BIGINT identity surrogate key (a guaranteed-unique, monotonic tiebreaker), so we
// carry it as an int64 — distinct from the inbox cursor whose id is a UUID string.
// The fleet list is newest-first (DESC).
type logCursor struct {
	TS time.Time `json:"t"`
	ID int64     `json:"i"`
}

// encodeLogCursor / decodeLogCursor mirror the inbox-cursor codec for the
// delivery-log page token.
func encodeLogCursor(c logCursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeLogCursor(token string) (*logCursor, error) {
	if token == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("invalid delivery-log page token (bad base64): %w", err)
	}
	var c logCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("invalid delivery-log page token (bad payload): %w", err)
	}
	return &c, nil
}

// rowScanner is the minimal surface both pgx.Row (single-row QueryRow result) and
// pgx.Rows (iterated Query result) share: a Scan method. Abstracting it lets one
// scan helper serve BOTH single-row reads and list iteration, so the column order
// and array decoding are written exactly once per entity.
type rowScanner interface {
	Scan(dest ...any) error
}

// defaultPageSize is the adapter-level fallback when a list arrives with no page
// size. The SERVICE already clamps page size (default 20, max 100) before calling
// the repo, so in practice this is never hit; it is a defensive backstop so the
// adapter never issues an unbounded LIMIT on its own.
const defaultPageSize = 20
