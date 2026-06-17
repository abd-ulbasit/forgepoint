// cursor.go — keyset (cursor) pagination tokens for the two list ports.
//
// ============================================================================
// THE OPAQUE CURSORS (keyset pagination's "page token")
// ============================================================================
//
// Both list ports (ListMonitors, ListDriftReports) return an opaque nextToken the
// client echoes back for the next page. "Opaque" is a deliberate contract: the
// client must NOT parse or construct it — it is our internal sort tuple, base64url
// encoded so it is URL/header-safe and obviously not meant to be hand-edited.
// Encoding it (rather than exposing raw columns) means we can change the cursor's
// shape later without breaking clients, and the OWNER_TEAM filter is applied
// SEPARATELY (server-side, from auth claims) so a forged cursor can never scan
// outside its tenancy — it only positions WITHIN the already-team-scoped query.
//
// WHY a COMPOSITE key, never a single column: the leading sort column is not unique
// (two monitors created the same instant; two reports whose windows ended the same
// instant), so a cursor on it alone could skip or repeat rows at a tie boundary.
// Appending the row id makes the sort key TOTAL — every row has a distinct tuple —
// so "strictly after the cursor" is unambiguous and page boundaries are exact.
//
// WHY keyset and not LIMIT/OFFSET: OFFSET re-counts from the top and can skip or
// duplicate rows when the underlying set shifts under concurrent inserts (reports
// arrive continuously as windows close). Keyset is stable: it always says "rows
// after THIS position", independent of inserts elsewhere — which is exactly why the
// domain's ListOptions mandates a page token, not an offset.
// ============================================================================
package postgres

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

// listCursor is the decoded keyset position for both lists: the (timestamp, id) of
// the last row of the previous page. For monitors the timestamp is created_at; for
// reports it is window_end. Both lists are newest-first DESC, so the next page is
// "rows strictly BEFORE this (ts, id)".
type listCursor struct {
	TS time.Time `json:"t"`
	ID string    `json:"i"`
}

// encodeListCursor serializes a listCursor to the opaque base64url token. JSON (not
// a hand-rolled format) keeps it trivial to evolve; base64url without padding keeps
// it safe in URLs and gRPC metadata.
func encodeListCursor(c listCursor) string {
	b, _ := json.Marshal(c) // a time+string never fails to marshal
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodeListCursor parses a client page token back into a listCursor. An empty token
// means "first page" (returns nil, nil). A malformed token is surfaced as an error
// (rather than silently treated as page 1, which would hide a client bug) — the
// service maps it to InvalidArgument.
func decodeListCursor(token string) (*listCursor, error) {
	if token == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("invalid page token (bad base64): %w", err)
	}
	var c listCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("invalid page token (bad payload): %w", err)
	}
	return &c, nil
}

// defaultPageSize is the adapter-level fallback when a list arrives with no page
// size. The SERVICE already clamps page size (DefaultListPageSize / MaxListPageSize)
// before calling the repo, so in practice this is never hit; it is a defensive
// backstop so the adapter never issues an unbounded LIMIT on its own.
const defaultPageSize = 20
