// cursor.go — keyset pagination cursor for ListInvoices.
//
// ============================================================================
// THE OPAQUE CURSOR (keyset pagination's "page token")
// ============================================================================
//
// ListInvoices returns an opaque nextToken the client echoes back to get the next
// page. "Opaque" is a deliberate contract: the client must NOT parse or construct
// it — it is our internal (created_at, id) tuple, base64url-encoded so it is
// URL/header-safe and obviously not meant to be hand-edited. Encoding it (rather
// than exposing raw created_at/id) lets us change the cursor's internal shape
// later without breaking clients, and a client cannot forge a cursor that scans
// outside its tenancy: the TEAM filter is applied SEPARATELY server-side (from
// auth claims), and the cursor only POSITIONS within that already-scoped set.
//
// WHY (created_at, id) and not just created_at: created_at alone is not unique
// (two invoices finalized in the same instant), so a cursor on it could skip or
// repeat rows at a tie boundary. Adding id (the PK, unique) makes the sort key
// TOTAL — every row has a distinct (created_at, id), so "strictly before the
// cursor" in DESC order is unambiguous and the page boundaries are exact. This is
// keyset (a.k.a. seek) pagination; it stays O(page) under concurrent inserts,
// unlike LIMIT/OFFSET which re-scans skipped rows and can drift when rows shift.
// ============================================================================
package postgres

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

// defaultPageSize is the adapter-level fallback when the options carry no page
// size. The SERVICE already clamps page size (default 20, max 100) before calling
// the repo, so in practice opts.PageSize is always set; this is a defensive
// backstop so the adapter never issues an unbounded LIMIT on its own.
const defaultPageSize = 20

// cursor is the decoded keyset position: the (created_at, id) of the last row of
// the previous page. The next page is "rows strictly before this in the DESC sort".
type cursor struct {
	CreatedAt time.Time `json:"c"`
	ID        string    `json:"i"`
}

// encodeCursor serializes a cursor to the opaque base64url token returned as
// nextToken. JSON keeps it trivial to evolve; base64url (no padding) keeps it safe
// in URLs and gRPC metadata.
func encodeCursor(c cursor) string {
	b, _ := json.Marshal(c) // a cursor of time+string never fails to marshal
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodeCursor parses a client-supplied page token back into a cursor. An empty
// token means "first page" → (nil, nil). A malformed token is a client error we
// surface (rather than silently treating it as page 1, which would hide a bug).
// The returned *cursor is nil for the first page so callers can branch on presence.
func decodeCursor(token string) (*cursor, error) {
	if token == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("invalid page token (bad base64): %w", err)
	}
	var c cursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("invalid page token (bad payload): %w", err)
	}
	return &c, nil
}
