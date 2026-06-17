// cursor.go — keyset pagination cursor + the row-scan abstraction.
//
// ============================================================================
// THE OPAQUE CURSOR (keyset pagination's "page token")
// ============================================================================
//
// Both list ports return an opaque nextToken that the client echoes back to get
// the next page. "Opaque" is a deliberate contract: the client must NOT parse or
// construct it — it is our internal (created_at, id) tuple, base64url-encoded so
// it is URL/header-safe and obviously not meant to be hand-edited. Encoding it
// (rather than exposing raw created_at/id) means we can change the cursor's
// internal shape later without breaking clients, and a client can't forge a
// cursor that scans outside its tenancy (the team filter is applied SEPARATELY,
// server-side, from auth claims — the cursor only positions WITHIN that scope).
//
// WHY (created_at, id) and not just created_at: created_at alone is not unique
// (two pipelines created in the same instant), so a cursor on it could skip or
// repeat rows at a tie boundary. Adding id (the PK, unique) makes the sort key
// TOTAL — every row has a distinct (created_at, id), so "strictly after the
// cursor" is unambiguous and the page boundaries are exact.
// ============================================================================
package postgres

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

// defaultPageSize is the adapter-level fallback when a filter carries no page
// size. The SERVICE already clamps page size (default 20, max 100) before
// calling the repo, so in practice f.List.PageSize is always set; this is a
// defensive backstop so the adapter never issues an unbounded LIMIT on its own.
const defaultPageSize = 20

// cursor is the decoded keyset position: the (created_at, id) of the last row of
// the previous page. The next page is "rows strictly after this in the sort".
type cursor struct {
	CreatedAt time.Time `json:"c"`
	ID        string    `json:"i"`
}

// encodeCursor serializes a cursor to the opaque base64url token returned as
// nextToken. JSON (not a hand-rolled format) keeps it trivial to evolve; base64url
// (no padding) keeps it safe in URLs and gRPC metadata.
func encodeCursor(c cursor) string {
	b, _ := json.Marshal(c) // a cursor of time+string never fails to marshal
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodeCursor parses a client-supplied page token back into a cursor. An empty
// token means "first page" → (nil, nil). A malformed token is a client error we
// surface (rather than silently treating as page 1, which would hide a bug). The
// returned *cursor is nil for the first page so callers can branch on presence.
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

// rowScanner is the minimal surface both pgx.Row (single-row QueryRow result)
// and pgx.Rows (iterated Query result) share: a Scan method. Abstracting it lets
// one scan helper (scanPipeline / scanExecution) serve BOTH the single-row reads
// and the list iteration, so the column order and JSONB decoding are written
// exactly once per entity.
type rowScanner interface {
	Scan(dest ...any) error
}
