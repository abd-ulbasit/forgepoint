// cursor.go — keyset pagination cursors + the shared row-scan abstraction.
//
// ============================================================================
// THE OPAQUE CURSORS (keyset pagination's "page token")
// ============================================================================
//
// Every list port (experiments, runs, metric history) returns an opaque
// nextToken the client echoes back for the next page. "Opaque" is a deliberate
// contract: the client must NOT parse or construct it — it is our internal sort
// tuple, base64url-encoded so it is URL/header-safe and obviously not meant to be
// hand-edited. Encoding it (rather than exposing raw columns) means we can change
// the cursor's internal shape later without breaking clients, and the team filter
// is applied SEPARATELY (server-side, from auth claims) so a forged cursor can
// never scan outside its tenancy — it only positions WITHIN the already-scoped
// query.
//
// WHY a COMPOSITE key, never a single column: the leading sort column is not
// unique (two experiments created the same instant; two metric points at the
// same step), so a cursor on it alone could skip or repeat rows at a tie
// boundary. Appending a unique tiebreaker (id, or for metrics the (key,step,ts)
// position) makes the sort key TOTAL — every row has a distinct tuple, so
// "strictly after the cursor" is unambiguous and page boundaries are exact.
//
// WHY keyset and not LIMIT/OFFSET: OFFSET re-counts and can skip/duplicate rows
// when the underlying set shifts under concurrent writes (the metrics table is
// append-heavy). Keyset is stable: it always says "rows after THIS position",
// independent of inserts elsewhere.
// ============================================================================
package postgres

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

// listCursor is the decoded keyset position for the experiment/run lists: the
// (created/started_at, id) of the last row of the previous page. The next page
// is "rows strictly before this" (the lists are newest-first DESC).
type listCursor struct {
	TS time.Time `json:"t"`
	ID string    `json:"i"`
}

// encodeListCursor serializes a listCursor to the opaque base64url token. JSON
// (not a hand-rolled format) keeps it trivial to evolve; base64url without
// padding keeps it safe in URLs and gRPC metadata.
func encodeListCursor(c listCursor) string {
	b, _ := json.Marshal(c) // time+string never fails to marshal
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodeListCursor parses a client page token back into a listCursor. Empty
// token => first page (nil, nil). A malformed token is a client error we surface
// (rather than silently treating it as page 1, which would hide a bug).
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

// metricCursor is the decoded keyset position for GetMetricHistory. The metric
// scan is ordered by (key, step, ts) — that TRIPLE is the total sort key (a run
// can have at most one point per (key, step) by the unique index, so (key, step)
// is already unique; ts is carried for completeness and stable tie-handling). A
// single column would be ambiguous across keys, hence all three.
type metricCursor struct {
	Key  string    `json:"k"`
	Step int64     `json:"s"`
	TS   time.Time `json:"t"`
}

// encodeMetricCursor / decodeMetricCursor mirror the list-cursor codec for the
// metric-history page token.
func encodeMetricCursor(c metricCursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeMetricCursor(token string) (*metricCursor, error) {
	if token == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("invalid metric page token (bad base64): %w", err)
	}
	var c metricCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("invalid metric page token (bad payload): %w", err)
	}
	return &c, nil
}

// rowScanner is the minimal surface both pgx.Row (single-row QueryRow result)
// and pgx.Rows (iterated Query result) share: a Scan method. Abstracting it lets
// one scan helper serve BOTH single-row reads and list iteration, so the column
// order and JSONB decoding are written exactly once per entity.
type rowScanner interface {
	Scan(dest ...any) error
}

// defaultPageSize is the adapter-level fallback when a list arrives with no page
// size. The SERVICE already clamps page size before calling the repo, so in
// practice this is never hit; it is a defensive backstop so the adapter never
// issues an unbounded LIMIT on its own.
const defaultPageSize = 20
