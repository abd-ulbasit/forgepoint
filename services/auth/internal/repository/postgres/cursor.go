// cursor.go — opaque keyset-pagination cursor for the users List query.
//
// ============================================================================
// WHY AN OPAQUE CURSOR (not "page number" or a raw timestamp in the API)
// ============================================================================
// The List port returns a `nextToken string` the client echoes back verbatim to
// fetch the next page. We encode the keyset boundary — (created_at, id) of the
// last returned row — into a base64 token. Two reasons it is OPAQUE:
//
//   - ENCAPSULATION: the client must not depend on the cursor's internal shape.
//     If we later add a secondary sort key or switch from created_at to a
//     sequence, the token format changes but the API contract ("send back what we
//     gave you") does not. This is exactly how Stripe (`starting_after`), GitHub,
//     and the AWS APIs (`NextToken`) model pagination.
//   - TAMPER-OBVIOUSNESS: a malformed/edited token fails to decode and we return
//     an error rather than silently mis-paginating.
//
// The token is NOT encrypted or signed — it carries no secret (just a timestamp
// and a public UUID the client already saw). If we needed to prevent a client
// from probing arbitrary boundaries we would sign it (HMAC); for a list of users
// the caller is already authorized to see, that is unnecessary complexity.
package postgres

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Pagination bounds. defaultPageSize applies when the caller passes 0 (the port
// documents "0 means use a default"). maxPageSize caps an over-eager client so a
// single request can't ask for a million rows and exhaust memory / a connection.
const (
	defaultPageSize = 50
	maxPageSize     = 200
)

// errBadCursor is returned (wrapped) when a page token cannot be decoded. The
// repository wraps it so the caller can errors.Is(err, errBadCursor) if it ever
// needs to, while the handler maps it to InvalidArgument.
var errBadCursor = errors.New("postgres: malformed page token")

// userCursor is the decoded keyset boundary. `first` distinguishes the
// first-page request (no boundary; return the newest rows) from a continuation
// (return rows strictly older than createdAt/id).
type userCursor struct {
	first     bool      // true on the first page → the SQL ignores the boundary
	createdAt time.Time // boundary: last returned row's created_at
	id        string    // boundary: last returned row's id (UUID), tiebreaker
}

// idForQuery returns the id to bind as the $3 (::uuid) parameter. On the first
// page there is no boundary id; we bind nil, which Postgres reads as NULL::uuid.
// That is safe because the WHERE clause short-circuits on $1 (first=true) and
// never evaluates the comparison against NULL — but the parameter must still
// type-check, and NULL is a valid uuid value for binding.
func (c userCursor) idForQuery() any {
	if c.first {
		return nil
	}
	return c.id
}

// encodeUserCursor serializes a boundary into an opaque base64 token.
//
// Wire format (before base64): "v1|<unixNanos>|<uuid>". We version it ("v1") so a
// future format change can be detected and either upgraded or rejected, rather
// than misparsed. unixNanos is an int64 with nanosecond precision so the cursor
// round-trips the created_at boundary exactly (a lossy second-granularity cursor
// would skip or repeat rows created within the same second).
func encodeUserCursor(c userCursor) string {
	raw := fmt.Sprintf("v1|%d|%s", c.createdAt.UnixNano(), c.id)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeUserCursor parses an opaque token back into a boundary. An EMPTY token is
// the legitimate "give me the first page" request and yields {first: true} with
// no error — empty is not malformed, it is the documented initial state.
//
// Any non-empty token that fails to decode (bad base64, wrong version, wrong
// field count, unparseable timestamp) is a client error, returned wrapped in
// errBadCursor so the handler can surface InvalidArgument.
func decodeUserCursor(token string) (userCursor, error) {
	if token == "" {
		return userCursor{first: true}, nil
	}

	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return userCursor{}, fmt.Errorf("%w: not base64", errBadCursor)
	}

	// Split into exactly 3 fields: version, unixNanos, uuid. SplitN with n=3 keeps
	// any '|' inside the uuid intact (UUIDs contain none, but being strict about
	// the field count is the cheap correctness guard).
	parts := strings.SplitN(string(decoded), "|", 3)
	if len(parts) != 3 || parts[0] != "v1" {
		return userCursor{}, fmt.Errorf("%w: bad structure or version", errBadCursor)
	}

	var unixNanos int64
	if _, err := fmt.Sscanf(parts[1], "%d", &unixNanos); err != nil {
		return userCursor{}, fmt.Errorf("%w: bad timestamp", errBadCursor)
	}
	id := parts[2]
	if id == "" {
		return userCursor{}, fmt.Errorf("%w: empty id", errBadCursor)
	}

	return userCursor{
		first:     false,
		createdAt: time.Unix(0, unixNanos).UTC(),
		id:        id,
	}, nil
}
