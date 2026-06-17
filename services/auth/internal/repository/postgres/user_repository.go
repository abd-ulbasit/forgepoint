// user_repository.go — Postgres adapter for domain.UserRepository.
//
// Implements: Create, GetByID, GetByEmail, List (cursor-paginated).
//
// ============================================================================
// THE ROLE-LOADING JOIN (why GetByID/GetByEmail are not plain SELECTs)
// ============================================================================
// The domain's User carries an embedded Role with its Permissions, and Login
// reads user.Role.Name straight into the JWT. So a user read must EAGERLY load
// the assigned role in one round-trip. We LEFT JOIN users → user_roles → roles:
//
//	users  ──(user_roles.user_id)──►  user_roles  ──(role_id)──►  roles
//
// LEFT (not INNER) JOIN because a freshly-created user has NO role yet
// (CreateUser persists no assignment; AssignRole adds it later). An INNER JOIN
// would make brand-new users invisible to GetByID — a correctness bug. With a
// LEFT JOIN the role columns come back NULL for an unassigned user, and we scan
// them into a zero-value Role (empty name → no permissions), which is exactly
// what the domain expects.
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"
)

// UserRepo is the Postgres-backed implementation of domain.UserRepository.
// The compile-time assertion below guarantees it satisfies the port; if a method
// signature drifts from the interface, the package fails to build here rather
// than at some distant wiring site.
type UserRepo struct {
	db *DB
}

var _ domain.UserRepository = (*UserRepo)(nil)

// NewUserRepo wires the shared pool into a UserRepository adapter.
func NewUserRepo(db *DB) *UserRepo {
	return &UserRepo{db: db}
}

// Create inserts a new user and returns it with DB-populated fields (id,
// timestamps). The freshly created user has NO role assignment yet (the domain
// assigns roles separately via AssignRole), so the returned User.Role is the
// zero value — consistent with what a later GetByID would return until a role
// is assigned.
//
// SQL DESIGN — RETURNING: we use INSERT ... RETURNING to get the server-generated
// id/created_at/updated_at in the SAME round-trip as the insert. The alternative
// (insert, then SELECT) is two round-trips and races with concurrent writers.
// RETURNING is the Postgres idiom for "give me back what you just wrote".
//
// SECURITY — PARAMETERIZED QUERY: every user-supplied value (email, name, team,
// password_hash) is bound as a parameter ($1..$4), NEVER concatenated into the
// SQL text. This is the categorical defense against SQL injection: the values can
// never be parsed as SQL because the driver sends the query and the arguments on
// separate channels of the wire protocol.
func (r *UserRepo) Create(ctx context.Context, user domain.User) (domain.User, error) {
	const q = `
		INSERT INTO users (email, name, team, password_hash, active)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, created_at, updated_at`

	row := r.db.pool.QueryRow(ctx, q,
		user.Email, user.Name, user.Team, user.PasswordHash, user.Active)

	if err := row.Scan(&user.ID, &user.CreatedAt, &user.UpdatedAt); err != nil {
		// A unique-constraint violation on users.email means the address is taken.
		// We translate the storage SQLSTATE into the STORAGE sentinel the domain
		// service understands (it maps that to the business ErrEmailAlreadyExists).
		if isPgErrCode(err, pgErrUniqueViolation) {
			return domain.User{}, domain.ErrRepoEmailExists
		}
		return domain.User{}, fmt.Errorf("inserting user: %w", err)
	}
	return user, nil
}

// userSelectColumns is the shared SELECT list + role JOIN used by GetByID and
// GetByEmail. Declared once so both lookups return IDENTICAL column ordering and
// scan into the same helper (scanUserWithRole). The role columns are nullable
// because of the LEFT JOIN (see the file header).
const userSelectColumns = `
	SELECT
		u.id, u.email, u.name, u.team, u.password_hash, u.active,
		u.created_at, u.updated_at,
		r.id, r.name, r.permissions, r.created_at
	FROM users u
	LEFT JOIN user_roles ur ON ur.user_id = u.id
	LEFT JOIN roles r       ON r.id = ur.role_id`

// GetByID returns the user with that id (role eagerly loaded), or ErrRepoNotFound.
func (r *UserRepo) GetByID(ctx context.Context, id string) (domain.User, error) {
	row := r.db.pool.QueryRow(ctx, userSelectColumns+` WHERE u.id = $1`, id)
	return scanUserWithRole(row)
}

// GetByEmail returns the user with that email (role eagerly loaded), or
// ErrRepoNotFound. The comparison is case-INSENSITIVE because the email column is
// CITEXT — "Ada@x.dev" matches "ada@x.dev" at the storage layer. The domain also
// lowercases before calling, so this is defense in depth; either alone suffices.
func (r *UserRepo) GetByEmail(ctx context.Context, email string) (domain.User, error) {
	row := r.db.pool.QueryRow(ctx, userSelectColumns+` WHERE u.email = $1`, email)
	return scanUserWithRole(row)
}

// rowScanner is the minimal interface satisfied by both pgx.Row (QueryRow) and
// pgx.Rows (iterated query). Abstracting over it lets scanUserWithRole serve the
// single-row lookups while List scans many rows with the same column mapping —
// one definition of "how a user row maps to a domain.User", no drift.
type rowScanner interface {
	Scan(dest ...any) error
}

// scanUserWithRole maps one joined (user, role) row into a domain.User.
//
// The role columns are NULLABLE (LEFT JOIN), so we scan them into pointer /
// nullable holders and only populate User.Role when a role is actually present.
// permissions arrives as JSONB bytes; we unmarshal it into the domain's
// []Permission. WHY decode here and not store a string: the domain works with
// typed Permission values (CheckPermission scans perm.Resource/perm.Action), so
// the adapter is the correct boundary to translate JSONB ↔ []Permission.
func scanUserWithRole(s rowScanner) (domain.User, error) {
	var (
		u domain.User

		// Nullable role holders. A user with no assignment yields NULLs here.
		roleID    *string
		roleName  *string
		rolePerms []byte // JSONB; nil when no role
		roleTime  *time.Time
	)

	err := s.Scan(
		&u.ID, &u.Email, &u.Name, &u.Team, &u.PasswordHash, &u.Active,
		&u.CreatedAt, &u.UpdatedAt,
		&roleID, &roleName, &rolePerms, &roleTime,
	)
	if err != nil {
		if noRows(err) {
			// Translate "no such row" into the storage sentinel. The domain service
			// turns this into the right business error (ErrUserNotFound, or
			// ErrInvalidCredentials on the login path for anti-enumeration).
			return domain.User{}, domain.ErrRepoNotFound
		}
		return domain.User{}, fmt.Errorf("scanning user: %w", err)
	}

	// Populate the embedded Role only if the LEFT JOIN actually matched a role.
	if roleID != nil {
		role := domain.Role{ID: *roleID}
		if roleName != nil {
			role.Name = *roleName
		}
		if roleTime != nil {
			role.CreatedAt = *roleTime
		}
		perms, perr := decodePermissions(rolePerms)
		if perr != nil {
			return domain.User{}, fmt.Errorf("decoding role permissions: %w", perr)
		}
		role.Permissions = perms
		u.Role = role
	}

	return u, nil
}

// decodePermissions unmarshals the roles.permissions JSONB (an array of
// {"resource","action"} objects) into the domain's []Permission. An empty or NULL
// column decodes to a nil slice, which the domain treats as "no permissions" —
// the safe, deny-by-default state.
func decodePermissions(raw []byte) ([]domain.Permission, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	// The JSON field names are lowercase ("resource"/"action") to match the seed
	// data and what a human writing a role would type. domain.Permission's fields
	// are exported Go names; we map them with a local typed shim so the domain
	// struct needs no json tags (keeping the domain free of serialization concerns).
	var dto []struct {
		Resource string `json:"resource"`
		Action   string `json:"action"`
	}
	if err := json.Unmarshal(raw, &dto); err != nil {
		return nil, err
	}
	perms := make([]domain.Permission, 0, len(dto))
	for _, p := range dto {
		perms = append(perms, domain.Permission{Resource: p.Resource, Action: p.Action})
	}
	return perms, nil
}

// List returns a page of users ordered created_at DESC, plus an opaque nextToken
// cursor for the following page.
//
// ============================================================================
// CURSOR (KEYSET) PAGINATION — why not LIMIT/OFFSET
// ============================================================================
// OFFSET-based pagination (LIMIT 20 OFFSET 40) re-scans and discards the skipped
// rows on every page (slow for deep pages) AND is UNSTABLE under concurrent
// writes: if a row is inserted/deleted while a client pages, OFFSET can skip a
// row or show one twice. Keyset pagination instead remembers the LAST row's sort
// key and asks for rows "after" it:
//
//	WHERE (created_at, id) < ($cursor_created_at, $cursor_id)
//	ORDER BY created_at DESC, id DESC
//	LIMIT $pageSize + 1
//
// We sort by (created_at, id) — created_at alone is NOT unique (two users can be
// created in the same microsecond), so we add id as a tiebreaker to get a TOTAL
// order; otherwise the cursor boundary is ambiguous and rows can be skipped at
// page edges. We fetch pageSize+1 rows: if the extra row exists there IS a next
// page, and its predecessor's (created_at,id) becomes the next cursor; we then
// trim the extra row from what we return.
//
// The cursor is an OPAQUE base64 token (encoded in cursor.go) so clients treat it
// as a black box and we can change its internal shape without breaking them.
func (r *UserRepo) List(ctx context.Context, opts domain.ListOptions) ([]domain.User, string, error) {
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}

	// Decode the incoming cursor (empty = first page). On an empty token we use a
	// sentinel that selects everything; on a present token we parse the boundary.
	cur, err := decodeUserCursor(opts.PageToken)
	if err != nil {
		// A malformed token is a client error, surfaced so the handler can map it
		// to InvalidArgument rather than silently returning page 1 (which would
		// hide the bug and confuse the caller).
		return nil, "", fmt.Errorf("%w: invalid page token", err)
	}

	// We ask for pageSize+1 rows to detect whether a further page exists without a
	// second COUNT query.
	const q = userSelectColumns + `
		WHERE ($1::boolean OR (u.created_at, u.id) < ($2::timestamptz, $3::uuid))
		ORDER BY u.created_at DESC, u.id DESC
		LIMIT $4`

	rows, err := r.db.pool.Query(ctx, q,
		cur.first,        // $1: true on the first page → ignore the boundary
		cur.createdAt,    // $2: boundary created_at (zero on first page, unused)
		cur.idForQuery(), // $3: boundary id (nil on first page, unused)
		pageSize+1,
	)
	if err != nil {
		return nil, "", fmt.Errorf("querying users: %w", err)
	}
	defer rows.Close()

	users := make([]domain.User, 0, pageSize+1)
	for rows.Next() {
		u, serr := scanUserWithRole(rows)
		if serr != nil {
			return nil, "", fmt.Errorf("scanning user row: %w", serr)
		}
		users = append(users, u)
	}
	// rows.Err() surfaces an error that occurred DURING iteration (e.g. the
	// connection dropped mid-result). Checking it is mandatory: rows.Next()
	// returning false can mean "done" OR "errored", and only rows.Err()
	// disambiguates.
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterating users: %w", err)
	}

	// If we got the extra (pageSize+1)th row, there IS a next page. The cursor for
	// it is the LAST row we actually return (index pageSize-1), then we trim the
	// extra row off the response.
	var nextToken string
	if len(users) > pageSize {
		last := users[pageSize-1]
		nextToken = encodeUserCursor(userCursor{
			first:     false,
			createdAt: last.CreatedAt,
			id:        last.ID,
		})
		users = users[:pageSize]
	}

	return users, nextToken, nil
}
