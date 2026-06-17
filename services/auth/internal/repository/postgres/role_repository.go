// role_repository.go — Postgres adapter for domain.RoleRepository.
//
// Implements: Create, GetByName, AssignToUser (idempotent upsert), GetUserRoles.
//
// ============================================================================
// JSONB PERMISSIONS — the round-trip
// ============================================================================
// A Role's permissions live in the roles.permissions JSONB column as an array of
// {"resource","action"} objects. Create ENCODES []domain.Permission → JSONB;
// GetByName / GetUserRoles DECODE JSONB → []domain.Permission (via the shared
// decodePermissions helper in user_repository.go). The domain never touches JSON
// — serialization is an adapter concern, which keeps the domain free of any
// encoding/json import (Clean Architecture: no infrastructure in the core).
package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"
)

// RoleRepo is the Postgres-backed implementation of domain.RoleRepository.
type RoleRepo struct {
	db *DB
}

var _ domain.RoleRepository = (*RoleRepo)(nil)

// NewRoleRepo wires the shared pool into a RoleRepository adapter.
func NewRoleRepo(db *DB) *RoleRepo {
	return &RoleRepo{db: db}
}

// permissionDTO is the on-the-wire shape of a permission inside the JSONB column.
// Lowercase json tags match the seed data and what GetUserRoles/decodePermissions
// read back. We keep this DTO in the adapter so domain.Permission needs no json
// tags (the domain stays serialization-agnostic).
type permissionDTO struct {
	Resource string `json:"resource"`
	Action   string `json:"action"`
}

// encodePermissions marshals []domain.Permission into the JSONB bytes stored in
// roles.permissions. A nil/empty slice encodes to "[]" (a valid empty JSON
// array), matching the column DEFAULT and what decodePermissions reads back as
// "no permissions".
func encodePermissions(perms []domain.Permission) ([]byte, error) {
	dto := make([]permissionDTO, 0, len(perms))
	for _, p := range perms {
		dto = append(dto, permissionDTO{Resource: p.Resource, Action: p.Action})
	}
	return json.Marshal(dto)
}

// Create persists a new role with its permission set. Returns the role with its
// DB-generated id and created_at.
//
// The permissions slice is encoded to JSONB and bound as a parameter — even
// though it is structured data, it still flows through the parameterized-query
// channel ($3), never string-concatenated. (JSON-as-a-string concatenated into
// SQL would be a textbook injection vector.)
func (r *RoleRepo) Create(ctx context.Context, role domain.Role) (domain.Role, error) {
	permsJSON, err := encodePermissions(role.Permissions)
	if err != nil {
		return domain.Role{}, fmt.Errorf("encoding permissions: %w", err)
	}

	const q = `
		INSERT INTO roles (name, permissions)
		VALUES ($1, $2)
		RETURNING id, created_at`

	row := r.db.pool.QueryRow(ctx, q, role.Name, permsJSON)
	if err := row.Scan(&role.ID, &role.CreatedAt); err != nil {
		// roles.name is UNIQUE; a duplicate name is the "already exists" case. We
		// map it to the storage not-found-family sentinel for "already exists" —
		// the domain reuses ErrRepoEmailExists semantics only for users, so for
		// roles we surface a clear wrapped error. (The role-create path is admin
		// tooling, not a user-enumeration surface, so a descriptive error is fine.)
		if isPgErrCode(err, pgErrUniqueViolation) {
			return domain.Role{}, fmt.Errorf("role %q already exists: %w", role.Name, domain.ErrRepoEmailExists)
		}
		return domain.Role{}, fmt.Errorf("inserting role: %w", err)
	}
	return role, nil
}

// GetByName returns the role with that unique name (permissions decoded), or
// ErrRepoNotFound. AssignRole uses this to resolve a role NAME → its ID before
// writing the assignment.
func (r *RoleRepo) GetByName(ctx context.Context, name string) (domain.Role, error) {
	const q = `SELECT id, name, permissions, created_at FROM roles WHERE name = $1`

	var (
		role     domain.Role
		permsRaw []byte
	)
	err := r.db.pool.QueryRow(ctx, q, name).Scan(
		&role.ID, &role.Name, &permsRaw, &role.CreatedAt)
	if err != nil {
		if noRows(err) {
			return domain.Role{}, domain.ErrRepoNotFound
		}
		return domain.Role{}, fmt.Errorf("querying role by name: %w", err)
	}

	perms, derr := decodePermissions(permsRaw)
	if derr != nil {
		return domain.Role{}, fmt.Errorf("decoding permissions: %w", derr)
	}
	role.Permissions = perms
	return role, nil
}

// AssignToUser upserts the user's single role assignment.
//
// ============================================================================
// IDEMPOTENT, RACE-SAFE UPSERT — the heart of "a user has exactly one role"
// ============================================================================
// user_roles has user_id as its PRIMARY KEY (one row per user). The statement:
//
//	INSERT ... ON CONFLICT (user_id) DO UPDATE SET role_id = EXCLUDED.role_id
//
// means:
//   - first assignment      → INSERT a row.
//   - re-assigning the SAME role  → ON CONFLICT fires, sets role_id to itself:
//     a no-op end state. IDEMPOTENT — calling twice equals calling once.
//   - assigning a DIFFERENT role  → ON CONFLICT fires, REPLACES role_id. The
//     user ends with exactly the new role (the single-role rule).
//
// WHY upsert instead of "DELETE then INSERT" or "SELECT then branch":
//   - It is a SINGLE atomic statement. DELETE+INSERT is two statements with a
//     window where the user has NO role (a concurrent CheckPermission could see
//     the gap). SELECT-then-INSERT/UPDATE has a classic check-then-act RACE: two
//     concurrent assigners both see "no row" and both INSERT, and one hits a PK
//     violation. ON CONFLICT pushes the conflict resolution into Postgres, which
//     serializes it correctly under the row lock — no application-level race.
//
// FOREIGN KEYS: a bad userID or roleID raises a 23503 foreign-key violation,
// which we translate to ErrRepoNotFound. (The domain service pre-checks both
// exist, so this is a defense-in-depth backstop, not the primary guard.)
//
// INTERVIEW: "How do you make an assignment idempotent and concurrency-safe?"
// INSERT ... ON CONFLICT DO UPDATE — let the database's unique index + row lock
// arbitrate, rather than a read-modify-write in the app that races.
func (r *RoleRepo) AssignToUser(ctx context.Context, userID, roleID string) error {
	const q = `
		INSERT INTO user_roles (user_id, role_id)
		VALUES ($1, $2)
		ON CONFLICT (user_id) DO UPDATE SET role_id = EXCLUDED.role_id`

	if _, err := r.db.pool.Exec(ctx, q, userID, roleID); err != nil {
		if isPgErrCode(err, pgErrForeignKeyViolation) {
			return domain.ErrRepoNotFound
		}
		return fmt.Errorf("assigning role to user: %w", err)
	}
	return nil
}

// GetUserRoles returns the user's roles with permissions eagerly loaded, so the
// domain's CheckPermission can scan grants without a second round-trip.
//
// Because user_roles is keyed by user_id (one role per user under the current
// rule), this returns 0 or 1 role today. We still return a SLICE to honor the
// port's many-roles signature: if the rule relaxes to multi-role later, ONLY the
// user_roles PK and this query's natural multi-row result change — the interface
// and every caller stay the same. Designing the read for the general case now
// avoids a breaking signature change later.
func (r *RoleRepo) GetUserRoles(ctx context.Context, userID string) ([]domain.Role, error) {
	const q = `
		SELECT r.id, r.name, r.permissions, r.created_at
		FROM user_roles ur
		JOIN roles r ON r.id = ur.role_id
		WHERE ur.user_id = $1
		ORDER BY r.name`

	rows, err := r.db.pool.Query(ctx, q, userID)
	if err != nil {
		return nil, fmt.Errorf("querying user roles: %w", err)
	}
	defer rows.Close()

	roles := make([]domain.Role, 0)
	for rows.Next() {
		var (
			role     domain.Role
			permsRaw []byte
		)
		if err := rows.Scan(&role.ID, &role.Name, &permsRaw, &role.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning role row: %w", err)
		}
		perms, derr := decodePermissions(permsRaw)
		if derr != nil {
			return nil, fmt.Errorf("decoding permissions: %w", derr)
		}
		role.Permissions = perms
		roles = append(roles, role)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating user roles: %w", err)
	}
	return roles, nil
}
