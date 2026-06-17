// Package repository defines the data-access interfaces for the Auth service.
//
// ============================================================================
// DEPENDENCY INVERSION VIA REPOSITORY INTERFACES
// ============================================================================
//
// WHY interfaces live here and not in the domain package:
//
//   There are two camps in Clean Architecture Go practice:
//
//   Option A — interfaces in domain:
//     domain.UserRepository is defined in the domain package. The domain service
//     depends on it. The postgres implementation lives in repository/postgres/.
//     Pro: the domain is truly self-contained; you can read the domain package
//     and see everything the service needs.
//     Con: the domain package imports "context" and nothing else, but then also
//     defines storage contracts — some argue that's still a form of coupling to
//     the idea of "persistent storage".
//
//   Option B — interfaces in repository (THIS APPROACH):
//     The repository package defines the contracts in terms of domain types.
//     The domain package defines the data models and the service interface.
//     The concrete postgres implementation satisfies the repository interfaces.
//     Pro: cleaner separation of concerns; the domain package is pure data models
//     and service contract; the repository package owns the storage abstraction.
//     Con: the domain service (AuthServiceImpl in Task 1.3) must import
//     repository to reference the interface types — one additional package dep.
//
//   We chose Option B because it keeps the domain package focused on models and
//   business rules, while repository owns the data-access abstraction. This
//   mirrors how projects like go-clean-arch (bxcodec) and go-kit structure
//   repositories.
//
// HOW DEPENDENCY INVERSION WORKS HERE:
//
//   ┌─────────────────────────────────────────────────────────────────────┐
//   │                                                                     │
//   │  handler.AuthHandler                                                │
//   │       │ depends on                                                  │
//   │       ▼                                                             │
//   │  domain.AuthService (interface) ←── AuthServiceImpl (Task 1.3)     │
//   │                                          │ depends on               │
//   │                                          ▼                          │
//   │                             repository.UserRepository (interface)   │
//   │                             repository.APIKeyRepository (interface) │
//   │                             repository.RoleRepository (interface)   │
//   │                                          ▲                          │
//   │                           postgres.UserRepo (Task 1.4 impl)         │
//   │                                                                     │
//   └─────────────────────────────────────────────────────────────────────┘
//
//   The arrows flow from outer → inner (handler → domain → domain types).
//   The repository INTERFACE depends on domain types (inward arrow).
//   The postgres IMPLEMENTATION depends on the repository interface (it satisfies
//   it). This is dependency inversion: the domain drives the shape of the DB
//   contract, not the other way around.
//
// WHEN THE POSTGRES IMPL LANDS (Task 1.4):
//   package postgres
//   type UserRepo struct { db *pgxpool.Pool }
//   func (r *UserRepo) Create(ctx, user) (User, error) { ... }
//   // UserRepo now satisfies repository.UserRepository automatically.
//
// ============================================================================
package repository

import (
	"context"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"
)

// ============================================================================
// PAGINATION
// ============================================================================

// ListOptions carries pagination parameters for list queries.
//
// WHY cursor-based pagination (PageToken) over offset-based (LIMIT/OFFSET):
//   LIMIT/OFFSET is simple but has a classic problem: if a row is inserted
//   before the current page, the next OFFSET skips a row (or duplicates one).
//   Cursor-based pagination (a pointer into the ordered result set — typically
//   an opaque base64 encoding of the last row's sort key) is stable under
//   concurrent writes.
//
//   For Forgepoint's user list (typically < 10,000 users) the difference is
//   academic, but cursor-based is the correct pattern for a platform claiming
//   production readiness. The proto ListUsersResponse.next_page_token mirrors
//   this design (matching Google API Design Guide conventions).
type ListOptions struct {
	PageSize  int    // number of items to return; 0 means use a default
	PageToken string // opaque cursor; empty means "start from the beginning"
}

// ============================================================================
// USER REPOSITORY
// ============================================================================

// UserRepository is the data-access contract for User persistence.
// The postgres implementation (Task 1.4) satisfies this interface.
// Tests inject a mock or in-memory implementation.
type UserRepository interface {
	// Create persists a new User and returns it with DB-populated fields
	// (ID, CreatedAt, UpdatedAt). The caller must not set these fields;
	// the repository generates them (e.g., UUID v4 for ID, NOW() for timestamps).
	//
	// ERROR: returns ErrEmailAlreadyExists if user.Email violates the unique
	// constraint. All other DB errors are returned wrapped with context.
	Create(ctx context.Context, user domain.User) (domain.User, error)

	// GetByID retrieves a User by its primary key.
	// Returns ErrNotFound if no user with that ID exists.
	GetByID(ctx context.Context, id string) (domain.User, error)

	// GetByEmail retrieves a User by email address (case-insensitive lookup).
	// Returns ErrNotFound if no user with that email exists.
	// Used by Login to look up the user before bcrypt comparison.
	GetByEmail(ctx context.Context, email string) (domain.User, error)

	// List returns a paginated slice of Users, ordered by created_at DESC.
	//
	// opts.PageToken is the opaque cursor from the previous call's nextToken.
	// An empty PageToken starts from the first page.
	//
	// Returns:
	//   users     — the current page of users (len ≤ opts.PageSize).
	//   nextToken — opaque cursor for the next page; empty string = last page.
	//   err       — DB or context error.
	//
	// WHY return nextToken as a string:
	//   The caller (ListUsers handler) passes it directly into
	//   ListUsersResponse.next_page_token without any transformation. Keeping it
	//   opaque here means the postgres impl can change the cursor encoding (from
	//   keyset to UUID-based) without changing the handler or service.
	List(ctx context.Context, opts ListOptions) (users []domain.User, nextToken string, err error)
}

// ============================================================================
// API KEY REPOSITORY
// ============================================================================

// APIKeyRepository is the data-access contract for APIKey persistence.
//
// CRITICAL: The repository stores and queries by KEY HASH (SHA-256 of the raw
// key), never the raw key itself. The raw key is only held in memory during
// the CreateAPIKey RPC and returned once to the caller. After that, it's gone.
type APIKeyRepository interface {
	// Create persists a new APIKey. The APIKey.KeyHash must be set by the caller
	// (the domain service hashes the raw key before calling Create).
	// Returns the APIKey with DB-populated fields (ID, CreatedAt).
	Create(ctx context.Context, key domain.APIKey) (domain.APIKey, error)

	// GetByKeyHash looks up an APIKey by its SHA-256 hash.
	// This is called on EVERY API-key-authenticated request — it must be fast.
	// The postgres index on key_hash makes this a single B-tree lookup.
	//
	// Returns ErrNotFound if no key with that hash exists.
	// The caller (AuthServiceImpl.ValidateToken) then calls key.IsValid(now).
	GetByKeyHash(ctx context.Context, keyHash string) (domain.APIKey, error)

	// Revoke sets the key's revoked_at timestamp to now.
	// The key immediately becomes invalid for future ValidateToken calls
	// (IsValid returns false for non-nil RevokedAt).
	//
	// WHY not delete: Soft deletes (revoked_at) preserve the audit trail.
	// You can answer "who revoked key fp_a1b2 and when?" from the DB.
	// Hard deletes make that impossible after the fact.
	Revoke(ctx context.Context, keyID string, now time.Time) error

	// ListByUser returns all API keys belonging to a user (active and revoked).
	// Used by the UI to display a user's key history with status indicators.
	// Does NOT return KeyHash — that field is zero-valued in list results
	// (no need to expose hashes in list APIs; GetByKeyHash is the lookup path).
	ListByUser(ctx context.Context, userID string) ([]domain.APIKey, error)
}

// ============================================================================
// ROLE REPOSITORY
// ============================================================================

// RoleRepository is the data-access contract for Role and user-role assignment.
//
// DESIGN NOTE: Roles are shared across users (a "roles" table, not embedded in
// the users table). This allows adding a new role without touching every user
// row, and enables listing all users with a given role for audit purposes.
//
// The user_roles join table links users to their single role. We enforce
// single-role-per-user at the application layer (AssignRole replaces the
// existing assignment) rather than with a UNIQUE constraint, to make
// bulk role migrations transactional without constraint violations.
type RoleRepository interface {
	// Create persists a new Role definition with its permissions.
	// Role names must be unique (enforced by a DB unique constraint).
	// Returns the created Role with its DB-assigned ID.
	Create(ctx context.Context, role domain.Role) (domain.Role, error)

	// GetByName retrieves a Role by its unique name (e.g., "admin").
	// Used by AuthServiceImpl when assigning a role to a user by name.
	// Returns ErrNotFound if no role with that name exists.
	GetByName(ctx context.Context, name string) (domain.Role, error)

	// AssignToUser sets the given role as the user's role, replacing any
	// existing assignment. This is an UPSERT on the user_roles table.
	//
	// WHY UPSERT over separate create/update:
	//   Whether the user previously had a role or not, the desired end state is
	//   the same: user has exactly this role. UPSERT (INSERT ... ON CONFLICT DO
	//   UPDATE) expresses intent precisely and is safe under concurrent calls
	//   (no TOCTOU race between SELECT and INSERT/UPDATE).
	AssignToUser(ctx context.Context, userID, roleID string) error

	// GetUserRoles returns all roles assigned to a user. For Forgepoint's flat
	// RBAC model this returns at most one role, but the slice return type
	// anticipates a potential future where admin users can hold multiple roles.
	//
	// Each returned Role includes its Permissions slice (eagerly loaded from the
	// role_permissions table). This avoids a second round-trip in CheckPermission.
	GetUserRoles(ctx context.Context, userID string) ([]domain.Role, error)
}

// ============================================================================
// SENTINEL ERRORS
// ============================================================================

// ErrNotFound is returned by repository methods when a requested record does
// not exist. Callers (domain service, handlers) check for this to return the
// appropriate gRPC status code (codes.NotFound).
//
// WHY a sentinel error over a custom type:
//   errors.Is(err, repository.ErrNotFound) is readable, testable, and doesn't
//   require a type assertion. A custom struct error type is warranted only if
//   callers need to extract additional structured data from the error.
var ErrNotFound = repositoryError("not found")

// ErrEmailAlreadyExists is returned by UserRepository.Create when the email
// violates the unique constraint. The handler maps this to codes.AlreadyExists.
var ErrEmailAlreadyExists = repositoryError("email already exists")

// repositoryError is an unexported string type for sentinel errors.
// It satisfies the error interface without needing a struct.
type repositoryError string

func (e repositoryError) Error() string { return string(e) }
