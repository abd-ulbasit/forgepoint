// ports.go — the repository PORTS (data-access interfaces) the domain depends on.
//
// ============================================================================
// WHY THESE INTERFACES LIVE IN THE DOMAIN PACKAGE (Hexagonal "consumer-owned ports")
// ============================================================================
//
// The scaffold originally placed these interfaces in `internal/repository`, with
// the interfaces referencing domain types (the "Option B" described in
// repository/interfaces.go). That works right up until a CONCRETE domain
// consumer needs them: authService (auth_service_impl.go) lives in `domain` and
// must reference these interfaces — but `repository` already imports `domain`
// for the types. domain → repository → domain is an import CYCLE, which Go
// rejects outright (it is not a test-only artifact; it breaks the build).
//
// The idiomatic Go / Hexagonal Architecture resolution is "the CONSUMER defines
// the interface it needs." The domain is the consumer of persistence, so the
// PORT interfaces belong in the domain. The Postgres ADAPTER (Task 1.4, package
// `postgres`) will import `domain` and implement these ports — a single inward
// arrow, no cycle:
//
//	domain  (defines AuthService + repository PORTS + models)   ← stdlib only
//	   ▲
//	   │ implements
//	postgres adapter (Task 1.4)        handler (proto ↔ domain)
//
// `internal/repository` is kept as a thin compatibility shim (type aliases to
// these definitions) so existing prose/imports that reference
// `repository.UserRepository` still resolve and the Postgres impl can satisfy
// either name — but the SOURCE OF TRUTH is here.
//
// INTERVIEW: "Where do repository interfaces belong — with the data layer or the
// business layer?" In Hexagonal Architecture, the port is owned by the side that
// USES it (the domain). The adapter (DB) implements the port. Defining the port
// in the DB package and the consumer in the domain creates a cycle the moment a
// domain type implements another domain type's contract — which is exactly what
// happened here.
// ============================================================================
package domain

import (
	"context"
	"errors"
	"time"
)

// ============================================================================
// SENTINEL ERRORS (storage outcomes the domain translates to business errors)
// ============================================================================

// ErrRepoNotFound is the STORAGE sentinel returned by repository ports when a
// requested record does not exist. The domain service translates it into the
// right BUSINESS error (ErrUserNotFound, ErrRoleNotFound, ErrInvalidToken) or
// into ErrInvalidCredentials on the login path (anti-enumeration).
//
// WHY distinct from the business errors in errors.go: a storage outcome
// ("no row") is not the same fact as a business outcome ("invalid credentials").
// The service is the single translation point between the two vocabularies, so
// the handler never needs to know storage exists.
var ErrRepoNotFound = errors.New("repository: not found")

// ErrRepoEmailExists is the STORAGE sentinel returned by UserRepository.Create
// when the email violates the unique constraint. CreateUser maps it to the
// business error domain.ErrEmailAlreadyExists.
var ErrRepoEmailExists = errors.New("repository: email already exists")

// ============================================================================
// PAGINATION
// ============================================================================

// ListOptions carries cursor-based pagination parameters. See the original
// rationale in repository/interfaces.go: cursor pagination is stable under
// concurrent writes where LIMIT/OFFSET can skip or duplicate rows.
type ListOptions struct {
	PageSize  int    // 0 means use a default
	PageToken string // opaque cursor; empty = start from the beginning
}

// ============================================================================
// REPOSITORY PORTS
// ============================================================================

// UserRepository is the persistence port for User. The Postgres adapter
// (Task 1.4) implements it; tests inject mocks.
type UserRepository interface {
	// Create persists a new User and returns it with DB-populated fields.
	// Returns ErrEmailAlreadyExists on a unique-constraint violation.
	Create(ctx context.Context, user User) (User, error)
	// GetByID returns the User with that id, or ErrNotFound.
	GetByID(ctx context.Context, id string) (User, error)
	// GetByEmail returns the User with that (case-insensitive) email, or
	// ErrNotFound. Used by Login before the bcrypt comparison.
	GetByEmail(ctx context.Context, email string) (User, error)
	// List returns a page of Users (created_at DESC) plus a nextToken cursor.
	List(ctx context.Context, opts ListOptions) (users []User, nextToken string, err error)
}

// APIKeyRepository is the persistence port for APIKey.
//
// CRITICAL: it stores and queries by KEY HASH (SHA-256 of the raw key), never
// the raw key. The raw key lives only in memory during CreateAPIKey.
type APIKeyRepository interface {
	// Create persists a new APIKey whose KeyHash is already set by the domain
	// service (it hashes the raw key before calling Create).
	Create(ctx context.Context, key APIKey) (APIKey, error)
	// GetByKeyHash looks up an APIKey by SHA-256 hash; called on every
	// API-key-authenticated request. Returns ErrNotFound if absent.
	GetByKeyHash(ctx context.Context, keyHash string) (APIKey, error)
	// Revoke sets the key's revoked_at to now (soft delete; preserves audit trail).
	Revoke(ctx context.Context, keyID string, now time.Time) error
	// ListByUser returns all of a user's keys (active and revoked) for the UI.
	ListByUser(ctx context.Context, userID string) ([]APIKey, error)
}

// RoleRepository is the persistence port for Role and user-role assignment.
type RoleRepository interface {
	// Create persists a Role with its permissions (name must be unique).
	Create(ctx context.Context, role Role) (Role, error)
	// GetByName returns the Role with that unique name, or ErrNotFound.
	GetByName(ctx context.Context, name string) (Role, error)
	// AssignToUser upserts the user's role assignment (idempotent, race-safe).
	AssignToUser(ctx context.Context, userID, roleID string) error
	// GetUserRoles returns the user's roles, each with its Permissions eagerly
	// loaded (so CheckPermission needs no second round-trip).
	GetUserRoles(ctx context.Context, userID string) ([]Role, error)
}
