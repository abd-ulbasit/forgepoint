// Package repository is a thin COMPATIBILITY SHIM over the repository ports that
// now live in the domain package (see services/auth/internal/domain/ports.go).
//
// ============================================================================
// WHY THE PORTS MOVED TO domain, AND WHY THIS SHIM EXISTS
// ============================================================================
//
// Originally these interfaces were defined HERE, referencing domain types (the
// "Option B" layout documented in the project notes). That is a perfectly good
// layout — until a concrete domain consumer needs the interfaces. The domain
// service (AuthServiceImpl) lives in `domain` and must reference these ports;
// but `repository` already imports `domain` for the types. That makes:
//
//	domain → repository → domain          ← an import CYCLE (Go rejects it)
//
// The idiomatic Hexagonal-Architecture fix is "the CONSUMER owns the port": the
// domain is the consumer of persistence, so the port interfaces belong in
// `domain`. The Postgres ADAPTER (Task 1.4) implements `domain.UserRepository`
// directly — a single inward dependency, no cycle.
//
// This file is retained as ALIASES so that:
//   - any code or docs that still say `repository.UserRepository`,
//     `repository.ErrNotFound`, etc. keep compiling, and
//   - the Postgres adapter may satisfy EITHER name (they are the same type).
//
// New code should prefer the domain names directly. The SOURCE OF TRUTH is
// services/auth/internal/domain/ports.go.
//
// INTERVIEW: "You had an import cycle between domain and repository — how did you
// resolve it?" Move the port to the consumer (domain). In Hexagonal terms the
// port is defined by the side that uses it; the adapter implements it. The cycle
// was the symptom of putting the port on the adapter side.
// ============================================================================
package repository

import "github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"

// Port interfaces — type aliases to the canonical definitions in domain.
// A type alias (=) makes these the SAME type, so a value satisfying
// domain.UserRepository also satisfies repository.UserRepository and vice versa.
type (
	UserRepository   = domain.UserRepository
	APIKeyRepository = domain.APIKeyRepository
	RoleRepository   = domain.RoleRepository
	ListOptions      = domain.ListOptions
)

// Sentinel errors — re-exported so existing `errors.Is(err, repository.ErrNotFound)`
// call sites keep working. These are the SAME error values as in domain.
var (
	// ErrNotFound aliases domain.ErrRepoNotFound (storage "no such record").
	ErrNotFound = domain.ErrRepoNotFound
	// ErrEmailAlreadyExists aliases domain.ErrRepoEmailExists (unique violation).
	ErrEmailAlreadyExists = domain.ErrRepoEmailExists
)
