// ports.go — the PORTS the prompt-registry domain depends on (consumer-owned).
//
// ============================================================================
// PORTS LIVE IN THE DOMAIN (Hexagonal "consumer-owned ports")
// ============================================================================
//
// As in the Model Registry, the interfaces the domain CONSUMES are defined HERE,
// in the domain package, not in repository/. The reason is the import-cycle trap:
// the service (service.go) lives in this package and must reference these ports; if
// the ports lived in repository/, then repository would import this package (for
// the Prompt type) AND this package would import repository (for the ports) — a
// cycle Go rejects. Consumer-owns-the-port breaks it: the Postgres ADAPTER imports
// this package and implements PromptRepository one-way.
//
// ============================================================================
// WHY ONE STORE (not the Registry's two) — no CQRS split here
// ============================================================================
//
// The Model Registry splits WRITE (Postgres) from READ (Redis) because it is a
// read-heavy, projection-friendly aggregate. The prompt registry is LOW-volume,
// strongly-consistent, and small: a team has a handful of prompts, each with a few
// versions, read occasionally (mostly at render time). There is no read-amplification
// problem to solve, so a SINGLE Postgres-backed repository serves both commands and
// queries. Adding Redis here would be complexity with no payoff — a deliberate
// "don't pattern-match CQRS onto everything" choice worth defending in an interview.
package prompt

import (
	"context"
	"time"
)

// PromptRepository is the persistence PORT for the prompt registry. The Postgres
// adapter (internal/repository/postgres) implements it; the service depends only on
// this interface, so tests inject an in-memory fake with no database.
//
// CONTRACT for EVERY method:
//   - TEAM-SCOPED: every method takes a team and MUST scope to it. A read for a
//     (team,name) the caller doesn't own returns ErrRecordNotFound — never another
//     team's row. This is the tenancy boundary enforced at the storage edge (and
//     again by the WHERE team = $1 in the SQL).
//   - STORAGE SENTINELS ONLY: methods return ErrRecordNotFound / ErrWriteConflict
//     (defined in errors.go), never driver-specific errors. The service maps those
//     to business errors.
type PromptRepository interface {
	// CreateNextVersion inserts p as the NEXT version of (p.Team, p.Name) ATOMICALLY:
	// the adapter computes "next = COALESCE(MAX(version),0)+1" and INSERTs the row in
	// ONE transaction, so the per-(team,name) version number has no race window. The
	// caller passes p with Version UNSET (0); the adapter assigns and returns the
	// final Version on p. idempotencyKey is recorded inline ("" = no dedup).
	//
	// Returns ErrWriteConflict on a (team,name,version) OR (team,idempotency_key)
	// unique-index violation — the service distinguishes them by having checked the
	// idempotency key FIRST and by retrying the version race.
	CreateNextVersion(ctx context.Context, p Prompt, idempotencyKey string) (Prompt, error)

	// LookupByIdempotencyKey returns the prompt previously created with this key in
	// this team, or ErrRecordNotFound if none. An EMPTY key never matches (the caller
	// opted out) — the adapter short-circuits without a query. This is the READ half
	// of CREATE-idempotency: the service checks it BEFORE cutting a new version so a
	// retried CreatePrompt returns the original version, no duplicate.
	LookupByIdempotencyKey(ctx context.Context, team, key string) (Prompt, error)

	// GetVersion returns a specific (team, name, version), or ErrRecordNotFound.
	GetVersion(ctx context.Context, team, name string, version int) (Prompt, error)

	// GetLatest returns the HIGHEST-version row for (team, name), or ErrRecordNotFound
	// if the name has no versions in this team. "Latest" = max(version) regardless of
	// stage — the fallback GetPrompt uses when no version is pinned and no version is
	// in PRODUCTION.
	GetLatest(ctx context.Context, team, name string) (Prompt, error)

	// GetLatestProduction returns the highest-version row whose Stage = PRODUCTION for
	// (team, name), or ErrRecordNotFound if the name has no production version. This is
	// the PREFERRED target of GetPrompt(version=0): "give me the blessed prompt".
	GetLatestProduction(ctx context.Context, team, name string) (Prompt, error)

	// List returns a team-scoped, NEWEST-FIRST page of prompts plus a next-page
	// cursor (keyset pagination on (created_at, id), mirroring the Registry). An
	// empty nextToken means the last page. opts.PageToken == "" starts from newest.
	//
	// SEMANTICS: List returns ALL versions of all the team's prompts newest-first by
	// created_at (each version is a row). A "latest version per name" view is a
	// possible future refinement; L3 lists rows, matching the proto's flat repeated
	// Prompt response.
	List(ctx context.Context, team string, opts ListOptions) (prompts []Prompt, nextToken string, err error)
}

// ListOptions carries keyset pagination into List. PageToken is an OPAQUE cursor
// (empty = first page); PageSize is the max rows to return (the service clamps it).
// Modeled exactly on the Registry's ListOptions so the pagination contract is
// identical across services.
type ListOptions struct {
	PageSize  int
	PageToken string // opaque cursor; empty = start from the newest
}

// IDGenerator mints server-authoritative prompt ids. Production injects a UUIDv4
// generator; tests inject a deterministic sequence ("prompt-1", "prompt-2", ...).
// Centralizing minting behind a port means a client can NEVER supply an id — the
// structural defense behind "id is server-authoritative" (same as the Registry).
type IDGenerator interface {
	NewID() string
}

// Clock returns the current time. Injected so tests can pin CreatedAt deterministically
// (a fixed clock) and production uses the wall clock. The domain never calls time.Now
// directly — all time flows through this port, the standard testable-time seam.
type Clock interface {
	Now() time.Time
}
