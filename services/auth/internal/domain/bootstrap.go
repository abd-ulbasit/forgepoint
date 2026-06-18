// bootstrap.go — the trusted, in-process admin-bootstrap path.
//
// ============================================================================
// THE FIRST-ADMIN CHICKEN-AND-EGG (and why this is NOT an RPC)
// ============================================================================
//
// The migration seeds the three ROLES (admin/engineer/viewer) but ZERO users,
// and the CreateUser RPC is admin-gated (requireAdmin in the handler). On a fresh
// database that is a deadlock: you need an admin to create a user, but there is no
// admin and no way to create the first one over the API. Every platform faces
// this and solves it with a TRUSTED OUT-OF-BAND path:
//
//	Grafana  → GF_SECURITY_ADMIN_USER/PASSWORD env at boot
//	Argo CD  → an initial-admin-secret generated on install
//	Postgres → `trust`/`peer` local auth to create the first superuser
//	Keycloak → KEYCLOAK_ADMIN/KEYCLOAK_ADMIN_PASSWORD env at boot
//
// Forgepoint uses the SAME shape: if FP_BOOTSTRAP_ADMIN_EMAIL and
// FP_BOOTSTRAP_ADMIN_PASSWORD are both set, main.go calls BootstrapAdmin at boot,
// IN-PROCESS, before serving any gRPC. Because it runs in the trusted boot path
// (the operator who set those env vars IS the trust anchor) and NOT through the
// gRPC CreateUser handler, it legitimately bypasses the RPC admin-gate without
// weakening it: the gate still protects every network caller.
//
// SECURITY POSTURE / INTERVIEW FRAMING:
//   - WHY not a SQL seed in the migration? Because the password must be bcrypted,
//     and a migration cannot bcrypt a runtime-supplied secret without baking a
//     credential into committed SQL. Doing it in Go reuses the exact CreateUser
//     hashing path and keeps the password a runtime env input, never in git.
//   - WHY idempotent? The same env config is present on EVERY pod and EVERY
//     restart/rollout. A non-idempotent bootstrap would either error or create
//     duplicates on the 2nd..Nth boot. "Create iff absent, else skip" makes it
//     safe to run unconditionally on every start (converges to one admin).
//   - WHY reuse CreateUser + AssignRole and not hand-roll the writes? So the
//     bootstrapped admin is byte-for-byte identical to an API-created one (same
//     bcrypt cost, same normalization, same role-assignment upsert) — no special
//     second code path to keep in sync or to reason about in an audit.
//
// ============================================================================
package domain

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// adminRoleName is the seeded role granted to the bootstrap admin. It MUST match
// a roles.name the migration inserts (001_init_auth_schema.up.sql seeds 'admin'
// with the {*,*} grant), because AssignRole resolves the role by NAME → id.
const adminRoleName = "admin"

// AdminBootstrapper is the narrow capability interface for the trusted boot path.
//
// WHY a SEPARATE interface from AuthService (not another AuthService method):
//
//	BootstrapAdmin is deliberately NOT part of AuthService. AuthService is the
//	RPC-facing port the handler exposes; putting a privilege-bypassing operation
//	on it would (a) imply it's a callable RPC, (b) force the eventing decorator
//	and every handler-test mock to implement it, and (c) blur the security story.
//	A dedicated, segregated interface (the Interface Segregation Principle) keeps
//	the bypass off the network surface and lets main.go depend on JUST this one
//	method. The production *authService satisfies both interfaces; main.go obtains
//	this one via a type assertion at the wiring root (the only place that should
//	know the concrete type exposes a boot-only capability).
type AdminBootstrapper interface {
	// BootstrapAdmin ensures an active admin user with the given email exists.
	//
	//   - If no user with that (normalized) email exists, it CREATES one with the
	//     password bcrypt-hashed (via the same path as CreateUser) and ASSIGNS the
	//     seeded 'admin' role, returning (user, created=true, nil).
	//   - If the email already exists, it is a NO-OP: returns the existing user
	//     with created=false and does NOT touch the password or role assignment
	//     (idempotent — safe to call on every boot of every replica).
	//
	// It rejects an empty email or password with ErrValidation (a half-set env
	// config must not create a passwordless/anonymous admin).
	BootstrapAdmin(ctx context.Context, email, password string) (user User, created bool, err error)
}

// Compile-time proof the production service satisfies the bootstrap port. If the
// method signature drifts, the package fails to build here rather than at the
// type assertion in main.go.
var _ AdminBootstrapper = (*authService)(nil)

// BootstrapAdmin implements AdminBootstrapper on the concrete service.
//
// IDEMPOTENCY MECHANISM — check-then-act, but SAFE here:
//
//	We first GetByEmail; if the user exists we skip. A check-then-act pattern is
//	normally race-prone, but this runs at single-process BOOT, before gRPC serves,
//	and even if two replicas boot simultaneously the underlying users.email UNIQUE
//	constraint makes a concurrent double-create fail the loser's INSERT — which we
//	then resolve to "already exists" below. So the convergent end state ("exactly
//	one admin") holds even under a boot race; the GetByEmail is the fast path, the
//	unique index is the correctness backstop.
func (s *authService) BootstrapAdmin(ctx context.Context, email, password string) (User, bool, error) {
	// Normalize identically to CreateUser/Login so the existence check, the unique
	// constraint, and any later Login all agree on the same canonical email.
	normEmail := strings.ToLower(strings.TrimSpace(email))

	// Reject a half-configured bootstrap up front. main.go only calls this when
	// BOTH env vars are set, but defending here keeps the invariant local: this
	// method never produces a passwordless or anonymous admin regardless of caller.
	if normEmail == "" {
		return User{}, false, fmt.Errorf("%w: bootstrap admin email is required", ErrValidation)
	}
	if password == "" {
		return User{}, false, fmt.Errorf("%w: bootstrap admin password is required", ErrValidation)
	}

	// FAST-PATH IDEMPOTENCY: if the admin already exists, do nothing. We surface
	// the existing user so main.go can log "already present" with its id.
	existing, err := s.userRepo.GetByEmail(ctx, normEmail)
	if err == nil {
		return existing, false, nil
	}
	// Only a genuine "not found" means we should create. Any other lookup error is
	// infrastructure (DB down) and must abort the bootstrap loudly — we will NOT
	// blindly attempt a create on an ambiguous read.
	if !errors.Is(err, ErrRepoNotFound) {
		return User{}, false, fmt.Errorf("bootstrap: checking for existing admin: %w", err)
	}

	// CREATE the admin reusing the exact CreateUser path (validation, 72-byte
	// guard, bcrypt at DefaultCost, email normalization, Active=true). This is the
	// single source of truth for "how a user is made" — no parallel hashing logic.
	created, err := s.CreateUser(ctx, CreateUserInput{
		Email:    normEmail,
		Name:     "Bootstrap Admin",
		Password: password,
		Team:     "platform",
	})
	if err != nil {
		// A concurrent boot (another replica) could have won the create between our
		// GetByEmail and this CreateUser — the unique index then rejects ours as
		// ErrEmailAlreadyExists. Treat that as the idempotent "already exists" case:
		// re-read and return it as created=false rather than failing the boot.
		if errors.Is(err, ErrEmailAlreadyExists) {
			again, getErr := s.userRepo.GetByEmail(ctx, normEmail)
			if getErr != nil {
				return User{}, false, fmt.Errorf("bootstrap: admin existed on create but re-read failed: %w", getErr)
			}
			return again, false, nil
		}
		return User{}, false, fmt.Errorf("bootstrap: creating admin user: %w", err)
	}

	// ASSIGN the seeded 'admin' role via the same AssignRole path the API uses
	// (resolves name→id, idempotent upsert). Without this the new user would have
	// NO role and thus no permissions — useless as an admin.
	if err := s.AssignRole(ctx, created.ID, adminRoleName); err != nil {
		// The user row is committed but rolen't assigned. We FAIL the bootstrap so
		// the operator notices (the pod won't start), rather than leaving a
		// permission-less "admin" that silently can't do anything. On the next boot
		// the user now exists, so we'd hit the idempotent skip and never retry the
		// assignment — therefore a hard failure here is the correct, visible signal.
		return User{}, false, fmt.Errorf("bootstrap: assigning admin role to %s: %w", created.ID, err)
	}

	// Reflect the assigned role on the returned value so a caller that logs it sees
	// the admin role even though CreateUser returned the pre-assignment user.
	created.Role = Role{Name: adminRoleName}
	return created, true, nil
}
