// bootstrap_test.go — TDD specification for the admin-bootstrap path.
//
// ============================================================================
// WHY BOOTSTRAP EXISTS (the chicken-and-egg this closes)
// ============================================================================
//
// The auth migration seeds the three ROLES (admin/engineer/viewer) but NO users,
// and CreateUser is admin-gated at the RPC layer. So on a fresh database there is
// literally no identity that can authenticate — and therefore no way to ever
// create the first admin through the normal RPC path. That is the classic
// first-admin bootstrap problem (every platform has it: Grafana's admin/admin,
// Argo CD's initial-admin-secret, Postgres' trust-auth bootstrap).
//
// BootstrapAdmin is the trusted, IN-PROCESS escape hatch: it runs at service boot
// (main.go), NOT over gRPC, so it legitimately bypasses the RPC admin-gate — the
// process operator who set FP_BOOTSTRAP_ADMIN_* env vars IS the trust anchor. It
// reuses the SAME bcrypt + role-assign logic as CreateUser/AssignRole so the
// bootstrapped admin is indistinguishable from one created via the API.
//
// These tests are written BEFORE the implementation and assert REAL outcomes
// (bcrypt verifies, the admin role is resolved+assigned, a second call is a true
// no-op), not mock call counts where a behavior can be checked directly.
package domain_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/auth/internal/repository"
)

// bootstrapper narrows the service to the bootstrap capability. The production
// *authService returned by NewAuthService satisfies domain.AdminBootstrapper; we
// assert to it here exactly as main.go does at the wiring root.
func asBootstrapper(t *testing.T, svc domain.AuthService) domain.AdminBootstrapper {
	t.Helper()
	b, ok := svc.(domain.AdminBootstrapper)
	if !ok {
		t.Fatalf("AuthService %T does not implement domain.AdminBootstrapper", svc)
	}
	return b
}

// TestBootstrapAdmin_CreatesAdminWithAdminRole_AndPasswordVerifies is the happy
// path: on a database with the admin role seeded but no matching user, the first
// BootstrapAdmin must CREATE the user (bcrypting the password), ASSIGN the admin
// role (resolved by name → id), and report created=true. We then prove the stored
// hash actually verifies the password — i.e. the bootstrapped admin can log in.
func TestBootstrapAdmin_CreatesAdminWithAdminRole_AndPasswordVerifies(t *testing.T) {
	const (
		email = "Admin@Forgepoint.Local" // mixed case → must be normalized on store
		pass  = "bootstrap-strong-pass-123"
	)

	var (
		createdUser    domain.User
		assignedUserID string
		assignedRoleID string
		userExists     bool // flips true once Create runs, so a second GetByEmail finds it
	)

	userRepo := &mockUserRepo{
		// Before creation the user does not exist; after, GetByEmail finds it. This
		// models the real DB so the SECOND bootstrap call (next test) is a true no-op.
		getByEmailFn: func(ctx context.Context, e string) (domain.User, error) {
			if userExists {
				return createdUser, nil
			}
			return domain.User{}, repository.ErrNotFound
		},
		createFn: func(ctx context.Context, u domain.User) (domain.User, error) {
			u.ID = "user-admin-1"
			u.CreatedAt = time.Now()
			createdUser = u
			userExists = true
			return u, nil
		},
	}
	roleRepo := &mockRoleRepo{
		// AssignRole pre-checks the user exists (GetByID) then resolves the role
		// NAME → id and upserts the assignment.
		getByNameFn: func(ctx context.Context, name string) (domain.Role, error) {
			if name != "admin" {
				t.Fatalf("BootstrapAdmin resolved role %q, want admin", name)
			}
			return domain.Role{ID: "role-admin", Name: "admin"}, nil
		},
		assignFn: func(ctx context.Context, userID, roleID string) error {
			assignedUserID, assignedRoleID = userID, roleID
			return nil
		},
	}
	// AssignRole calls userRepo.GetByID to confirm the user before assigning.
	userRepo.getByIDFn = func(ctx context.Context, id string) (domain.User, error) {
		return domain.User{ID: id, Active: true}, nil
	}

	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), roleRepo, []byte(testSecret), time.Hour)
	b := asBootstrapper(t, svc)

	user, created, err := b.BootstrapAdmin(context.Background(), email, pass)
	if err != nil {
		t.Fatalf("BootstrapAdmin: unexpected error: %v", err)
	}
	if !created {
		t.Fatal("BootstrapAdmin reported created=false on a fresh database; want true")
	}
	if user.ID == "" {
		t.Error("returned admin has empty ID; expected DB-populated ID")
	}

	// REAL behavior: the password the operator supplied must verify against the
	// stored bcrypt hash — i.e. this admin can authenticate via Login.
	if err := bcrypt.CompareHashAndPassword([]byte(createdUser.PasswordHash), []byte(pass)); err != nil {
		t.Fatalf("stored bootstrap-admin hash does not verify the password: %v", err)
	}
	// Email normalized (lowercased+trimmed) exactly like CreateUser.
	if createdUser.Email != "admin@forgepoint.local" {
		t.Errorf("stored email = %q, want normalized admin@forgepoint.local", createdUser.Email)
	}
	if !createdUser.Active {
		t.Error("bootstrap admin should be Active")
	}
	// The admin role was resolved by name and assigned to the new user.
	if assignedRoleID != "role-admin" {
		t.Errorf("assigned roleID = %q, want role-admin (resolved from name)", assignedRoleID)
	}
	if assignedUserID != "user-admin-1" {
		t.Errorf("assigned userID = %q, want the created user's id", assignedUserID)
	}
}

// TestBootstrapAdmin_Idempotent_SecondCallNoOps proves the idempotency contract:
// when a user with the bootstrap email already exists, BootstrapAdmin must NOT
// create a second user, must NOT re-assign the role, and must report created=false.
// (A panic in createFn/assignFn — they are intentionally left to fail the test —
// would surface any accidental write on the already-exists path.)
func TestBootstrapAdmin_Idempotent_SecondCallNoOps(t *testing.T) {
	existing := domain.User{
		ID:           "user-admin-1",
		Email:        "admin@forgepoint.local",
		Active:       true,
		PasswordHash: "$2a$10$existing-hash-placeholder",
	}
	userRepo := &mockUserRepo{
		getByEmailFn: func(ctx context.Context, e string) (domain.User, error) {
			return existing, nil // the admin already exists
		},
		createFn: func(ctx context.Context, u domain.User) (domain.User, error) {
			t.Fatal("Create must NOT be called when the admin already exists (not idempotent)")
			return domain.User{}, nil
		},
	}
	roleRepo := &mockRoleRepo{
		assignFn: func(ctx context.Context, userID, roleID string) error {
			t.Fatal("AssignToUser must NOT be called when the admin already exists")
			return nil
		},
		getByNameFn: func(ctx context.Context, name string) (domain.Role, error) {
			t.Fatal("GetByName must NOT be called when the admin already exists")
			return domain.Role{}, nil
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), roleRepo, []byte(testSecret), time.Hour)
	b := asBootstrapper(t, svc)

	user, created, err := b.BootstrapAdmin(context.Background(), "admin@forgepoint.local", "irrelevant")
	if err != nil {
		t.Fatalf("BootstrapAdmin (idempotent path): unexpected error: %v", err)
	}
	if created {
		t.Fatal("BootstrapAdmin reported created=true for an already-existing admin; must be a no-op")
	}
	if user.ID != existing.ID {
		t.Errorf("returned user ID = %q, want the existing admin's id %q", user.ID, existing.ID)
	}
}

// TestBootstrapAdmin_CreatedAdminCanLogin is the end-to-end domain proof: after
// BootstrapAdmin creates the admin, Login with the same credentials must succeed
// and mint a JWT carrying the admin role. This exercises the real bcrypt verify
// and JWT mint paths against a bootstrapped identity (one repo backs both calls).
func TestBootstrapAdmin_CreatedAdminCanLogin(t *testing.T) {
	const (
		email = "admin@forgepoint.local"
		pass  = "another-strong-pass-456"
	)
	var stored domain.User
	exists := false

	userRepo := &mockUserRepo{
		getByEmailFn: func(ctx context.Context, e string) (domain.User, error) {
			if exists {
				return stored, nil
			}
			return domain.User{}, repository.ErrNotFound
		},
		createFn: func(ctx context.Context, u domain.User) (domain.User, error) {
			u.ID = "user-admin-1"
			// Reflect the assigned admin role so Login embeds it in the JWT, just as
			// GetByEmail's role JOIN would after AssignRole committed.
			u.Role = domain.Role{Name: "admin"}
			stored = u
			exists = true
			return u, nil
		},
		getByIDFn: func(ctx context.Context, id string) (domain.User, error) {
			return domain.User{ID: id, Active: true}, nil
		},
	}
	roleRepo := &mockRoleRepo{
		getByNameFn: func(ctx context.Context, name string) (domain.Role, error) {
			return domain.Role{ID: "role-admin", Name: "admin"}, nil
		},
		assignFn: func(ctx context.Context, userID, roleID string) error { return nil },
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), roleRepo, []byte(testSecret), time.Hour)
	b := asBootstrapper(t, svc)

	if _, _, err := b.BootstrapAdmin(context.Background(), email, pass); err != nil {
		t.Fatalf("BootstrapAdmin: %v", err)
	}

	// Now the bootstrapped admin must be able to authenticate.
	tok, err := svc.Login(context.Background(), email, pass)
	if err != nil {
		t.Fatalf("Login for bootstrapped admin failed: %v", err)
	}
	claims, err := domain.ValidateToken(tok, []byte(testSecret))
	if err != nil {
		t.Fatalf("bootstrap-admin JWT failed validation: %v", err)
	}
	if claims.Role != "admin" {
		t.Errorf("token Role = %q, want admin", claims.Role)
	}
	if claims.Email != email {
		t.Errorf("token Email = %q, want %q", claims.Email, email)
	}
}

// TestBootstrapAdmin_RejectsEmptyCredentials guards the trusted path against an
// empty email or password (a half-set env config). It must refuse with a
// validation error rather than create a passwordless or anonymous admin.
func TestBootstrapAdmin_RejectsEmptyCredentials(t *testing.T) {
	userRepo := &mockUserRepo{
		getByEmailFn: func(ctx context.Context, e string) (domain.User, error) {
			return domain.User{}, repository.ErrNotFound
		},
		createFn: func(ctx context.Context, u domain.User) (domain.User, error) {
			t.Fatal("Create must not run for empty bootstrap credentials")
			return domain.User{}, nil
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), &mockRoleRepo{}, []byte(testSecret), time.Hour)
	b := asBootstrapper(t, svc)

	cases := []struct{ email, pass string }{
		{"", "password123"},
		{"admin@forgepoint.local", ""},
	}
	for _, tc := range cases {
		if _, _, err := b.BootstrapAdmin(context.Background(), tc.email, tc.pass); !errors.Is(err, domain.ErrValidation) {
			t.Errorf("BootstrapAdmin(%q,%q) error = %v, want ErrValidation", tc.email, tc.pass, err)
		}
	}
}
