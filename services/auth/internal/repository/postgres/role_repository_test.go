package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"
)

// TestRoleRepo_Create_AndGetByName verifies a role's permissions encode to JSONB
// on insert and decode back identically on read.
func TestRoleRepo_Create_AndGetByName(t *testing.T) {
	db := newTestDB(t)
	repo := NewRoleRepo(db)
	ctx := context.Background()

	in := domain.Role{
		Name: "auditor",
		Permissions: []domain.Permission{
			{Resource: "billing", Action: "read"},
			{Resource: "audit", Action: "*"},
		},
	}
	created, err := repo.Create(ctx, in)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.ID == "" || created.CreatedAt.IsZero() {
		t.Errorf("expected server id/created_at, got id=%q created=%v", created.ID, created.CreatedAt)
	}

	got, err := repo.GetByName(ctx, "auditor")
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if got.ID != created.ID || got.Name != "auditor" {
		t.Errorf("GetByName mismatch: got %+v", got)
	}
	if len(got.Permissions) != 2 {
		t.Fatalf("expected 2 permissions decoded, got %d (%+v)", len(got.Permissions), got.Permissions)
	}
	if !containsPerm(got.Permissions, domain.Permission{Resource: "audit", Action: "*"}) {
		t.Errorf("wildcard permission did not round-trip: %+v", got.Permissions)
	}
}

// TestRoleRepo_Create_DuplicateName verifies the UNIQUE(name) constraint is
// reported as a domain sentinel rather than a raw pg error.
func TestRoleRepo_Create_DuplicateName(t *testing.T) {
	db := newTestDB(t)
	repo := NewRoleRepo(db)
	ctx := context.Background()

	// "admin" is seeded by the migration → inserting it again must conflict.
	_, err := repo.Create(ctx, domain.Role{Name: "admin"})
	if !errors.Is(err, domain.ErrRepoEmailExists) {
		t.Fatalf("expected ErrRepoEmailExists (already-exists) for duplicate role name, got %v", err)
	}
}

// TestRoleRepo_GetByName_NotFound verifies the not-found sentinel (AssignRole
// maps this to the business ErrRoleNotFound).
func TestRoleRepo_GetByName_NotFound(t *testing.T) {
	db := newTestDB(t)
	repo := NewRoleRepo(db)

	_, err := repo.GetByName(context.Background(), "ghost-role")
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("expected ErrRepoNotFound, got %v", err)
	}
}

// TestRoleRepo_SeededRoles verifies the migration's seed data is present and
// decodes — these roles are infrastructure other tests/services rely on.
func TestRoleRepo_SeededRoles(t *testing.T) {
	db := newTestDB(t)
	repo := NewRoleRepo(db)
	ctx := context.Background()

	admin, err := repo.GetByName(ctx, "admin")
	if err != nil {
		t.Fatalf("GetByName admin: %v", err)
	}
	// admin is the {*,*} grant.
	if !containsPerm(admin.Permissions, domain.Permission{Resource: "*", Action: "*"}) {
		t.Errorf("admin must have {*,*}, got %+v", admin.Permissions)
	}

	for _, name := range []string{"engineer", "viewer"} {
		if _, err := repo.GetByName(ctx, name); err != nil {
			t.Errorf("seeded role %q missing: %v", name, err)
		}
	}
}

// TestRoleRepo_AssignToUser_Idempotent verifies assigning the SAME role twice is
// a no-op (the upsert's idempotency) and GetUserRoles returns exactly one role.
func TestRoleRepo_AssignToUser_Idempotent(t *testing.T) {
	db := newTestDB(t)
	roleRepo := NewRoleRepo(db)
	ctx := context.Background()
	userID := seedUser(t, db, "assign1@example.com")

	viewer, err := roleRepo.GetByName(ctx, "viewer")
	if err != nil {
		t.Fatalf("GetByName viewer: %v", err)
	}

	// Assign twice — the second must not error and must not create a duplicate row.
	if err := roleRepo.AssignToUser(ctx, userID, viewer.ID); err != nil {
		t.Fatalf("AssignToUser #1: %v", err)
	}
	if err := roleRepo.AssignToUser(ctx, userID, viewer.ID); err != nil {
		t.Fatalf("AssignToUser #2 (idempotent): %v", err)
	}

	roles, err := roleRepo.GetUserRoles(ctx, userID)
	if err != nil {
		t.Fatalf("GetUserRoles: %v", err)
	}
	if len(roles) != 1 {
		t.Fatalf("expected exactly 1 role after idempotent re-assign, got %d", len(roles))
	}
	if roles[0].Name != "viewer" {
		t.Errorf("expected viewer, got %q", roles[0].Name)
	}
	// Permissions eagerly loaded for CheckPermission (no second round-trip).
	if len(roles[0].Permissions) != 3 {
		t.Errorf("expected 3 viewer permissions, got %d", len(roles[0].Permissions))
	}
}

// TestRoleRepo_AssignToUser_Replaces verifies re-assigning a DIFFERENT role
// REPLACES the prior one (single-role rule via the user_id PK upsert).
func TestRoleRepo_AssignToUser_Replaces(t *testing.T) {
	db := newTestDB(t)
	roleRepo := NewRoleRepo(db)
	ctx := context.Background()
	userID := seedUser(t, db, "assign2@example.com")

	viewer, _ := roleRepo.GetByName(ctx, "viewer")
	engineer, _ := roleRepo.GetByName(ctx, "engineer")

	if err := roleRepo.AssignToUser(ctx, userID, viewer.ID); err != nil {
		t.Fatalf("assign viewer: %v", err)
	}
	if err := roleRepo.AssignToUser(ctx, userID, engineer.ID); err != nil {
		t.Fatalf("assign engineer (replace): %v", err)
	}

	roles, err := roleRepo.GetUserRoles(ctx, userID)
	if err != nil {
		t.Fatalf("GetUserRoles: %v", err)
	}
	if len(roles) != 1 {
		t.Fatalf("expected exactly 1 role after replace, got %d (%+v)", len(roles), roles)
	}
	if roles[0].Name != "engineer" {
		t.Errorf("expected role replaced to engineer, got %q", roles[0].Name)
	}
}

// TestRoleRepo_AssignToUser_ForeignKeyViolation verifies a bad userID or roleID
// is rejected as ErrRepoNotFound (defense-in-depth backstop for the FKs).
func TestRoleRepo_AssignToUser_ForeignKeyViolation(t *testing.T) {
	db := newTestDB(t)
	roleRepo := NewRoleRepo(db)
	ctx := context.Background()
	userID := seedUser(t, db, "assign3@example.com")
	viewer, _ := roleRepo.GetByName(ctx, "viewer")

	// Bad role id.
	err := roleRepo.AssignToUser(ctx, userID, "00000000-0000-0000-0000-000000000000")
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("expected ErrRepoNotFound for bad role id, got %v", err)
	}
	// Bad user id.
	err = roleRepo.AssignToUser(ctx, "00000000-0000-0000-0000-000000000000", viewer.ID)
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("expected ErrRepoNotFound for bad user id, got %v", err)
	}
}

// TestRoleRepo_GetUserRoles_Empty verifies a user with no assignment returns an
// empty slice (not an error) — the normal state for a freshly created user.
func TestRoleRepo_GetUserRoles_Empty(t *testing.T) {
	db := newTestDB(t)
	roleRepo := NewRoleRepo(db)
	ctx := context.Background()
	userID := seedUser(t, db, "norole@example.com")

	roles, err := roleRepo.GetUserRoles(ctx, userID)
	if err != nil {
		t.Fatalf("GetUserRoles: %v", err)
	}
	if len(roles) != 0 {
		t.Errorf("expected no roles for unassigned user, got %d", len(roles))
	}
}
