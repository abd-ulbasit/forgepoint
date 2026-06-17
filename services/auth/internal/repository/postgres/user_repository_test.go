package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"
)

// newUser is a small fixture builder so each test states only the field it cares
// about (email here, since it's the unique key) and gets sensible defaults for
// the rest. Keeping fixtures terse keeps the ASSERTION the focus of each test.
func newUser(email string) domain.User {
	return domain.User{
		Email:        email,
		Name:         "Test User",
		Team:         "platform",
		PasswordHash: "$2a$10$abcdefghijklmnopqrstuvwx", // not a real bcrypt verify target; stored opaquely
		Active:       true,
	}
}

// TestUserRepo_Create_AssignsServerFields verifies the INSERT ... RETURNING path
// populates the DB-generated id and timestamps, and round-trips the input fields.
func TestUserRepo_Create_AssignsServerFields(t *testing.T) {
	db := newTestDB(t)
	repo := NewUserRepo(db)
	ctx := context.Background()

	in := newUser("ada@example.com")
	got, err := repo.Create(ctx, in)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if got.ID == "" {
		t.Error("expected server-generated ID, got empty")
	}
	if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
		t.Errorf("expected timestamps to be set, got created=%v updated=%v", got.CreatedAt, got.UpdatedAt)
	}
	if got.Email != in.Email || got.Name != in.Name || got.Team != in.Team {
		t.Errorf("round-trip mismatch: got %+v want fields from %+v", got, in)
	}
	// A freshly created user has NO role yet (CreateUser assigns none); the
	// embedded Role must be the zero value.
	if got.Role.ID != "" || got.Role.Name != "" {
		t.Errorf("expected zero-value Role for a new user, got %+v", got.Role)
	}
}

// TestUserRepo_Create_DuplicateEmail_ReturnsSentinel verifies the unique
// constraint on users.email maps to the storage sentinel ErrRepoEmailExists,
// and that the comparison is CASE-INSENSITIVE (citext): "Ada@" collides with
// "ada@".
func TestUserRepo_Create_DuplicateEmail_ReturnsSentinel(t *testing.T) {
	db := newTestDB(t)
	repo := NewUserRepo(db)
	ctx := context.Background()

	if _, err := repo.Create(ctx, newUser("dup@example.com")); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	// Same email, different case → citext folds them → unique violation.
	_, err := repo.Create(ctx, newUser("DUP@example.com"))
	if !errors.Is(err, domain.ErrRepoEmailExists) {
		t.Fatalf("expected ErrRepoEmailExists for case-insensitive duplicate, got %v", err)
	}
}

// TestUserRepo_GetByID verifies lookup-by-id and the not-found sentinel.
func TestUserRepo_GetByID(t *testing.T) {
	db := newTestDB(t)
	repo := NewUserRepo(db)
	ctx := context.Background()

	created, err := repo.Create(ctx, newUser("byid@example.com"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := repo.GetByID(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.ID != created.ID || got.Email != created.Email {
		t.Errorf("GetByID mismatch: got %+v want id=%s email=%s", got, created.ID, created.Email)
	}

	// A random, valid-looking UUID that was never inserted → ErrRepoNotFound.
	_, err = repo.GetByID(ctx, "00000000-0000-0000-0000-000000000000")
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("expected ErrRepoNotFound for missing id, got %v", err)
	}
}

// TestUserRepo_GetByEmail_CaseInsensitive verifies the Login lookup path: email
// matching is case-insensitive at the storage layer (citext), independent of the
// domain's own lowercasing.
func TestUserRepo_GetByEmail_CaseInsensitive(t *testing.T) {
	db := newTestDB(t)
	repo := NewUserRepo(db)
	ctx := context.Background()

	created, err := repo.Create(ctx, newUser("login@example.com"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Look up with a DIFFERENT case than stored; citext must still match.
	got, err := repo.GetByEmail(ctx, "LOGIN@EXAMPLE.COM")
	if err != nil {
		t.Fatalf("GetByEmail case-insensitive: %v", err)
	}
	if got.ID != created.ID {
		t.Errorf("expected to find user %s, got %s", created.ID, got.ID)
	}
	// PasswordHash must round-trip so Login can bcrypt-compare against it.
	if got.PasswordHash != created.PasswordHash {
		t.Errorf("PasswordHash not round-tripped: got %q want %q", got.PasswordHash, created.PasswordHash)
	}

	_, err = repo.GetByEmail(ctx, "missing@example.com")
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("expected ErrRepoNotFound for missing email, got %v", err)
	}
}

// TestUserRepo_GetByID_EagerLoadsRole is the cross-repository test that the
// user-read JOIN actually populates the embedded Role with its decoded
// permissions — the property Login depends on (it reads user.Role.Name into the
// JWT) and CheckPermission depends on (it scans role permissions).
func TestUserRepo_GetByID_EagerLoadsRole(t *testing.T) {
	db := newTestDB(t)
	userRepo := NewUserRepo(db)
	roleRepo := NewRoleRepo(db)
	ctx := context.Background()

	user, err := userRepo.Create(ctx, newUser("withrole@example.com"))
	if err != nil {
		t.Fatalf("Create user: %v", err)
	}

	// Resolve the seeded "engineer" role and assign it.
	engineer, err := roleRepo.GetByName(ctx, "engineer")
	if err != nil {
		t.Fatalf("GetByName engineer: %v", err)
	}
	if err := roleRepo.AssignToUser(ctx, user.ID, engineer.ID); err != nil {
		t.Fatalf("AssignToUser: %v", err)
	}

	got, err := userRepo.GetByID(ctx, user.ID)
	if err != nil {
		t.Fatalf("GetByID after assign: %v", err)
	}
	if got.Role.Name != "engineer" {
		t.Fatalf("expected role 'engineer' eagerly loaded, got %q", got.Role.Name)
	}
	// engineer has 6 permissions in the seed; confirm they decoded from JSONB.
	if len(got.Role.Permissions) != 6 {
		t.Fatalf("expected 6 permissions on engineer, got %d (%+v)", len(got.Role.Permissions), got.Role.Permissions)
	}
	// Spot-check one permission decoded correctly (resource+action both present).
	wantPerm := domain.Permission{Resource: "models", Action: "write"}
	if !containsPerm(got.Role.Permissions, wantPerm) {
		t.Errorf("expected permission %+v in %+v", wantPerm, got.Role.Permissions)
	}
}

// TestUserRepo_List_CursorPagination is the heart of the pagination contract:
// pages cover all rows exactly once, in created_at DESC order, with a stable
// cursor and a terminating empty nextToken.
func TestUserRepo_List_CursorPagination(t *testing.T) {
	db := newTestDB(t)
	repo := NewUserRepo(db)
	ctx := context.Background()

	// Insert 5 users with DISTINCT created_at by spacing the inserts. We set
	// created_at explicitly via a direct UPDATE so the ordering is deterministic
	// and the (created_at, id) tiebreaker is exercised without relying on clock
	// resolution between fast inserts.
	const total = 5
	ids := make([]string, 0, total)
	base := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < total; i++ {
		u, err := repo.Create(ctx, newUser(fmt.Sprintf("page%d@example.com", i)))
		if err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
		// Stamp a strictly increasing created_at so DESC order is u4,u3,u2,u1,u0.
		ts := base.Add(time.Duration(i) * time.Minute)
		if _, err := db.pool.Exec(ctx, `UPDATE users SET created_at = $1 WHERE id = $2`, ts, u.ID); err != nil {
			t.Fatalf("stamp created_at: %v", err)
		}
		ids = append(ids, u.ID)
	}

	// Page through with pageSize=2: expect pages of [2,2,1] and then empty token.
	var seen []string
	token := ""
	pages := 0
	for {
		users, next, err := repo.List(ctx, domain.ListOptions{PageSize: 2, PageToken: token})
		if err != nil {
			t.Fatalf("List page %d: %v", pages, err)
		}
		for _, u := range users {
			seen = append(seen, u.ID)
		}
		pages++
		if next == "" {
			break
		}
		token = next
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}

	if len(seen) != total {
		t.Fatalf("expected to see %d users across pages, saw %d (%v)", total, len(seen), seen)
	}
	// No duplicates and no skips: the set of seen ids must equal the inserted set.
	if !sameSet(seen, ids) {
		t.Errorf("paged ids %v do not match inserted ids %v", seen, ids)
	}
	// Ordering: newest first. ids were inserted oldest→newest (ids[4] newest), so
	// the first id seen must be the last inserted.
	if seen[0] != ids[total-1] {
		t.Errorf("expected newest user %s first, got %s", ids[total-1], seen[0])
	}
}

// TestUserRepo_List_DefaultPageSizeAndEmpty verifies the empty-token first page,
// the default page size path, and that a malformed token is rejected.
func TestUserRepo_List_DefaultPageSizeAndEmpty(t *testing.T) {
	db := newTestDB(t)
	repo := NewUserRepo(db)
	ctx := context.Background()

	// Empty database → empty page, empty next token, no error.
	users, next, err := repo.List(ctx, domain.ListOptions{})
	if err != nil {
		t.Fatalf("List on empty db: %v", err)
	}
	if len(users) != 0 || next != "" {
		t.Fatalf("expected empty result on empty db, got %d users next=%q", len(users), next)
	}

	// Insert 3 < defaultPageSize: a single page with PageSize=0 (default) returns
	// all of them and an empty next token (no further page).
	for i := 0; i < 3; i++ {
		if _, err := repo.Create(ctx, newUser(fmt.Sprintf("d%d@example.com", i))); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
	}
	users, next, err = repo.List(ctx, domain.ListOptions{PageSize: 0})
	if err != nil {
		t.Fatalf("List default page size: %v", err)
	}
	if len(users) != 3 || next != "" {
		t.Fatalf("expected 3 users and empty next, got %d users next=%q", len(users), next)
	}

	// A garbage page token is a client error → non-nil error (handler maps to
	// InvalidArgument). It must NOT silently return page 1.
	_, _, err = repo.List(ctx, domain.ListOptions{PageToken: "not-a-valid-cursor!!!"})
	if err == nil {
		t.Fatal("expected error for malformed page token, got nil")
	}
}

// ----------------------------------------------------------------------------
// small test helpers
// ----------------------------------------------------------------------------

func containsPerm(perms []domain.Permission, want domain.Permission) bool {
	for _, p := range perms {
		if p == want {
			return true
		}
	}
	return false
}

// sameSet reports whether a and b contain exactly the same elements (order- and
// duplicate-insensitive for our use, where inputs have no intentional dups).
func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]int, len(a))
	for _, x := range a {
		m[x]++
	}
	for _, y := range b {
		m[y]--
	}
	for _, v := range m {
		if v != 0 {
			return false
		}
	}
	return true
}
