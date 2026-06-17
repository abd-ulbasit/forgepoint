package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"
)

// TestRoleRepo_AssignToUser_ConcurrentNoRace proves the design's headline
// concurrency claim: AssignToUser is RACE-SAFE because the conflict is resolved
// by Postgres (the user_roles PK + ON CONFLICT), not by a check-then-act in the
// application.
//
// ============================================================================
// WHAT THIS TEST DEFENDS AGAINST
// ============================================================================
// A naive "SELECT then INSERT or UPDATE" assignment has a classic check-then-act
// race: two goroutines both observe "no row for this user", both INSERT, and one
// crashes with a primary-key violation (or, worse, with a different schema, both
// succeed and the user ends with two roles). The ON CONFLICT (user_id) DO UPDATE
// upsert eliminates the window entirely: Postgres takes the row lock and
// serializes the concurrent writers, so EVERY concurrent assigner succeeds and
// the final state is deterministically "exactly one role".
//
// We launch N goroutines that concurrently assign DIFFERENT roles to the SAME
// user. The assertions:
//   - no goroutine returns an error (no leaked PK violation),
//   - GetUserRoles returns exactly ONE role afterward (the single-role invariant
//     held under concurrency),
//   - that one role is one of the roles we tried to assign (a real winner, not a
//     corrupted state).
//
// Run under `-race`: the Go race detector also watches our own code (the pool is
// safe for concurrent use; sharing a single *pgx.Conn across goroutines would be
// the bug, which we avoid by going through the pool).
func TestRoleRepo_AssignToUser_ConcurrentNoRace(t *testing.T) {
	db := newTestDB(t)
	roleRepo := NewRoleRepo(db)
	ctx := context.Background()
	userID := seedUser(t, db, "concurrent@example.com")

	// Resolve the three seeded roles; concurrent goroutines fight to assign them.
	roleIDs := make([]string, 0, 3)
	roleNames := map[string]bool{}
	for _, name := range []string{"admin", "engineer", "viewer"} {
		r, err := roleRepo.GetByName(ctx, name)
		if err != nil {
			t.Fatalf("GetByName %s: %v", name, err)
		}
		roleIDs = append(roleIDs, r.ID)
		roleNames[name] = true
	}

	const goroutines = 24
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		roleID := roleIDs[i%len(roleIDs)] // round-robin over the three roles
		go func() {
			defer wg.Done()
			// Each goroutine borrows its OWN connection from the pool for the
			// duration of this Exec — that is what makes concurrent use safe.
			if err := roleRepo.AssignToUser(ctx, userID, roleID); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		// Any error here would be a leaked PK violation or lost upsert — the exact
		// failure the ON CONFLICT design prevents.
		t.Fatalf("concurrent AssignToUser returned an error: %v", err)
	}

	// Invariant: exactly one role, and it's a legitimate winner.
	roles, err := roleRepo.GetUserRoles(ctx, userID)
	if err != nil {
		t.Fatalf("GetUserRoles after concurrency: %v", err)
	}
	if len(roles) != 1 {
		t.Fatalf("single-role invariant violated under concurrency: got %d roles (%+v)", len(roles), roles)
	}
	if !roleNames[roles[0].Name] {
		t.Errorf("final role %q is not one of the assigned roles", roles[0].Name)
	}
}

// TestUserRepo_Create_UniqueEmail_ConcurrentOneWinner proves the email-uniqueness
// guarantee holds under a concurrent race: when many goroutines try to create the
// SAME email at once, the unique index lets EXACTLY ONE win and the rest get the
// ErrRepoEmailExists sentinel — no two rows, no panic. This is the "constraint is
// enforced by the database, not the application" property: there is no
// check-then-insert window for an attacker (or a retry storm) to exploit.
func TestUserRepo_Create_UniqueEmail_ConcurrentOneWinner(t *testing.T) {
	db := newTestDB(t)
	repo := NewUserRepo(db)
	ctx := context.Background()

	const goroutines = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	conflicts := 0

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := repo.Create(ctx, newUser("racer@example.com"))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				successes++
			case errors.Is(err, domain.ErrRepoEmailExists):
				conflicts++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if successes != 1 {
		t.Fatalf("expected exactly 1 successful create, got %d (conflicts=%d)", successes, conflicts)
	}
	if conflicts != goroutines-1 {
		t.Fatalf("expected %d conflicts, got %d", goroutines-1, conflicts)
	}
}
