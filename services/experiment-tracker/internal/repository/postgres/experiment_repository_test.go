package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/domain"
)

// TestExperimentCreateAndGet verifies a created experiment round-trips through
// Postgres byte-for-byte at the domain level — including the tags JSONB and the
// nil ArchivedAt (active) marker. This is the "stored data is REAL, not a mock
// echo" check.
func TestExperimentCreateAndGet(t *testing.T) {
	s, ctx := newTestStore(t)
	repo := s.Experiments()

	exp := newExperiment("team-a", "fraud-v3")
	exp.Tags = map[string]string{"env": "prod", "owner": "ml-team"}

	created, err := repo.Create(ctx, exp)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.ID != exp.ID {
		t.Fatalf("create returned id %q, want %q", created.ID, exp.ID)
	}

	got, err := repo.GetByID(ctx, exp.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != "fraud-v3" || got.Team != "team-a" || got.OwnerID != exp.OwnerID {
		t.Fatalf("identity mismatch: %+v", got)
	}
	if got.IsArchived() {
		t.Fatalf("fresh experiment should be active, got ArchivedAt=%v", got.ArchivedAt)
	}
	if len(got.Tags) != 2 || got.Tags["env"] != "prod" || got.Tags["owner"] != "ml-team" {
		t.Fatalf("tags JSONB did not round-trip: %#v", got.Tags)
	}
	if !got.CreatedAt.Equal(exp.CreatedAt) {
		t.Fatalf("created_at mismatch: got %v want %v", got.CreatedAt, exp.CreatedAt)
	}
}

// TestExperimentGetNotFound: a missing id yields the storage sentinel
// ErrRepoNotFound (which the service maps to ErrExperimentNotFound).
func TestExperimentGetNotFound(t *testing.T) {
	s, ctx := newTestStore(t)
	_, err := s.Experiments().GetByID(ctx, newID())
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("want ErrRepoNotFound, got %v", err)
	}
}

// TestExperimentNameUniquePerTeam: the partial unique index rejects a second
// ACTIVE (team, name) with ErrRepoConflict — the storage form of
// ErrExperimentNameExists.
func TestExperimentNameUniquePerTeam(t *testing.T) {
	s, ctx := newTestStore(t)
	repo := s.Experiments()

	mustCreateExperiment(t, ctx, repo, newExperiment("team-a", "dup"))

	_, err := repo.Create(ctx, newExperiment("team-a", "dup"))
	if !errors.Is(err, domain.ErrRepoConflict) {
		t.Fatalf("want ErrRepoConflict on duplicate name, got %v", err)
	}

	// Same NAME but a DIFFERENT team is allowed — tenancy isolates the namespace.
	if _, err := repo.Create(ctx, newExperiment("team-b", "dup")); err != nil {
		t.Fatalf("same name in another team should succeed, got %v", err)
	}
}

// TestExperimentArchiveFreesName: archiving an experiment (Update with ArchivedAt
// set) lets the SAME name be reused by a fresh active experiment — the precise
// reason the unique index is PARTIAL (WHERE archived_at IS NULL).
func TestExperimentArchiveFreesName(t *testing.T) {
	s, ctx := newTestStore(t)
	repo := s.Experiments()

	first := mustCreateExperiment(t, ctx, repo, newExperiment("team-a", "reuse"))

	// Before archive, the name is taken.
	if _, err := repo.Create(ctx, newExperiment("team-a", "reuse")); !errors.Is(err, domain.ErrRepoConflict) {
		t.Fatalf("name should be taken while active, got %v", err)
	}

	// Archive the first (soft delete via Update with ArchivedAt).
	now := tnow()
	first.ArchivedAt = ptr(now)
	first.UpdatedAt = now
	archived, err := repo.Update(ctx, first)
	if err != nil {
		t.Fatalf("archive update: %v", err)
	}
	if !archived.IsArchived() {
		t.Fatalf("expected archived, got %+v", archived)
	}

	// Now the name is free for a new active experiment.
	if _, err := repo.Create(ctx, newExperiment("team-a", "reuse")); err != nil {
		t.Fatalf("name should be reusable after archive, got %v", err)
	}
}

// TestExperimentUpdateFields: name/description/tags edits persist and tags are
// REPLACED (not merged), proving the wholesale tags write.
func TestExperimentUpdateFields(t *testing.T) {
	s, ctx := newTestStore(t)
	repo := s.Experiments()

	exp := mustCreateExperiment(t, ctx, repo, newExperiment("team-a", "orig"))
	exp.Name = "renamed"
	exp.Description = "new desc"
	exp.Tags = map[string]string{"only": "one"} // replaces {"env":"test"}
	exp.UpdatedAt = tnow()

	updated, err := repo.Update(ctx, exp)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.Name != "renamed" || updated.Description != "new desc" {
		t.Fatalf("update did not persist scalar fields: %+v", updated)
	}
	if len(updated.Tags) != 1 || updated.Tags["only"] != "one" {
		t.Fatalf("tags were not replaced wholesale: %#v", updated.Tags)
	}
	// Re-read confirms durability (not just the RETURNING echo).
	got, _ := repo.GetByID(ctx, exp.ID)
	if got.Name != "renamed" || len(got.Tags) != 1 {
		t.Fatalf("re-read mismatch: %+v", got)
	}
}

// TestExperimentUpdateNotFound: updating a vanished row yields ErrRepoNotFound
// (RETURNING produces no row → no-rows → mapped sentinel).
func TestExperimentUpdateNotFound(t *testing.T) {
	s, ctx := newTestStore(t)
	ghost := newExperiment("team-a", "ghost") // never inserted
	_, err := s.Experiments().Update(ctx, ghost)
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("want ErrRepoNotFound updating missing row, got %v", err)
	}
}

// TestExperimentListPaginationAndArchiveFilter exercises keyset pagination
// (stable newest-first ordering, exact page boundaries via the cursor) AND the
// includeArchived toggle. This is the pattern's persistence semantics: a page is
// a clean index scan and the cursor positions WITHIN the team scope.
func TestExperimentListPaginationAndArchiveFilter(t *testing.T) {
	s, ctx := newTestStore(t)
	repo := s.Experiments()

	// Create 5 experiments with strictly increasing created_at so the DESC order
	// is deterministic (newest first = e5, e4, e3, e2, e1).
	const team = "team-pg"
	var created []domain.Experiment
	base := tnow()
	for i := 0; i < 5; i++ {
		e := newExperiment(team, "exp-"+uuid.NewString()[:8])
		e.CreatedAt = base.Add(time.Duration(i) * time.Second)
		e.UpdatedAt = e.CreatedAt
		created = append(created, mustCreateExperiment(t, ctx, repo, e))
	}

	// Page size 2 → expect pages [e5,e4], [e3,e2], [e1], then empty.
	var seen []string
	token := ""
	pages := 0
	for {
		page, next, err := repo.List(ctx, team, false, domain.ListOptions{PageSize: 2, PageToken: token})
		if err != nil {
			t.Fatalf("list page: %v", err)
		}
		for _, e := range page {
			seen = append(seen, e.ID)
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
	if len(seen) != 5 {
		t.Fatalf("expected to page through 5 experiments, saw %d (%v)", len(seen), seen)
	}
	// Verify newest-first order: seen[0] must be the last-created.
	if seen[0] != created[4].ID || seen[4] != created[0].ID {
		t.Fatalf("ordering wrong: got %v", seen)
	}
	// No duplicates across pages (the keyset boundary is exact).
	uniq := map[string]bool{}
	for _, id := range seen {
		if uniq[id] {
			t.Fatalf("duplicate id %q across pages", id)
		}
		uniq[id] = true
	}

	// Archive e1; default list (includeArchived=false) now returns 4, the
	// includeArchived=true list still returns 5.
	e1 := created[0]
	e1.ArchivedAt = ptr(tnow())
	e1.UpdatedAt = tnow()
	if _, err := repo.Update(ctx, e1); err != nil {
		t.Fatalf("archive: %v", err)
	}
	active, _, err := repo.List(ctx, team, false, domain.ListOptions{PageSize: 100})
	if err != nil {
		t.Fatalf("list active: %v", err)
	}
	if len(active) != 4 {
		t.Fatalf("includeArchived=false should hide the archived row, got %d", len(active))
	}
	all, _, err := repo.List(ctx, team, true, domain.ListOptions{PageSize: 100})
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 5 {
		t.Fatalf("includeArchived=true should show all 5, got %d", len(all))
	}
}

// TestExperimentListTeamScoped: List never returns another team's rows — the
// anti-IDOR boundary at the storage level.
func TestExperimentListTeamScoped(t *testing.T) {
	s, ctx := newTestStore(t)
	repo := s.Experiments()
	mustCreateExperiment(t, ctx, repo, newExperiment("team-a", "a1"))
	mustCreateExperiment(t, ctx, repo, newExperiment("team-b", "b1"))

	page, _, err := repo.List(ctx, "team-a", false, domain.ListOptions{PageSize: 100})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page) != 1 || page[0].Team != "team-a" {
		t.Fatalf("team scope leaked: %+v", page)
	}
}
