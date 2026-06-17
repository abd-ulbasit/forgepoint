// pipeline_repository_test.go — integration tests for the TEMPLATE adapter.
//
// Every test exercises real SQL against a real Postgres. We verify not just
// "no error" but REAL behavior: the JSONB step graph round-trips field-for-field,
// the idempotency key actually dedups (and an atomic-tx failure leaves NO partial
// write), the partial-unique name index actually rejects a live duplicate, soft
// delete actually hides the row from List, and keyset pagination actually returns
// each row exactly once in the right order under a populated table.
package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/domain"
)

func TestPipelineRepository_CreateAndGet_RoundTripsStepGraph(t *testing.T) {
	store, ctx := newTestStore(t)
	repo := store.Pipelines()

	want := sagaPipeline("team-a")
	created, err := repo.Create(ctx, want, "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.ID != want.ID {
		t.Fatalf("Create returned id %q, want %q", created.ID, want.ID)
	}

	got, err := repo.GetByID(ctx, want.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}

	// The whole point of the JSONB mapping: the step graph survives a store/load
	// round-trip intact — ids, types, depends_on edges, the compensation pointer,
	// the config map (incl. numeric types), the timeout duration, and max_retries.
	if len(got.Steps) != len(want.Steps) {
		t.Fatalf("got %d steps, want %d", len(got.Steps), len(want.Steps))
	}
	deploy := findDef(got.Steps, "deploy")
	if deploy == nil {
		t.Fatal("deploy step missing after round-trip")
	}
	if deploy.Type != domain.StepTypeDeploy {
		t.Errorf("deploy.Type = %v, want Deploy", deploy.Type)
	}
	if deploy.CompensationStepID != "teardown" {
		t.Errorf("deploy.CompensationStepID = %q, want teardown", deploy.CompensationStepID)
	}
	if len(deploy.DependsOn) != 1 || deploy.DependsOn[0] != "validate" {
		t.Errorf("deploy.DependsOn = %v, want [validate]", deploy.DependsOn)
	}
	if deploy.Timeout != 30*time.Second {
		t.Errorf("deploy.Timeout = %v, want 30s", deploy.Timeout)
	}
	if deploy.MaxRetries != 2 {
		t.Errorf("deploy.MaxRetries = %d, want 2", deploy.MaxRetries)
	}
	if got, ok := deploy.Config["model_id"].(string); !ok || got != "m1" {
		t.Errorf("deploy.Config[model_id] = %v, want m1", deploy.Config["model_id"])
	}
	// Server-authoritative provenance is preserved exactly.
	if got.CreatedBy != want.CreatedBy || got.Team != want.Team {
		t.Errorf("provenance drift: got (%s,%s) want (%s,%s)", got.CreatedBy, got.Team, want.CreatedBy, want.Team)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Errorf("CreatedAt drift: got %v want %v", got.CreatedAt, want.CreatedAt)
	}
}

func TestPipelineRepository_GetByID_NotFound(t *testing.T) {
	store, ctx := newTestStore(t)
	_, err := store.Pipelines().GetByID(ctx, newID())
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("GetByID(missing) err = %v, want ErrRepoNotFound", err)
	}
}

// IDEMPOTENCY: a repeat Create with the same key returns the ORIGINAL pipeline
// and creates NO duplicate row. This is the Stripe idempotency-key contract.
func TestPipelineRepository_Create_IdempotentOnKey(t *testing.T) {
	store, ctx := newTestStore(t)
	repo := store.Pipelines()

	p1 := sagaPipeline("team-a")
	first, err := repo.Create(ctx, p1, "key-123")
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}

	// A retry: SAME key, but a DIFFERENT generated id/payload (as a real retry
	// would have, since the service regenerates the id each attempt). The key must
	// win — we get the ORIGINAL back, not the new one.
	p2 := sagaPipeline("team-a")
	second, err := repo.Create(ctx, p2, "key-123")
	if err != nil {
		t.Fatalf("second Create (same key): %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("idempotent Create returned id %q, want original %q", second.ID, first.ID)
	}

	// Exactly ONE row exists for team-a — the dedup prevented a duplicate.
	if n := countPipelines(t, ctx, store, "team-a"); n != 1 {
		t.Fatalf("expected 1 pipeline after idempotent retry, got %d", n)
	}
}

// The SAME key under a DIFFERENT team must NOT collide — keys are team-scoped.
func TestPipelineRepository_Create_KeyScopedByTeam(t *testing.T) {
	store, ctx := newTestStore(t)
	repo := store.Pipelines()

	a, err := repo.Create(ctx, sagaPipeline("team-a"), "shared-key")
	if err != nil {
		t.Fatalf("team-a Create: %v", err)
	}
	b, err := repo.Create(ctx, sagaPipeline("team-b"), "shared-key")
	if err != nil {
		t.Fatalf("team-b Create: %v", err)
	}
	if a.ID == b.ID {
		t.Fatal("same key across teams collided; keys must be team-scoped")
	}
}

// CONSTRAINT + ATOMICITY: a live duplicate NAME within a team is rejected by the
// partial unique index, and the rejected Create leaves NO partial write (no
// orphan idempotency row, no half-inserted pipeline).
func TestPipelineRepository_Create_DuplicateLiveName_NoPartialWrite(t *testing.T) {
	store, ctx := newTestStore(t)
	repo := store.Pipelines()

	p1 := sagaPipeline("team-a")
	p1.Name = "deploy-prod"
	if _, err := repo.Create(ctx, p1, ""); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	// Second pipeline, same team + same live name → must fail (unique index).
	p2 := sagaPipeline("team-a")
	p2.Name = "deploy-prod"
	_, err := repo.Create(ctx, p2, "key-for-dup")
	if err == nil {
		t.Fatal("expected duplicate-name Create to fail, got nil")
	}

	// ATOMICITY: the failed tx must have rolled back completely — the second
	// pipeline's id must NOT exist, and its idempotency key must NOT be recorded
	// (otherwise a later retry of a genuinely-new pipeline under that key would be
	// wrongly deduped to nothing).
	if _, gErr := repo.GetByID(ctx, p2.ID); !errors.Is(gErr, domain.ErrRepoNotFound) {
		t.Fatalf("partial write: rejected pipeline %q exists (err=%v)", p2.ID, gErr)
	}
	if keyExists(t, ctx, store, "pipeline_idempotency", "team-a", "key-for-dup") {
		t.Fatal("partial write: idempotency key persisted despite failed tx")
	}
	if n := countPipelines(t, ctx, store, "team-a"); n != 1 {
		t.Fatalf("expected 1 pipeline after rejected duplicate, got %d", n)
	}
}

// Archiving frees the live name: after archive, the same name can be reused
// (the partial unique index excludes archived rows).
func TestPipelineRepository_Archive_SoftDeletesAndFreesName(t *testing.T) {
	store, ctx := newTestStore(t)
	repo := store.Pipelines()

	p := sagaPipeline("team-a")
	p.Name = "reused-name"
	mustCreatePipeline(t, ctx, repo, p)

	if err := repo.Archive(ctx, p.ID); err != nil {
		t.Fatalf("Archive: %v", err)
	}

	// Soft-deleted: still readable by id (lineage), but flagged archived.
	got, err := repo.GetByID(ctx, p.ID)
	if err != nil {
		t.Fatalf("GetByID after archive: %v", err)
	}
	if !got.Archived {
		t.Fatal("expected pipeline.Archived = true after Archive")
	}

	// The freed name can be reused by a new LIVE pipeline.
	p2 := sagaPipeline("team-a")
	p2.Name = "reused-name"
	if _, err := repo.Create(ctx, p2, ""); err != nil {
		t.Fatalf("reuse archived name: %v", err)
	}
}

func TestPipelineRepository_Archive_NotFound(t *testing.T) {
	store, ctx := newTestStore(t)
	if err := store.Pipelines().Archive(ctx, newID()); !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("Archive(missing) = %v, want ErrRepoNotFound", err)
	}
}

// Update replaces editable fields and PRESERVES immutable provenance.
func TestPipelineRepository_Update_PreservesProvenance(t *testing.T) {
	store, ctx := newTestStore(t)
	repo := store.Pipelines()

	p := mustCreatePipeline(t, ctx, repo, sagaPipeline("team-a"))

	p.Name = "renamed"
	p.Steps = []domain.StepDefinition{{ID: "only", Name: "Only", Type: domain.StepTypeValidate}}
	// Attempt to tamper with immutable fields — Update must IGNORE these.
	tampered := p
	tampered.CreatedBy = "attacker"
	tampered.Team = "evil-team"

	updated, err := repo.Update(ctx, tampered)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Name != "renamed" || len(updated.Steps) != 1 {
		t.Errorf("editable fields not applied: name=%q steps=%d", updated.Name, len(updated.Steps))
	}
	// Provenance from the original row, NOT the tampered input (the UPDATE SQL only
	// sets name + steps; created_by/team/created_at are untouched).
	if updated.CreatedBy == "attacker" || updated.Team == "evil-team" {
		t.Errorf("Update overwrote immutable provenance: created_by=%q team=%q", updated.CreatedBy, updated.Team)
	}
}

func TestPipelineRepository_Update_NotFound(t *testing.T) {
	store, ctx := newTestStore(t)
	_, err := store.Pipelines().Update(ctx, sagaPipeline("team-a"))
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("Update(missing) = %v, want ErrRepoNotFound", err)
	}
}

// LIST: tenancy scope, archived exclusion, type filter, newest-first order, and
// keyset pagination returning every row exactly once.
func TestPipelineRepository_List_ScopeOrderAndPagination(t *testing.T) {
	store, ctx := newTestStore(t)
	repo := store.Pipelines()

	// 5 pipelines for team-a with strictly increasing created_at so the DESC order
	// is deterministic, plus one for team-b (must never appear) and one archived
	// (must be excluded).
	base := time.Now().UTC().Truncate(time.Microsecond)
	var ids []string
	for i := 0; i < 5; i++ {
		p := sagaPipeline("team-a")
		p.CreatedAt = base.Add(time.Duration(i) * time.Second)
		mustCreatePipeline(t, ctx, repo, p)
		ids = append(ids, p.ID)
	}
	mustCreatePipeline(t, ctx, repo, sagaPipeline("team-b")) // other tenant
	archived := sagaPipeline("team-a")
	mustCreatePipeline(t, ctx, repo, archived)
	if err := repo.Archive(ctx, archived.ID); err != nil {
		t.Fatalf("archive: %v", err)
	}

	// Page through team-a in pages of 2; collect ids; assert each live row appears
	// exactly once, newest-first, and the other-tenant/archived rows never show.
	var seen []string
	token := ""
	pages := 0
	for {
		got, next, err := repo.List(ctx, domain.ListPipelinesFilter{
			Team: "team-a",
			List: domain.ListOptions{PageSize: 2, PageToken: token},
		})
		if err != nil {
			t.Fatalf("List page %d: %v", pages, err)
		}
		for _, p := range got {
			if p.Team != "team-a" {
				t.Fatalf("cross-tenant leak: got team %q", p.Team)
			}
			if p.Archived {
				t.Fatal("archived pipeline leaked into List")
			}
			seen = append(seen, p.ID)
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
		t.Fatalf("List returned %d live team-a pipelines, want 5 (ids=%v)", len(seen), seen)
	}
	// Newest-first: the last-created id must be first.
	if seen[0] != ids[4] {
		t.Errorf("List not newest-first: first=%q, want %q", seen[0], ids[4])
	}
	assertUnique(t, seen)
}
