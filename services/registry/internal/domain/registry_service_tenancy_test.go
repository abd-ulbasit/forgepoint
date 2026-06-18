// registry_service_tenancy_test.go — UNIT tests for the cross-tenant security fixes
// (BUG A: version MUTATION; BUG B: version READ/IDOR) and the read-model count fix
// (BUG D), all with in-memory fakes (no testcontainers).
//
// ============================================================================
// WHAT THESE PROVE (the security contract, not mock-call counts)
// ============================================================================
//
//	BUG A — PromoteVersion / MarkVersionReady fetch a version by its GLOBAL id (the
//	  unscoped write-store lookup) and then MUTATE it. Without a team gate, a JWT from
//	  another team could promote/demote/confirm a version it does not own. The fix
//	  re-applies loadOwnedModel after the fetch; these tests prove a cross-team caller
//	  gets ErrVersionNotFound (indistinguishable from "no such version") and the stored
//	  state is UNCHANGED, while the OWNING team still succeeds.
//
//	BUG B — GetVersion / ListVersions read a version by global id/model_id and dropped
//	  the actor's team, so any team could read another team's version metadata. The fix
//	  gates on the version's parent model through the TEAM-SCOPED read path; these tests
//	  prove a cross-team read is ErrVersionNotFound while the owner reads it fine.
//
//	BUG D — the read model now maintains an accurate model COUNT, and ListModels returns
//	  it as Page.Total (→ proto total_count → the dashboard tile). The test proves the
//	  Total the service returns equals the number of models the list would show.
package domain

import (
	"context"
	"errors"
	"testing"
	"time"
)

// ============================================================================
// fakeReadStore — an in-memory domain.ReadStore that ENFORCES team scoping exactly
// like the production Redis adapter (GetModelByID returns not-found across tenants),
// stores versions, and computes CountModels through the same filter ListModels uses.
// Using a real fake (not stubbed returns) is what lets these tests assert the genuine
// cross-tenant behavior end-to-end through the service.
// ============================================================================

type fakeReadStore struct {
	models   map[string]Model        // id -> projected model
	versions map[string]ModelVersion // id -> projected version
}

func newFakeReadStore() *fakeReadStore {
	return &fakeReadStore{models: map[string]Model{}, versions: map[string]ModelVersion{}}
}

func (r *fakeReadStore) putModel(m Model)     { r.models[m.ID] = m }
func (r *fakeReadStore) putVersion(v ModelVersion) { r.versions[v.ID] = v }

// GetModelByID mirrors the Redis adapter: load the model, then enforce the team gate —
// a model owned by another team is reported as not-found (anti cross-tenant enumeration).
func (r *fakeReadStore) GetModelByID(_ context.Context, team, id string) (Model, error) {
	m, ok := r.models[id]
	if !ok || m.Team != team {
		return Model{}, ErrRecordNotFound
	}
	return m, nil
}

func (r *fakeReadStore) GetModelByName(_ context.Context, team, name string) (Model, error) {
	for _, m := range r.models {
		if m.Team == team && m.Name == name {
			return m, nil
		}
	}
	return Model{}, ErrRecordNotFound
}

func (r *fakeReadStore) ListModels(_ context.Context, team string, f ListModelsFilter, _ ListOptions) ([]Model, string, error) {
	var out []Model
	for _, m := range r.models {
		if m.Team == team && readModelMatchesFilter(m, f) {
			out = append(out, m)
		}
	}
	return out, "", nil
}

// CountModels counts via the SAME predicate ListModels uses, so the count can never
// disagree with the list (the heart of the BUG D fix).
func (r *fakeReadStore) CountModels(_ context.Context, team string, f ListModelsFilter) (int, error) {
	n := 0
	for _, m := range r.models {
		if m.Team == team && readModelMatchesFilter(m, f) {
			n++
		}
	}
	return n, nil
}

// GetVersionByID / GetVersionByLabel are UNSCOPED (the version entity carries no team)
// — exactly the surface the service must gate via the parent model. The fake returns
// the version regardless of caller team; the SERVICE is responsible for the tenancy
// gate, which is precisely what BUG B's tests exercise.
func (r *fakeReadStore) GetVersionByID(_ context.Context, id string) (ModelVersion, error) {
	v, ok := r.versions[id]
	if !ok {
		return ModelVersion{}, ErrRecordNotFound
	}
	return v, nil
}

func (r *fakeReadStore) GetVersionByLabel(_ context.Context, modelID, label string) (ModelVersion, error) {
	for _, v := range r.versions {
		if v.ModelID == modelID && v.Version == label {
			return v, nil
		}
	}
	return ModelVersion{}, ErrRecordNotFound
}

func (r *fakeReadStore) ListVersions(_ context.Context, modelID string, stage ModelStage, _ ListOptions) ([]ModelVersion, string, error) {
	var out []ModelVersion
	for _, v := range r.versions {
		if v.ModelID == modelID && (stage == StageUnspecified || v.Stage == stage) {
			out = append(out, v)
		}
	}
	return out, "", nil
}

// readModelMatchesFilter mirrors the adapter's filter (archived hidden by default,
// task_type/framework narrowing) so the fake's list and count agree with production.
func readModelMatchesFilter(m Model, f ListModelsFilter) bool {
	if !f.IncludeArchived && m.IsArchived() {
		return false
	}
	if f.TaskType != "" && m.TaskType != f.TaskType {
		return false
	}
	if f.Framework != "" && m.Framework != f.Framework {
		return false
	}
	return true
}

// ----------------------------------------------------------------------------
// tenancyHarness wires a service over a REAL in-memory write store AND a REAL
// in-memory read store, so a command's write-side team gate and a query's read-side
// team gate are both exercised against true storage semantics.
// ----------------------------------------------------------------------------

type tenancyHarness struct {
	svc   RegistryService
	write *fakeWriteStore
	read  *fakeReadStore
	now   time.Time
}

func newTenancyHarness() *tenancyHarness {
	now := time.Date(2026, 6, 18, 9, 0, 0, 0, time.UTC)
	write := newFakeWriteStore()
	read := newFakeReadStore()
	svc := NewRegistryService(write, read, &recordingEmitter{}, fixedClock{t: now}, &seqIDGen{})
	return &tenancyHarness{svc: svc, write: write, read: read, now: now}
}

// seedReadyVersion creates (in the WRITE store, the truth) a model owned by `team`
// with one READY version at the given stage, and mirrors both into the READ store so
// the query paths can find them. Returns the model + version.
func (h *tenancyHarness) seedReadyVersion(t *testing.T, team, owner, name string, stage ModelStage) (Model, ModelVersion) {
	t.Helper()
	actor := Actor{UserID: owner, Team: team}
	m, err := h.svc.RegisterModel(context.Background(), actor, RegisterModelInput{Name: name})
	if err != nil {
		t.Fatalf("seed RegisterModel(%s/%s): %v", team, name, err)
	}
	v, err := h.svc.CreateVersion(context.Background(), actor, CreateVersionInput{ModelID: m.ID})
	if err != nil {
		t.Fatalf("seed CreateVersion: %v", err)
	}
	rv, err := h.svc.MarkVersionReady(context.Background(), actor, MarkVersionReadyInput{
		VersionID: v.ID, Success: true, ArtifactDigest: "sha256:" + v.ID, SizeBytes: 1,
	})
	if err != nil {
		t.Fatalf("seed MarkVersionReady: %v", err)
	}
	if stage != StageDev {
		// Walk DEV→STAGING(→PRODUCTION) as the owner so the version reaches `stage`.
		if _, err := h.svc.PromoteVersion(context.Background(), actor, PromoteVersionInput{VersionID: v.ID, TargetStage: StageStaging}); err != nil {
			t.Fatalf("seed promote→STAGING: %v", err)
		}
		if stage == StageProduction {
			if _, err := h.svc.PromoteVersion(context.Background(), actor, PromoteVersionInput{VersionID: v.ID, TargetStage: StageProduction}); err != nil {
				t.Fatalf("seed promote→PRODUCTION: %v", err)
			}
		}
		rv, _ = h.write.GetVersion(context.Background(), v.ID)
	}
	// Mirror the truth into the read projection (the test plays the projection's role).
	mm, _ := h.write.GetModel(context.Background(), m.ID)
	h.read.putModel(mm)
	h.read.putVersion(rv)
	return mm, rv
}

var (
	teamA = Actor{UserID: "alice", Team: "team-a"}
	teamB = Actor{UserID: "bob", Team: "team-b"}
)

// ============================================================================
// BUG A — cross-tenant version MUTATION (PromoteVersion / MarkVersionReady).
// ============================================================================

func TestPromoteVersion_CrossTeamRejectedAsNotFound(t *testing.T) {
	h := newTenancyHarness()
	// team-a owns a READY version sitting in STAGING (so a promote→PRODUCTION would be
	// a legal transition IF the caller were allowed — proving it's the TEAM gate, not
	// the state machine, that rejects the cross-team caller).
	_, vA := h.seedReadyVersion(t, teamA.Team, teamA.UserID, "fraud", StageStaging)

	// team-b attempts to promote team-a's version to PRODUCTION.
	_, err := h.svc.PromoteVersion(context.Background(), teamB, PromoteVersionInput{
		VersionID: vA.ID, TargetStage: StageProduction,
	})
	if !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("cross-team promote: got %v want ErrVersionNotFound (indistinguishable from absent)", err)
	}
	// The stored version must be UNCHANGED — still STAGING, not promoted.
	if stored, _ := h.write.GetVersion(context.Background(), vA.ID); stored.Stage != StageStaging {
		t.Errorf("cross-team promote mutated another team's version: stage now %v", stored.Stage)
	}

	// The OWNING team can still promote it — the gate blocks only the intruder.
	res, err := h.svc.PromoteVersion(context.Background(), teamA, PromoteVersionInput{
		VersionID: vA.ID, TargetStage: StageProduction,
	})
	if err != nil {
		t.Fatalf("owner promote should succeed: %v", err)
	}
	if res.Promoted.Stage != StageProduction {
		t.Errorf("owner promote: got stage %v want PRODUCTION", res.Promoted.Stage)
	}
}

// A cross-team promote must NOT be able to demote another team's production version
// either — the single-production swap is the most dangerous cross-tenant effect.
func TestPromoteVersion_CrossTeamCannotDemoteAnotherTeamsProd(t *testing.T) {
	h := newTenancyHarness()
	_, prodA := h.seedReadyVersion(t, teamA.Team, teamA.UserID, "ranker", StageProduction)

	// team-b seeds its OWN ready version, then tries to promote it into team-a's model
	// id is not possible (versions are bound to their model); instead team-b tries to
	// ARCHIVE team-a's prod version via promote→ARCHIVED. Must be rejected, prod intact.
	_, err := h.svc.PromoteVersion(context.Background(), teamB, PromoteVersionInput{
		VersionID: prodA.ID, TargetStage: StageArchived,
	})
	if !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("cross-team archive-promote: got %v want ErrVersionNotFound", err)
	}
	if stored, _ := h.write.GetVersion(context.Background(), prodA.ID); stored.Stage != StageProduction {
		t.Errorf("cross-team caller demoted another team's PRODUCTION version: now %v", stored.Stage)
	}
}

func TestMarkVersionReady_CrossTeamRejectedAsNotFound(t *testing.T) {
	h := newTenancyHarness()
	// team-a creates a PENDING_UPLOAD version (not yet ready).
	m, err := h.svc.RegisterModel(context.Background(), teamA, RegisterModelInput{Name: "img-clf"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	v, err := h.svc.CreateVersion(context.Background(), teamA, CreateVersionInput{ModelID: m.ID})
	if err != nil {
		t.Fatalf("create version: %v", err)
	}

	// team-b tries to confirm team-a's upload as READY with a forged digest.
	_, err = h.svc.MarkVersionReady(context.Background(), teamB, MarkVersionReadyInput{
		VersionID: v.ID, Success: true, ArtifactDigest: "sha256:forged", SizeBytes: 9,
	})
	if !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("cross-team confirm: got %v want ErrVersionNotFound", err)
	}
	// The version must still be PENDING_UPLOAD with no forged artifact facts.
	stored, _ := h.write.GetVersion(context.Background(), v.ID)
	if stored.Status != StatusPendingUpload || stored.ArtifactDigest != "" {
		t.Errorf("cross-team confirm mutated another team's version: %+v", stored)
	}

	// The owner can confirm it normally.
	got, err := h.svc.MarkVersionReady(context.Background(), teamA, MarkVersionReadyInput{
		VersionID: v.ID, Success: true, ArtifactDigest: "sha256:real", SizeBytes: 9,
	})
	if err != nil {
		t.Fatalf("owner confirm should succeed: %v", err)
	}
	if got.Status != StatusReady || got.ArtifactDigest != "sha256:real" {
		t.Errorf("owner confirm wrong result: %+v", got)
	}
}

// ============================================================================
// BUG B — cross-tenant version READ (GetVersion / ListVersions).
// ============================================================================

func TestGetVersion_CrossTeamRejectedAsNotFound(t *testing.T) {
	h := newTenancyHarness()
	mA, vA := h.seedReadyVersion(t, teamA.Team, teamA.UserID, "spam", StageDev)

	// team-b reads team-a's version BY ID — must be not-found (no metadata leak).
	if _, err := h.svc.GetVersion(context.Background(), teamB, vA.ID, "", ""); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("cross-team GetVersion by id: got %v want ErrVersionNotFound", err)
	}
	// And BY (model_id, label) — same gate, same not-found.
	if _, err := h.svc.GetVersion(context.Background(), teamB, "", mA.ID, vA.Version); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("cross-team GetVersion by label: got %v want ErrVersionNotFound", err)
	}

	// The OWNING team reads it fine — the gate blocks only the intruder.
	got, err := h.svc.GetVersion(context.Background(), teamA, vA.ID, "", "")
	if err != nil {
		t.Fatalf("owner GetVersion should succeed: %v", err)
	}
	if got.ID != vA.ID {
		t.Errorf("owner GetVersion returned wrong version: %q want %q", got.ID, vA.ID)
	}
}

func TestListVersions_CrossTeamRejectedAsNotFound(t *testing.T) {
	h := newTenancyHarness()
	mA, _ := h.seedReadyVersion(t, teamA.Team, teamA.UserID, "nlp", StageDev)

	// team-b lists team-a's model's versions — must be not-found, never the rows.
	if _, err := h.svc.ListVersions(context.Background(), teamB, mA.ID, StageUnspecified, 50, ""); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("cross-team ListVersions: got %v want ErrVersionNotFound", err)
	}

	// The owner lists them and sees the version.
	page, err := h.svc.ListVersions(context.Background(), teamA, mA.ID, StageUnspecified, 50, "")
	if err != nil {
		t.Fatalf("owner ListVersions should succeed: %v", err)
	}
	if len(page.Items) != 1 {
		t.Errorf("owner ListVersions: got %d versions want 1", len(page.Items))
	}
}

// ============================================================================
// BUG D — read-model count matches the list.
// ============================================================================

func TestListModels_TotalMatchesList(t *testing.T) {
	h := newTenancyHarness()
	// Seed three ACTIVE models for team-a and one for team-b (must NOT be counted in
	// team-a's total — the count is team-scoped).
	for _, name := range []string{"m1", "m2", "m3"} {
		mm, err := h.svc.RegisterModel(context.Background(), teamA, RegisterModelInput{Name: name})
		if err != nil {
			t.Fatalf("register %s: %v", name, err)
		}
		full, _ := h.write.GetModel(context.Background(), mm.ID)
		h.read.putModel(full)
	}
	other, _ := h.svc.RegisterModel(context.Background(), teamB, RegisterModelInput{Name: "x"})
	bfull, _ := h.write.GetModel(context.Background(), other.ID)
	h.read.putModel(bfull)

	// The dashboard fetches with a tiny page (page_size=1) but reads Total. Total must
	// be the FULL team-a count (3), not the page length (≤1) — the exact BUG D symptom.
	page, err := h.svc.ListModels(context.Background(), teamA, ListModelsInput{PageSize: 1})
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if page.Total != 3 {
		t.Errorf("Total: got %d want 3 (team-scoped active models)", page.Total)
	}

	// A full-page list returns all three items, and the count agrees with that length.
	full, err := h.svc.ListModels(context.Background(), teamA, ListModelsInput{PageSize: 100})
	if err != nil {
		t.Fatalf("ListModels(full): %v", err)
	}
	if len(full.Items) != full.Total {
		t.Errorf("count disagrees with list: Total=%d len(Items)=%d", full.Total, len(full.Items))
	}
	if full.Total != 3 {
		t.Errorf("full list Total: got %d want 3", full.Total)
	}
}

// An archived model is excluded from the count just as it is from the default list —
// proving the count uses the SAME filter the list does (the disagreement's root cause).
func TestListModels_TotalExcludesArchivedByDefault(t *testing.T) {
	h := newTenancyHarness()
	var ids []string
	for _, name := range []string{"a", "b"} {
		mm, err := h.svc.RegisterModel(context.Background(), teamA, RegisterModelInput{Name: name})
		if err != nil {
			t.Fatalf("register %s: %v", name, err)
		}
		ids = append(ids, mm.ID)
	}
	// Archive one, and mirror BOTH (archived + active) into the read projection.
	if _, err := h.svc.ArchiveModel(context.Background(), teamA, ids[0], ""); err != nil {
		t.Fatalf("archive: %v", err)
	}
	for _, id := range ids {
		full, _ := h.write.GetModel(context.Background(), id)
		h.read.putModel(full)
	}

	// Default (archived hidden): count and list both see ONLY the active model.
	page, err := h.svc.ListModels(context.Background(), teamA, ListModelsInput{PageSize: 100})
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 {
		t.Errorf("default scope: Total=%d items=%d want 1/1 (archived excluded)", page.Total, len(page.Items))
	}

	// IncludeArchived: both rise to 2, in lockstep.
	all, err := h.svc.ListModels(context.Background(), teamA, ListModelsInput{
		Filter: ListModelsFilter{IncludeArchived: true}, PageSize: 100,
	})
	if err != nil {
		t.Fatalf("ListModels(includeArchived): %v", err)
	}
	if all.Total != 2 || len(all.Items) != 2 {
		t.Errorf("includeArchived: Total=%d items=%d want 2/2", all.Total, len(all.Items))
	}
}
