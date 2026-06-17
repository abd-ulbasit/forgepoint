// read_store_test.go — integration tests for the Redis ReadStore (CQRS read projection).
//
// ============================================================================
// THESE TESTS RUN AGAINST A REAL REDIS (testcontainers), NOT A MOCK
// ============================================================================
//
// The read side's behavior — newest-first ZSET ordering, cursor pagination that is
// stable across same-timestamp ties, team-scoped name indexes, HGETALL round-tripping
// the full entity, stage filtering — is exactly the kind of thing a mock would fake and
// get subtly wrong. So these tests drive the adapter against a real redis:7 container
// from pkg/testutil.StartRedis.
//
// THE TEST PLAYS THE PROJECTION CONSUMER: in production a NATS consumer calls
// UpsertModel/UpsertVersion to (re)build the projection from events. Here the test calls
// them directly to SEED the projection, then asserts the read-after-write the queries
// observe — i.e. it exercises the full CQRS read path end to end.
package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/domain"
)

// newTestReadStore spins up Redis and returns a ReadStore over a shared client.
func newTestReadStore(t *testing.T) *ReadStore {
	t.Helper()
	testutil.SkipIfNoDocker(t)

	ctx := context.Background()
	addr := testutil.StartRedis(t) // returns a redis:// URL

	opt, err := goredis.ParseURL(addr)
	if err != nil {
		t.Fatalf("parse redis url %q: %v", addr, err)
	}
	rdb := goredis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}
	return NewReadStoreFromClient(rdb)
}

// projModel builds a projected model with a controllable created_at (so list ORDER is
// deterministic — the projection uses created_at as the ZSET score).
func projModel(id, team, name string, createdAt time.Time) domain.Model {
	return domain.Model{
		ID:        id,
		Name:      name,
		Team:      team,
		OwnerID:   "user-1",
		Framework: "pytorch",
		TaskType:  "classification",
		Tags:      map[string]string{"env": "prod"},
		CreatedAt: createdAt,
		UpdatedAt: createdAt,
	}
}

func projVersion(id, modelID, label string, stage domain.ModelStage, createdAt time.Time) domain.ModelVersion {
	return domain.ModelVersion{
		ID:        id,
		ModelID:   modelID,
		Version:   label,
		Stage:     stage,
		Status:    domain.StatusReady,
		Metrics:   map[string]float64{"accuracy": 0.9},
		CreatedBy: "user-1",
		CreatedAt: createdAt,
	}
}

// ----------------------------------------------------------------------------
// MODEL READ-AFTER-WRITE: id / name / team scoping
// ----------------------------------------------------------------------------

func TestUpsertAndGetModel_RoundTrips(t *testing.T) {
	store := newTestReadStore(t)
	ctx := context.Background()

	m := projModel("m-1", "acme", "fraud", time.Unix(1000, 0).UTC())
	if err := store.UpsertModel(ctx, m); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	// By id (team-scoped).
	got, err := store.GetModelByID(ctx, "acme", "m-1")
	if err != nil {
		t.Fatalf("GetModelByID: %v", err)
	}
	if got.Name != "fraud" || got.Team != "acme" || got.TaskType != "classification" {
		t.Fatalf("model fields mismatch: %+v", got)
	}
	if got.Tags["env"] != "prod" {
		t.Fatalf("tags did not round-trip: %#v", got.Tags)
	}
	if !got.CreatedAt.Equal(time.Unix(1000, 0).UTC()) {
		t.Fatalf("created_at round-trip: got %v", got.CreatedAt)
	}

	// By name (team-scoped name index).
	byName, err := store.GetModelByName(ctx, "acme", "fraud")
	if err != nil || byName.ID != "m-1" {
		t.Fatalf("GetModelByName = %+v, %v; want m-1", byName, err)
	}
}

func TestGetModel_TeamScoping_CrossTenantIsNotFound(t *testing.T) {
	store := newTestReadStore(t)
	ctx := context.Background()
	if err := store.UpsertModel(ctx, projModel("m-1", "acme", "fraud", time.Unix(1, 0).UTC())); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	// A DIFFERENT team must not be able to read acme's model by id — even with the id.
	if _, err := store.GetModelByID(ctx, "intruder", "m-1"); err != domain.ErrRecordNotFound {
		t.Fatalf("cross-team id read: got %v, want ErrRecordNotFound", err)
	}
	// And the name index is team-scoped — intruder's namespace has no "fraud".
	if _, err := store.GetModelByName(ctx, "intruder", "fraud"); err != domain.ErrRecordNotFound {
		t.Fatalf("cross-team name read: got %v, want ErrRecordNotFound", err)
	}
}

func TestGetModel_Miss_NotFound(t *testing.T) {
	store := newTestReadStore(t)
	if _, err := store.GetModelByID(context.Background(), "acme", "ghost"); err != domain.ErrRecordNotFound {
		t.Fatalf("missing model: got %v, want ErrRecordNotFound", err)
	}
}

// ----------------------------------------------------------------------------
// LIST MODELS: newest-first ordering, filtering, archived hiding
// ----------------------------------------------------------------------------

func TestListModels_NewestFirstAndFilters(t *testing.T) {
	store := newTestReadStore(t)
	ctx := context.Background()

	base := time.Unix(2000, 0).UTC()
	// Insert out of chronological order to prove the ZSET sorts by score, not insert order.
	mNew := projModel("m-new", "acme", "new", base.Add(2*time.Hour))
	mMid := projModel("m-mid", "acme", "mid", base.Add(1*time.Hour))
	mOld := projModel("m-old", "acme", "old", base)
	mMid.Framework = "onnx" // for the framework filter test
	for _, m := range []domain.Model{mOld, mNew, mMid} {
		if err := store.UpsertModel(ctx, m); err != nil {
			t.Fatalf("upsert %s: %v", m.ID, err)
		}
	}

	// Default list: newest-first, all three.
	got, next, err := store.ListModels(ctx, "acme", domain.ListModelsFilter{}, domain.ListOptions{PageSize: 10})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if next != "" {
		t.Fatalf("expected no next token for a full single page, got %q", next)
	}
	wantOrder := []string{"m-new", "m-mid", "m-old"}
	assertModelOrder(t, got, wantOrder)

	// Framework filter narrows to the one onnx model.
	got, _, err = store.ListModels(ctx, "acme", domain.ListModelsFilter{Framework: "onnx"}, domain.ListOptions{PageSize: 10})
	if err != nil {
		t.Fatalf("filtered list: %v", err)
	}
	assertModelOrder(t, got, []string{"m-mid"})
}

func TestListModels_HidesArchivedByDefault(t *testing.T) {
	store := newTestReadStore(t)
	ctx := context.Background()
	base := time.Unix(3000, 0).UTC()

	active := projModel("m-active", "acme", "active", base.Add(time.Hour))
	archived := projModel("m-arch", "acme", "arch", base)
	archived.ArchivedAt = base.Add(30 * time.Minute) // soft-deleted
	for _, m := range []domain.Model{active, archived} {
		if err := store.UpsertModel(ctx, m); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}

	// Default hides archived.
	got, _, _ := store.ListModels(ctx, "acme", domain.ListModelsFilter{}, domain.ListOptions{PageSize: 10})
	assertModelOrder(t, got, []string{"m-active"})

	// IncludeArchived surfaces both (newest-first).
	got, _, _ = store.ListModels(ctx, "acme", domain.ListModelsFilter{IncludeArchived: true}, domain.ListOptions{PageSize: 10})
	assertModelOrder(t, got, []string{"m-active", "m-arch"})
}

func TestListModels_CursorPagination_NoOverlapNoGap(t *testing.T) {
	store := newTestReadStore(t)
	ctx := context.Background()
	base := time.Unix(4000, 0).UTC()

	// 5 models, distinct timestamps. Page size 2 → pages of [4,3],[2,1],[0].
	const n = 5
	for i := 0; i < n; i++ {
		m := projModel(fmt.Sprintf("m-%d", i), "acme", fmt.Sprintf("name-%d", i), base.Add(time.Duration(i)*time.Minute))
		if err := store.UpsertModel(ctx, m); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}

	var seen []string
	token := ""
	pages := 0
	for {
		got, next, err := store.ListModels(ctx, "acme", domain.ListModelsFilter{}, domain.ListOptions{PageSize: 2, PageToken: token})
		if err != nil {
			t.Fatalf("page %d: %v", pages, err)
		}
		for _, m := range got {
			seen = append(seen, m.ID)
		}
		pages++
		if next == "" {
			break
		}
		token = next
		if pages > 10 {
			t.Fatalf("pagination did not terminate")
		}
	}
	// Every model exactly once, newest-first, no duplicates, no gaps.
	want := []string{"m-4", "m-3", "m-2", "m-1", "m-0"}
	if len(seen) != len(want) {
		t.Fatalf("paginated set size = %d (%v), want %d (%v)", len(seen), seen, len(want), want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("paginated order[%d] = %s, want %s (full: %v)", i, seen[i], want[i], seen)
		}
	}
}

func TestListModels_CursorPagination_SameTimestampTies(t *testing.T) {
	store := newTestReadStore(t)
	ctx := context.Background()
	// All FOUR models share the SAME created_at — the hard case for cursor pagination.
	// Without an id tiebreaker, a score-only cursor would skip or duplicate tied items.
	// The (score,id) cursor must still walk all four exactly once.
	same := time.Unix(5000, 0).UTC()
	ids := []string{"m-a", "m-b", "m-c", "m-d"}
	for _, id := range ids {
		if err := store.UpsertModel(ctx, projModel(id, "acme", id, same)); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}

	seen := map[string]int{}
	token := ""
	for pages := 0; ; pages++ {
		got, next, err := store.ListModels(ctx, "acme", domain.ListModelsFilter{}, domain.ListOptions{PageSize: 1, PageToken: token})
		if err != nil {
			t.Fatalf("tie page: %v", err)
		}
		for _, m := range got {
			seen[m.ID]++
		}
		if next == "" {
			break
		}
		token = next
		if pages > 20 {
			t.Fatalf("tie pagination did not terminate")
		}
	}
	if len(seen) != 4 {
		t.Fatalf("expected all 4 tied models exactly once, got %v", seen)
	}
	for _, id := range ids {
		if seen[id] != 1 {
			t.Fatalf("model %s seen %d times (want 1): %v", id, seen[id], seen)
		}
	}
}

// ----------------------------------------------------------------------------
// VERSION READ-AFTER-WRITE + LIST + stage filter
// ----------------------------------------------------------------------------

func TestUpsertAndGetVersion_RoundTrips(t *testing.T) {
	store := newTestReadStore(t)
	ctx := context.Background()

	v := projVersion("v-1", "m-1", "1.0.0", domain.StageProduction, time.Unix(6000, 0).UTC())
	v.ArtifactDigest = "sha256:xyz"
	v.SizeBytes = 999
	if err := store.UpsertVersion(ctx, v); err != nil {
		t.Fatalf("upsert version: %v", err)
	}

	byID, err := store.GetVersionByID(ctx, "v-1")
	if err != nil {
		t.Fatalf("GetVersionByID: %v", err)
	}
	if byID.Version != "1.0.0" || byID.Stage != domain.StageProduction || byID.Status != domain.StatusReady {
		t.Fatalf("version fields mismatch: %+v", byID)
	}
	if byID.ArtifactDigest != "sha256:xyz" || byID.SizeBytes != 999 {
		t.Fatalf("artifact facts mismatch: %+v", byID)
	}
	if byID.Metrics["accuracy"] != 0.9 {
		t.Fatalf("metrics did not round-trip: %#v", byID.Metrics)
	}

	byLabel, err := store.GetVersionByLabel(ctx, "m-1", "1.0.0")
	if err != nil || byLabel.ID != "v-1" {
		t.Fatalf("GetVersionByLabel = %+v, %v; want v-1", byLabel, err)
	}
}

func TestListVersions_NewestFirstAndStageFilter(t *testing.T) {
	store := newTestReadStore(t)
	ctx := context.Background()
	base := time.Unix(7000, 0).UTC()

	// v1 (DEV, oldest), v2 (STAGING), v3 (PRODUCTION, newest).
	v1 := projVersion("v-1", "m-1", "1", domain.StageDev, base)
	v2 := projVersion("v-2", "m-1", "2", domain.StageStaging, base.Add(time.Minute))
	v3 := projVersion("v-3", "m-1", "3", domain.StageProduction, base.Add(2*time.Minute))
	for _, v := range []domain.ModelVersion{v2, v3, v1} { // insert unordered
		if err := store.UpsertVersion(ctx, v); err != nil {
			t.Fatalf("upsert %s: %v", v.ID, err)
		}
	}

	// Any-stage list: newest-first.
	got, _, err := store.ListVersions(ctx, "m-1", domain.StageUnspecified, domain.ListOptions{PageSize: 10})
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	assertVersionOrder(t, got, []string{"v-3", "v-2", "v-1"})

	// Stage filter → only the PRODUCTION one.
	got, _, err = store.ListVersions(ctx, "m-1", domain.StageProduction, domain.ListOptions{PageSize: 10})
	if err != nil {
		t.Fatalf("filtered list: %v", err)
	}
	assertVersionOrder(t, got, []string{"v-3"})

	// A DIFFERENT model's version list is isolated (per-model ZSET).
	if err := store.UpsertVersion(ctx, projVersion("v-x", "m-2", "1", domain.StageDev, base)); err != nil {
		t.Fatalf("upsert other model version: %v", err)
	}
	got, _, _ = store.ListVersions(ctx, "m-1", domain.StageUnspecified, domain.ListOptions{PageSize: 10})
	assertVersionOrder(t, got, []string{"v-3", "v-2", "v-1"}) // m-2's version not present
}

func TestListVersions_StageFilterAcrossPages(t *testing.T) {
	store := newTestReadStore(t)
	ctx := context.Background()
	base := time.Unix(8000, 0).UTC()

	// 6 versions alternating PRODUCTION/DEV; we filter PRODUCTION with a small page size
	// to prove the filter holds ACROSS pages (the adapter over-fetches to fill a page).
	var wantProd []string
	for i := 0; i < 6; i++ {
		stage := domain.StageDev
		if i%2 == 0 {
			stage = domain.StageProduction
		}
		id := fmt.Sprintf("v-%d", i)
		if err := store.UpsertVersion(ctx, projVersion(id, "m-1", id, stage, base.Add(time.Duration(i)*time.Minute))); err != nil {
			t.Fatalf("upsert: %v", err)
		}
		if stage == domain.StageProduction {
			wantProd = append(wantProd, id)
		}
	}
	// wantProd in newest-first order: v-4, v-2, v-0.
	wantNewestFirst := []string{"v-4", "v-2", "v-0"}

	var seen []string
	token := ""
	for pages := 0; ; pages++ {
		got, next, err := store.ListVersions(ctx, "m-1", domain.StageProduction, domain.ListOptions{PageSize: 1, PageToken: token})
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		for _, v := range got {
			seen = append(seen, v.ID)
		}
		if next == "" {
			break
		}
		token = next
		if pages > 20 {
			t.Fatalf("did not terminate")
		}
	}
	if len(seen) != len(wantNewestFirst) {
		t.Fatalf("filtered pagination got %v, want %v", seen, wantNewestFirst)
	}
	for i := range wantNewestFirst {
		if seen[i] != wantNewestFirst[i] {
			t.Fatalf("filtered page order[%d]=%s want %s (%v)", i, seen[i], wantNewestFirst[i], seen)
		}
	}
}

func TestListModels_MalformedCursor_IsValidationError(t *testing.T) {
	store := newTestReadStore(t)
	// A garbage page token is bad client input → ErrValidation (handler maps to
	// InvalidArgument), not an internal error / panic.
	_, _, err := store.ListModels(context.Background(), "acme", domain.ListModelsFilter{}, domain.ListOptions{PageSize: 10, PageToken: "!!!not-base64!!!"})
	if err == nil {
		t.Fatalf("expected error for malformed cursor")
	}
	if !isValidationErr(err) {
		t.Fatalf("malformed cursor: got %v, want ErrValidation", err)
	}
}

// ----------------------------------------------------------------------------
// assertion helpers
// ----------------------------------------------------------------------------

func assertModelOrder(t *testing.T, got []domain.Model, wantIDs []string) {
	t.Helper()
	if len(got) != len(wantIDs) {
		gotIDs := make([]string, len(got))
		for i, m := range got {
			gotIDs[i] = m.ID
		}
		t.Fatalf("model list size = %d %v, want %d %v", len(got), gotIDs, len(wantIDs), wantIDs)
	}
	for i, id := range wantIDs {
		if got[i].ID != id {
			t.Fatalf("model[%d] = %s, want %s", i, got[i].ID, id)
		}
	}
}

func assertVersionOrder(t *testing.T, got []domain.ModelVersion, wantIDs []string) {
	t.Helper()
	if len(got) != len(wantIDs) {
		gotIDs := make([]string, len(got))
		for i, v := range got {
			gotIDs[i] = v.ID
		}
		t.Fatalf("version list size = %d %v, want %d %v", len(got), gotIDs, len(wantIDs), wantIDs)
	}
	for i, id := range wantIDs {
		if got[i].ID != id {
			t.Fatalf("version[%d] = %s, want %s", i, got[i].ID, id)
		}
	}
}

// isValidationErr reports whether err is (wraps) domain.ErrValidation.
func isValidationErr(err error) bool {
	return errors.Is(err, domain.ErrValidation)
}
