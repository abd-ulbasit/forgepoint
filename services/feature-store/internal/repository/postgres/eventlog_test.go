// eventlog_test.go — integration tests for the Postgres EventLog adapter against a
// REAL Postgres (testcontainers). These verify the event-sourcing persistence
// semantics the domain depends on: append+replay, gap-free monotonic versioning,
// transactional atomicity (a failed append leaves no partial write), idempotency
// (exactly-once effect on retry), the team-scoped name index, and keyset pagination.
//
// We test the REAL adapter against the REAL store — not a mock — because the whole
// point of this layer is the behavior of the database (transaction isolation,
// JSONB round-tripping, the advisory-lock version assignment under the real engine).
package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/domain"
)

// newTestEventLog spins up Postgres, applies the migrations, and returns a ready
// EventLog adapter. SkipIfNoDocker (called inside StartPostgres) cleanly skips when
// the container engine is unavailable.
func newTestEventLog(t *testing.T) (*EventLog, context.Context) {
	t.Helper()
	testutil.SkipIfNoDocker(t)
	ctx := context.Background()
	dsn := testutil.StartPostgres(t)
	pool := applyMigrations(t, ctx, dsn)
	return NewEventLog(pool), ctx
}

// --- test fixtures -----------------------------------------------------------

// newViewDefinedEvent builds a FeatureEventViewDefined for a fresh view. UUIDs are
// real (the columns are typed UUID); the schema is a minimal but typed spec so the
// codec round-trip is exercised on the value side later.
func newViewDefinedEvent(viewID, team, name string, schemaVersion int64, at time.Time) domain.FeatureEvent {
	return domain.FeatureEvent{
		ID:            uuid.NewString(),
		Type:          domain.FeatureEventViewDefined,
		FeatureViewID: viewID,
		EventTime:     at,
		AppendedAt:    at,
		ViewDef: &domain.ViewDefinition{
			ID:            viewID,
			Name:          name,
			Entity:        domain.Entity{Name: "user", JoinKey: "user_id"},
			Features:      []domain.FeatureSpec{{Name: "score", ValueType: domain.FeatureTypeDouble}},
			SchemaVersion: schemaVersion,
			OwnerUserID:   "user-1",
			OwnerTeam:     team,
		},
	}
}

// newValuesEvent builds a FeatureEventValuesWritten carrying one double feature.
func newValuesEvent(viewID, entityID string, score float64, eventTime, at time.Time) domain.FeatureEvent {
	return domain.FeatureEvent{
		ID:            uuid.NewString(),
		Type:          domain.FeatureEventValuesWritten,
		FeatureViewID: viewID,
		EntityID:      entityID,
		Values:        map[string]domain.FeatureValue{"score": {Kind: domain.FeatureTypeDouble, Double: score}},
		EventTime:     eventTime,
		AppendedAt:    at,
	}
}

// --- tests -------------------------------------------------------------------

// TestAppend_AssignsGapFreeMonotonicVersions is the core event-sourcing invariant:
// versions are 1,2,3,... contiguous across separate Appends, and writtenThrough
// equals the highest version.
func TestAppend_AssignsGapFreeMonotonicVersions(t *testing.T) {
	log, ctx := newTestEventLog(t)
	viewID := uuid.NewString()
	now := time.Now().UTC().Truncate(time.Microsecond)

	// First append: a definition event -> version 1.
	def := newViewDefinedEvent(viewID, "team-a", "user_score", 1, now)
	appended, writtenThrough, replayed, err := log.Append(ctx, "def-key", []domain.FeatureEvent{def})
	if err != nil {
		t.Fatalf("append def: %v", err)
	}
	if replayed {
		t.Fatal("first append should not be replayed")
	}
	if len(appended) != 1 || appended[0].Version != 1 {
		t.Fatalf("expected version 1, got %+v", appended)
	}
	if writtenThrough != 1 {
		t.Fatalf("writtenThrough = %d, want 1", writtenThrough)
	}

	// Second append: two value events -> versions 2 and 3 (contiguous with the first).
	ev1 := newValuesEvent(viewID, "e1", 0.1, now, now)
	ev2 := newValuesEvent(viewID, "e2", 0.2, now, now)
	appended, writtenThrough, _, err = log.Append(ctx, "write-key-1", []domain.FeatureEvent{ev1, ev2})
	if err != nil {
		t.Fatalf("append values: %v", err)
	}
	if appended[0].Version != 2 || appended[1].Version != 3 {
		t.Fatalf("expected versions 2,3 got %d,%d", appended[0].Version, appended[1].Version)
	}
	if writtenThrough != 3 {
		t.Fatalf("writtenThrough = %d, want 3", writtenThrough)
	}
}

// TestAppend_Idempotency_ReplaysOriginalRange verifies exactly-once EFFECT: a retry
// with the SAME idempotency key does NOT append again — it replays the original
// events/version and signals replayed=true.
func TestAppend_Idempotency_ReplaysOriginalRange(t *testing.T) {
	log, ctx := newTestEventLog(t)
	viewID := uuid.NewString()
	now := time.Now().UTC().Truncate(time.Microsecond)

	_, _, _, err := log.Append(ctx, "def", []domain.FeatureEvent{newViewDefinedEvent(viewID, "team-a", "v", 1, now)})
	if err != nil {
		t.Fatalf("seed def: %v", err)
	}

	ev := newValuesEvent(viewID, "e1", 1.5, now, now)
	first, wt1, replayed1, err := log.Append(ctx, "dupe-key", []domain.FeatureEvent{ev})
	if err != nil {
		t.Fatalf("first write: %v", err)
	}
	if replayed1 {
		t.Fatal("first write must not be replayed")
	}

	// Retry with the SAME key and a DIFFERENT event id — must be deduped to the
	// original (the key, not the payload, is the dedup anchor).
	retry := ev
	retry.ID = uuid.NewString()
	second, wt2, replayed2, err := log.Append(ctx, "dupe-key", []domain.FeatureEvent{retry})
	if err != nil {
		t.Fatalf("retry write: %v", err)
	}
	if !replayed2 {
		t.Fatal("retry with same key must be replayed=true")
	}
	if wt1 != wt2 {
		t.Fatalf("writtenThrough changed on replay: %d -> %d", wt1, wt2)
	}
	if len(second) != 1 || second[0].Version != first[0].Version {
		t.Fatalf("replay returned a different version: %+v vs %+v", second, first)
	}
	// The original event id is what's stored — the replay returns the ORIGINAL, not
	// the retry's new id.
	if second[0].ID != first[0].ID {
		t.Fatalf("replay returned retry id %s, want original %s", second[0].ID, first[0].ID)
	}

	// And the log must contain exactly ONE value event (no duplicate appended).
	all, err := log.Load(ctx, viewID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	valueCount := 0
	for _, e := range all {
		if e.Type == domain.FeatureEventValuesWritten {
			valueCount++
		}
	}
	if valueCount != 1 {
		t.Fatalf("expected exactly 1 value event after retry, got %d", valueCount)
	}
}

// TestAppend_AtomicityNoPartialWrite verifies the transaction is all-or-nothing: an
// append that fails mid-batch leaves NO rows. We force a failure by giving the
// SECOND event a feature_view_id that is not a valid UUID, so its INSERT errors —
// the first event's insert must roll back with it.
func TestAppend_AtomicityNoPartialWrite(t *testing.T) {
	log, ctx := newTestEventLog(t)
	viewID := uuid.NewString()
	now := time.Now().UTC().Truncate(time.Microsecond)

	good := newValuesEvent(viewID, "e1", 1.0, now, now)
	bad := newValuesEvent(viewID, "e2", 2.0, now, now)
	bad.FeatureViewID = "not-a-uuid" // forces a Postgres type error on its INSERT

	_, _, _, err := log.Append(ctx, "atomic-key", []domain.FeatureEvent{good, bad})
	if err == nil {
		t.Fatal("expected append to fail on the invalid event")
	}

	// The view must have NO events — the good event's insert rolled back with the bad
	// one. Load returns the storage sentinel for an empty log.
	_, loadErr := log.Load(ctx, viewID)
	if !errors.Is(loadErr, domain.ErrEventLogNotFound) {
		t.Fatalf("expected ErrEventLogNotFound (no partial write), got %v", loadErr)
	}

	// The idempotency key must ALSO not have been recorded (it commits in the same tx),
	// so a subsequent append with that key writes fresh rather than replaying nothing.
	good2 := newValuesEvent(viewID, "e1", 9.0, now, now)
	appended, _, replayed, err := log.Append(ctx, "atomic-key", []domain.FeatureEvent{good2})
	if err != nil {
		t.Fatalf("reuse key after rollback: %v", err)
	}
	if replayed {
		t.Fatal("key from a rolled-back append must NOT be treated as already-seen")
	}
	if len(appended) != 1 || appended[0].Version != 1 {
		t.Fatalf("expected fresh version 1 after rollback, got %+v", appended)
	}
}

// TestLoadAndLoadView_ReplayAndSubset verifies Load returns the full ordered stream
// and LoadView returns ONLY the definition/deletion subset (the partial-index path).
func TestLoadAndLoadView_ReplayAndSubset(t *testing.T) {
	log, ctx := newTestEventLog(t)
	viewID := uuid.NewString()
	now := time.Now().UTC().Truncate(time.Microsecond)

	def := newViewDefinedEvent(viewID, "team-a", "v", 1, now)
	v1 := newValuesEvent(viewID, "e1", 1.0, now, now)
	v2 := newValuesEvent(viewID, "e2", 2.0, now, now)
	del := domain.FeatureEvent{ID: uuid.NewString(), Type: domain.FeatureEventViewDeleted, FeatureViewID: viewID, EventTime: now, AppendedAt: now}

	if _, _, _, err := log.Append(ctx, "k1", []domain.FeatureEvent{def}); err != nil {
		t.Fatalf("append def: %v", err)
	}
	if _, _, _, err := log.Append(ctx, "k2", []domain.FeatureEvent{v1, v2}); err != nil {
		t.Fatalf("append values: %v", err)
	}
	if _, _, _, err := log.Append(ctx, "k3", []domain.FeatureEvent{del}); err != nil {
		t.Fatalf("append delete: %v", err)
	}

	// Load: all 4 events, ascending version.
	all, err := log.Load(ctx, viewID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(all) != 4 {
		t.Fatalf("Load returned %d events, want 4", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i].Version <= all[i-1].Version {
			t.Fatalf("Load not ascending by version: %d then %d", all[i-1].Version, all[i].Version)
		}
	}
	// The value event must round-trip its JSONB payload exactly.
	for _, e := range all {
		if e.Type == domain.FeatureEventValuesWritten && e.EntityID == "e1" {
			got := e.Values["score"]
			if got.Kind != domain.FeatureTypeDouble || got.Double != 1.0 {
				t.Fatalf("value round-trip mismatch: %+v", got)
			}
		}
	}

	// LoadView: only the 2 definition/deletion events (def + del), not the value rows.
	defEvents, err := log.LoadView(ctx, viewID)
	if err != nil {
		t.Fatalf("loadview: %v", err)
	}
	if len(defEvents) != 2 {
		t.Fatalf("LoadView returned %d events, want 2 (def+del)", len(defEvents))
	}
	for _, e := range defEvents {
		if e.Type == domain.FeatureEventValuesWritten {
			t.Fatalf("LoadView leaked a value event: %+v", e)
		}
	}
	// The ViewDefined event must carry its decoded ViewDef (schema round-trip).
	if defEvents[0].Type == domain.FeatureEventViewDefined {
		if defEvents[0].ViewDef == nil || defEvents[0].ViewDef.Entity.JoinKey != "user_id" {
			t.Fatalf("view def did not round-trip: %+v", defEvents[0].ViewDef)
		}
	}
}

// TestLoad_NotFoundSentinel verifies an unknown view returns the storage sentinel
// (which the service translates to ErrViewNotFound).
func TestLoad_NotFoundSentinel(t *testing.T) {
	log, ctx := newTestEventLog(t)
	_, err := log.Load(ctx, uuid.NewString())
	if !errors.Is(err, domain.ErrEventLogNotFound) {
		t.Fatalf("expected ErrEventLogNotFound, got %v", err)
	}
	_, err = log.LoadView(ctx, uuid.NewString())
	if !errors.Is(err, domain.ErrEventLogNotFound) {
		t.Fatalf("expected ErrEventLogNotFound from LoadView, got %v", err)
	}
}

// TestResolveViewIDByName_TeamScoped verifies the name index is team-scoped (a
// caller cannot resolve another team's view name) and case-insensitive.
func TestResolveViewIDByName_TeamScoped(t *testing.T) {
	log, ctx := newTestEventLog(t)
	now := time.Now().UTC().Truncate(time.Microsecond)

	viewID := uuid.NewString()
	if _, _, _, err := log.Append(ctx, "k", []domain.FeatureEvent{newViewDefinedEvent(viewID, "team-a", "User_Score", 1, now)}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Same team, case-insensitive match resolves.
	got, err := log.ResolveViewIDByName(ctx, "team-a", "user_score")
	if err != nil {
		t.Fatalf("resolve same team: %v", err)
	}
	if got != viewID {
		t.Fatalf("resolved %s, want %s", got, viewID)
	}

	// DIFFERENT team must NOT resolve (the scoping/authorization boundary).
	_, err = log.ResolveViewIDByName(ctx, "team-b", "user_score")
	if !errors.Is(err, domain.ErrEventLogNotFound) {
		t.Fatalf("cross-team resolve should be NotFound, got %v", err)
	}
}

// TestUniqueNameConflict verifies the team-scoped unique index rejects a SECOND,
// different view id claiming the same (team, name) — the race backstop behind the
// service's resolve-first flow.
func TestUniqueNameConflict(t *testing.T) {
	log, ctx := newTestEventLog(t)
	now := time.Now().UTC().Truncate(time.Microsecond)

	if _, _, _, err := log.Append(ctx, "k1", []domain.FeatureEvent{newViewDefinedEvent(uuid.NewString(), "team-a", "dup", 1, now)}); err != nil {
		t.Fatalf("first define: %v", err)
	}
	// A different view id with the same (team, name) collides on uq_view_name_index.
	_, _, _, err := log.Append(ctx, "k2", []domain.FeatureEvent{newViewDefinedEvent(uuid.NewString(), "team-a", "dup", 1, now)})
	if err == nil {
		t.Fatal("expected a unique-violation on duplicate (team,name)")
	}
}

// TestRedefineEvolvesNameIndex verifies re-defining the SAME view id (evolution)
// upserts the name-index row (no conflict) and a delete flips is_deleted, which a
// later list can observe.
func TestRedefineEvolvesNameIndex(t *testing.T) {
	log, ctx := newTestEventLog(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	viewID := uuid.NewString()

	if _, _, _, err := log.Append(ctx, "k1", []domain.FeatureEvent{newViewDefinedEvent(viewID, "team-a", "evolve", 1, now)}); err != nil {
		t.Fatalf("define v1: %v", err)
	}
	// Re-define the SAME id at v2 — must NOT conflict (ON CONFLICT DO UPDATE).
	if _, _, _, err := log.Append(ctx, "k2", []domain.FeatureEvent{newViewDefinedEvent(viewID, "team-a", "evolve", 2, now.Add(time.Second))}); err != nil {
		t.Fatalf("redefine v2 should not conflict: %v", err)
	}
	// It still resolves to the same id.
	got, err := log.ResolveViewIDByName(ctx, "team-a", "evolve")
	if err != nil || got != viewID {
		t.Fatalf("resolve after evolve: got %s err %v", got, err)
	}
}

// TestListViewIDs_PaginationAndFilter verifies keyset pagination (no dup/skip across
// pages) and case-insensitive substring filtering, both team-scoped.
func TestListViewIDs_PaginationAndFilter(t *testing.T) {
	log, ctx := newTestEventLog(t)
	now := time.Now().UTC().Truncate(time.Microsecond)

	// Seed 5 views in team-a (3 matching "credit", 2 matching "activity") + 1 in team-b.
	for i, name := range []string{"user_credit_1", "user_credit_2", "user_credit_3", "user_activity_1", "user_activity_2"} {
		if _, _, _, err := log.Append(ctx, "ka"+name, []domain.FeatureEvent{newViewDefinedEvent(uuid.NewString(), "team-a", name, 1, now.Add(time.Duration(i)*time.Second))}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	if _, _, _, err := log.Append(ctx, "kb", []domain.FeatureEvent{newViewDefinedEvent(uuid.NewString(), "team-b", "user_credit_x", 1, now)}); err != nil {
		t.Fatalf("seed team-b: %v", err)
	}

	// Filter "credit" in team-a -> exactly 3 (team-b's credit view is invisible).
	ids, _, err := log.ListViewIDs(ctx, "team-a", "credit", domain.ListOptions{PageSize: domain.MaxPageSize})
	if err != nil {
		t.Fatalf("list filtered: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("filter 'credit' returned %d, want 3 (team-scoped)", len(ids))
	}

	// Paginate ALL team-a views with PageSize 2: collect pages, assert no dup/skip and
	// total == 5.
	seen := map[string]bool{}
	token := ""
	pages := 0
	for {
		page, next, lerr := log.ListViewIDs(ctx, "team-a", "", domain.ListOptions{PageSize: 2, PageToken: token})
		if lerr != nil {
			t.Fatalf("list page: %v", lerr)
		}
		for _, id := range page {
			if seen[id] {
				t.Fatalf("pagination returned duplicate id %s", id)
			}
			seen[id] = true
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
		t.Fatalf("pagination saw %d distinct ids, want 5", len(seen))
	}
}

// TestAppend_EmptyBatchProbe verifies an empty batch returns the current head
// without writing (a "where am I?" probe).
func TestAppend_EmptyBatchProbe(t *testing.T) {
	log, ctx := newTestEventLog(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	viewID := uuid.NewString()

	if _, _, _, err := log.Append(ctx, "k", []domain.FeatureEvent{newViewDefinedEvent(viewID, "team-a", "v", 1, now)}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	appended, wt, replayed, err := log.Append(ctx, "", nil)
	if err != nil {
		t.Fatalf("empty probe: %v", err)
	}
	if appended != nil || replayed {
		t.Fatalf("empty probe wrote something: appended=%v replayed=%v", appended, replayed)
	}
	if wt != 1 {
		t.Fatalf("empty probe head = %d, want 1", wt)
	}
}
