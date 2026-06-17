// offline_test.go — integration tests for the Postgres OfflineViewStore against a
// REAL Postgres. Verifies the CQRS offline read model: Rebuild materializes a
// projection atomically (delete-then-insert), GetAsOf serves it filtered by entity
// and by event_time ≤ asOf with keyset pagination, and a re-Rebuild prunes vanished
// entities (idempotent replace). Also pins the JSONB value codec across ALL
// FeatureValue kinds via a round-trip through the real column.
package postgres

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/domain"
)

func newTestOffline(t *testing.T) (*OfflineViewStore, context.Context) {
	t.Helper()
	testutil.SkipIfNoDocker(t)
	ctx := context.Background()
	dsn := testutil.StartPostgres(t)
	pool := applyMigrations(t, ctx, dsn)
	return NewOfflineViewStore(pool), ctx
}

func vec(entityID string, version int64, eventTime time.Time, values map[string]domain.FeatureValue) domain.FeatureVector {
	return domain.FeatureVector{
		EntityID:      entityID,
		Values:        values,
		AsOfVersion:   version,
		EventTime:     eventTime,
		SchemaVersion: 1,
	}
}

// TestOffline_RebuildThenGetAsOf verifies the basic materialize-and-read cycle and
// that provenance (version/event_time/schema_version) round-trips.
func TestOffline_RebuildThenGetAsOf(t *testing.T) {
	store, ctx := newTestOffline(t)
	viewID := uuid.NewString()
	now := time.Now().UTC().Truncate(time.Microsecond)

	vectors := map[string]domain.FeatureVector{
		"e1": vec("e1", 10, now, map[string]domain.FeatureValue{"score": {Kind: domain.FeatureTypeDouble, Double: 0.9}}),
		"e2": vec("e2", 11, now, map[string]domain.FeatureValue{"score": {Kind: domain.FeatureTypeDouble, Double: 0.8}}),
	}
	if err := store.Rebuild(ctx, viewID, vectors); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	got, next, err := store.GetAsOf(ctx, viewID, []string{"e1", "e2"}, now, domain.ListOptions{PageSize: domain.MaxPageSize})
	if err != nil {
		t.Fatalf("get as-of: %v", err)
	}
	if next != "" {
		t.Fatalf("unexpected next token %q", next)
	}
	if len(got) != 2 {
		t.Fatalf("got %d vectors, want 2", len(got))
	}
	if got["e1"].AsOfVersion != 10 || got["e1"].Values["score"].Double != 0.9 {
		t.Fatalf("e1 round-trip mismatch: %+v", got["e1"])
	}
	if !got["e1"].EventTime.Equal(now) {
		t.Fatalf("event_time not preserved: %v vs %v", got["e1"].EventTime, now)
	}
}

// TestOffline_GetAsOf_TimeFilter verifies a row whose event_time is AFTER the
// requested asOf is excluded (point-in-time honesty — never return a value newer
// than the instant asked for).
func TestOffline_GetAsOf_TimeFilter(t *testing.T) {
	store, ctx := newTestOffline(t)
	viewID := uuid.NewString()
	base := time.Now().UTC().Truncate(time.Microsecond)

	vectors := map[string]domain.FeatureVector{
		"old": vec("old", 1, base.Add(-time.Hour), map[string]domain.FeatureValue{"v": {Kind: domain.FeatureTypeInt64, Int: 1}}),
		"new": vec("new", 2, base.Add(time.Hour), map[string]domain.FeatureValue{"v": {Kind: domain.FeatureTypeInt64, Int: 2}}),
	}
	if err := store.Rebuild(ctx, viewID, vectors); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	// as-of = base: "old" qualifies (before), "new" does not (after).
	got, _, err := store.GetAsOf(ctx, viewID, []string{"old", "new"}, base, domain.ListOptions{})
	if err != nil {
		t.Fatalf("get as-of: %v", err)
	}
	if _, ok := got["old"]; !ok {
		t.Fatal("expected 'old' (event_time before asOf) to be present")
	}
	if _, ok := got["new"]; ok {
		t.Fatal("expected 'new' (event_time after asOf) to be EXCLUDED")
	}
}

// TestOffline_RebuildPrunesVanishedEntities verifies Rebuild REPLACES (not merges):
// an entity present in the first rebuild but absent from the second is removed.
func TestOffline_RebuildPrunesVanishedEntities(t *testing.T) {
	store, ctx := newTestOffline(t)
	viewID := uuid.NewString()
	now := time.Now().UTC().Truncate(time.Microsecond)

	if err := store.Rebuild(ctx, viewID, map[string]domain.FeatureVector{
		"keep":   vec("keep", 1, now, map[string]domain.FeatureValue{"v": {Kind: domain.FeatureTypeInt64, Int: 1}}),
		"vanish": vec("vanish", 2, now, map[string]domain.FeatureValue{"v": {Kind: domain.FeatureTypeInt64, Int: 2}}),
	}); err != nil {
		t.Fatalf("first rebuild: %v", err)
	}
	// Second rebuild without "vanish".
	if err := store.Rebuild(ctx, viewID, map[string]domain.FeatureVector{
		"keep": vec("keep", 3, now, map[string]domain.FeatureValue{"v": {Kind: domain.FeatureTypeInt64, Int: 9}}),
	}); err != nil {
		t.Fatalf("second rebuild: %v", err)
	}

	got, _, err := store.GetAsOf(ctx, viewID, []string{"keep", "vanish"}, now, domain.ListOptions{})
	if err != nil {
		t.Fatalf("get as-of: %v", err)
	}
	if _, ok := got["vanish"]; ok {
		t.Fatal("expected 'vanish' to be pruned by the replace-style rebuild")
	}
	if got["keep"].Values["v"].Int != 9 {
		t.Fatalf("expected 'keep' updated to 9, got %d", got["keep"].Values["v"].Int)
	}
}

// TestOffline_GetAsOf_Pagination verifies keyset pagination over the entity set.
func TestOffline_GetAsOf_Pagination(t *testing.T) {
	store, ctx := newTestOffline(t)
	viewID := uuid.NewString()
	now := time.Now().UTC().Truncate(time.Microsecond)

	ids := []string{"a", "b", "c", "d", "e"}
	vectors := map[string]domain.FeatureVector{}
	for i, id := range ids {
		vectors[id] = vec(id, int64(i+1), now, map[string]domain.FeatureValue{"v": {Kind: domain.FeatureTypeInt64, Int: int64(i)}})
	}
	if err := store.Rebuild(ctx, viewID, vectors); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	seen := map[string]bool{}
	token := ""
	pages := 0
	for {
		got, next, err := store.GetAsOf(ctx, viewID, ids, now, domain.ListOptions{PageSize: 2, PageToken: token})
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		for id := range got {
			if seen[id] {
				t.Fatalf("duplicate id %s across pages", id)
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
		t.Fatalf("pagination saw %d ids, want 5", len(seen))
	}
}

// TestOffline_ValueCodecAllKinds round-trips EVERY FeatureValue kind through the
// real JSONB column, pinning the codec. This is the train/serve-skew guard at the
// persistence layer: a value must read back EXACTLY as written, including []float64
// embeddings, structs, and timestamps.
func TestOffline_ValueCodecAllKinds(t *testing.T) {
	store, ctx := newTestOffline(t)
	viewID := uuid.NewString()
	ts := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	values := map[string]domain.FeatureValue{
		"i":      {Kind: domain.FeatureTypeInt64, Int: 42},
		"d":      {Kind: domain.FeatureTypeDouble, Double: 3.14159},
		"s":      {Kind: domain.FeatureTypeString, Str: "hello; DROP TABLE feature_events;--"}, // injection-shaped data stays data
		"b":      {Kind: domain.FeatureTypeBool, Bool: true},
		"t":      {Kind: domain.FeatureTypeTimestamp, Time: ts},
		"embed":  {Kind: domain.FeatureTypeDoubleList, List: []float64{0.1, 0.2, 0.3}},
		"meta":   {Kind: domain.FeatureTypeStruct, Struct: map[string]any{"k": "v", "n": float64(7)}},
	}
	in := vec("e1", 1, ts, values)
	if err := store.Rebuild(ctx, viewID, map[string]domain.FeatureVector{"e1": in}); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	got, _, err := store.GetAsOf(ctx, viewID, []string{"e1"}, ts, domain.ListOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	out := got["e1"].Values

	if out["i"].Int != 42 {
		t.Errorf("int64: %+v", out["i"])
	}
	if out["d"].Double != 3.14159 {
		t.Errorf("double: %+v", out["d"])
	}
	if out["s"].Str != values["s"].Str {
		t.Errorf("string (injection-shaped) corrupted: %q", out["s"].Str)
	}
	if out["b"].Bool != true {
		t.Errorf("bool: %+v", out["b"])
	}
	if !out["t"].Time.Equal(ts) {
		t.Errorf("timestamp: %v vs %v", out["t"].Time, ts)
	}
	if !reflect.DeepEqual(out["embed"].List, []float64{0.1, 0.2, 0.3}) {
		t.Errorf("double_list embedding: %+v", out["embed"].List)
	}
	if !reflect.DeepEqual(out["meta"].Struct, map[string]any{"k": "v", "n": float64(7)}) {
		t.Errorf("struct: %+v", out["meta"].Struct)
	}
}

// TestOffline_GetAsOf_Empty verifies an empty entity list and an unknown view both
// return an empty result (no error) — the service reports the entities missing.
func TestOffline_GetAsOf_Empty(t *testing.T) {
	store, ctx := newTestOffline(t)

	got, next, err := store.GetAsOf(ctx, uuid.NewString(), nil, time.Now(), domain.ListOptions{})
	if err != nil {
		t.Fatalf("empty entities: %v", err)
	}
	if len(got) != 0 || next != "" {
		t.Fatalf("expected empty result, got %d vectors next=%q", len(got), next)
	}

	got, _, err = store.GetAsOf(ctx, uuid.NewString(), []string{"x"}, time.Now(), domain.ListOptions{})
	if err != nil {
		t.Fatalf("unknown view: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("unknown view should be empty, got %d", len(got))
	}
}
