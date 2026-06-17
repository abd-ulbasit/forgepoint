// online_test.go — integration tests for the Redis OnlineViewStore against a REAL
// Redis (testcontainers). Verifies the online "latest" read model: batched
// GetLatest (present + missing entities), idempotent Put (upsert + entity index),
// Purge (tears down the whole view's projection via the index set, not SCAN), and a
// full value-codec round-trip through real Redis.
package redis

import (
	"context"
	"reflect"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/domain"
)

// newTestOnline spins up Redis, builds a client, and returns the adapter. The client
// is closed on cleanup. testutil.StartRedis returns a redis:// URL we parse with
// go-redis' option parser (handles host:port and any auth/db in the URL).
func newTestOnline(t *testing.T) (*OnlineViewStore, *goredis.Client, context.Context) {
	t.Helper()
	testutil.SkipIfNoDocker(t)
	ctx := context.Background()
	connStr := testutil.StartRedis(t)

	opts, err := goredis.ParseURL(connStr)
	if err != nil {
		t.Fatalf("parse redis url %q: %v", connStr, err)
	}
	client := goredis.NewClient(opts)
	t.Cleanup(func() { _ = client.Close() })

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	return NewOnlineViewStore(client), client, ctx
}

func onlineVec(entityID string, version int64, values map[string]domain.FeatureValue) domain.FeatureVector {
	return domain.FeatureVector{
		EntityID:      entityID,
		Values:        values,
		AsOfVersion:   version,
		EventTime:     time.Now().UTC().Truncate(time.Microsecond),
		SchemaVersion: 1,
	}
}

// TestOnline_PutThenGetLatest verifies a written vector reads back, and that an
// entity never written is reported missing (absent from the result map).
func TestOnline_PutThenGetLatest(t *testing.T) {
	store, _, ctx := newTestOnline(t)
	viewID := "view-1"

	in := map[string]domain.FeatureVector{
		"e1": onlineVec("e1", 5, map[string]domain.FeatureValue{"score": {Kind: domain.FeatureTypeDouble, Double: 0.7}}),
		"e2": onlineVec("e2", 6, map[string]domain.FeatureValue{"score": {Kind: domain.FeatureTypeDouble, Double: 0.4}}),
	}
	if err := store.Put(ctx, viewID, in); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Ask for e1, e2, and a never-written e3.
	got, err := store.GetLatest(ctx, viewID, []string{"e1", "e2", "e3"})
	if err != nil {
		t.Fatalf("get latest: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d vectors, want 2 (e3 missing)", len(got))
	}
	if got["e1"].AsOfVersion != 5 || got["e1"].Values["score"].Double != 0.7 {
		t.Fatalf("e1 round-trip mismatch: %+v", got["e1"])
	}
	if _, ok := got["e3"]; ok {
		t.Fatal("e3 was never written; must be absent (missing)")
	}
}

// TestOnline_PutIsIdempotentUpsert verifies re-Putting an entity overwrites (latest
// wins) rather than duplicating, and the entity index does not grow.
func TestOnline_PutIsIdempotentUpsert(t *testing.T) {
	store, client, ctx := newTestOnline(t)
	viewID := "view-upsert"

	if err := store.Put(ctx, viewID, map[string]domain.FeatureVector{
		"e1": onlineVec("e1", 1, map[string]domain.FeatureValue{"v": {Kind: domain.FeatureTypeInt64, Int: 1}}),
	}); err != nil {
		t.Fatalf("put v1: %v", err)
	}
	// Overwrite e1 with a newer value.
	if err := store.Put(ctx, viewID, map[string]domain.FeatureVector{
		"e1": onlineVec("e1", 2, map[string]domain.FeatureValue{"v": {Kind: domain.FeatureTypeInt64, Int: 99}}),
	}); err != nil {
		t.Fatalf("put v2: %v", err)
	}

	got, err := store.GetLatest(ctx, viewID, []string{"e1"})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got["e1"].Values["v"].Int != 99 || got["e1"].AsOfVersion != 2 {
		t.Fatalf("upsert did not take latest: %+v", got["e1"])
	}
	// The entity index SET must have exactly one member (SADD is idempotent).
	card, err := client.SCard(ctx, entitiesKey(viewID)).Result()
	if err != nil {
		t.Fatalf("scard: %v", err)
	}
	if card != 1 {
		t.Fatalf("entity index cardinality = %d, want 1", card)
	}
}

// TestOnline_Purge verifies Purge removes ALL of a view's vector keys plus the index
// set, and that it is idempotent (purging an empty view is a no-op).
func TestOnline_Purge(t *testing.T) {
	store, client, ctx := newTestOnline(t)
	viewID := "view-purge"

	if err := store.Put(ctx, viewID, map[string]domain.FeatureVector{
		"e1": onlineVec("e1", 1, map[string]domain.FeatureValue{"v": {Kind: domain.FeatureTypeInt64, Int: 1}}),
		"e2": onlineVec("e2", 2, map[string]domain.FeatureValue{"v": {Kind: domain.FeatureTypeInt64, Int: 2}}),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := store.Purge(ctx, viewID); err != nil {
		t.Fatalf("purge: %v", err)
	}

	// All vector keys gone.
	got, err := store.GetLatest(ctx, viewID, []string{"e1", "e2"})
	if err != nil {
		t.Fatalf("get after purge: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty after purge, got %d", len(got))
	}
	// The index set is gone too (no leaked keys to leave the keyspace dirty).
	exists, err := client.Exists(ctx, entitiesKey(viewID)).Result()
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if exists != 0 {
		t.Fatal("entity index set should be deleted by purge")
	}

	// Idempotent: purging again is a clean no-op.
	if err := store.Purge(ctx, viewID); err != nil {
		t.Fatalf("second purge should be no-op: %v", err)
	}
}

// TestOnline_PurgeIsViewScoped verifies Purge of one view does not touch another
// view's projection (the keys are namespaced by view id).
func TestOnline_PurgeIsViewScoped(t *testing.T) {
	store, _, ctx := newTestOnline(t)

	if err := store.Put(ctx, "view-a", map[string]domain.FeatureVector{
		"e1": onlineVec("e1", 1, map[string]domain.FeatureValue{"v": {Kind: domain.FeatureTypeInt64, Int: 1}}),
	}); err != nil {
		t.Fatalf("seed a: %v", err)
	}
	if err := store.Put(ctx, "view-b", map[string]domain.FeatureVector{
		"e1": onlineVec("e1", 1, map[string]domain.FeatureValue{"v": {Kind: domain.FeatureTypeInt64, Int: 2}}),
	}); err != nil {
		t.Fatalf("seed b: %v", err)
	}

	if err := store.Purge(ctx, "view-a"); err != nil {
		t.Fatalf("purge a: %v", err)
	}

	// view-b must be intact.
	got, err := store.GetLatest(ctx, "view-b", []string{"e1"})
	if err != nil {
		t.Fatalf("get b: %v", err)
	}
	if got["e1"].Values["v"].Int != 2 {
		t.Fatalf("purge of view-a corrupted view-b: %+v", got["e1"])
	}
}

// TestOnline_GetLatest_EmptyInput verifies a nil/empty entity list returns an empty
// map without a round-trip error.
func TestOnline_GetLatest_EmptyInput(t *testing.T) {
	store, _, ctx := newTestOnline(t)
	got, err := store.GetLatest(ctx, "view-x", nil)
	if err != nil {
		t.Fatalf("empty get: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty, got %d", len(got))
	}
}

// TestOnline_ValueCodecAllKinds round-trips every FeatureValue kind through real
// Redis, ensuring the online wire format matches the offline one (a vector read from
// either store is identical).
func TestOnline_ValueCodecAllKinds(t *testing.T) {
	store, _, ctx := newTestOnline(t)
	viewID := "view-codec"
	ts := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	values := map[string]domain.FeatureValue{
		"i":     {Kind: domain.FeatureTypeInt64, Int: 42},
		"d":     {Kind: domain.FeatureTypeDouble, Double: 3.14159},
		"s":     {Kind: domain.FeatureTypeString, Str: "hello"},
		"b":     {Kind: domain.FeatureTypeBool, Bool: true},
		"t":     {Kind: domain.FeatureTypeTimestamp, Time: ts},
		"embed": {Kind: domain.FeatureTypeDoubleList, List: []float64{0.1, 0.2, 0.3}},
		"meta":  {Kind: domain.FeatureTypeStruct, Struct: map[string]any{"k": "v", "n": float64(7)}},
	}
	if err := store.Put(ctx, viewID, map[string]domain.FeatureVector{"e1": onlineVec("e1", 1, values)}); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, err := store.GetLatest(ctx, viewID, []string{"e1"})
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
	if out["s"].Str != "hello" {
		t.Errorf("string: %+v", out["s"])
	}
	if out["b"].Bool != true {
		t.Errorf("bool: %+v", out["b"])
	}
	if !out["t"].Time.Equal(ts) {
		t.Errorf("timestamp: %v vs %v", out["t"].Time, ts)
	}
	if !reflect.DeepEqual(out["embed"].List, []float64{0.1, 0.2, 0.3}) {
		t.Errorf("double_list: %+v", out["embed"].List)
	}
	if !reflect.DeepEqual(out["meta"].Struct, map[string]any{"k": "v", "n": float64(7)}) {
		t.Errorf("struct: %+v", out["meta"].Struct)
	}
}
