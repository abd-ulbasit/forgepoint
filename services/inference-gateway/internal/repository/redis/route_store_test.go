// route_store_test.go — integration tests for the RouteStore adapter against a
// REAL Redis (testcontainers). These verify REAL behavior, not mock interactions:
// the deep-copy snapshot isolation actually holds under a concurrent writer, the
// Redis mirror actually round-trips, Delete is actually idempotent, and cursor
// pagination is actually stable across a mid-page insert.
package redisrepo

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/domain"
)

// newRedis spins up a real Redis container and returns a connected client. The
// client and container are torn down via t.Cleanup. Every test gets an isolated
// instance — no shared state, no cross-test interference.
func newRedis(t *testing.T) *goredis.Client {
	t.Helper()
	testutil.SkipIfNoDocker(t) // skip cleanly when Docker/remote engine is unavailable

	addr := testutil.StartRedis(t) // returns a redis:// connection URL
	opt, err := goredis.ParseURL(addr)
	if err != nil {
		t.Fatalf("parse redis url %q: %v", addr, err)
	}
	rdb := goredis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })

	// Fail fast with a clear message if the container isn't actually reachable.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	return rdb
}

// sampleRoute builds a small valid route for a model with one stable + one canary.
func sampleRoute(model string) domain.Route {
	return domain.Route{
		ModelName: model,
		Targets: []domain.RouteTarget{
			{Version: "v1", Endpoint: model + "-v1.fp-models.svc:9090", WeightBps: 9000, Status: domain.TargetStatusActive, IsStable: true},
			{Version: "v2", Endpoint: model + "-v2.fp-models.svc:9090", WeightBps: 1000, Status: domain.TargetStatusActive, IsStable: false},
		},
		UpdatedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
}

func TestRouteStore_UpsertGet_RoundTripAndNoRoute(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	store := NewRouteStore(rdb)

	// Absent model → ErrNoRoute (the sentinel the use-case maps to NO_ROUTE).
	if _, err := store.Get(ctx, "ghost"); err != domain.ErrNoRoute {
		t.Fatalf("Get(absent) = %v, want ErrNoRoute", err)
	}

	want := sampleRoute("iris")
	if err := store.Upsert(ctx, want); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	got, err := store.Get(ctx, "iris")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ModelName != want.ModelName || len(got.Targets) != len(want.Targets) {
		t.Fatalf("Get returned %+v, want %+v", got, want)
	}
	if got.Targets[0].Version != "v1" || got.Targets[0].WeightBps != 9000 || !got.Targets[0].IsStable {
		t.Fatalf("stable target round-tripped wrong: %+v", got.Targets[0])
	}
	if got.Targets[1].Version != "v2" || got.Targets[1].Status != domain.TargetStatusActive {
		t.Fatalf("canary target round-tripped wrong: %+v", got.Targets[1])
	}
}

// TestRouteStore_MirrorPersistsToRedis proves Upsert actually writes the Redis
// mirror (not just the in-memory map): a SECOND, independent store warms from the
// same Redis and sees the route — exactly the freshly-scheduled-replica scenario.
func TestRouteStore_MirrorPersistsToRedis(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()

	writer := NewRouteStore(rdb)
	if err := writer.Upsert(ctx, sampleRoute("fraud")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// A brand-new replica: fresh in-memory map, same Redis. Before warming it must
	// NOT know the route (proves Get reads memory, not Redis).
	replica := NewRouteStore(rdb)
	if _, err := replica.Get(ctx, "fraud"); err != domain.ErrNoRoute {
		t.Fatalf("cold replica Get = %v, want ErrNoRoute (must read memory, not Redis)", err)
	}

	// After Warm it recovers the full table from the mirror.
	if err := replica.Warm(ctx); err != nil {
		t.Fatalf("Warm: %v", err)
	}
	got, err := replica.Get(ctx, "fraud")
	if err != nil {
		t.Fatalf("warmed replica Get: %v", err)
	}
	if len(got.Targets) != 2 || got.Targets[0].Endpoint != "fraud-v1.fp-models.svc:9090" {
		t.Fatalf("warmed route wrong: %+v", got)
	}
}

// TestRouteStore_Delete_IdempotentAndMirror verifies Delete removes the route from
// both memory and the mirror, and that deleting an absent route is a safe no-op
// (idempotent consumers: a redelivered ModelUndeployed must not error).
func TestRouteStore_Delete_IdempotentAndMirror(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	store := NewRouteStore(rdb)

	if err := store.Upsert(ctx, sampleRoute("churn")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := store.Delete(ctx, "churn"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Get(ctx, "churn"); err != domain.ErrNoRoute {
		t.Fatalf("after Delete Get = %v, want ErrNoRoute", err)
	}
	// Mirror must be gone too: a warmed replica shouldn't resurrect it.
	if n, err := rdb.HExists(ctx, routesHashKey, "churn").Result(); err != nil || n {
		t.Fatalf("mirror field still present after Delete (exists=%v err=%v)", n, err)
	}
	// Idempotent: second delete (now absent) is a no-op, not an error.
	if err := store.Delete(ctx, "churn"); err != nil {
		t.Fatalf("idempotent Delete(absent): %v", err)
	}
	if err := store.Delete(ctx, "never-existed"); err != nil {
		t.Fatalf("Delete(never-existed): %v", err)
	}
}

// TestRouteStore_SnapshotIsolation is the load-bearing test for the ports.go
// contract: a Route returned by Get must be a DEEP COPY that a concurrent Upsert
// cannot mutate underneath the reader. We run it with -race; a shared backing
// array would both (a) let the assertion below see mutated weights and (b) trip
// the race detector.
func TestRouteStore_SnapshotIsolation(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	store := NewRouteStore(rdb)

	if err := store.Upsert(ctx, sampleRoute("iso")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// Reader: repeatedly Get and scan Targets (the hot-path access pattern).
	// Writer: repeatedly Upsert new weights for the same model.
	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			r, err := store.Get(ctx, "iso")
			if err != nil {
				continue
			}
			// Read every target field — if Get aliased the store's backing array,
			// the writer below mutating WeightBps concurrently is a data race here.
			sum := 0
			for _, tgt := range r.Targets {
				sum += tgt.WeightBps + int(tgt.Status)
			}
			_ = sum
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			r := sampleRoute("iso")
			r.Targets[0].WeightBps = 5000 + (i % 100) // churn the weights
			r.Targets[1].WeightBps = 5000 - (i % 100)
			_ = store.Upsert(ctx, r)
		}
		close(stop)
	}()

	wg.Wait()

	// Additionally prove copy-on-read at the value level: mutate a returned slice
	// and confirm the store is untouched.
	got, err := store.Get(ctx, "iso")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got.Targets[0].WeightBps = -999 // caller scribbles on its copy
	again, err := store.Get(ctx, "iso")
	if err != nil {
		t.Fatalf("Get again: %v", err)
	}
	if again.Targets[0].WeightBps == -999 {
		t.Fatal("caller mutation of a returned Route leaked into the store (copy-on-read broken)")
	}
}

// TestRouteStore_List_CursorPaginationStable verifies cursor pagination returns
// every route exactly once across pages and is STABLE under a mid-pagination
// insert (the property that motivates a name-cursor over LIMIT/OFFSET).
func TestRouteStore_List_CursorPaginationStable(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	store := NewRouteStore(rdb)

	// Insert models m00..m09 (sorted names → deterministic order).
	for i := 0; i < 10; i++ {
		name := "m" + pad2(i)
		if err := store.Upsert(ctx, sampleRoute(name)); err != nil {
			t.Fatalf("Upsert %s: %v", name, err)
		}
	}

	// Page through in size-3 pages, collecting names.
	seen := map[string]int{}
	token := ""
	pages := 0
	for {
		routes, next, err := store.List(ctx, domain.ListOptions{PageSize: 3, PageToken: token})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		for _, r := range routes {
			seen[r.ModelName]++
		}
		pages++
		if next == "" {
			break
		}
		token = next
		if pages > 20 {
			t.Fatal("pagination did not terminate")
		}
		// Simulate a concurrent insert AFTER the first page: m05x sorts between
		// m05 and m06. A LIMIT/OFFSET pager could skip or duplicate a neighbor; a
		// name cursor must not. (We insert once.)
		if pages == 1 {
			if err := store.Upsert(ctx, sampleRoute("m05x")); err != nil {
				t.Fatalf("mid-page Upsert: %v", err)
			}
		}
	}

	// Every original model seen exactly once; none duplicated.
	for i := 0; i < 10; i++ {
		name := "m" + pad2(i)
		if seen[name] != 1 {
			t.Fatalf("model %s seen %d times, want exactly 1", name, seen[name])
		}
	}
	// The mid-page insert that sorts AFTER the cursor is included exactly once;
	// stability means: never duplicated, and no original is lost.
	if seen["m05x"] > 1 {
		t.Fatalf("mid-page insert m05x seen %d times, want 0 or 1 (never duplicated)", seen["m05x"])
	}
}

// TestRouteStore_List_DefaultPageSize confirms a zero PageSize falls back to the
// default rather than returning an empty page forever.
func TestRouteStore_List_DefaultPageSize(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	store := NewRouteStore(rdb)
	for i := 0; i < 5; i++ {
		if err := store.Upsert(ctx, sampleRoute("d"+pad2(i))); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
	}
	routes, next, err := store.List(ctx, domain.ListOptions{PageSize: 0})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(routes) != 5 || next != "" {
		t.Fatalf("default page returned %d routes (next=%q), want all 5 in one page", len(routes), next)
	}
}

func pad2(i int) string {
	s := strconv.Itoa(i)
	if len(s) == 1 {
		return "0" + s
	}
	return s
}
