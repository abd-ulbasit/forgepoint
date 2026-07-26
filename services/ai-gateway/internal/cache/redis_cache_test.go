// redis_cache_test.go — INTEGRATION tests for the Redis semantic cache adapter.
//
// These run against a REAL Redis (testcontainers) because the whole point of the
// adapter is its Redis behavior — the per-team key namespacing (the SECURITY
// invariant), the LTRIM size bound, and the TTL — none of which a mock would exercise
// faithfully. testutil.StartRedis skips the test when Docker is unavailable (the
// remote-offload setup), so the suite stays green on a Docker-less machine.
//
// THE HEADLINE TEST (TestCache_TeamIsolation) proves the security-critical invariant:
// a completion Stored for Team A is NEVER returned to a Team B Lookup — a cross-tenant
// cache leak would expose one team's proprietary prompt/response to another.
package cache

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
)

// newTestCache spins up a real Redis and returns a cache adapter + the raw client (for
// assertions on the underlying keys). The given Config tunes the bound/TTL per test.
func newTestCache(t *testing.T, cfg Config) (*RedisCache, *goredis.Client) {
	t.Helper()
	addr := testutil.StartRedis(t) // skips if no Docker.
	opts, err := goredis.ParseURL(addr)
	if err != nil {
		t.Fatalf("parse redis url %q: %v", addr, err)
	}
	rdb := goredis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	return NewRedisCache(rdb, cfg), rdb
}

// vec is a tiny helper for a 3-dim embedding.
func vec(a, b, c float32) []float32 { return []float32{a, b, c} }

func TestCache_TeamIsolation(t *testing.T) {
	t.Parallel()
	c, rdb := newTestCache(t, Config{})
	ctx := context.Background()

	// Team A stores a proprietary completion.
	teamAEntry := domain.CachedCompletion{
		Embedding: vec(1, 0, 0),
		Prompt:    "user: team A secret question",
		Response:  "team A's confidential answer",
		Usage:     domain.TokenUsage{PromptTokens: 5, CompletionTokens: 3, TotalTokens: 8},
		ServedBy:  domain.ProviderKindOllama,
	}
	if err := c.Store(ctx, "team-a", teamAEntry); err != nil {
		t.Fatalf("store for team-a: %v", err)
	}

	// Team B looks up with the EXACT SAME embedding A stored. If isolation were broken,
	// cosine 1.0 would clear any threshold and A's answer would leak to B. It must NOT:
	// B has a cold cache, so found=false.
	_, _, found, err := c.Lookup(ctx, "team-b", vec(1, 0, 0))
	if err != nil {
		t.Fatalf("lookup for team-b: %v", err)
	}
	if found {
		t.Fatalf("CROSS-TENANT LEAK: team-b lookup returned an entry team-a stored")
	}

	// Sanity: team A itself DOES get its own entry back (the cache works).
	entry, sim, foundA, err := c.Lookup(ctx, "team-a", vec(1, 0, 0))
	if err != nil {
		t.Fatalf("lookup for team-a: %v", err)
	}
	if !foundA {
		t.Fatalf("team-a should find its own stored entry")
	}
	if sim < 0.999 {
		t.Fatalf("identical embedding should score ~1.0, got %v", sim)
	}
	if entry.Response != "team A's confidential answer" {
		t.Fatalf("team-a got the wrong entry: %q", entry.Response)
	}

	// The keys must be physically distinct namespaces.
	if rdb.Exists(ctx, "ai:cache:team-a").Val() != 1 {
		t.Fatalf("expected ai:cache:team-a to exist")
	}
	if rdb.Exists(ctx, "ai:cache:team-b").Val() != 0 {
		t.Fatalf("team-b must have NO cache key (it stored nothing)")
	}
}

func TestCache_BoundedByMaxEntries(t *testing.T) {
	t.Parallel()
	// Store more than the cap and prove the per-team LIST is LTRIM'd to maxEntries — the
	// store-boundedness guarantee (it can't grow unbounded).
	const cap = 3
	c, rdb := newTestCache(t, Config{MaxEntries: cap})
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		entry := domain.CachedCompletion{
			Embedding: vec(float32(i), 1, 0),
			Prompt:    "user: q",
			Response:  "a",
		}
		if err := c.Store(ctx, "team-a", entry); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}

	n := rdb.LLen(ctx, "ai:cache:team-a").Val()
	if n != cap {
		t.Fatalf("per-team list length = %d, want capped at %d (LTRIM bound)", n, cap)
	}
}

func TestCache_StoreSetsTTL(t *testing.T) {
	t.Parallel()
	// Prove every write sets a TTL so an idle team's cache self-evicts (the TIME bound).
	c, rdb := newTestCache(t, Config{TTL: 90 * time.Second})
	ctx := context.Background()

	if err := c.Store(ctx, "team-a", domain.CachedCompletion{Embedding: vec(1, 0, 0), Response: "a"}); err != nil {
		t.Fatalf("store: %v", err)
	}
	ttl := rdb.TTL(ctx, "ai:cache:team-a").Val()
	if ttl <= 0 || ttl > 90*time.Second {
		t.Fatalf("expected a TTL in (0, 90s], got %v", ttl)
	}
}

func TestCache_ColdLookupIsMissNotError(t *testing.T) {
	t.Parallel()
	c, _ := newTestCache(t, Config{})
	_, _, found, err := c.Lookup(context.Background(), "team-nobody", vec(1, 0, 0))
	if err != nil {
		t.Fatalf("a cold lookup must not error, got %v", err)
	}
	if found {
		t.Fatalf("a cold cache must report found=false")
	}
}
