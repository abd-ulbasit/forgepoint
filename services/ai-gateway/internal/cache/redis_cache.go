// redis_cache.go — the per-team SEMANTIC CACHE over Redis. Implements
// domain.SemanticCache.
//
// ============================================================================
// WHAT THIS ADAPTER IS (and the security-critical tenancy invariant)
// ============================================================================
//
// The domain owns the cache POLICY (embed, threshold, hit/miss, fail-open); this
// adapter owns the I/O and, crucially, the PER-TEAM ISOLATION. Every entry for a team
// lives under a key namespaced BY that team — ai:cache:<team> — so a Lookup for team
// "A" can ONLY read A's keyspace and can NEVER return team B's cached completion. That
// matters because a cached completion contains another team's PROPRIETARY prompt and
// response text; a cross-tenant leak here is a data breach, not just a wrong answer.
// The team is the KEYSPACE, never a field on the stored entry, so there is no tenant
// field a caller could forge (mirroring how ChatRequest keeps `team` out of the body).
//
// ============================================================================
// STORE LAYOUT — a bounded, TTL'd per-team LIST (why a LIST, why bounded)
// ============================================================================
//
//	KEY                 TYPE  CONTENTS
//	──────────────────────────────────────────────────────────────────────────
//	ai:cache:<team>     LIST  up to `maxEntries` JSON-encoded CachedCompletion blobs,
//	                          newest at the HEAD (LPUSH), oldest evicted (LTRIM).
//
// WHY a capped LIST + TTL (the store-boundedness the task asks about): a semantic
// cache that grows without bound is a memory leak AND a slow lookup (the brute-force
// kNN scans every entry). We bound it TWO ways so it can never grow unbounded:
//   - SIZE: after each LPUSH we LTRIM 0..maxEntries-1, so a team holds at most
//     maxEntries recent completions (an LRU-ish window — newest kept, oldest dropped).
//   - TIME: every write resets a TTL on the key, so an idle team's cache self-evicts
//     and a stale answer can't be served forever (LLM answers go stale).
//
// LPUSH+LTRIM+EXPIRE in a single MULTI/EXEC pipeline makes the write atomic — a reader
// never sees a half-trimmed list, and the cap is enforced on every write.
//
// WHY a LIST and not Redis vector search (RediSearch/HNSW): the cosine kNN lives in
// the DOMAIN (cache.go, nearestByCosine) over a SMALL per-team bounded set — a linear
// scan of maxEntries (hundreds) × dim (~384) is microseconds and needs no Redis
// module. The adapter just stores/returns the candidate set; the domain ranks. A
// production multi-tenant cache at scale would shard into a real ANN index — the
// documented scaling path (see nearestByCosine's doc). Keeping the ranking in the
// domain also keeps the threshold/hit-miss POLICY unit-testable with a fake, no Redis.
//
// FAIL-OPEN CONTRACT: Lookup returns its error so the use-case treats a Redis blip as
// a MISS (serve normally); Store's error is best-effort (the client already has its
// answer). A broken cache must NEVER block or fail a chat — it is an optimization.
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
	goredis "github.com/redis/go-redis/v9"
)

// defaultMaxEntries / defaultTTL bound a team's cache when the config leaves them zero.
// 256 recent completions per team keeps the linear kNN scan cheap while giving a useful
// hit rate; a 24h TTL evicts idle teams and bounds answer staleness.
const (
	defaultMaxEntries = 256
	defaultTTL        = 24 * time.Hour
	keyPrefix         = "ai:cache:" // ai:cache:<team> — the per-team keyspace boundary.
)

// Config tunes the bounded store. Zero values fall back to the defaults above.
type Config struct {
	// MaxEntries caps a team's stored completions (LTRIM bound). <= 0 → defaultMaxEntries.
	MaxEntries int
	// TTL is the per-key expiry reset on every write. <= 0 → defaultTTL.
	TTL time.Duration
}

// RedisCache is the Redis-backed, per-team, bounded+TTL'd domain.SemanticCache.
type RedisCache struct {
	rdb        *goredis.Client
	maxEntries int
	ttl        time.Duration
}

// storedEntry is the JSON wire form of one cached completion. It MIRRORS
// domain.CachedCompletion EXCEPT it carries NO team — the team is the Redis key, not
// data, so a stored blob can't carry a forged tenant (the tenancy invariant made
// structural). The embedding travels as []float32 so a lookup can re-rank by cosine.
type storedEntry struct {
	Embedding []float32 `json:"embedding"`
	Prompt    string    `json:"prompt"`
	Response  string    `json:"response"`
	// Usage is flattened to its scalar fields (no nested domain type on the wire) so the
	// stored format is independent of the domain struct's Go shape.
	PromptTokens     int32 `json:"promptTokens"`
	CompletionTokens int32 `json:"completionTokens"`
	TotalTokens      int32 `json:"totalTokens"`
	CostMicroUSD     int64 `json:"costMicroUsd"`
	ServedBy         int   `json:"servedBy"` // domain.ProviderKind as its int code.
}

// NewRedisCache builds the adapter. A zero Config uses the bounded defaults.
func NewRedisCache(rdb *goredis.Client, cfg Config) *RedisCache {
	maxEntries := cfg.MaxEntries
	if maxEntries <= 0 {
		maxEntries = defaultMaxEntries
	}
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = defaultTTL
	}
	return &RedisCache{rdb: rdb, maxEntries: maxEntries, ttl: ttl}
}

// Compile-time proof we satisfy the domain port.
var _ domain.SemanticCache = (*RedisCache)(nil)

// key namespaces the team's keyspace. The team comes from verified claims (never a
// request field), and the fixed prefix bounds the keyspace a principal can address —
// the structural per-team isolation: a Lookup for team A can only ever read ai:cache:A.
func (c *RedisCache) key(team string) string { return keyPrefix + team }

// Lookup reads THIS TEAM's stored entries and returns the NEAREST to `embedding` by
// cosine similarity, plus its score. The hit/miss DECISION (threshold) stays in the
// domain — we return the best candidate and let the use-case apply s.cacheThreshold.
//
// found=false means the team has no cached entries (cold cache). A decode error on a
// single stored blob is SKIPPED (a corrupt entry shouldn't sink the whole lookup);
// only a Redis transport error is returned (fail-open → the use-case treats it a miss).
func (c *RedisCache) Lookup(ctx context.Context, team string, embedding []float32) (domain.CachedCompletion, float64, bool, error) {
	blobs, err := c.rdb.LRange(ctx, c.key(team), 0, int64(c.maxEntries-1)).Result()
	if err != nil {
		return domain.CachedCompletion{}, 0, false, fmt.Errorf("cache: LRange team %q: %w", team, err)
	}
	if len(blobs) == 0 {
		return domain.CachedCompletion{}, 0, false, nil
	}

	entries := make([]domain.CachedCompletion, 0, len(blobs))
	for _, blob := range blobs {
		var se storedEntry
		if err := json.Unmarshal([]byte(blob), &se); err != nil {
			// Skip a corrupt blob rather than fail the lookup — a poison entry must not
			// deny the team every future hit. It ages out via LTRIM/TTL.
			continue
		}
		entries = append(entries, se.toDomain())
	}
	if len(entries) == 0 {
		return domain.CachedCompletion{}, 0, false, nil
	}

	// Rank in the DOMAIN: the use-case exposes the nearest-neighbor search so the cosine
	// policy lives in one place. NearestByCosine returns (-1, 0) for an empty set, which
	// we already handled above.
	idx, sim := domain.NearestByCosine(embedding, entries)
	if idx < 0 {
		return domain.CachedCompletion{}, 0, false, nil
	}
	return entries[idx], sim, true, nil
}

// Store appends entry to THIS TEAM's bounded cache: LPUSH the new blob at the head,
// LTRIM to maxEntries (evict the oldest), and reset the TTL — all in ONE atomic
// pipeline so a concurrent reader never sees a half-trimmed list and the cap holds on
// every write. Best-effort per the port: a store error doesn't fail the completion the
// client already received (the use-case logs it).
func (c *RedisCache) Store(ctx context.Context, team string, entry domain.CachedCompletion) error {
	blob, err := json.Marshal(fromDomain(entry))
	if err != nil {
		return fmt.Errorf("cache: marshal entry for team %q: %w", team, err)
	}
	key := c.key(team)
	// TxPipeline = MULTI/EXEC: the three ops apply atomically on the Redis server.
	pipe := c.rdb.TxPipeline()
	pipe.LPush(ctx, key, blob)                     // newest at the head
	pipe.LTrim(ctx, key, 0, int64(c.maxEntries-1)) // SIZE bound: keep only maxEntries
	pipe.Expire(ctx, key, c.ttl)                   // TIME bound: reset idle-eviction TTL
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("cache: store pipeline for team %q: %w", team, err)
	}
	return nil
}

// toDomain maps the wire blob back to the domain entry.
func (se storedEntry) toDomain() domain.CachedCompletion {
	return domain.CachedCompletion{
		Embedding: se.Embedding,
		Prompt:    se.Prompt,
		Response:  se.Response,
		Usage: domain.TokenUsage{
			PromptTokens:     se.PromptTokens,
			CompletionTokens: se.CompletionTokens,
			TotalTokens:      se.TotalTokens,
			CostMicroUSD:     se.CostMicroUSD,
		},
		ServedBy: domain.ProviderKind(se.ServedBy),
	}
}

// fromDomain maps the domain entry to the wire blob (flattening Usage, dropping no
// team field because there is none — the team is the key).
func fromDomain(e domain.CachedCompletion) storedEntry {
	return storedEntry{
		Embedding:        e.Embedding,
		Prompt:           e.Prompt,
		Response:         e.Response,
		PromptTokens:     e.Usage.PromptTokens,
		CompletionTokens: e.Usage.CompletionTokens,
		TotalTokens:      e.Usage.TotalTokens,
		CostMicroUSD:     e.Usage.CostMicroUSD,
		ServedBy:         int(e.ServedBy),
	}
}
