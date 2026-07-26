// redis_usage.go — the per-team prompt/completion token ACCUMULATOR over Redis.
// Implements domain.UsageStore (the GetUsage breakdown fix).
//
// ============================================================================
// WHY A SEPARATE STORE FROM THE BUDGET (the bug this fixes)
// ============================================================================
//
// GetUsage used to report a correct TOTAL but prompt=0/completion=0. The only per-team
// counter was the BudgetStore, whose Deduct takes a single `tokens int64` (the total)
// — it never saw the prompt/completion SPLIT, so the breakdown was always zero. The
// budget bucket is the right shape for a REFILLING rate cap; it is the WRONG shape for
// an accurate cumulative breakdown. This accumulator is the fix: a MONOTONIC per-team
// counter that records BOTH parts of every served completion.
//
//   - the budget REFILLS over a window (a rate cap that goes back up);
//   - this usage ACCUMULATES (a lifetime/window counter that only goes up) and keeps
//     prompt vs completion DISTINCT for the breakdown.
//
// ============================================================================
// STORE LAYOUT — one HASH per team, two atomic HINCRBY counters
// ============================================================================
//
//	KEY                 TYPE  FIELDS                       PURPOSE
//	──────────────────────────────────────────────────────────────────────────
//	ai:usage:<team>     HASH  prompt (int)                 cumulative prompt tokens
//	                          completion (int)             cumulative completion tokens
//
// total is DERIVED (prompt + completion) on read, never stored — it can't drift from
// the two parts if it isn't a third independent counter. HINCRBY is atomic on the
// Redis server, so concurrent completions from multiple gateway replicas accumulate
// without a lost update (the same single-atomic-op discipline as the budget's Lua).
//
// The TTL bounds an idle team's key (and gives the "window" a soft horizon); an active
// team's writes keep resetting it. The team comes from verified claims (never a request
// field) and the fixed prefix bounds the keyspace a principal can address — per-team
// isolated, same as the budget and cache.
package usage

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
)

const (
	keyPrefix       = "ai:usage:" // ai:usage:<team>
	fieldPrompt     = "prompt"
	fieldCompletion = "completion"
	defaultTTL      = 30 * 24 * time.Hour // 30d horizon for an idle team's accumulator.
)

// RedisUsage is the Redis-backed per-team token accumulator (domain.UsageStore).
type RedisUsage struct {
	rdb *goredis.Client
	ttl time.Duration
}

// Config tunes the accumulator. A zero TTL falls back to defaultTTL.
type Config struct {
	// TTL bounds an idle team's key. <= 0 → defaultTTL.
	TTL time.Duration
}

// NewRedisUsage builds the adapter.
func NewRedisUsage(rdb *goredis.Client, cfg Config) *RedisUsage {
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = defaultTTL
	}
	return &RedisUsage{rdb: rdb, ttl: ttl}
}

// Compile-time proof we satisfy the domain port.
var _ domain.UsageStore = (*RedisUsage)(nil)

func (u *RedisUsage) key(team string) string { return keyPrefix + team }

// Add records one served completion's prompt + completion tokens. Two atomic HINCRBYs
// plus a TTL refresh in ONE pipeline. Best-effort per the port — the use-case logs a
// failure and never surfaces it (the client already has its answer). A zero-token
// completion is a no-op (nothing to accumulate, and we don't want to create/refresh a
// key for it).
func (u *RedisUsage) Add(ctx context.Context, team string, promptTokens, completionTokens int32) error {
	if promptTokens <= 0 && completionTokens <= 0 {
		return nil
	}
	key := u.key(team)
	pipe := u.rdb.TxPipeline()
	if promptTokens > 0 {
		pipe.HIncrBy(ctx, key, fieldPrompt, int64(promptTokens))
	}
	if completionTokens > 0 {
		pipe.HIncrBy(ctx, key, fieldCompletion, int64(completionTokens))
	}
	pipe.Expire(ctx, key, u.ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("usage: accumulate for team %q: %w", team, err)
	}
	return nil
}

// Get returns the team's accumulated prompt/completion/total. total is DERIVED so it
// can't drift from the parts. A cold team (no key) reads as all-zeros, not an error —
// HMGET on a missing hash returns nils, which scan to 0.
func (u *RedisUsage) Get(ctx context.Context, team string) (promptTokens, completionTokens, totalTokens int64, err error) {
	vals, err := u.rdb.HMGet(ctx, u.key(team), fieldPrompt, fieldCompletion).Result()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("usage: read for team %q: %w", team, err)
	}
	prompt := parseRedisInt(vals[0])
	completion := parseRedisInt(vals[1])
	return prompt, completion, prompt + completion, nil
}

// parseRedisInt turns an HMGet result element (a string, or nil for a missing field)
// into an int64. A missing/garbage field reads as 0 (a cold counter), never an error —
// the accumulator is a best-effort observability counter, not the money ledger.
func parseRedisInt(v any) int64 {
	s, ok := v.(string)
	if !ok {
		return 0 // nil (missing field) or unexpected type → cold counter.
	}
	var n int64
	// Redis stores integers as decimal strings; Sscan tolerates leading/trailing space.
	if _, err := fmt.Sscan(s, &n); err != nil {
		return 0
	}
	return n
}
