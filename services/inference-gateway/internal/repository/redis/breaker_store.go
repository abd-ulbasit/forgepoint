// breaker_store.go — the Redis-backed CROSS-REPLICA circuit-breaker state store.
//
// ============================================================================
// WHY THIS IS A STORE, NOT A `domain.BreakerRegistry` IMPLEMENTATION
// ============================================================================
//
// The domain's BreakerRegistry port returns a *domain.CircuitBreaker — a concrete,
// PURE type whose state machine (CLOSED→OPEN→HALF_OPEN) is in-process logic over
// an injected clock and a mutex, with all fields UNEXPORTED. That is correct by
// design: the breaker's transition arithmetic is domain logic and must stay
// unit-testable without I/O (see circuit_breaker.go). The in-memory breakerRegistry
// in the domain already satisfies the port for single-replica deployments and tests.
//
// A Redis adapter therefore CANNOT meaningfully "implement BreakerRegistry" by
// returning a *CircuitBreaker — it can't push that struct's private, lock-guarded
// counters into Redis, and it shouldn't try (that would duplicate the state machine
// in Lua and split the source of truth). What a multi-replica deployment actually
// needs is different and narrower:
//
//	SHARE THE FAILURE SIGNAL across replicas so one replica's observation of a sick
//	backend can trip the breaker for ALL replicas — instead of each replica having
//	to independently rack up `failureThreshold` failures before it protects the
//	backend (N replicas → up to N× the failures hit the sick backend before anyone
//	trips). This is the cross-replica outlier-detection signal, à la Envoy sharing
//	outlier stats, or a shared resilience4j registry.
//
// So this file is a focused, well-contracted STORE — not a registry. It holds the
// shared per-backend counters/state in Redis with ATOMIC increments and a single
// observable state field. The intended wiring (a later layer): a thin composite
// registry returns a *CircuitBreaker as today (the local fast gate) AND, on each
// RecordFailure/Success, mirrors the count here; replicas periodically read the
// shared state and force-open their local breaker when the GLOBAL count crosses the
// threshold. Keeping that composition out of this file keeps the adapter a clean,
// independently-testable persistence unit (the brief: test every store method
// against real Redis).
//
// ============================================================================
// STATE LAYOUT
// ============================================================================
//
//	KEY                              TYPE  FIELDS                  PURPOSE
//	──────────────────────────────────────────────────────────────────────────
//	fp:ig:breaker:<model>|<version>  HASH  state (int)            shared CircuitState
//	                                       fails (int)            global consecutive fails
//	                                       opened_ms (int, ms)    last OPEN transition
//
// One hash per (model,version) backend. WHY a struct-style "<model>|<version>" key:
// breakers are per backend, never per model (v2 OPEN while v1 CLOSED is the whole
// point during a bad canary). The '|' separator is safe because model names and
// version labels are validated identifiers upstream (no '|'); we still scope under
// fp:ig:breaker: so a backend key can't collide with rate-limit/quota/route keys.
// A TTL evicts breakers for backends that stop receiving traffic (undeployed
// versions), bounding memory without a sweeper.
package redisrepo

import (
	"context"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/abd-ulbasit/forgepoint/services/inference-gateway/internal/domain"
)

// BreakerStore holds shared, cross-replica circuit-breaker state in Redis.
type BreakerStore struct {
	rdb    *goredis.Client
	ttl    time.Duration
	prefix string
}

// BreakerSharedState is the read-back view of one backend's shared breaker state.
// It mirrors the observable fields of domain.CircuitState + the global counter, so
// the observability RPCs (or the composite registry) can read the cross-replica
// truth without reaching into a *CircuitBreaker.
type BreakerSharedState struct {
	State               domain.CircuitState
	ConsecutiveFailures int
	OpenedAt            time.Time // zero if never opened
}

// NewBreakerStore builds the adapter. ttl bounds how long an idle backend's state
// lingers (refreshed on every write); pass 0 to use a sensible default.
func NewBreakerStore(rdb *goredis.Client, ttl time.Duration) *BreakerStore {
	if ttl <= 0 {
		// 10 minutes: long enough to outlive normal traffic gaps and the breaker's
		// own 30s reset cycle many times over, short enough that an undeployed
		// version's state evicts itself well before it could mislead an operator.
		ttl = 10 * time.Minute
	}
	return &BreakerStore{rdb: rdb, ttl: ttl, prefix: "fp:ig:breaker:"}
}

// key builds the namespaced per-backend hash key. model/version are validated
// identifiers from the route table (server-authoritative), not client input.
func (s *BreakerStore) key(model, version string) string {
	return s.prefix + model + "|" + version
}

// recordFailureScript atomically increments the shared consecutive-failure count
// and, if it crosses failureThreshold (ARGV[1]) while CLOSED, flips the shared
// state to OPEN and stamps opened_ms. Doing the read-modify-write in one Lua call
// makes the cross-replica increment race-free: two replicas recording a failure at
// once can't both read the same pre-increment count and under-count toward the trip.
//
//	KEYS[1]            backend hash key
//	ARGV[1] threshold  failureThreshold (trip when fails >= threshold)
//	ARGV[2] now_ms     wall clock (we stamp opened_ms from the caller for testability)
//	ARGV[3] ttl_ms     idle-eviction TTL
//
// State integer encoding matches domain.CircuitState (0=CLOSED,1=OPEN,2=HALF_OPEN).
// RETURNS the new state integer.
const recordFailureScript = `
local threshold = tonumber(ARGV[1])
local now_ms    = tonumber(ARGV[2])
local ttl_ms    = tonumber(ARGV[3])

local state = tonumber(redis.call('HGET', KEYS[1], 'state')) or 0
local fails = tonumber(redis.call('HGET', KEYS[1], 'fails')) or 0

if state == 2 then
  -- HALF_OPEN: any failure re-opens immediately and restarts the cooldown.
  state = 1
  redis.call('HSET', KEYS[1], 'state', 1, 'fails', 0, 'opened_ms', now_ms)
else
  fails = fails + 1
  if state == 0 and fails >= threshold then
    state = 1
    redis.call('HSET', KEYS[1], 'state', 1, 'fails', fails, 'opened_ms', now_ms)
  else
    redis.call('HSET', KEYS[1], 'fails', fails)
  end
end
redis.call('PEXPIRE', KEYS[1], ttl_ms)
return state
`

// recordFailureScript is compiled once and reused (EVALSHA with EVAL fallback).
func (s *BreakerStore) script() *goredis.Script { return goredis.NewScript(recordFailureScript) }

// RecordFailure atomically bumps the shared failure count for (model,version) and
// returns the resulting shared CircuitState. failureThreshold is passed in so the
// store stays config-free (the domain owns the tuning). openedAt is stamped from
// the supplied now for deterministic tests; pass time.Now() in production.
func (s *BreakerStore) RecordFailure(ctx context.Context, model, version string, failureThreshold int, now time.Time) (domain.CircuitState, error) {
	res, err := s.script().Run(ctx, s.rdb,
		[]string{s.key(model, version)},
		failureThreshold,
		now.UnixMilli(),
		s.ttl.Milliseconds(),
	).Int64()
	if err != nil {
		return domain.CircuitClosed, fmt.Errorf("redisrepo: breaker record-failure %s/%s: %w", model, version, err)
	}
	return domain.CircuitState(res), nil
}

// RecordSuccess clears the shared failure count and returns the state to CLOSED.
// A success is unambiguous good news, so it's a simple HSET (no race-sensitive
// read-modify-write): whatever the count was, a success means "healthy now".
// We refresh the TTL in the same pipeline so an actively-succeeding backend's
// state never evicts mid-use.
func (s *BreakerStore) RecordSuccess(ctx context.Context, model, version string) error {
	key := s.key(model, version)
	pipe := s.rdb.TxPipeline() // MULTI/EXEC: the HSET + PEXPIRE apply atomically together
	pipe.HSet(ctx, key, "state", int(domain.CircuitClosed), "fails", 0)
	pipe.PExpire(ctx, key, s.ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redisrepo: breaker record-success %s/%s: %w", model, version, err)
	}
	return nil
}

// Get reads the shared breaker state for (model,version). A missing key means the
// backend has had no recorded failures → CLOSED with zero count (the safe, healthy
// default), so Get never errors on absence — it returns the CLOSED zero state.
func (s *BreakerStore) Get(ctx context.Context, model, version string) (BreakerSharedState, error) {
	fields, err := s.rdb.HMGet(ctx, s.key(model, version), "state", "fails", "opened_ms").Result()
	if err != nil {
		return BreakerSharedState{}, fmt.Errorf("redisrepo: breaker get %s/%s: %w", model, version, err)
	}
	return parseSharedState(fields), nil
}

// parseSharedState converts the HMGET result (a []interface{} of string|nil) into
// the typed view. Absent fields (nil) default to the healthy CLOSED zero state.
func parseSharedState(fields []interface{}) BreakerSharedState {
	out := BreakerSharedState{State: domain.CircuitClosed}
	if len(fields) >= 1 {
		if v, ok := toInt(fields[0]); ok {
			out.State = domain.CircuitState(v)
		}
	}
	if len(fields) >= 2 {
		if v, ok := toInt(fields[1]); ok {
			out.ConsecutiveFailures = v
		}
	}
	if len(fields) >= 3 {
		if v, ok := toInt(fields[2]); ok && v > 0 {
			out.OpenedAt = time.UnixMilli(int64(v))
		}
	}
	return out
}

// toInt parses a Redis hash field (go-redis returns hash values as strings) into
// an int. Returns ok=false for nil/missing/unparseable so callers keep the default.
func toInt(v interface{}) (int, bool) {
	s, ok := v.(string)
	if !ok {
		return 0, false
	}
	// Hash values can be stored as floats by the Lua bucket math elsewhere, but the
	// breaker fields are always integers; parse leniently via float then truncate.
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return int(f), true
}
