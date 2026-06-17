// Package redisrepo is the Redis-backed ADAPTER for the Notification service's
// domain.IdempotencyStore port — the dedup guard on BOTH the async consumer path
// (NATS at-least-once redelivery) and the sync mutation path (client retries with
// an idempotency key).
//
// ============================================================================
// WHY REDIS FOR THIS PORT (and Postgres for the others)
// ============================================================================
//
// The IdempotencyStore port (ports.go) is "have I already done this (key)?" with
// an optional stored outcome and a TTL after which the key may be reclaimed. That
// is the textbook Redis use case: a single key, an atomic test-and-set, and a
// native per-key expiry. The port's own doc says it: "In production this is Redis
// SET NX with a TTL; in tests it is a map." So the inbox/prefs/delivery-log (the
// durable record-of-truth) live in Postgres, and the SHORT-LIVED dedup keys live in
// Redis where TTL eviction is free and the hot Seen()/Record() path costs one round
// trip.
//
// WHY a TTL (dedup is a BOUNDED-time guarantee, not forever): the keys exist only
// to swallow DUPLICATES within the redelivery/retry window. NATS won't redeliver an
// acked message indefinitely, and a client won't retry a request days later, so a
// key sized to outlive those windows (the service passes 24h) is enough. Keeping
// them forever would grow Redis unboundedly for no benefit — and the DURABLE
// idempotency backstop for the consumer path is the UNIQUE (event_id, recipient)
// index in Postgres anyway (belt and suspenders): even if a Redis key expires and a
// late redelivery slips past Seen(), the Create() still fails with
// ErrRepoAlreadyExists. So Redis is the fast path; Postgres is the durable floor.
//
// ============================================================================
// KEY LAYOUT
// ============================================================================
//
//	KEY (built by the SERVICE, opaque to us)        TYPE    TTL
//	────────────────────────────────────────────────────────────────────────
//	fp:notif:idem:<full-key>                         STRING  caller-supplied
//
// The SERVICE constructs the logical key namespace (event:<id>:<user> for the
// consumer path, idem:prefs:<user>:<key> / idem:test:<user>:<key> for the sync
// path — see notification_service_impl.go), so collisions between the two uses are
// already impossible. We add ONE physical prefix (fp:notif:idem:) so these keys
// never collide with any other Forgepoint Redis user sharing the instance.
// ============================================================================
package redisrepo

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/abd-ulbasit/forgepoint/services/notification/internal/domain"
)

// Compile-time proof the adapter satisfies the domain port.
var _ domain.IdempotencyStore = (*IdempotencyStore)(nil)

// keyPrefix namespaces every idempotency key under one physical prefix so the
// dedup keys can share a Redis instance with the gateway/feature-store/etc. without
// colliding.
const keyPrefix = "fp:notif:idem:"

// presenceMarker is what we store when Record is called with a NIL/empty result
// blob. A Redis STRING value cannot be empty-but-present in a way GET can
// distinguish from absent (GET returns redis.Nil for a missing key; an empty-string
// value is technically storable but conflates "stored nothing" with "not stored").
// So when the caller has no result bytes we store this sentinel byte to record
// PRESENCE unambiguously, and Seen() reports storedResult=nil for it (the caller
// asked us to store nothing, so we hand nothing back). This keeps "seen with no
// stored result" (the common case — every Record call in the service passes nil)
// crisply distinct from "not seen".
var presenceMarker = []byte{0x01}

// IdempotencyStore is the Redis adapter implementing domain.IdempotencyStore.
type IdempotencyStore struct {
	rdb *goredis.Client
}

// NewIdempotencyStore builds the adapter over an existing go-redis client. The
// client's lifecycle (Close) is owned by whoever created it (main.go / the test),
// not by this adapter — same ownership rule as the Postgres Store/pool split.
func NewIdempotencyStore(rdb *goredis.Client) *IdempotencyStore {
	return &IdempotencyStore{rdb: rdb}
}

// key applies the physical prefix to the caller's logical key.
func (s *IdempotencyStore) key(logical string) string { return keyPrefix + logical }

// Seen reports whether key has been recorded already AND, if so, the stored result
// bytes from the first time (nil if none were stored). A false `seen` means the
// caller should proceed and then call Record.
//
// One GET. A miss (redis.Nil) is the NORMAL first-call path and is NOT an error —
// we return (false, nil, nil) and the caller proceeds. The presenceMarker is
// translated back to a nil storedResult so "recorded with no payload" reads as seen
// with nothing stored.
func (s *IdempotencyStore) Seen(ctx context.Context, key string) (bool, []byte, error) {
	val, err := s.rdb.Get(ctx, s.key(key)).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return false, nil, nil // miss → caller proceeds with the operation
		}
		return false, nil, fmt.Errorf("redisrepo: idempotency seen: %w", err)
	}
	// Present. If it is the presence sentinel, the first call stored no payload.
	if len(val) == len(presenceMarker) && val[0] == presenceMarker[0] {
		return true, nil, nil
	}
	return true, val, nil
}

// Record marks key as processed, optionally storing a small result blob, with a TTL
// after which the key may be reclaimed.
//
// We use SET with an expiry (NOT SETNX): Record is the caller's deliberate "I have
// now completed this operation" — it runs AFTER the work succeeded, so it should
// always land the (possibly refreshed) value and reset the TTL window. The
// last-write-wins semantics are correct here: if a key were somehow Recorded twice
// the second time carries the same logical outcome (same key ⇒ same operation), so
// overwriting is harmless and keeps the TTL fresh. A non-positive ttl is rejected
// (we never want a key with NO expiry — that would defeat the bounded-time dedup and
// leak keys forever); the service always passes a positive idempotencyTTL.
func (s *IdempotencyStore) Record(ctx context.Context, key string, result []byte, ttl time.Duration) error {
	if ttl <= 0 {
		// Defensive: an unbounded idempotency key is a memory leak and a correctness
		// hazard (dedup must be bounded). Reject rather than silently storing forever.
		return fmt.Errorf("redisrepo: idempotency record: ttl must be positive, got %v", ttl)
	}
	val := result
	if len(val) == 0 {
		// No payload to store → record PRESENCE with the sentinel so Seen() can report
		// "seen" for it (an empty/nil result must still register the key as processed).
		val = presenceMarker
	}
	if err := s.rdb.Set(ctx, s.key(key), val, ttl).Err(); err != nil {
		return fmt.Errorf("redisrepo: idempotency record: %w", err)
	}
	return nil
}
