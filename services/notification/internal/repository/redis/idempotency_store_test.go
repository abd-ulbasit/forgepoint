// idempotency_store_test.go — integration tests for the Redis IdempotencyStore
// against a REAL Redis (testcontainers). Verifies the Seen/Record contract on both
// dedup paths: a miss before Record, a hit after, the stored-result round-trip, the
// presence-with-no-payload case (the common service usage), TTL eviction, and the
// rejection of a non-positive TTL. Run -race.
package redisrepo

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
)

// newStore spins up a real Redis container and returns the adapter + a live client
// (for direct TTL assertions) + a context. Per-test container → full isolation.
func newStore(t *testing.T) (*IdempotencyStore, *goredis.Client, context.Context) {
	t.Helper()
	testutil.SkipIfNoDocker(t) // skip cleanly when the remote engine is unavailable

	addr := testutil.StartRedis(t) // returns a redis:// URL
	opt, err := goredis.ParseURL(addr)
	if err != nil {
		t.Fatalf("parse redis url %q: %v", addr, err)
	}
	rdb := goredis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })

	ctx := context.Background()
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	return NewIdempotencyStore(rdb), rdb, ctx
}

// TestIdempotency_MissThenRecordThenHit is the core dedup lifecycle: a key is unseen
// (so the caller proceeds), gets Recorded, and is then Seen — exactly the
// consumer/sync-mutation flow the port serves.
func TestIdempotency_MissThenRecordThenHit(t *testing.T) {
	store, _, ctx := newStore(t)
	key := "event:abc:user-alice"

	// Miss: never recorded → seen=false, no stored result, no error.
	seen, res, err := store.Seen(ctx, key)
	if err != nil {
		t.Fatalf("Seen(miss): %v", err)
	}
	if seen || res != nil {
		t.Fatalf("Seen(miss) = (%v, %v), want (false, nil)", seen, res)
	}

	// Record with NO payload (nil) — the common case (the service passes nil).
	if err := store.Record(ctx, key, nil, time.Hour); err != nil {
		t.Fatalf("Record: %v", err)
	}

	// Hit: now seen, but storedResult is nil because we stored no payload.
	seen, res, err = store.Seen(ctx, key)
	if err != nil {
		t.Fatalf("Seen(hit): %v", err)
	}
	if !seen {
		t.Fatal("Seen(hit) = false, want true after Record")
	}
	if res != nil {
		t.Fatalf("Seen(hit) storedResult = %v, want nil (no payload was stored)", res)
	}
}

// TestIdempotency_StoresAndReturnsResult verifies the optional result blob round-
// trips: a retry-return payload stored on Record comes back verbatim from Seen.
func TestIdempotency_StoresAndReturnsResult(t *testing.T) {
	store, _, ctx := newStore(t)
	key := "idem:prefs:user-bob:req-42"
	payload := []byte(`{"status":"applied"}`)

	if err := store.Record(ctx, key, payload, time.Hour); err != nil {
		t.Fatalf("Record: %v", err)
	}
	seen, res, err := store.Seen(ctx, key)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if !seen {
		t.Fatal("Seen = false, want true")
	}
	if string(res) != string(payload) {
		t.Fatalf("storedResult = %q, want %q", res, payload)
	}
}

// TestIdempotency_KeysAreNamespaced confirms two DIFFERENT logical keys don't
// collide (the per-user/per-event namespaces the service builds stay distinct), and
// that the same key is the only one that reads as seen.
func TestIdempotency_KeysAreNamespaced(t *testing.T) {
	store, _, ctx := newStore(t)

	if err := store.Record(ctx, "idem:test:user-alice:k1", nil, time.Hour); err != nil {
		t.Fatalf("Record alice: %v", err)
	}

	// A different user's same-suffix key is independent → unseen.
	seen, _, err := store.Seen(ctx, "idem:test:user-bob:k1")
	if err != nil {
		t.Fatalf("Seen bob: %v", err)
	}
	if seen {
		t.Fatal("bob's key reads as seen — namespaces collided")
	}
	// alice's exact key is seen.
	seen, _, err = store.Seen(ctx, "idem:test:user-alice:k1")
	if err != nil {
		t.Fatalf("Seen alice: %v", err)
	}
	if !seen {
		t.Fatal("alice's recorded key reads as unseen")
	}
}

// TestIdempotency_SetsTTL asserts Record applies the expiry under the physical key
// prefix, so dedup keys self-evict after the bounded window (no unbounded growth).
func TestIdempotency_SetsTTL(t *testing.T) {
	store, rdb, ctx := newStore(t)
	key := "event:ttl:user-x"

	if err := store.Record(ctx, key, nil, 30*time.Second); err != nil {
		t.Fatalf("Record: %v", err)
	}
	// Inspect the PHYSICAL key (prefixed) — the adapter applies keyPrefix.
	ttl, err := rdb.PTTL(ctx, keyPrefix+key).Result()
	if err != nil {
		t.Fatalf("PTTL: %v", err)
	}
	if ttl <= 0 || ttl > 30*time.Second {
		t.Fatalf("TTL = %v, want (0, 30s]", ttl)
	}
}

// TestIdempotency_RejectsNonPositiveTTL guards against an unbounded (leaking) key:
// Record must refuse a zero/negative TTL rather than store a never-expiring key.
func TestIdempotency_RejectsNonPositiveTTL(t *testing.T) {
	store, rdb, ctx := newStore(t)
	key := "event:no-ttl:user-y"

	if err := store.Record(ctx, key, nil, 0); err == nil {
		t.Fatal("Record with ttl=0 returned nil error, want a rejection")
	}
	// And nothing was written (no key with no expiry leaked).
	n, err := rdb.Exists(ctx, keyPrefix+key).Result()
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if n != 0 {
		t.Fatalf("a key was written despite the rejected ttl (exists=%d)", n)
	}
}

// TestIdempotency_RecordOverwrites confirms last-write-wins on Record (SET, not
// SETNX): re-Recording the same key with a new payload returns the NEW value. (The
// service never relies on this, but the semantics must be intentional, not
// accidental.)
func TestIdempotency_RecordOverwrites(t *testing.T) {
	store, _, ctx := newStore(t)
	key := "idem:prefs:user-z:req-1"

	if err := store.Record(ctx, key, []byte("first"), time.Hour); err != nil {
		t.Fatalf("Record first: %v", err)
	}
	if err := store.Record(ctx, key, []byte("second"), time.Hour); err != nil {
		t.Fatalf("Record second: %v", err)
	}
	_, res, err := store.Seen(ctx, key)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if string(res) != "second" {
		t.Fatalf("storedResult = %q, want %q (last-write-wins)", res, "second")
	}
}
