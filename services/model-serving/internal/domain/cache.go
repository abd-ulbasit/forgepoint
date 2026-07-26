// cache.go — the Predict idempotency result cache.
//
// ============================================================================
// WHY A RESULT CACHE ON A "PURE" PREDICT (the subtle point)
// ============================================================================
//
// Predict is effectively pure (inputs → outputs math) and emits NO event, so
// you might think a retry is harmless. It IS harmless for the COMMON case — a
// network retry after a successful inference whose RESPONSE was lost — and that
// retry pays the full compute cost again. On an expensive model that doubles
// latency and CPU for no benefit. The cache, keyed by the client's
// idempotency_key, returns the prior result without re-running the engine.
//
// CORRECTNESS HAZARD (the part the old comment got wrong, now fixed):
// keying PURELY on the client-supplied idempotency_key is NOT correct on its
// own. If a buggy or malicious client reuses the SAME key with DIFFERENT inputs,
// a key-only cache would return the FIRST input's output for the second
// request — a silent WRONG prediction, and on a multi-tenant gateway a
// cross-request data-confusion hazard. The cache is "harmless for correctness"
// ONLY if the cached identity is bound to the request inputs.
//
// THE FIX — bind the cache identity to the inputs:
// each entry stores a fingerprint (a hash over the canonical, sorted input
// tensors). On lookup we recompute the caller's input fingerprint and compare:
//   - key present AND fingerprint matches → a genuine retry → cache HIT.
//   - key present BUT fingerprint differs → key REUSE with different inputs →
//     treated as a MISS (the engine recomputes the correct answer for THESE
//     inputs, and the new (key,inputs) result overwrites the stale entry).
// Treating the collision as a miss (recompute) rather than an error is the
// safe, caller-friendly choice: a confused client still gets the RIGHT answer
// for its actual inputs instead of a wrong cached one or a hard failure.
//
// CRITICAL FRAMING: this is a LATENCY/COMPUTE optimization, and the
// input-fingerprint binding is what makes it correctness-SAFE. Billing is the
// gateway's job (off events.InferenceCompleted); the serving pod emits nothing,
// so a cache hit vs. a recompute has no metering consequence. We flag from_cache
// purely as an observability signal. Saying "the cache is for cost, but it must
// bind to the inputs or it silently corrupts results on key reuse" shows you
// understand both WHY the pod is outside the billing path AND the subtle
// correctness trap of a key-only idempotency cache.
//
// EVICTION: a simple bounded FIFO. WHY not LRU: the access pattern is "a client
// retries a request it JUST made" — recency of INSERTION, not of access, is what
// matters, and FIFO is allocation-light and trivially correct. A pathological
// client can't grow memory unboundedly because the map is capped; the oldest
// keys roll off. (A production pod would add a short TTL too; the bounded size is
// the essential safety property and what we implement + test here.)
//
// CONCURRENCY: its own mutex so a cache hit never contends on the registry lock.
// ============================================================================
package domain

import (
	"encoding/binary"
	"hash/fnv"
	"sort"
	"sync"
)

// cacheEntry is one stored prediction: the output tensors PLUS the fingerprint
// of the inputs that produced them. The fingerprint is the correctness binding
// (see the file header): a key hit whose fingerprint disagrees is a miss.
type cacheEntry struct {
	fingerprint uint64
	outputs     map[string]Tensor
}

// resultCache is a bounded FIFO cache from idempotency key → cacheEntry.
type resultCache struct {
	mu      sync.Mutex
	max     int // capacity; <=0 disables caching
	entries map[string]cacheEntry
	order   []string // insertion order for FIFO eviction
}

// newResultCache returns a cache holding up to max entries. A max <= 0 yields a
// disabled cache (get always misses, put is a no-op) — lets a deployment turn
// the optimization off via config without branching at the call site.
func newResultCache(max int) *resultCache {
	return &resultCache{
		max:     max,
		entries: make(map[string]cacheEntry),
	}
}

// get returns the cached outputs for key IF the stored entry's input
// fingerprint equals `fingerprint`. A key present with a DIFFERENT fingerprint
// (the same idempotency key reused with different inputs) returns ok=false — a
// deliberate MISS so the engine recomputes the correct answer for THESE inputs
// rather than returning a stale, wrong result. This is the correctness binding
// the file header describes.
func (c *resultCache) get(key string, fingerprint uint64) (map[string]Tensor, bool) {
	if c.max <= 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if e.fingerprint != fingerprint {
		// Key reuse with different inputs → treat as a miss. We do NOT serve the
		// stale outputs (that would be the cross-request data-confusion bug).
		return nil, false
	}
	return e.outputs, true
}

// put stores outputs under key bound to their input `fingerprint`, evicting the
// oldest entry if at capacity. The caller passes an already-cloned map
// (cloneTensorMap), so the cache owns an immutable snapshot — see
// cloneTensorMap's rationale.
func (c *resultCache) put(key string, fingerprint uint64, outputs map[string]Tensor) {
	if c.max <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.entries[key]; exists {
		// Overwrite in place; keep its position in the FIFO order. This is also the
		// path that REPLACES a stale entry after a key-reuse miss: the recomputed
		// result for the new inputs (with the new fingerprint) supersedes the old.
		c.entries[key] = cacheEntry{fingerprint: fingerprint, outputs: outputs}
		return
	}

	// Evict the oldest while at capacity.
	for len(c.order) >= c.max && len(c.order) > 0 {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.entries, oldest)
	}
	c.entries[key] = cacheEntry{fingerprint: fingerprint, outputs: outputs}
	c.order = append(c.order, key)
}

// fingerprintInputs computes a stable, order-independent hash over the input
// tensor map so the cache can bind a cached result to the inputs that produced
// it. WHY this exact construction:
//
//   - DETERMINISTIC & ORDER-INDEPENDENT: a Go map has no iteration order, so we
//     SORT the tensor names first. Two requests with identical inputs always
//     produce the same fingerprint regardless of map iteration order.
//   - COLLISION-RESISTANT ENOUGH for this job: we are guarding against a client
//     ACCIDENTALLY (or sloppily) reusing a key with different inputs, not against
//     a cryptographic adversary who has crafted a hash collision — a 64-bit FNV
//     fingerprint is ample and dependency-light (stdlib hash/fnv). If two
//     genuinely different inputs ever collided, the only effect is a stale cache
//     hit for that one pathological pair; we are NOT using this for integrity or
//     auth, so a non-cryptographic hash is the right tradeoff.
//   - FULL FIELD COVERAGE: we fold in the tensor NAME, DTYPE, SHAPE, and DATA so
//     a change to ANY of them changes the fingerprint. Length prefixes between
//     fields prevent a "field-boundary" ambiguity (e.g. name "ab"+data "c" must
//     not hash like name "a"+data "bc").
//
// The returned hash is compared in get/put; it never leaves the domain.
func fingerprintInputs(inputs map[string]Tensor) uint64 {
	h := fnv.New64a()
	var scratch [8]byte

	// Sort the keys so iteration order does not affect the hash.
	names := make([]string, 0, len(inputs))
	for name := range inputs {
		names = append(names, name)
	}
	sort.Strings(names)

	writeChunk := func(b []byte) {
		// Length-prefix every chunk so adjacent fields can't be confused for one
		// another (canonical, unambiguous encoding).
		binary.BigEndian.PutUint64(scratch[:], uint64(len(b)))
		_, _ = h.Write(scratch[:])
		_, _ = h.Write(b)
	}

	for _, name := range names {
		t := inputs[name]
		writeChunk([]byte(name))

		// DType (fold the enum in as 8 bytes).
		binary.BigEndian.PutUint64(scratch[:], uint64(t.DType))
		_, _ = h.Write(scratch[:])

		// Shape: count then each dimension, so [1,4] and [4,1] differ.
		binary.BigEndian.PutUint64(scratch[:], uint64(len(t.Shape)))
		_, _ = h.Write(scratch[:])
		for _, dim := range t.Shape {
			binary.BigEndian.PutUint64(scratch[:], uint64(dim))
			_, _ = h.Write(scratch[:])
		}

		// Raw data bytes (length-prefixed).
		writeChunk(t.Data)
	}
	return h.Sum64()
}
