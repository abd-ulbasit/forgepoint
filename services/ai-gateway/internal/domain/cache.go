// cache.go — the SEMANTIC CACHE ports + the cosine-similarity math (pure domain).
//
// ============================================================================
// WHAT A SEMANTIC CACHE IS (and why it's different from a normal cache)
// ============================================================================
//
// A normal cache keys on an EXACT match: cache["GET /user/42"] hits only for that
// exact string. An LLM prompt cache keyed that way almost never hits — "What is the
// capital of France?" and "what's the capital of france" are different bytes but the
// SAME question. A SEMANTIC cache keys on MEANING: it embeds the prompt into a vector
// and a new prompt HITS if its vector is close enough (cosine similarity) to a stored
// one. The payoff is large: a cache hit skips the whole LLM call (latency → ~ms) and
// costs ZERO model tokens. This is exactly what GPTCache / Portkey / Helicone do.
//
//	prompt ──embed──▶ query vector ──nearest-neighbor by cosine──▶ best stored entry
//	                                                                  │
//	                          best similarity ≥ threshold? ──yes──▶ CACHE HIT (replay)
//	                                                          └─no──▶ MISS (call LLM, store)
//
// ============================================================================
// WHY THE PORTS LIVE HERE (Hexagonal: the domain owns the port)
// ============================================================================
//
// The use-case CONSUMES two new outbound capabilities — turning text into a vector
// (Embedder) and looking-up/storing cached completions (SemanticCache). Per the same
// discipline as ports.go, the CONSUMER declares the interface here and the adapters
// (the Ollama embeddings HTTP client, the Redis cache) implement it one ring out. The
// domain stays import-free of HTTP and Redis; the cosine math is pure and unit-tested.
//
// ============================================================================
// TENANCY — the security-critical invariant (per-team isolation)
// ============================================================================
//
// The cache is PER TEAM. Every Lookup/Store carries the claims-derived `team`, and
// the adapter namespaces its storage by that team so Team A can NEVER be served Team
// B's cached answer (a cross-tenant data leak — a completion can contain another
// team's proprietary prompt/response text). The team is a SEPARATE argument (never a
// field on the cached entry the caller could forge), mirroring how ChatRequest keeps
// `team` out of the request body. The port shape makes the isolation structural: a
// Lookup for team "A" passes "A"; the adapter can only read A's keyspace.
package domain

import (
	"context"
	"math"
)

// ============================================================================
// EMBEDDER PORT — text → vector
// ============================================================================

// Embedder turns text into a dense float32 vector (an "embedding"). The semantic
// cache embeds the prompt to compare meanings. It is a PORT because the concrete
// embedder is an HTTP call to Ollama's /api/embeddings (a tiny CPU model like
// all-minilm) — an outbound dependency the domain must not import directly.
//
// FAIL-OPEN CONTRACT: an embedding error (model cold, backend down) is RETURNED, not
// hidden. The use-case treats it as "cache unavailable" and serves via the providers
// anyway — a broken embedder must NEVER block a chat (the cache is an optimization,
// not a dependency). See the use-case's cache-check block.
type Embedder interface {
	// Embed returns the embedding vector for text. A non-nil error means the embedder
	// is unavailable; the caller falls back to a normal (uncached) completion.
	Embed(ctx context.Context, text string) ([]float32, error)
}

// ============================================================================
// SEMANTIC CACHE PORT — per-team nearest-neighbor store of completions
// ============================================================================

// CachedCompletion is one stored entry: the prompt's embedding (for similarity), the
// original prompt text (for debugging / exact-collision checks), the FULL response
// text to replay on a hit, the usage to report, and which provider originally served
// it (so a hit reports the same served_by). NO team field — the team is the keyspace,
// not data, so an entry can't carry a forged tenant.
type CachedCompletion struct {
	Embedding []float32
	Prompt    string
	Response  string
	Usage     TokenUsage
	ServedBy  ProviderKind
}

// SemanticCache is the per-team store of recent completions, queried by vector
// similarity. The adapter is Redis-backed (a capped per-team list with a TTL), but
// the port hides that: the use-case only knows "look up the nearest entry for THIS
// team, store a new one for THIS team".
//
// WHY a port and not a direct Redis call: same reason as BudgetStore — the domain
// owns the cache POLICY (threshold, hit/miss handling, fail-open) and the adapter
// owns the I/O. It also makes the per-team isolation and the threshold logic
// unit-testable with a fake map, no Redis.
type SemanticCache interface {
	// Lookup returns the nearest cached entry for `team` to `embedding` and its cosine
	// similarity. found=false means the team has no cached entries (cold cache). The
	// use-case applies the THRESHOLD to similarity to decide hit vs miss — the adapter
	// returns the BEST candidate and its score, never makes the hit/miss decision
	// (keeping the policy in the domain). A non-nil error (Redis down) is fail-open:
	// the use-case treats it as a miss and serves normally.
	Lookup(ctx context.Context, team string, embedding []float32) (entry CachedCompletion, similarity float64, found bool, err error)
	// Store appends entry to `team`'s cache (capped + TTL'd by the adapter so it can't
	// grow unbounded). Best-effort: a store error doesn't fail the completion the
	// client already received.
	Store(ctx context.Context, team string, entry CachedCompletion) error
}

// ============================================================================
// COSINE SIMILARITY — the meaning-distance metric (pure, unit-tested)
// ============================================================================
//
// Cosine similarity measures the ANGLE between two vectors, ignoring magnitude:
//
//	cos(θ) = (a · b) / (‖a‖ · ‖b‖)   ∈ [-1, 1]
//
// 1.0 = identical direction (same meaning), 0 = orthogonal (unrelated), -1 = opposite.
// We use cosine (not Euclidean distance) because embedding models place SEMANTICALLY
// similar text at similar ANGLES regardless of vector length — the industry-standard
// choice for text-embedding retrieval. A threshold near 0.95 means "near-duplicate
// question"; lower thresholds (0.85) trade precision for more hits.
//
// EDGE CASES (returned as 0 = "not similar", the safe default that yields a MISS):
//   - mismatched lengths (different embedding models / a corrupt entry): can't compare.
//   - a zero vector (‖·‖ == 0): division by zero; treat as no-similarity.
// Returning 0 (never NaN) keeps the threshold comparison total — a corrupt entry can
// never accidentally clear a 0.95 threshold.

// cosineSimilarity returns the cosine similarity of a and b in [-1, 1], or 0 for the
// degenerate cases (length mismatch, zero-norm) so a bad pair is always a MISS.
func cosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, normA, normB float64
	for i := range a {
		av, bv := float64(a[i]), float64(b[i])
		dot += av * bv
		normA += av * av
		normB += bv * bv
	}
	if normA == 0 || normB == 0 {
		return 0 // a zero vector has no direction — undefined angle, treat as not-similar.
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}

// NearestByCosine scans entries and returns the index of the one most similar to
// query and its similarity. Returns (-1, 0) for an empty set. This is a brute-force
// kNN (k=1) over a SMALL, per-team bounded set — O(n·d) where n is the per-team cap
// (hundreds) and d the embedding dim (~384 for all-minilm). At that size a linear
// scan in Go is faster and far simpler than standing up a vector index (FAISS/HNSW);
// the bound on n (the cache cap) is what keeps it cheap. A production multi-tenant
// cache at scale would shard into a real ANN index — called out as the scaling path.
//
// EXPORTED so the Redis cache adapter (internal/cache) can rank a team's candidate set
// here in the DOMAIN: the adapter fetches the bounded per-team list and this function
// applies the cosine policy, keeping the meaning-distance metric in one place (the
// adapter never re-implements similarity). The domain owns the ranking; the adapter
// owns the I/O.
func NearestByCosine(query []float32, entries []CachedCompletion) (int, float64) {
	best := -1
	var bestSim float64
	for i := range entries {
		sim := cosineSimilarity(query, entries[i].Embedding)
		if best == -1 || sim > bestSim {
			best, bestSim = i, sim
		}
	}
	return best, bestSim
}
