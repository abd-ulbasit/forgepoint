// cache_service_test.go — UNIT tests for the SEMANTIC CACHE stage of ChatCompletion.
//
// These prove the four cache-correctness behaviors the task calls out, with FAKE
// embedder + cache (no Redis, no HTTP), so they are fast and -race clean:
//   - near-dup HIT: a prompt whose embedding clears the threshold replays the stored
//     answer WITHOUT calling any provider and WITHOUT deducting tokens;
//   - different-prompt MISS: an embedding below threshold serves via the provider;
//   - embedder-error FAIL-OPEN: an Embed error serves normally (cache never blocks);
//   - store-on-MISS: a served completion is written to the cache for next time.
//
// They complement gateway_service_test.go (which wires NO embedder/cache, proving the
// cache-OFF path is unchanged).
package domain

import (
	"context"
	"errors"
	"testing"
	"time"
)

// --- cache fakes ------------------------------------------------------------

// fakeEmbedder returns a CANNED vector per prompt text (a tiny lookup table) so a test
// controls exactly which prompts are "near" each other. An unknown prompt returns the
// zeroVec (orthogonal to everything → similarity 0 → miss). embedErr forces the
// fail-open path.
type fakeEmbedder struct {
	vectors  map[string][]float32
	embedErr error
	calls    int
}

func (e *fakeEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	e.calls++
	if e.embedErr != nil {
		return nil, e.embedErr
	}
	if v, ok := e.vectors[text]; ok {
		return v, nil
	}
	return []float32{0, 0, 0}, nil
}

// fakeCache is an in-memory per-team SemanticCache. It stores entries per team and
// returns the nearest by cosine (reusing the domain's own NearestByCosine so the test
// exercises the real ranking). lookupErr / storeErr force the error paths.
type fakeCache struct {
	byTeam     map[string][]CachedCompletion
	lookupErr  error
	storeErr   error
	storeCalls int
	lastTeam   string
}

func newFakeCache() *fakeCache { return &fakeCache{byTeam: map[string][]CachedCompletion{}} }

func (c *fakeCache) Lookup(_ context.Context, team string, embedding []float32) (CachedCompletion, float64, bool, error) {
	if c.lookupErr != nil {
		return CachedCompletion{}, 0, false, c.lookupErr
	}
	entries := c.byTeam[team]
	if len(entries) == 0 {
		return CachedCompletion{}, 0, false, nil
	}
	idx, sim := NearestByCosine(embedding, entries)
	if idx < 0 {
		return CachedCompletion{}, 0, false, nil
	}
	return entries[idx], sim, true, nil
}

func (c *fakeCache) Store(_ context.Context, team string, entry CachedCompletion) error {
	c.storeCalls++
	c.lastTeam = team
	if c.storeErr != nil {
		return c.storeErr
	}
	c.byTeam[team] = append(c.byTeam[team], entry)
	return nil
}

// newCachedService wires a service with a cache, an embedder, and an optional budget.
func newCachedService(t *testing.T, emb Embedder, cache SemanticCache, budget BudgetStore, usage UsageStore, provs ...Provider) GatewayService {
	t.Helper()
	return NewGatewayService(ServiceDeps{
		Providers:      NewProviderRegistry(provs...),
		Breakers:       NewBreakerRegistry(BreakerTuning{}, time.Now),
		Budget:         budget,
		Usage:          usage,
		Embedder:       emb,
		Cache:          cache,
		CacheThreshold: 0.95,
		Now:            time.Now,
	})
}

// userReq builds a one-message request. The concatenated prompt promptForEmbedding
// produces for it is "user: <content>" — that is the key the fakeEmbedder maps.
func userReq(content string) ChatRequest {
	return ChatRequest{Messages: []Message{{Role: RoleUser, Content: content}}}
}

// --- tests ------------------------------------------------------------------

func TestCache_NearDuplicateHitSkipsProviderAndDeduct(t *testing.T) {
	t.Parallel()
	// Two prompts map to IDENTICAL vectors → cosine 1.0 ≥ 0.95 threshold → HIT. The
	// provider must NOT be called and the budget must NOT be deducted (a hit costs zero
	// tokens). The client must receive the STORED response text.
	vec := []float32{1, 0, 0}
	emb := &fakeEmbedder{vectors: map[string][]float32{
		"user: What is the capital of France?": vec,
		"user: what's the capital of france":   vec, // near-duplicate → same vector.
	}}
	cache := newFakeCache()
	// Pre-seed team-a's cache with the stored answer for the first phrasing.
	cache.byTeam["team-a"] = []CachedCompletion{{
		Embedding: vec,
		Prompt:    "user: What is the capital of France?",
		Response:  "Paris.",
		Usage:     TokenUsage{PromptTokens: 5, CompletionTokens: 1, TotalTokens: 6},
		ServedBy:  ProviderKindOllama,
	}}

	prov := &fakeProvider{kind: ProviderKindStub, words: []string{"SHOULD NOT BE CALLED"}}
	bud := &fakeBudget{budget: 1000}
	svc := newCachedService(t, emb, cache, bud, nil, prov)

	var got []string
	completion, err := svc.ChatCompletion(context.Background(), "team-a",
		userReq("what's the capital of france"), collectSink(&got))
	if err != nil {
		t.Fatalf("expected a cache hit, got error: %v", err)
	}
	if !completion.CacheHit {
		t.Fatalf("expected CacheHit=true on a near-duplicate, got false")
	}
	if prov.calls != 0 {
		t.Fatalf("provider must NOT be called on a cache hit, calls = %d", prov.calls)
	}
	if bud.deductCalls != 0 {
		t.Fatalf("budget must NOT be deducted on a cache hit, deductCalls = %d", bud.deductCalls)
	}
	if join(got) != "Paris." {
		t.Fatalf("client should receive the cached response, got %q", join(got))
	}
	if completion.ServedBy != ProviderKindOllama {
		t.Fatalf("hit should report the ORIGINAL served-by (ollama), got %v", completion.ServedBy)
	}
}

func TestCache_DifferentPromptMissesAndServes(t *testing.T) {
	t.Parallel()
	// The query vector is ORTHOGONAL to the stored entry (cosine 0 < 0.95) → MISS → the
	// provider serves. The provider IS called, and the answer streams from it.
	emb := &fakeEmbedder{vectors: map[string][]float32{
		"user: a completely different question": {0, 1, 0}, // orthogonal to the stored {1,0,0}.
	}}
	cache := newFakeCache()
	cache.byTeam["team-a"] = []CachedCompletion{{
		Embedding: []float32{1, 0, 0},
		Prompt:    "user: capital of france",
		Response:  "Paris.",
		Usage:     TokenUsage{PromptTokens: 5, CompletionTokens: 1},
		ServedBy:  ProviderKindOllama,
	}}

	prov := &fakeProvider{kind: ProviderKindStub, words: []string{"fresh ", "answer"}, promptTok: 2, outputTok: 2}
	svc := newCachedService(t, emb, cache, nil, nil, prov)

	var got []string
	completion, err := svc.ChatCompletion(context.Background(), "team-a",
		userReq("a completely different question"), collectSink(&got))
	if err != nil {
		t.Fatalf("expected a miss + serve, got error: %v", err)
	}
	if completion.CacheHit {
		t.Fatalf("expected CacheHit=false on a different prompt, got true")
	}
	if prov.calls != 1 {
		t.Fatalf("provider must serve on a miss, calls = %d", prov.calls)
	}
	if join(got) != "fresh answer" {
		t.Fatalf("client should receive the provider's answer, got %q", join(got))
	}
}

func TestCache_EmbedderErrorFailsOpenAndServes(t *testing.T) {
	t.Parallel()
	// The embedder errors → FAIL-OPEN: the cache is skipped and the provider serves. No
	// lookup, no store (the embedding never existed).
	emb := &fakeEmbedder{embedErr: errors.New("embedding model cold")}
	cache := newFakeCache()
	prov := &fakeProvider{kind: ProviderKindStub, words: []string{"served ", "anyway"}, promptTok: 1, outputTok: 1}
	svc := newCachedService(t, emb, cache, nil, nil, prov)

	var got []string
	completion, err := svc.ChatCompletion(context.Background(), "team-a",
		userReq("anything"), collectSink(&got))
	if err != nil {
		t.Fatalf("embedder error must fail open (serve), got error: %v", err)
	}
	if completion.CacheHit {
		t.Fatalf("a fail-open serve is never a hit, got CacheHit=true")
	}
	if prov.calls != 1 {
		t.Fatalf("provider must serve on fail-open, calls = %d", prov.calls)
	}
	if cache.storeCalls != 0 {
		t.Fatalf("a fail-open (no embedding) request must NOT store, storeCalls = %d", cache.storeCalls)
	}
	if join(got) != "served anyway" {
		t.Fatalf("client should receive the provider's answer, got %q", join(got))
	}
}

func TestCache_StoresOnMiss(t *testing.T) {
	t.Parallel()
	// A cold cache → MISS → serve, and then the completion is STORED for THIS team with
	// the embedding, the streamed response text, and the served usage.
	queryVec := []float32{0.5, 0.5, 0}
	emb := &fakeEmbedder{vectors: map[string][]float32{
		"user: brand new question": queryVec,
	}}
	cache := newFakeCache() // empty → cold.
	prov := &fakeProvider{kind: ProviderKindStub, words: []string{"the ", "answer"}, promptTok: 3, outputTok: 2}
	svc := newCachedService(t, emb, cache, nil, nil, prov)

	var got []string
	if _, err := svc.ChatCompletion(context.Background(), "team-a",
		userReq("brand new question"), collectSink(&got)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cache.storeCalls != 1 {
		t.Fatalf("expected exactly one store-on-miss, got %d", cache.storeCalls)
	}
	if cache.lastTeam != "team-a" {
		t.Fatalf("store team = %q, want team-a", cache.lastTeam)
	}
	stored := cache.byTeam["team-a"]
	if len(stored) != 1 {
		t.Fatalf("expected one stored entry, got %d", len(stored))
	}
	if stored[0].Response != "the answer" {
		t.Fatalf("stored response = %q, want %q (the streamed text)", stored[0].Response, "the answer")
	}
	if stored[0].Usage.PromptTokens != 3 || stored[0].Usage.CompletionTokens != 2 {
		t.Fatalf("stored usage = %+v, want prompt=3 completion=2", stored[0].Usage)
	}
	// The stored embedding must be the QUERY embedding (so a re-ask hits).
	if len(stored[0].Embedding) != 3 || stored[0].Embedding[0] != 0.5 {
		t.Fatalf("stored embedding = %v, want the query vector", stored[0].Embedding)
	}
}

func TestCache_LookupErrorFailsOpenAndStillStores(t *testing.T) {
	t.Parallel()
	// A Redis blip on LOOKUP is fail-open (serve normally). Because the embedding DID
	// succeed (cacheEligible), the miss-store still runs — the lookup error doesn't
	// disable the store half.
	emb := &fakeEmbedder{vectors: map[string][]float32{"user: q": {1, 0, 0}}}
	cache := newFakeCache()
	cache.lookupErr = errors.New("redis down on lookup")
	prov := &fakeProvider{kind: ProviderKindStub, words: []string{"ok"}, promptTok: 1, outputTok: 1}
	svc := newCachedService(t, emb, cache, nil, nil, prov)

	if _, err := svc.ChatCompletion(context.Background(), "team-a",
		userReq("q"), func(Delta) error { return nil }); err != nil {
		t.Fatalf("lookup error must fail open, got %v", err)
	}
	if prov.calls != 1 {
		t.Fatalf("provider must serve on lookup fail-open, calls = %d", prov.calls)
	}
	if cache.storeCalls != 1 {
		t.Fatalf("store should still run after a lookup error (embedding was valid), got %d", cache.storeCalls)
	}
}

func TestCache_RecordsUsageBreakdownOnServe(t *testing.T) {
	t.Parallel()
	// THE BREAKDOWN FIX at the domain level: a served (uncached) completion records the
	// prompt/completion SPLIT in the UsageStore, and Usage() returns that non-zero
	// breakdown — not prompt=0/completion=0.
	usage := &fakeUsage{}
	prov := &fakeProvider{kind: ProviderKindStub, words: []string{"hi"}, promptTok: 9, outputTok: 4}
	// No embedder/cache here — the breakdown fix is independent of the cache.
	svc := NewGatewayService(ServiceDeps{
		Providers: NewProviderRegistry(prov),
		Breakers:  NewBreakerRegistry(BreakerTuning{}, time.Now),
		Usage:     usage,
		Now:       time.Now,
	})

	if _, err := svc.ChatCompletion(context.Background(), "team-a",
		userReq("x"), func(Delta) error { return nil }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if usage.addCalls != 1 {
		t.Fatalf("expected one usage.Add, got %d", usage.addCalls)
	}
	summary, err := svc.Usage(context.Background(), "team-a")
	if err != nil {
		t.Fatalf("Usage error: %v", err)
	}
	if summary.PromptTokens != 9 || summary.CompletionTokens != 4 || summary.TotalTokens != 13 {
		t.Fatalf("breakdown = prompt %d / completion %d / total %d, want 9/4/13 (the fix)",
			summary.PromptTokens, summary.CompletionTokens, summary.TotalTokens)
	}
}

// fakeUsage is an in-memory UsageStore accumulator for the breakdown test.
type fakeUsage struct {
	prompt     int64
	completion int64
	addCalls   int
}

func (u *fakeUsage) Add(_ context.Context, _ string, promptTokens, completionTokens int32) error {
	u.addCalls++
	u.prompt += int64(promptTokens)
	u.completion += int64(completionTokens)
	return nil
}

func (u *fakeUsage) Get(_ context.Context, _ string) (int64, int64, int64, error) {
	return u.prompt, u.completion, u.prompt + u.completion, nil
}
