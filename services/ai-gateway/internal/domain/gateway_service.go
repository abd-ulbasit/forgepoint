// gateway_service.go — the GatewayService use-case: the resilience + cost core.
//
// ============================================================================
// WHAT THIS SERVICE COMPOSES (the request lifecycle, top to bottom)
// ============================================================================
//
//	ChatCompletion(team, req, sink):
//	  1. VALIDATE        — reject empty/malformed requests (ErrInvalidInput).
//	  2. BUDGET GATE     — Check the team's token budget; over budget → reject
//	                       (ErrBudgetExceeded) BEFORE any provider call. Team comes
//	                       from claims (the caller), never the request body.
//	  3. SEMANTIC CACHE  — embed the prompt and look up the nearest cached entry for
//	                       THIS TEAM by cosine similarity. If best ≥ threshold → CACHE
//	                       HIT: replay the stored response WITHOUT calling the LLM and
//	                       WITHOUT deducting model tokens (a hit costs no tokens). The
//	                       cache is FAIL-OPEN: an embedder/cache error just skips to a
//	                       normal completion (never blocks chat). Per-team isolated.
//	  4. WARM SIGNAL     — publish the warm event FIRST so a scaled-to-zero Ollama
//	                       starts coming up (KEDA 0->1) while we attempt the call.
//	  5. ROUTE + FAILOVER— build the ordered candidate list (pinned provider, else
//	                       the default order), and for each candidate:
//	                         - SKIP if its circuit is OPEN (breaker.Allow() == false),
//	                         - else stream it; on a SYNCHRONOUS start error or an
//	                           in-band error frame, RecordFailure and try the next;
//	                         - on a clean terminal frame, RecordSuccess and stop.
//	                       If every candidate is skipped/fails → emit a terminal
//	                       FinishReasonError frame and return ErrAllProvidersFailed.
//	  6. METER + DEDUCT  — compute cost, Deduct the used tokens from the team budget
//	                       (post-serve, best-effort), record the per-team usage
//	                       breakdown, STORE the (embedding, prompt, response, usage)
//	                       in the semantic cache, publish fp.ai.completion.served.
//
// ============================================================================
// WHY A SINK CALLBACK INSTEAD OF RETURNING A CHANNEL
// ============================================================================
//
// The handler must forward each delta to the gRPC ServerStream AS it arrives. If
// the domain returned a channel, the handler would range it — fine — but then the
// domain couldn't cleanly (a) substitute the terminal frame's served_by/usage that
// only IT knows post-failover, and (b) keep the failover loop's bookkeeping in one
// place. Instead the handler passes a `sink func(Delta) error`: the domain calls
// it for every NON-terminal delta (the live tokens) and RETURNS the terminal
// Completion summary for the handler to build the final frame. This keeps the
// domain in control of the resilience/cost decisions while the handler owns the
// wire format — and the sink's error (client disconnected) cancels the loop. This
// is the same "push, don't return a channel" shape gRPC server-streaming wants.
//
// FAILOVER + STREAMING TENSION (interview-critical): once we've streamed tokens
// from provider A and A then errors mid-stream, we CANNOT silently retry on B —
// the client already saw A's partial tokens, and replaying B's full answer would
// duplicate/garble the output. So mid-stream failures are TERMINAL (the client
// gets a FinishReasonError frame). Failover only happens BEFORE the first token is
// forwarded: a provider that fails to START (or trips OPEN) is skipped with nothing
// emitted yet. We therefore buffer NOTHING and forward the first delta only after
// the provider has proven it can stream — see the per-candidate logic below.
//
// HOW THE INVARIANT IS ENFORCED (not just documented): streamOne tracks whether it
// has forwarded ANY non-terminal delta for THIS attempt. If it has, and the SAME
// stream then errors (an in-band FinishReasonError frame OR an early channel close
// with no Done frame — e.g. an OOM-killed Ollama dropping mid-stream), streamOne
// returns the errStreamAborted sentinel. The failover loop treats errStreamAborted
// exactly like errSinkFailed: STOP, emit the terminal FinishReasonError frame, do
// NOT attempt the next provider. Only a START error (Chat returns non-nil) or an
// error frame seen BEFORE any token was forwarded is recoverable failover.
// ============================================================================
package domain

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// Sink is the callback the handler passes to ChatCompletion. The domain calls it
// for each incremental (non-terminal) delta; the handler forwards it to the gRPC
// stream. A non-nil return (client gone / send failed) aborts the completion.
type Sink func(Delta) error

// GatewayService is the AI Gateway use-case port the handler depends on. One
// method that streams via the sink and returns the terminal summary (or an error).
type GatewayService interface {
	// ChatCompletion routes req to a provider with failover, enforces the team
	// budget, meters cost, and pushes deltas to sink. team is the claims-derived
	// tenant (never the request body). Returns the terminal Completion summary; on
	// total failure returns (summary-with-FinishReasonError, ErrAllProvidersFailed).
	ChatCompletion(ctx context.Context, team string, req ChatRequest, sink Sink) (Completion, error)
	// Providers returns the read-only snapshot list for ListProviders.
	Providers() []ProviderSnapshot
	// Usage returns the team's usage for GetUsage: the prompt/completion/total token
	// BREAKDOWN (from the UsageStore accumulator — the bug fix) plus the budget +
	// remaining (from the BudgetStore). Returning the split here is what makes GetUsage
	// report a non-zero prompt/completion breakdown instead of prompt=0/completion=0.
	Usage(ctx context.Context, team string) (UsageSummary, error)
}

// UsageSummary is the GetUsage read model: the token BREAKDOWN from the usage
// accumulator and the budget/remaining from the budget bucket. It exists so the
// handler gets the prompt/completion SPLIT (the breakdown fix) in one value rather
// than a five-return-value signature.
type UsageSummary struct {
	PromptTokens     int64
	CompletionTokens int64
	TotalTokens      int64
	BudgetTokens     int64
	RemainingTokens  int64
}

// ServiceDeps is the constructor injection bag. Everything is a PORT; the service
// depends on interfaces, never concrete adapters.
type ServiceDeps struct {
	// Providers is the registry of enabled backends, in DEFAULT FAILOVER ORDER. The
	// first that can serve a request (circuit CLOSED, model matches) wins; the rest
	// are fallbacks. Order matters: put the preferred backend first (Ollama), the
	// always-available deterministic stub LAST so it's the ultimate fallback.
	Providers *ProviderRegistry
	// Breakers guards each provider; the failover loop consults it before each call.
	Breakers BreakerRegistry
	// Budget is the per-team token budget gate (check pre-flight, deduct post-serve).
	Budget BudgetStore
	// Usage is the per-team prompt/completion/total accumulator that backs GetUsage's
	// BREAKDOWN (the bug fix). Separate from Budget because usage accumulates while the
	// budget refills. Nil disables the accumulator (Usage then falls back to Budget's
	// total only); main.go always wires it.
	Usage UsageStore
	// Embedder turns a prompt into a vector for the semantic cache. Nil = cache OFF
	// (the AI_CACHE_ENABLED=false / fail-open-at-wiring path): the use-case skips the
	// cache check/store entirely and serves every request via the providers.
	Embedder Embedder
	// Cache is the per-team semantic cache. Nil = cache OFF (same as a nil Embedder).
	Cache SemanticCache
	// CacheThreshold is the minimum cosine similarity for a CACHE HIT (default 0.95 if
	// <= 0). Higher = stricter (fewer, more-exact hits); lower = more hits, looser.
	CacheThreshold float64
	// Publisher emits the served event + the warm signal. Best-effort, off the path.
	Publisher EventPublisher
	// EvalIncludeText opts the gateway INTO attaching the raw prompt+response text to
	// the fp.ai.completion.served event (M7/L4 LLM-as-judge quality eval). DEFAULT
	// false (PII discipline): the cost/audit event carries NO content unless an
	// operator turns this on for the quality-eval pipeline. See CompletionServed's PII
	// note. When false, the monitor's judge degrades to UNSCORED rather than blind.
	EvalIncludeText bool
	// Now is the injected clock for latency measurement (tests pass a fake).
	Now func() time.Time
}

// defaultCacheThreshold is the cosine-similarity bar for a cache HIT when none is
// configured. 0.95 ≈ "near-duplicate question" — strict enough that a DIFFERENT
// question misses, loose enough that paraphrases/case/punctuation differences hit.
const defaultCacheThreshold = 0.95

// gatewayService is the concrete use-case.
type gatewayService struct {
	providers       *ProviderRegistry
	breakers        BreakerRegistry
	budget          BudgetStore
	usage           UsageStore
	embedder        Embedder
	cache           SemanticCache
	cacheThreshold  float64
	publisher       EventPublisher
	evalIncludeText bool
	now             func() time.Time
}

// Compile-time proof we satisfy the port.
var _ GatewayService = (*gatewayService)(nil)

// NewGatewayService wires the use-case. A nil Now defaults to time.Now; a
// non-positive CacheThreshold defaults to defaultCacheThreshold.
func NewGatewayService(deps ServiceDeps) GatewayService {
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	threshold := deps.CacheThreshold
	if threshold <= 0 {
		threshold = defaultCacheThreshold
	}
	return &gatewayService{
		providers:       deps.Providers,
		breakers:        deps.Breakers,
		budget:          deps.Budget,
		usage:           deps.Usage,
		embedder:        deps.Embedder,
		cache:           deps.Cache,
		cacheThreshold:  threshold,
		publisher:       deps.Publisher,
		evalIncludeText: deps.EvalIncludeText,
		now:             now,
	}
}

// ChatCompletion is the orchestrator. See the file header for the lifecycle.
func (s *gatewayService) ChatCompletion(ctx context.Context, team string, req ChatRequest, sink Sink) (Completion, error) {
	// --- 1. VALIDATE ---------------------------------------------------------
	if len(req.Messages) == 0 {
		return Completion{}, ErrInvalidInput
	}

	// --- 2. BUDGET GATE (pre-flight, claims-derived team) --------------------
	// Check whether the team is ALREADY over budget. The token cost of THIS call is
	// unknown until it finishes (we can't reserve it), so this only rejects a team
	// that has already exhausted its window — the honest soft-cap shape (see the
	// BudgetStore port doc). A non-nil error is an infra failure: we FAIL OPEN (serve
	// anyway) because a transient Redis blip must not block all LLM traffic; the cap
	// is soft and Billing reconciles the exact ledger.
	if team != "" && s.budget != nil {
		allowed, _, err := s.budget.Check(ctx, team)
		if err == nil && !allowed {
			return Completion{}, ErrBudgetExceeded
		}
		// err != nil → fail open (fall through and serve).
	}

	// --- 3. SEMANTIC CACHE LOOKUP (per-team, fail-open) ----------------------
	// If both the embedder and the cache are wired (AI_CACHE_ENABLED), embed THIS
	// prompt and look up the nearest cached completion for THIS TEAM. A hit (best
	// similarity >= threshold) REPLAYS the stored response WITHOUT calling any provider
	// and WITHOUT deducting tokens — a cache hit costs zero model tokens (the whole
	// point: GPTCache/Portkey semantics). The cache is FAIL-OPEN at every step: a nil
	// embedder/cache, an embed error, or a lookup error all fall through to a normal
	// completion (the cache is an optimization, NEVER a dependency that can block chat).
	//
	// queryEmbedding is threaded down to the MISS path so a successful serve can STORE
	// (embedding, prompt, response, usage) without re-embedding. cacheEligible records
	// that the cache is on AND we have a usable embedding, so the store-on-miss only
	// runs when a lookup actually happened (a fail-open embed error must not then store
	// against a zero vector). promptText is the concatenated prompt we embed and store.
	var (
		queryEmbedding []float32
		cacheEligible  bool
		promptText     string
	)
	if s.embedder != nil && s.cache != nil {
		cacheStart := s.now()
		promptText = promptForEmbedding(req.Messages)
		vec, embedErr := s.embedder.Embed(ctx, promptText)
		if embedErr != nil {
			// FAIL-OPEN: a broken/cold embedder must never block a chat. Fall through to a
			// normal completion; we simply skip the cache for this request (no lookup, and
			// the miss-store below is gated on cacheEligible so it won't fire either).
			slog.WarnContext(ctx, "ai-gateway: semantic cache embed failed, serving uncached (fail-open)",
				slog.String("team", team), slog.String("error", embedErr.Error()))
		} else {
			queryEmbedding = vec
			cacheEligible = true
			entry, similarity, found, lookupErr := s.cache.Lookup(ctx, team, vec)
			if lookupErr != nil {
				// FAIL-OPEN: a Redis blip on lookup is a MISS, not a failure — serve normally.
				slog.WarnContext(ctx, "ai-gateway: semantic cache lookup failed, serving uncached (fail-open)",
					slog.String("team", team), slog.String("error", lookupErr.Error()))
			} else if found && similarity >= s.cacheThreshold {
				// CACHE HIT. Replay the stored response as a SINGLE non-terminal delta, then
				// return the terminal summary the handler turns into the final frame. We skip
				// the provider call AND the budget Deduct (a hit costs no tokens), and publish
				// the served event with CacheHit=true so Billing/audit see a zero-provider-cost
				// served fact. If the sink fails mid-replay (client gone) we abort like any
				// other sink failure — no provider was touched.
				if entry.Response != "" {
					if sinkErr := sink(Delta{Text: entry.Response}); sinkErr != nil {
						return Completion{ServedBy: entry.ServedBy, FinishReason: FinishReasonError}, errSinkFailed
					}
				}
				return s.finishCacheHit(ctx, team, req, entry, s.now().Sub(cacheStart)), nil
			}
			// found but below threshold, or cold cache → MISS: fall through and serve. The
			// embedding is retained in queryEmbedding for the store-on-miss below.
		}
	}

	// --- 4. WARM SIGNAL FIRST (wake a scaled-to-zero Ollama) -----------------
	// Published BEFORE the provider call so KEDA can begin scaling Ollama 0->1 while
	// the Ollama adapter retries-with-backoff against a cold backend. Best-effort.
	if s.publisher != nil {
		warm := WarmSignal{RequestID: req.RequestID, Team: team, Model: req.Model}
		// The warm signal targets the preferred (first) provider's kind for labelling.
		if cands := s.providers.Candidates(req.Provider, req.Model); len(cands) > 0 {
			warm.Provider = cands[0].Kind()
		}
		_ = s.publisher.PublishWarmSignal(ctx, warm) //nolint:errcheck // best-effort, off the response path
	}

	// --- 5. ROUTE + FAILOVER -------------------------------------------------
	candidates := s.providers.Candidates(req.Provider, req.Model)
	if len(candidates) == 0 {
		// Nothing eligible to even attempt (unknown pinned provider / empty registry).
		_ = sink(errorFrame()) //nolint:errcheck // surface a terminal error frame; ignore send error on a doomed stream
		return Completion{FinishReason: FinishReasonError}, ErrNoProvider
	}

	// CAPTURE-FOR-STORE / CAPTURE-FOR-EVAL: we need the FULL response text in two
	// cases — to store it on a cache miss (cacheEligible), and to attach it to the
	// served event for the L4 quality judge (evalIncludeText). Either way the response
	// only exists as it STREAMS through the sink, so we wrap the caller's sink to ALSO
	// accumulate each non-terminal delta's text into responseText. The wrapper is
	// transparent (it forwards the delta and propagates the sink's error unchanged), so
	// the failover invariants are untouched — it only observes. When NEITHER consumer
	// needs the text we use the caller's sink directly (zero overhead / no buffering).
	// NOTE on mid-stream failure: if a provider streams partial tokens and then aborts
	// (errStreamAborted), we never reach the publish/store block (the loop returns
	// terminal), so a partial answer is never captured — only a CLEAN completion is.
	captureText := cacheEligible || s.evalIncludeText
	var responseText string
	streamSink := sink
	if captureText {
		var b strings.Builder
		streamSink = func(d Delta) error {
			if err := sink(d); err != nil {
				return err
			}
			b.WriteString(d.Text)
			responseText = b.String()
			return nil
		}
	}

	start := s.now()
	var (
		served   bool
		servedBy ProviderKind
		usage    TokenUsage
		finish   = FinishReasonError
		lastErr  = ErrAllProvidersFailed
	)

	for _, p := range candidates {
		breaker := s.breakers.Get(p.Kind())
		// SKIP a provider whose circuit is OPEN — this skip IS the failover. Allow()
		// also realizes the OPEN→HALF_OPEN edge and reserves a probe slot when due.
		if !breaker.Allow() {
			continue
		}

		u, fr, err := s.streamOne(ctx, p, req, streamSink)
		if err != nil {
			// streamOne classifies the failure into three buckets:
			//
			//   errSinkFailed    — forwarding to the CLIENT failed (disconnect/shutdown).
			//                      Not the provider's fault: abort WITHOUT a breaker hit,
			//                      no failover (the sink is dead — retrying is a no-op).
			//
			//   errStreamAborted — the provider failed AFTER we already forwarded ≥1 token
			//                      for THIS attempt (an in-band error frame or an early
			//                      channel close). This is the no-tokens-forwarded
			//                      invariant firing: the client has already seen partial
			//                      output from THIS provider, so failing over to the next
			//                      would concatenate a second provider's full answer onto
			//                      the partial one — garbled/duplicated output. We RECORD
			//                      the failure (the provider really did break) but DO NOT
			//                      fail over: emit the terminal error frame and stop.
			//
			//   <provider error> — a START error or an error frame seen BEFORE any token
			//                      was forwarded: nothing reached the client, so failover
			//                      is safe. RecordFailure and try the next provider.
			if errors.Is(err, errSinkFailed) {
				// Client gone or shutdown: abort, do not penalize the provider.
				return Completion{ServedBy: p.Kind(), FinishReason: FinishReasonError}, err
			}
			if errors.Is(err, errStreamAborted) {
				// Mid-stream provider failure after tokens were forwarded: TERMINAL. The
				// provider broke, so record it against its breaker, but do NOT try the next
				// provider — emit the terminal error frame onto the SAME stream and return
				// the failover-exhausted contract error (errors.Is(_, ErrAllProvidersFailed)).
				breaker.RecordFailure()
				_ = sink(errorFrame()) //nolint:errcheck // partial stream already sent; best-effort terminal frame
				return Completion{ServedBy: p.Kind(), FinishReason: FinishReasonError, Latency: s.now().Sub(start)}, wrapFailover(err)
			}
			breaker.RecordFailure()
			lastErr = err
			continue
		}
		// Clean stream: record success and stop failing over.
		breaker.RecordSuccess()
		served, servedBy, usage, finish = true, p.Kind(), u, fr
		break
	}

	latency := s.now().Sub(start)

	if !served {
		// Every candidate was skipped (OPEN) or errored. Emit the terminal error frame
		// the client expects (FinishReasonError) and return the failover-exhausted error.
		// We WRAP ErrAllProvidersFailed around the last provider's cause so callers can
		// errors.Is(err, ErrAllProvidersFailed) (the stable contract the handler maps to
		// a terminal error frame) while the underlying cause stays available for logs.
		_ = sink(errorFrame()) //nolint:errcheck // doomed stream; best-effort terminal frame
		return Completion{FinishReason: FinishReasonError, Latency: latency}, wrapFailover(lastErr)
	}

	// --- 6. METER + DEDUCT + STORE + EVENT ----------------------------------
	// This is the MISS path: a provider actually served, so we price it, deduct the
	// real tokens, record the usage breakdown, STORE the completion in the cache (so a
	// near-duplicate next time hits), and publish the served event (CacheHit=false).
	//
	// Price the completion from the served provider's rate and the real token counts.
	usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens
	usage.CostMicroUSD = costMicroUSD(servedBy, usage.PromptTokens, usage.CompletionTokens)

	completion := Completion{
		ServedBy:     servedBy,
		Usage:        usage,
		FinishReason: finish,
		CacheHit:     false, // a provider served this request — by definition a cache MISS.
		Latency:      latency,
	}

	// Deduct the ACTUAL tokens used from the team's window (post-serve). Best-effort:
	// the client already has its answer, so a deduct error is swallowed (logging is the
	// adapter's job). This is the second half of the two-phase budget (see port doc).
	if team != "" && s.budget != nil && usage.TotalTokens > 0 {
		_, _ = s.budget.Deduct(ctx, team, int64(usage.TotalTokens)) //nolint:errcheck // best-effort soft cap
	}

	// Record the prompt/completion SPLIT in the monotonic usage accumulator (the
	// GetUsage breakdown fix). Separate from the budget bucket on purpose: the budget
	// REFILLS (a rate cap) and only ever saw the TOTAL via Deduct, which is exactly
	// what zeroed the breakdown; this accumulator keeps prompt vs completion distinct.
	// Best-effort, post-serve: a record error is logged, never surfaced.
	if team != "" && s.usage != nil && usage.TotalTokens > 0 {
		if addErr := s.usage.Add(ctx, team, usage.PromptTokens, usage.CompletionTokens); addErr != nil {
			slog.WarnContext(ctx, "ai-gateway: usage accumulator Add failed (breakdown may lag)",
				slog.String("team", team), slog.String("error", addErr.Error()))
		}
	}

	// STORE-ON-MISS: append this completion to THIS TEAM's cache so a future near-
	// duplicate prompt hits. Gated on cacheEligible (the cache is on AND we have a real
	// query embedding from a successful Embed — never store against a fail-open zero
	// vector) and a non-empty answer. Best-effort: a store error doesn't fail the
	// completion the client already received. The adapter bounds+TTLs the per-team list
	// so it can't grow unbounded; the team is the keyspace, never a field on the entry.
	if cacheEligible && responseText != "" {
		entry := CachedCompletion{
			Embedding: queryEmbedding,
			Prompt:    promptText,
			Response:  responseText,
			Usage:     usage,
			ServedBy:  servedBy,
		}
		if storeErr := s.cache.Store(ctx, team, entry); storeErr != nil {
			slog.WarnContext(ctx, "ai-gateway: semantic cache store-on-miss failed (best-effort)",
				slog.String("team", team), slog.String("error", storeErr.Error()))
		}
	}

	// Publish the canonical cost/audit event (best-effort, off the response path).
	if s.publisher != nil {
		ev := CompletionServed{
			RequestID:    req.RequestID,
			Team:         team,
			Model:        req.Model,
			Provider:     servedBy,
			PromptTokens: usage.PromptTokens,
			OutputTokens: usage.CompletionTokens,
			TotalTokens:  usage.TotalTokens,
			CostMicroUSD: usage.CostMicroUSD,
			CacheHit:     false,
			LatencyMs:    latency.Milliseconds(),
		}
		// L4 QUALITY EVAL: attach the raw prompt+response ONLY when explicitly opted in
		// (evalIncludeText). promptText may already be set by the cache stage; if not
		// (cache off, eval on) we compute it here from the same concatenation the cache
		// would use. PII discipline: with the flag off, both stay "" and omitempty drops
		// them from the wire entirely (event identical to the pre-L4 shape).
		if s.evalIncludeText {
			pt := promptText
			if pt == "" {
				pt = promptForEmbedding(req.Messages)
			}
			ev.PromptText = pt
			ev.ResponseText = responseText
		}
		_ = s.publisher.PublishCompletionServed(ctx, ev) //nolint:errcheck // best-effort
	}

	return completion, nil
}

// errSinkFailed marks a failure to FORWARD a delta to the caller (client gone /
// stream send error / ctx cancelled). It is distinct from a provider error because
// it is NOT the provider's fault and must NOT trip the provider's breaker — the
// provider was healthy; the consumer left. The failover loop returns immediately on
// it rather than retrying on another provider (a no-op, since the sink is dead).
var errSinkFailed = errors.New("ai-gateway: sink (client stream) failed")

// errStreamAborted marks a PROVIDER failure that happened AFTER ≥1 non-terminal
// delta was already forwarded to the client for the current attempt (an in-band
// FinishReasonError frame, or the channel closing with no Done frame — the mid-
// stream drop case). It is the enforcement of the no-tokens-forwarded invariant:
// because the client has already seen partial output from THIS provider, failing
// over would garble the stream, so the loop treats this sentinel as TERMINAL (no
// failover) — unlike a plain provider error (start failure / pre-token error frame)
// which IS recoverable. It wraps the underlying provider cause for the logs.
var errStreamAborted = errors.New("ai-gateway: stream aborted after partial output")

// streamOne attempts ONE provider: start the stream, then forward each delta to the
// sink until the terminal frame. It returns:
//   - (usage, finishReason, nil)            on a clean completion,
//   - (_, _, <provider error>)              if the provider failed to START or sent an
//     in-band error frame BEFORE any token was forwarded (failover: try next),
//   - (_, _, errStreamAborted)              if the provider errored AFTER ≥1 token was
//     forwarded (in-band error frame or early channel close) — TERMINAL, no failover,
//   - (_, _, errSinkFailed)                 if forwarding to the caller failed (abort).
//
// THE NO-TOKENS-FORWARDED INVARIANT (enforced, not just hoped): we keep a `forwarded`
// flag that flips the first time we successfully push a non-terminal delta to the
// sink. A provider that fails to START (Chat returns non-nil) or sends an error frame
// while forwarded==false has shown the client NOTHING, so failover to the next
// provider is safe — we return a plain provider error and the loop continues. But the
// MOMENT forwarded==true, any failure on the SAME stream (an in-band FinishReasonError
// frame, OR the channel closing with no Done frame — the OOM-killed / mid-stream-drop
// case) becomes errStreamAborted: the client already saw partial output from THIS
// provider, so the loop must NOT stream a second provider's full answer on top of it.
// This is the difference between "retry safe" and "garbled output" — and it is now
// a state flag, not a comment.
func (s *gatewayService) streamOne(ctx context.Context, p Provider, req ChatRequest, sink Sink) (TokenUsage, FinishReason, error) {
	ch, err := p.Chat(ctx, req)
	if err != nil {
		// START failure — nothing forwarded; the loop fails over to the next provider.
		return TokenUsage{}, FinishReasonError, err
	}

	// forwarded tracks whether ANY non-terminal delta reached the client on this
	// attempt. It is the hinge of the no-tokens-forwarded invariant: it decides
	// whether a later provider failure is recoverable (failover) or terminal (abort).
	forwarded := false
	for delta := range ch {
		if delta.Done {
			// Terminal frame.
			if delta.FinishReason == FinishReasonError {
				// An in-band error frame. If we have ALREADY forwarded tokens, failing over
				// would garble the client's stream → errStreamAborted (terminal). If nothing
				// was forwarded yet, it is an ordinary provider failure → failover.
				if forwarded {
					return TokenUsage{}, FinishReasonError, fmt.Errorf("%w: in-band error frame", errStreamAborted)
				}
				return TokenUsage{}, FinishReasonError, ErrAllProvidersFailed
			}
			return delta.Usage, delta.FinishReason, nil
		}
		// Non-terminal token delta: forward it to the caller. A send error means the
		// client/stream is gone — abort the whole completion (not a provider fault).
		if err := sink(delta); err != nil {
			return TokenUsage{}, FinishReasonError, errSinkFailed
		}
		forwarded = true
	}
	// Channel closed without a Done frame: the provider violated the contract (or ctx
	// was cancelled, closing it early). If we had already forwarded tokens, this is the
	// mid-stream-drop case (e.g. an OOM-killed Ollama): TERMINAL, no failover. If nothing
	// was forwarded, it is a clean start-time miss and the loop can safely fail over.
	if forwarded {
		return TokenUsage{}, FinishReasonError, fmt.Errorf("%w: channel closed without terminal frame", errStreamAborted)
	}
	return TokenUsage{}, FinishReasonError, ErrAllProvidersFailed
}

// finishCacheHit builds the terminal Completion for a CACHE HIT and publishes the
// served event with CacheHit=true. It is the deliberate counterpart to the MISS
// path's METER+DEDUCT block — and it OMITS exactly the two things a hit must skip:
//
//   - NO budget Deduct: a cache hit costs ZERO model tokens (no provider was called),
//     so deducting the stored entry's tokens would wrongly charge the team for an LLM
//     call that never happened. This skip is the cache's entire economic payoff.
//   - NO usage.Add: same reasoning — the GetUsage breakdown should reflect tokens
//     ACTUALLY consumed from a provider; a replayed answer consumed none.
//
// We DO publish the served event (CacheHit=true, the stored usage for observability)
// so Billing/audit see a served fact — and billing's AIConsumer, seeing total_tokens
// on a cache hit, can choose to meter or treat it as a zero-cost served event (its
// token-count guard already no-ops a zero-token completion). The event's CacheHit flag
// is the signal that this served fact incurred no provider cost.
func (s *gatewayService) finishCacheHit(ctx context.Context, team string, req ChatRequest, entry CachedCompletion, latency time.Duration) Completion {
	usage := entry.Usage
	usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens

	if s.publisher != nil {
		_ = s.publisher.PublishCompletionServed(ctx, CompletionServed{ //nolint:errcheck // best-effort
			RequestID:    req.RequestID,
			Team:         team,
			Model:        req.Model,
			Provider:     entry.ServedBy,
			PromptTokens: usage.PromptTokens,
			OutputTokens: usage.CompletionTokens,
			TotalTokens:  usage.TotalTokens,
			CostMicroUSD: usage.CostMicroUSD,
			CacheHit:     true, // the served-event signal that this incurred no provider cost.
			LatencyMs:    latency.Milliseconds(),
		})
	}

	return Completion{
		ServedBy:     entry.ServedBy,
		Usage:        usage,
		FinishReason: FinishReasonStop, // a replayed answer is a complete, clean stop.
		CacheHit:     true,
		Latency:      latency,
	}
}

// promptForEmbedding concatenates the conversation's messages into the single string
// the embedder turns into a query vector. We join ALL turns (system + user +
// assistant), not just the last user message, so two requests with the SAME final
// question but DIFFERENT context (a different system prompt or prior turns) embed
// differently and don't collide — caching one team's answer for a materially
// different conversation would be a correctness bug. Roles are prefixed so "user: X"
// and "assistant: X" don't collapse to the same vector. This is the SAME text stored
// as entry.Prompt, so a hit's stored prompt is exactly what produced its embedding.
func promptForEmbedding(messages []Message) string {
	var b strings.Builder
	for i, m := range messages {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(m.Role.label())
		b.WriteString(": ")
		b.WriteString(m.Content)
	}
	return b.String()
}

// errorFrame builds the standard terminal error delta the handler/clients expect
// when failover is exhausted: Done with FinishReasonError and no usage.
func errorFrame() Delta {
	return Delta{Done: true, FinishReason: FinishReasonError}
}

// wrapFailover ensures the returned failover-exhausted error always satisfies
// errors.Is(err, ErrAllProvidersFailed) while preserving the last provider's cause
// for the logs. If cause already IS ErrAllProvidersFailed (every candidate was an
// OPEN-circuit skip, so lastErr was never overwritten), it is returned unchanged.
func wrapFailover(cause error) error {
	if cause == nil || errors.Is(cause, ErrAllProvidersFailed) {
		return ErrAllProvidersFailed
	}
	return fmt.Errorf("%w: %v", ErrAllProvidersFailed, cause)
}

// Providers returns the snapshot list for ListProviders, attaching each provider's
// LIVE circuit state from the registry.
func (s *gatewayService) Providers() []ProviderSnapshot {
	return s.providers.Snapshot(s.breakers)
}

// Usage assembles the GetUsage read model from TWO sources, each the right shape for
// its job (the conflation of these two is exactly the bug this fixes):
//
//   - the UsageStore accumulator gives the prompt/completion/total BREAKDOWN. It is
//     MONOTONIC (only goes up) and keeps prompt vs completion distinct — the budget
//     bucket never could, because Deduct only ever saw the TOTAL, which is why the
//     breakdown read prompt=0/completion=0.
//   - the BudgetStore gives the budget + remaining (a REFILLING rate cap), the right
//     source for "how much allowance is left", which an ever-rising accumulator can't
//     answer.
//
// Each source is independently nil-safe (tests wire only what they assert on): a nil
// UsageStore yields a zero breakdown, a nil BudgetStore yields zero budget/remaining.
// An empty team short-circuits to all zeros. TotalTokens prefers the accumulator's
// total (the true consumed count); when the accumulator is absent it falls back to the
// budget's consumed so a deployment without the accumulator still reports a total.
func (s *gatewayService) Usage(ctx context.Context, team string) (UsageSummary, error) {
	if team == "" {
		return UsageSummary{}, nil
	}

	var summary UsageSummary

	// BREAKDOWN from the usage accumulator (the fix).
	if s.usage != nil {
		prompt, completion, total, err := s.usage.Get(ctx, team)
		if err != nil {
			return UsageSummary{}, err
		}
		summary.PromptTokens = prompt
		summary.CompletionTokens = completion
		summary.TotalTokens = total
	}

	// BUDGET + REMAINING from the budget bucket.
	if s.budget != nil {
		consumed, budget, err := s.budget.Usage(ctx, team)
		if err != nil {
			return UsageSummary{}, err
		}
		summary.BudgetTokens = budget
		remaining := budget - consumed
		if remaining < 0 {
			remaining = 0
		}
		summary.RemainingTokens = remaining
		// Fallback: with no usage accumulator wired, report the budget's consumed as the
		// total so GetUsage isn't blank (the breakdown stays zero, but the total is real).
		if s.usage == nil {
			summary.TotalTokens = consumed
		}
	}

	return summary, nil
}
