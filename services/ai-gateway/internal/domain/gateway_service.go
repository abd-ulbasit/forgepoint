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
//	  3. WARM SIGNAL     — publish the warm event FIRST so a scaled-to-zero Ollama
//	                       starts coming up (KEDA 0->1) while we attempt the call.
//	  4. ROUTE + FAILOVER— build the ordered candidate list (pinned provider, else
//	                       the default order), and for each candidate:
//	                         - SKIP if its circuit is OPEN (breaker.Allow() == false),
//	                         - else stream it; on a SYNCHRONOUS start error or an
//	                           in-band error frame, RecordFailure and try the next;
//	                         - on a clean terminal frame, RecordSuccess and stop.
//	                       If every candidate is skipped/fails → emit a terminal
//	                       FinishReasonError frame and return ErrAllProvidersFailed.
//	  5. METER + DEDUCT  — compute cost, Deduct the used tokens from the team budget
//	                       (post-serve, best-effort), publish fp.ai.completion.served.
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
	// Usage returns the team's consumed/budget/remaining for GetUsage.
	Usage(ctx context.Context, team string) (consumed, budget, remaining int64, err error)
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
	// Publisher emits the served event + the warm signal. Best-effort, off the path.
	Publisher EventPublisher
	// Now is the injected clock for latency measurement (tests pass a fake).
	Now func() time.Time
}

// gatewayService is the concrete use-case.
type gatewayService struct {
	providers *ProviderRegistry
	breakers  BreakerRegistry
	budget    BudgetStore
	publisher EventPublisher
	now       func() time.Time
}

// Compile-time proof we satisfy the port.
var _ GatewayService = (*gatewayService)(nil)

// NewGatewayService wires the use-case. A nil Now defaults to time.Now.
func NewGatewayService(deps ServiceDeps) GatewayService {
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	return &gatewayService{
		providers: deps.Providers,
		breakers:  deps.Breakers,
		budget:    deps.Budget,
		publisher: deps.Publisher,
		now:       now,
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

	// --- 3. WARM SIGNAL FIRST (wake a scaled-to-zero Ollama) -----------------
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

	// --- 4. ROUTE + FAILOVER -------------------------------------------------
	candidates := s.providers.Candidates(req.Provider, req.Model)
	if len(candidates) == 0 {
		// Nothing eligible to even attempt (unknown pinned provider / empty registry).
		_ = sink(errorFrame()) //nolint:errcheck // surface a terminal error frame; ignore send error on a doomed stream
		return Completion{FinishReason: FinishReasonError}, ErrNoProvider
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

		u, fr, err := s.streamOne(ctx, p, req, sink)
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

	// --- 5. METER + DEDUCT + EVENT ------------------------------------------
	// Price the completion from the served provider's rate and the real token counts.
	usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens
	usage.CostMicroUSD = costMicroUSD(servedBy, usage.PromptTokens, usage.CompletionTokens)

	completion := Completion{
		ServedBy:     servedBy,
		Usage:        usage,
		FinishReason: finish,
		CacheHit:     false, // semantic cache is L2; always a miss in this core.
		Latency:      latency,
	}

	// Deduct the ACTUAL tokens used from the team's window (post-serve). Best-effort:
	// the client already has its answer, so a deduct error is swallowed (logging is the
	// adapter's job). This is the second half of the two-phase budget (see port doc).
	if team != "" && s.budget != nil && usage.TotalTokens > 0 {
		_, _ = s.budget.Deduct(ctx, team, int64(usage.TotalTokens)) //nolint:errcheck // best-effort soft cap
	}

	// Publish the canonical cost/audit event (best-effort, off the response path).
	if s.publisher != nil {
		_ = s.publisher.PublishCompletionServed(ctx, CompletionServed{ //nolint:errcheck // best-effort
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
		})
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

// Usage returns the team's consumed/budget/remaining for GetUsage. A nil budget
// store (tests) yields zeros with no error.
func (s *gatewayService) Usage(ctx context.Context, team string) (consumed, budget, remaining int64, err error) {
	if s.budget == nil || team == "" {
		return 0, 0, 0, nil
	}
	consumed, budget, err = s.budget.Usage(ctx, team)
	if err != nil {
		return 0, 0, 0, err
	}
	remaining = budget - consumed
	if remaining < 0 {
		remaining = 0
	}
	return consumed, budget, remaining, nil
}
