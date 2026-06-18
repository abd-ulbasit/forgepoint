// gateway_service_test.go — UNIT tests for the failover + budget + cost core.
//
// These tests use FAKE providers and a FAKE budget store (no Redis, no HTTP, no
// NATS) so they are fast, deterministic, and run under -race. They prove the four
// behaviors the task calls out:
//   - failover: a provider that trips its breaker is skipped → the next serves;
//   - all-down: every provider failing → terminal FinishReasonError + the error;
//   - budget: an over-budget team → ErrBudgetExceeded (no provider called);
//   - budget deduct: an under-budget team serves and the used tokens are deducted.
package domain

import (
	"context"
	"errors"
	"testing"
	"time"
)

// --- fakes ------------------------------------------------------------------

// fakeProvider is a configurable in-memory Provider for the failover tests. It can
// stream a canned answer, fail synchronously at start (the recoverable failover
// case), or emit an in-band error frame.
type fakeProvider struct {
	kind       ProviderKind
	words      []string
	promptTok  int32
	outputTok  int32
	startErr   error // non-nil → Chat returns this (failover: nothing streamed)
	inbandFail bool  // true → stream a terminal error frame instead of a clean one
	earlyClose bool  // true → after words, close the channel with NO terminal frame
	calls      int   // how many times Chat was invoked (asserts failover skipped it)
}

func (f *fakeProvider) Kind() ProviderKind { return f.kind }
func (f *fakeProvider) Models() []string   { return nil }

func (f *fakeProvider) Chat(ctx context.Context, _ ChatRequest) (<-chan Delta, error) {
	f.calls++
	if f.startErr != nil {
		return nil, f.startErr
	}
	out := make(chan Delta)
	go func() {
		defer close(out)
		for _, w := range f.words {
			select {
			case <-ctx.Done():
				return
			case out <- Delta{Text: w}:
			}
		}
		if f.earlyClose {
			// Mid-stream drop: close the channel WITHOUT a terminal Done frame (the
			// OOM-killed / flaky-Ollama case). After words have been forwarded this must
			// be terminal, not failover.
			return
		}
		if f.inbandFail {
			out <- Delta{Done: true, FinishReason: FinishReasonError}
			return
		}
		out <- Delta{
			Done:         true,
			FinishReason: FinishReasonStop,
			Usage:        TokenUsage{PromptTokens: f.promptTok, CompletionTokens: f.outputTok},
		}
	}()
	return out, nil
}

// fakeBudget is an in-memory BudgetStore. blocked controls Check; deducted records
// the last Deduct so a test can assert the post-serve deduction happened.
type fakeBudget struct {
	blocked     bool
	checkErr    error
	budget      int64
	consumed    int64
	deductCalls int
	lastDeduct  int64
}

func (b *fakeBudget) Check(_ context.Context, _ string) (bool, int64, error) {
	if b.checkErr != nil {
		return false, 0, b.checkErr
	}
	return !b.blocked, b.budget - b.consumed, nil
}

func (b *fakeBudget) Deduct(_ context.Context, _ string, tokens int64) (int64, error) {
	b.deductCalls++
	b.lastDeduct = tokens
	b.consumed += tokens
	return b.budget - b.consumed, nil
}

func (b *fakeBudget) Usage(_ context.Context, _ string) (int64, int64, error) {
	return b.consumed, b.budget, nil
}

// collectSink returns a Sink that appends each forwarded delta's text, and the
// accumulated slice pointer to assert on.
func collectSink(out *[]string) Sink {
	return func(d Delta) error {
		*out = append(*out, d.Text)
		return nil
	}
}

// newService wires a service over the given providers (in failover order), a budget,
// and a fast breaker so the test controls trip behavior.
func newService(t *testing.T, budget BudgetStore, tuning BreakerTuning, provs ...Provider) GatewayService {
	t.Helper()
	return NewGatewayService(ServiceDeps{
		Providers: NewProviderRegistry(provs...),
		Breakers:  NewBreakerRegistry(tuning, time.Now),
		Budget:    budget,
		Publisher: nil, // best-effort; nil is allowed (the service nil-guards it).
		Now:       time.Now,
	})
}

// --- tests ------------------------------------------------------------------

func TestChatCompletion_FailoverToSecondaryWhenPrimaryTrips(t *testing.T) {
	t.Parallel()
	// Primary fails to START on every call; secondary serves cleanly. Breaker trips
	// after 1 failure so the primary is OPEN immediately after the first attempt — but
	// even on the FIRST request, the primary's start error must fail over to secondary.
	primary := &fakeProvider{kind: ProviderKindOllama, startErr: errors.New("connection refused")}
	secondary := &fakeProvider{
		kind:      ProviderKindStub,
		words:     []string{"hi ", "there"},
		promptTok: 500, outputTok: 300, // large enough that the per-1K integer cost is non-zero.
	}
	svc := newService(t, nil, BreakerTuning{FailureThreshold: 1}, primary, secondary)

	var got []string
	completion, err := svc.ChatCompletion(context.Background(), "team-a",
		ChatRequest{Messages: []Message{{Role: RoleUser, Content: "hello"}}}, collectSink(&got))
	if err != nil {
		t.Fatalf("expected failover success, got error: %v", err)
	}
	if completion.ServedBy != ProviderKindStub {
		t.Fatalf("expected served by stub (secondary), got %v", completion.ServedBy)
	}
	if primary.calls != 1 {
		t.Fatalf("expected primary attempted once, got %d", primary.calls)
	}
	if secondary.calls != 1 {
		t.Fatalf("expected secondary attempted once, got %d", secondary.calls)
	}
	if want := "hi there"; join(got) != want {
		t.Fatalf("streamed text = %q, want %q", join(got), want)
	}
	// Cost was computed from the SERVED provider's rate (stub) and the real counts:
	// prompt (500 * 100/1000=50) + output (300 * 400/1000=120) = 170 micro-USD.
	if completion.Usage.TotalTokens != 800 {
		t.Fatalf("total tokens = %d, want 800", completion.Usage.TotalTokens)
	}
	if completion.Usage.CostMicroUSD != 170 {
		t.Fatalf("computed cost = %d micro-USD, want 170", completion.Usage.CostMicroUSD)
	}
}

func TestChatCompletion_SkipsOpenCircuitWithoutCalling(t *testing.T) {
	t.Parallel()
	// Trip the primary's breaker OPEN first (FailureThreshold=1), then a second request
	// must SKIP the primary entirely (no Chat call) and the secondary serves.
	primary := &fakeProvider{kind: ProviderKindOllama, startErr: errors.New("boom")}
	secondary := &fakeProvider{kind: ProviderKindStub, words: []string{"ok"}, promptTok: 1, outputTok: 1}
	svc := newService(t, nil, BreakerTuning{FailureThreshold: 1}, primary, secondary)

	// Request 1: primary attempted (fails, trips OPEN), secondary serves.
	var s1 []string
	if _, err := svc.ChatCompletion(context.Background(), "team-a",
		ChatRequest{Messages: []Message{{Content: "x"}}}, collectSink(&s1)); err != nil {
		t.Fatalf("req1 unexpected error: %v", err)
	}
	if primary.calls != 1 {
		t.Fatalf("req1: primary calls = %d, want 1", primary.calls)
	}

	// Request 2: primary circuit is OPEN → skipped without a Chat call.
	var s2 []string
	completion, err := svc.ChatCompletion(context.Background(), "team-a",
		ChatRequest{Messages: []Message{{Content: "y"}}}, collectSink(&s2))
	if err != nil {
		t.Fatalf("req2 unexpected error: %v", err)
	}
	if primary.calls != 1 {
		t.Fatalf("req2: primary should be skipped (OPEN), calls = %d, want still 1", primary.calls)
	}
	if completion.ServedBy != ProviderKindStub {
		t.Fatalf("req2 served by = %v, want stub", completion.ServedBy)
	}
}

func TestChatCompletion_AllProvidersFailErrorFrame(t *testing.T) {
	t.Parallel()
	// Both providers fail to start: the result is a terminal FinishReasonError frame
	// AND the failover-exhausted error.
	p1 := &fakeProvider{kind: ProviderKindOllama, startErr: errors.New("down")}
	p2 := &fakeProvider{kind: ProviderKindStub, startErr: errors.New("down")}
	svc := newService(t, nil, BreakerTuning{FailureThreshold: 5}, p1, p2)

	var got []Delta
	sink := func(d Delta) error { got = append(got, d); return nil }
	completion, err := svc.ChatCompletion(context.Background(), "team-a",
		ChatRequest{Messages: []Message{{Content: "x"}}}, sink)
	if !errors.Is(err, ErrAllProvidersFailed) {
		t.Fatalf("expected ErrAllProvidersFailed, got %v", err)
	}
	if completion.FinishReason != FinishReasonError {
		t.Fatalf("completion finish = %v, want FinishReasonError", completion.FinishReason)
	}
	// The sink received exactly the terminal error frame.
	if len(got) != 1 || !got[0].Done || got[0].FinishReason != FinishReasonError {
		t.Fatalf("expected one terminal error frame, got %+v", got)
	}
}

func TestChatCompletion_MidStreamErrorFrameIsTerminalNoFailover(t *testing.T) {
	t.Parallel()
	// THE INVARIANT UNDER TEST (regression for the silent-failover bug): provider A
	// forwards two real tokens and THEN emits a terminal {Done, FinishReasonError}
	// frame. Because the client has already seen A's partial output, failing over to
	// B would concatenate B's full answer onto A's partial one — garbled output. The
	// loop must therefore treat this as TERMINAL: B is NEVER called, and the client
	// gets A's tokens followed by a single terminal error frame (never B's answer).
	primary := &fakeProvider{kind: ProviderKindOllama, words: []string{"par", "tial"}, inbandFail: true}
	secondary := &fakeProvider{kind: ProviderKindStub, words: []string{"FULL ANSWER"}, promptTok: 1, outputTok: 1}
	svc := newService(t, nil, BreakerTuning{FailureThreshold: 5}, primary, secondary)

	var got []Delta
	sink := func(d Delta) error { got = append(got, d); return nil }
	completion, err := svc.ChatCompletion(context.Background(), "team-a",
		ChatRequest{Messages: []Message{{Content: "x"}}}, sink)

	// The contract error is still ErrAllProvidersFailed (the handler maps it to a
	// terminal error frame), and the completion's finish reason is Error.
	if !errors.Is(err, ErrAllProvidersFailed) {
		t.Fatalf("expected ErrAllProvidersFailed (terminal), got %v", err)
	}
	if completion.FinishReason != FinishReasonError {
		t.Fatalf("completion finish = %v, want FinishReasonError", completion.FinishReason)
	}
	// SECONDARY MUST NOT HAVE BEEN CALLED — this is the whole point of the fix.
	if secondary.calls != 0 {
		t.Fatalf("secondary must NOT be called after mid-stream failure, calls = %d", secondary.calls)
	}
	if completion.ServedBy != ProviderKindOllama {
		t.Fatalf("served-by should reflect the failed primary, got %v", completion.ServedBy)
	}
	// The client saw A's two partial tokens, then exactly one terminal error frame —
	// and NONE of B's answer text.
	if len(got) != 3 {
		t.Fatalf("expected 2 token deltas + 1 terminal frame, got %d: %+v", len(got), got)
	}
	if got[0].Text != "par" || got[1].Text != "tial" {
		t.Fatalf("client should have received A's partial tokens, got %+v", got[:2])
	}
	if !got[2].Done || got[2].FinishReason != FinishReasonError {
		t.Fatalf("final frame must be a terminal error frame, got %+v", got[2])
	}
	for _, d := range got {
		if d.Text == "FULL ANSWER" {
			t.Fatalf("client must NEVER receive B's answer after A streamed tokens")
		}
	}
}

func TestChatCompletion_MidStreamEarlyCloseIsTerminalNoFailover(t *testing.T) {
	t.Parallel()
	// Same invariant via the OTHER mid-stream failure mode: provider A forwards a token
	// and then its channel CLOSES with no Done frame (the OOM-killed / flaky-Ollama
	// decode-error path, ollama.go streamBody returning early). This must also be
	// terminal — B is not tried, the client never sees B's answer.
	primary := &fakeProvider{kind: ProviderKindOllama, words: []string{"half"}, earlyClose: true}
	secondary := &fakeProvider{kind: ProviderKindStub, words: []string{"FULL ANSWER"}, promptTok: 1, outputTok: 1}
	svc := newService(t, nil, BreakerTuning{FailureThreshold: 5}, primary, secondary)

	var got []Delta
	sink := func(d Delta) error { got = append(got, d); return nil }
	completion, err := svc.ChatCompletion(context.Background(), "team-a",
		ChatRequest{Messages: []Message{{Content: "x"}}}, sink)

	if !errors.Is(err, ErrAllProvidersFailed) {
		t.Fatalf("expected ErrAllProvidersFailed (terminal), got %v", err)
	}
	if completion.FinishReason != FinishReasonError {
		t.Fatalf("completion finish = %v, want FinishReasonError", completion.FinishReason)
	}
	if secondary.calls != 0 {
		t.Fatalf("secondary must NOT be called after early-close mid-stream, calls = %d", secondary.calls)
	}
	// A's one token, then a terminal error frame — and none of B's text.
	if len(got) != 2 {
		t.Fatalf("expected 1 token delta + 1 terminal frame, got %d: %+v", len(got), got)
	}
	if got[0].Text != "half" {
		t.Fatalf("client should have received A's partial token, got %+v", got[0])
	}
	if !got[1].Done || got[1].FinishReason != FinishReasonError {
		t.Fatalf("final frame must be a terminal error frame, got %+v", got[1])
	}
	for _, d := range got {
		if d.Text == "FULL ANSWER" {
			t.Fatalf("client must NEVER receive B's answer after A streamed a token")
		}
	}
}

func TestChatCompletion_PreTokenErrorFrameStillFailsOver(t *testing.T) {
	t.Parallel()
	// Guard the OTHER side of the invariant: an in-band error frame with NOTHING
	// forwarded yet (words is empty) is STILL recoverable — the loop must fail over to
	// the secondary, which serves cleanly. This proves we only suppress failover when
	// tokens were actually forwarded, not on every error frame.
	primary := &fakeProvider{kind: ProviderKindOllama, inbandFail: true} // no words → fails before any token
	secondary := &fakeProvider{kind: ProviderKindStub, words: []string{"recovered"}, promptTok: 1, outputTok: 1}
	svc := newService(t, nil, BreakerTuning{FailureThreshold: 5}, primary, secondary)

	var got []string
	completion, err := svc.ChatCompletion(context.Background(), "team-a",
		ChatRequest{Messages: []Message{{Content: "x"}}}, collectSink(&got))
	if err != nil {
		t.Fatalf("pre-token error frame should fail over cleanly, got error: %v", err)
	}
	if secondary.calls != 1 {
		t.Fatalf("secondary must serve after a pre-token failure, calls = %d", secondary.calls)
	}
	if completion.ServedBy != ProviderKindStub {
		t.Fatalf("served by = %v, want stub (secondary)", completion.ServedBy)
	}
	if join(got) != "recovered" {
		t.Fatalf("streamed text = %q, want %q", join(got), "recovered")
	}
}

func TestChatCompletion_OverBudgetRejectsBeforeProviderCall(t *testing.T) {
	t.Parallel()
	prov := &fakeProvider{kind: ProviderKindStub, words: []string{"hi"}, promptTok: 1, outputTok: 1}
	bud := &fakeBudget{blocked: true, budget: 100, consumed: 100}
	svc := newService(t, bud, BreakerTuning{}, prov)

	var got []string
	_, err := svc.ChatCompletion(context.Background(), "team-broke",
		ChatRequest{Messages: []Message{{Content: "x"}}}, collectSink(&got))
	if !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded, got %v", err)
	}
	if prov.calls != 0 {
		t.Fatalf("provider must NOT be called when over budget, calls = %d", prov.calls)
	}
	if len(got) != 0 {
		t.Fatalf("no tokens should stream when over budget, got %v", got)
	}
}

func TestChatCompletion_UnderBudgetServesAndDeducts(t *testing.T) {
	t.Parallel()
	prov := &fakeProvider{kind: ProviderKindStub, words: []string{"a ", "b ", "c"}, promptTok: 4, outputTok: 3}
	bud := &fakeBudget{blocked: false, budget: 1000}
	svc := newService(t, bud, BreakerTuning{}, prov)

	var got []string
	completion, err := svc.ChatCompletion(context.Background(), "team-ok",
		ChatRequest{Messages: []Message{{Content: "x"}}}, collectSink(&got))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bud.deductCalls != 1 {
		t.Fatalf("expected exactly one Deduct, got %d", bud.deductCalls)
	}
	// total = prompt(4) + output(3) = 7; the deducted amount must equal total tokens.
	if bud.lastDeduct != 7 {
		t.Fatalf("deducted %d tokens, want 7", bud.lastDeduct)
	}
	if completion.Usage.TotalTokens != 7 {
		t.Fatalf("completion total = %d, want 7", completion.Usage.TotalTokens)
	}
}

func TestChatCompletion_BudgetCheckErrorFailsOpen(t *testing.T) {
	t.Parallel()
	// An infra error on Check must FAIL OPEN (serve anyway) — a Redis blip can't block
	// all LLM traffic platform-wide.
	prov := &fakeProvider{kind: ProviderKindStub, words: []string{"ok"}, promptTok: 1, outputTok: 1}
	bud := &fakeBudget{checkErr: errors.New("redis down"), budget: 1000}
	svc := newService(t, bud, BreakerTuning{}, prov)

	var got []string
	if _, err := svc.ChatCompletion(context.Background(), "team-a",
		ChatRequest{Messages: []Message{{Content: "x"}}}, collectSink(&got)); err != nil {
		t.Fatalf("expected fail-open serve on budget check error, got %v", err)
	}
	if prov.calls != 1 {
		t.Fatalf("provider should serve on fail-open, calls = %d", prov.calls)
	}
}

func TestChatCompletion_EmptyMessagesRejected(t *testing.T) {
	t.Parallel()
	prov := &fakeProvider{kind: ProviderKindStub}
	svc := newService(t, nil, BreakerTuning{}, prov)
	_, err := svc.ChatCompletion(context.Background(), "team-a", ChatRequest{}, func(Delta) error { return nil })
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for empty messages, got %v", err)
	}
}

func TestChatCompletion_PinnedProviderDisablesFailover(t *testing.T) {
	t.Parallel()
	// Pin the stub: even though ollama is first in order, only the stub is attempted.
	ollama := &fakeProvider{kind: ProviderKindOllama, words: []string{"O"}, promptTok: 1, outputTok: 1}
	stub := &fakeProvider{kind: ProviderKindStub, words: []string{"S"}, promptTok: 1, outputTok: 1}
	svc := newService(t, nil, BreakerTuning{}, ollama, stub)

	var got []string
	completion, err := svc.ChatCompletion(context.Background(), "team-a",
		ChatRequest{Provider: ProviderKindStub, Messages: []Message{{Content: "x"}}}, collectSink(&got))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if completion.ServedBy != ProviderKindStub {
		t.Fatalf("pinned served by = %v, want stub", completion.ServedBy)
	}
	if ollama.calls != 0 {
		t.Fatalf("pinned to stub: ollama must not be called, calls = %d", ollama.calls)
	}
}

// join concatenates streamed fragments to reconstruct the answer.
func join(parts []string) string {
	out := ""
	for _, p := range parts {
		out += p
	}
	return out
}
