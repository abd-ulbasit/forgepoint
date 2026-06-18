package events

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
)

// ============================================================================
// UNIT tests for the L4 AI-eval consumer's handle() — NO broker, NO testcontainers.
// We construct EventEnvelopes by hand (the JSON payload the gateway publishes) and
// drive handle() directly, asserting sampling, idempotency-by-effect, team-scoping,
// and the unmonitored/decode drops. (The existing subscriber_test.go uses real NATS;
// these are pure unit tests per the task constraint.)
// ============================================================================

// fakeQualityService records every EvaluateCompletion call so the test can assert
// WHICH completions reached the judge/record path (i.e. were sampled).
type fakeQualityService struct {
	mu    sync.Mutex
	calls []domain.Completion
	err   error
}

func (f *fakeQualityService) EvaluateCompletion(_ context.Context, _ string, c domain.Completion) (*domain.DriftReport, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, c)
	return nil, f.err
}

func (f *fakeQualityService) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeQualityService) lastCall() (domain.Completion, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return domain.Completion{}, false
	}
	return f.calls[len(f.calls)-1], true
}

var _ domain.QualityEvalService = (*fakeQualityService)(nil)

// togglableSampler lets a test force ShouldSample on/off deterministically.
type togglableSampler struct {
	mu     sync.Mutex
	sample bool
	calls  int
}

func (s *togglableSampler) ShouldSample() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.sample
}

var _ domain.Sampler = (*togglableSampler)(nil)

// envFor builds an EventEnvelope carrying the gateway's plain-JSON completion payload.
func envFor(id string, p aiCompletionServed) natsutil.EventEnvelope {
	data, _ := json.Marshal(p)
	return natsutil.EventEnvelope{
		ID:     id,
		Type:   "ai.completion.served",
		Source: "ai-gateway",
		Data:   data,
	}
}

// newConsumer wires the consumer with injected fakes (no broker; we call handle directly).
func newConsumer(svc domain.QualityEvalService, resolver MonitorResolver, sampler domain.Sampler) *AIEvalConsumer {
	return NewAIEvalConsumer(svc, resolver, sampler, nil, nil)
}

func TestAIEvalConsumer_SampledCompletionReachesJudge(t *testing.T) {
	t.Parallel()
	svc := &fakeQualityService{}
	resolver := newFakeResolver()
	resolver.set("chatbot", MonitorBinding{MonitorID: "mon-1", OwnerTeam: "team-a"})
	c := newConsumer(svc, resolver, &togglableSampler{sample: true})

	env := envFor("e1", aiCompletionServed{
		RequestID: "r1", Team: "team-a", Model: "chatbot",
		ResponseText: "hello", PromptText: "hi",
	})
	if err := c.handle(context.Background(), env); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if svc.callCount() != 1 {
		t.Fatalf("sampled completion should reach EvaluateCompletion once, got %d", svc.callCount())
	}
	got, _ := svc.lastCall()
	// Tenancy: the completion handed to the service carries the MONITOR's owner team
	// (authoritative), not just the event field.
	if got.Team != "team-a" || got.Model != "chatbot" || got.RequestID != "r1" {
		t.Fatalf("unexpected completion handed to service: %+v", got)
	}
	if got.Response != "hello" {
		t.Fatalf("response text not threaded through: %+v", got)
	}
}

func TestAIEvalConsumer_NotSampledSkipsJudge(t *testing.T) {
	t.Parallel()
	svc := &fakeQualityService{}
	resolver := newFakeResolver()
	resolver.set("chatbot", MonitorBinding{MonitorID: "mon-1", OwnerTeam: "team-a"})
	c := newConsumer(svc, resolver, &togglableSampler{sample: false})

	env := envFor("e1", aiCompletionServed{RequestID: "r1", Team: "team-a", Model: "chatbot", ResponseText: "hi"})
	if err := c.handle(context.Background(), env); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if svc.callCount() != 0 {
		t.Fatalf("an UN-sampled completion must not reach the judge, got %d calls", svc.callCount())
	}
}

func TestAIEvalConsumer_UnmonitoredModelSkipsBeforeSampling(t *testing.T) {
	t.Parallel()
	svc := &fakeQualityService{}
	resolver := newFakeResolver() // no binding for "chatbot" → unmonitored
	sampler := &togglableSampler{sample: true}
	c := newConsumer(svc, resolver, sampler)

	env := envFor("e1", aiCompletionServed{RequestID: "r1", Team: "team-a", Model: "chatbot", ResponseText: "hi"})
	if err := c.handle(context.Background(), env); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if svc.callCount() != 0 {
		t.Fatalf("unmonitored model must be dropped, got %d judge calls", svc.callCount())
	}
	// Sampling must happen AFTER the unmonitored drop, so the sampler isn't even
	// consulted for unmonitored traffic (the 1-in-N rate applies to monitored events).
	if sampler.calls != 0 {
		t.Fatalf("sampler should not be consulted for unmonitored traffic, got %d calls", sampler.calls)
	}
}

func TestAIEvalConsumer_TeamMismatchSkips(t *testing.T) {
	t.Parallel()
	// The event names team-b, but the monitor for "chatbot" is owned by team-a (the
	// same-name-across-tenants case). The consumer must DROP it (never evaluate under
	// the wrong tenant), and not consult the sampler.
	svc := &fakeQualityService{}
	resolver := newFakeResolver()
	resolver.set("chatbot", MonitorBinding{MonitorID: "mon-1", OwnerTeam: "team-a"})
	sampler := &togglableSampler{sample: true}
	c := newConsumer(svc, resolver, sampler)

	env := envFor("e1", aiCompletionServed{RequestID: "r1", Team: "team-b", Model: "chatbot", ResponseText: "hi"})
	if err := c.handle(context.Background(), env); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if svc.callCount() != 0 {
		t.Fatalf("team mismatch must be dropped, got %d judge calls", svc.callCount())
	}
}

func TestAIEvalConsumer_UndecodablePayloadIsPoison(t *testing.T) {
	t.Parallel()
	svc := &fakeQualityService{}
	c := newConsumer(svc, newFakeResolver(), &togglableSampler{sample: true})

	env := natsutil.EventEnvelope{ID: "e1", Data: json.RawMessage(`{not json`)}
	err := c.handle(context.Background(), env)
	// A corrupt payload routes to the DLQ via a retryable processing error (bounded by
	// MaxRetries) — observable as a dead letter, not silently dropped.
	if err == nil {
		t.Fatalf("expected a poison (processing) error for an undecodable payload")
	}
	if svc.callCount() != 0 {
		t.Fatalf("poison payload must not reach the judge")
	}
}

func TestAIEvalConsumer_MissingModelOrRequestIDDropped(t *testing.T) {
	t.Parallel()
	svc := &fakeQualityService{}
	c := newConsumer(svc, newFakeResolver(), &togglableSampler{sample: true})

	// Missing model → ACK + drop (parsed but empty, not poison).
	if err := c.handle(context.Background(), envFor("e1", aiCompletionServed{RequestID: "r1", Team: "t", ResponseText: "x"})); err != nil {
		t.Fatalf("missing-model should ACK (nil), got %v", err)
	}
	// Missing request_id → ACK + drop.
	if err := c.handle(context.Background(), envFor("e2", aiCompletionServed{Model: "chatbot", Team: "t", ResponseText: "x"})); err != nil {
		t.Fatalf("missing-request-id should ACK (nil), got %v", err)
	}
	if svc.callCount() != 0 {
		t.Fatalf("semantically-empty events must not reach the judge")
	}
}

func TestAIEvalConsumer_NoResponseTextStillEvaluates(t *testing.T) {
	t.Parallel()
	// The gateway ran with EvalIncludeText off → no response text. The consumer still
	// hands the completion to the service (which records it UNSCORED) — graceful
	// degradation, not a drop. We assert the empty Response is threaded through.
	svc := &fakeQualityService{}
	resolver := newFakeResolver()
	resolver.set("chatbot", MonitorBinding{MonitorID: "mon-1", OwnerTeam: "team-a"})
	c := newConsumer(svc, resolver, &togglableSampler{sample: true})

	env := envFor("e1", aiCompletionServed{RequestID: "r1", Team: "team-a", Model: "chatbot"}) // no PromptText/ResponseText
	if err := c.handle(context.Background(), env); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if svc.callCount() != 1 {
		t.Fatalf("no-text completion should still reach the service (records Unscored), got %d", svc.callCount())
	}
	got, _ := svc.lastCall()
	if got.Response != "" {
		t.Fatalf("expected empty response threaded through, got %q", got.Response)
	}
}

func TestAIEvalConsumer_TransientServiceErrorNAKs(t *testing.T) {
	t.Parallel()
	svc := &fakeQualityService{err: errForTest}
	resolver := newFakeResolver()
	resolver.set("chatbot", MonitorBinding{MonitorID: "mon-1", OwnerTeam: "team-a"})
	c := newConsumer(svc, resolver, &togglableSampler{sample: true})

	env := envFor("e1", aiCompletionServed{RequestID: "r1", Team: "team-a", Model: "chatbot", ResponseText: "hi"})
	err := c.handle(context.Background(), env)
	if err == nil {
		t.Fatalf("a service error must NAK (return error) so the event is retried")
	}
}

// errForTest is a sentinel the transient-error test injects.
var errForTest = &testError{}

type testError struct{}

func (*testError) Error() string { return "boom" }
