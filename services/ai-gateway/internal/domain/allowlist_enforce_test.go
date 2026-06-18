// allowlist_enforce_test.go — the allow-list ENFORCEMENT POINT, through ChatCompletion.
//
// allowlist_test.go proves the AllowList value's Permits logic in isolation; this
// file proves the GatewayService actually ENFORCES it before routing: a disallowed
// model returns ErrModelNotAllowed and NO provider is ever called, while an allowed
// model serves normally. This is the data-plane governance gate working end to end in
// the use-case (the handler then maps ErrModelNotAllowed → PermissionDenied, audited).
package domain

import (
	"context"
	"errors"
	"testing"
	"time"
)

// newServiceWithAllowList wires a service with a specific allow-list (the failover
// providers in order). Mirrors newService but threads the AllowList through ServiceDeps.
func newServiceWithAllowList(t *testing.T, allow AllowList, provs ...Provider) GatewayService {
	t.Helper()
	return NewGatewayService(ServiceDeps{
		Providers: NewProviderRegistry(provs...),
		Breakers:  NewBreakerRegistry(BreakerTuning{FailureThreshold: 3}, time.Now),
		AllowList: allow,
		Now:       time.Now,
	})
}

func TestChatCompletion_AllowList_DisallowedModelDenied(t *testing.T) {
	t.Parallel()
	// The deployment permits only "smollm2:135m". A request for "gpt-4o" must be denied
	// with ErrModelNotAllowed BEFORE any provider call — the provider's Chat must never
	// run (governance rejects pre-routing).
	prov := &fakeProvider{kind: ProviderKindStub, words: []string{"should", "not", "stream"}, promptTok: 1, outputTok: 1}
	allow := NewAllowList([]string{"smollm2:135m"}, nil)
	svc := newServiceWithAllowList(t, allow, prov)

	var got []string
	_, err := svc.ChatCompletion(context.Background(), "team-a",
		ChatRequest{Model: "gpt-4o", Messages: []Message{{Role: RoleUser, Content: "hi"}}},
		collectSink(&got))

	if !errors.Is(err, ErrModelNotAllowed) {
		t.Fatalf("expected ErrModelNotAllowed for a disallowed model, got %v", err)
	}
	if prov.calls != 0 {
		t.Fatalf("provider must NOT be called for a disallowed model; got %d calls", prov.calls)
	}
	if len(got) != 0 {
		t.Fatalf("no tokens should be streamed on a governance deny; got %v", got)
	}
}

func TestChatCompletion_AllowList_AllowedModelServes(t *testing.T) {
	t.Parallel()
	// The same allow-list, but now the request names an ALLOWED model — it must route
	// and serve normally (the gate is a pass-through for permitted models).
	prov := &fakeProvider{kind: ProviderKindStub, words: []string{"hi ", "there"}, promptTok: 5, outputTok: 3}
	allow := NewAllowList([]string{"smollm2:135m"}, nil)
	svc := newServiceWithAllowList(t, allow, prov)

	var got []string
	completion, err := svc.ChatCompletion(context.Background(), "team-a",
		ChatRequest{Model: "smollm2:135m", Messages: []Message{{Role: RoleUser, Content: "hi"}}},
		collectSink(&got))

	if err != nil {
		t.Fatalf("an allowed model should serve; got error %v", err)
	}
	if prov.calls != 1 {
		t.Fatalf("expected the provider to serve once; got %d calls", prov.calls)
	}
	if completion.ServedBy != ProviderKindStub {
		t.Fatalf("served_by = %v, want stub", completion.ServedBy)
	}
	if join(got) != "hi there" {
		t.Fatalf("streamed text = %q, want %q", join(got), "hi there")
	}
}

func TestChatCompletion_AllowList_EmptyAllowsAnyModel(t *testing.T) {
	t.Parallel()
	// The ZERO-value AllowList (the dev default, allow all) must let any model through
	// — proving the permissive default is wired correctly through the use-case.
	prov := &fakeProvider{kind: ProviderKindStub, words: []string{"ok"}, promptTok: 1, outputTok: 1}
	svc := newServiceWithAllowList(t, AllowList{}, prov)

	var got []string
	_, err := svc.ChatCompletion(context.Background(), "team-a",
		ChatRequest{Model: "some-unlisted-model", Messages: []Message{{Role: RoleUser, Content: "hi"}}},
		collectSink(&got))

	if err != nil {
		t.Fatalf("empty allow-list should permit any model; got %v", err)
	}
	if prov.calls != 1 {
		t.Fatalf("provider should serve under an empty allow-list; got %d calls", prov.calls)
	}
}
