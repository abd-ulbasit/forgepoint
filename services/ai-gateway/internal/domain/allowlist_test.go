// allowlist_test.go — UNIT tests for the runtime governance allow-list (M7/L5).
//
// Pure, no infra: construct an AllowList from config-shaped inputs and assert
// Permits. Covers the three behaviors the task calls out — allowed model passes,
// disallowed model → denied, empty list = allow all — plus the provider-pin gate and
// the edge cases the doc commits to (case-insensitivity, empty-model-with-a-list).
package domain

import "testing"

func TestAllowList_EmptyAllowsAll(t *testing.T) {
	t.Parallel()
	// EMPTY list (the dev default) must permit any model + any provider, and report
	// AllowsAll so wiring can log the ungoverned posture.
	a := NewAllowList(nil, nil)
	if !a.AllowsAll() {
		t.Fatal("empty allow-list should report AllowsAll() == true")
	}
	cases := []struct {
		model    string
		provider ProviderKind
	}{
		{"smollm2:135m", ProviderKindOllama},
		{"gpt-4o-mini", ProviderKindOpenAI},
		{"", ProviderKindUnspecified}, // no model, no pin → provider default, allowed.
		{"anything-at-all", ProviderKindAnthropic},
	}
	for _, c := range cases {
		if !a.Permits(c.model, c.provider) {
			t.Errorf("empty allow-list should permit (%q, %v); got denied", c.model, c.provider)
		}
	}
}

func TestAllowList_ModelGate(t *testing.T) {
	t.Parallel()
	// A configured model list: only listed models pass; everything else is denied.
	a := NewAllowList([]string{"smollm2:135m", "gpt-4o-mini"}, nil)
	if a.AllowsAll() {
		t.Fatal("a configured model list must not report AllowsAll()")
	}

	// Allowed model passes (unpinned provider).
	if !a.Permits("smollm2:135m", ProviderKindUnspecified) {
		t.Error("an allowed model should pass")
	}
	// Case-insensitive: the doc commits to lower-cased exact match.
	if !a.Permits("GPT-4o-Mini", ProviderKindUnspecified) {
		t.Error("model match must be case-insensitive")
	}
	// Disallowed model → denied (the shadow-model block).
	if a.Permits("gpt-4o", ProviderKindUnspecified) {
		t.Error("a disallowed model must be denied")
	}
	// An EMPTY model when a list is configured is DENIED (can't implicitly bless a
	// provider-default model past an approved-models gate).
	if a.Permits("", ProviderKindUnspecified) {
		t.Error("empty model with a configured list must be denied")
	}
}

func TestAllowList_ProviderPinGate(t *testing.T) {
	t.Parallel()
	// Only provider kinds configured: a PINNED provider must be on the list; an
	// UNPINNED request (Unspecified) always passes the provider dimension (routing
	// follows the failover order). Model dimension is unconstrained here.
	a := NewAllowList(nil, []ProviderKind{ProviderKindOllama})

	// Pinned to an allowed provider → passes.
	if !a.Permits("any-model", ProviderKindOllama) {
		t.Error("a request pinned to an allowed provider should pass")
	}
	// Pinned to a NON-allowed provider → denied.
	if a.Permits("any-model", ProviderKindOpenAI) {
		t.Error("a request pinned to a non-allowed provider must be denied")
	}
	// UNPINNED (Unspecified) → passes the provider gate (model gate is open here too).
	if !a.Permits("any-model", ProviderKindUnspecified) {
		t.Error("an unpinned request should pass the provider gate")
	}
}

func TestAllowList_BothDimensionsMustPass(t *testing.T) {
	t.Parallel()
	// Both model AND provider configured: a request must satisfy BOTH gates.
	a := NewAllowList([]string{"gpt-4o-mini"}, []ProviderKind{ProviderKindOpenAI})

	// Allowed model + allowed pinned provider → passes.
	if !a.Permits("gpt-4o-mini", ProviderKindOpenAI) {
		t.Error("allowed model + allowed pinned provider should pass")
	}
	// Allowed model but DISALLOWED pinned provider → denied (provider gate fails).
	if a.Permits("gpt-4o-mini", ProviderKindAnthropic) {
		t.Error("a disallowed pinned provider must deny even with an allowed model")
	}
	// DISALLOWED model + allowed provider → denied (model gate fails).
	if a.Permits("gpt-4o", ProviderKindOpenAI) {
		t.Error("a disallowed model must deny even with an allowed provider")
	}
	// Allowed model, UNPINNED → passes (provider gate is open for Unspecified).
	if !a.Permits("gpt-4o-mini", ProviderKindUnspecified) {
		t.Error("allowed model, unpinned should pass")
	}
}

func TestAllowList_NormalizesInput(t *testing.T) {
	t.Parallel()
	// Whitespace + a trailing-comma blank (e.g. FP_AI_ALLOWED_MODELS="a, ,") must be
	// trimmed/dropped, and Unspecified provider kinds ignored, so the gate is built
	// from clean sets. A blank entry must NOT become a matchable "" model.
	a := NewAllowList([]string{" smollm2:135m ", "", "  "}, []ProviderKind{ProviderKindUnspecified, ProviderKindOllama})

	if !a.Permits("smollm2:135m", ProviderKindOllama) {
		t.Error("trimmed model should be permitted")
	}
	if a.Permits("", ProviderKindOllama) {
		t.Error("a dropped blank model entry must not make empty-model match")
	}
	// The Unspecified provider was dropped; only Ollama is allowed as a pin.
	if a.Permits("smollm2:135m", ProviderKindOpenAI) {
		t.Error("OpenAI pin should be denied (only Ollama configured)")
	}
}
