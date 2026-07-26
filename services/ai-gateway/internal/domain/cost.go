// cost.go — the per-provider cost rate table and the token→cost computation.
//
// ============================================================================
// COST METERING — attaching a price to tokens (the L2 metering seam)
// ============================================================================
//
// The gateway is the ONE place that knows both the token counts (from the
// provider's usage frame) AND the price per token (this table), so it is where a
// completion's cost is computed and stamped onto the served event. Billing (L2)
// reconciles its ledger from those events; it does not re-price.
//
// UNITS: cost is in MICRO-USD (1e-6 USD), an integer, to avoid float money math
// (the classic "never store money as a float" rule — rounding drift compounds
// across millions of completions). The rate table is expressed per-1K-tokens in
// micro-USD because that is how LLM vendors quote (e.g. "$0.50 / 1M tokens"), and
// integer math per-1K keeps the arithmetic exact.
//
// WHY a flat per-1K rate and not separate prompt/completion prices: real vendors
// price input and output tokens differently (output is ~3-4x), and the table is
// SHAPED to allow that (promptMicroPer1K vs outputMicroPer1K) — the local Ollama is
// "free" (self-hosted compute) but we still meter a NOMINAL internal price so cost
// dashboards have a non-zero signal and the accounting path is exercised end to end.
// ============================================================================
package domain

// providerRate is the price for one provider, split prompt vs output (vendors
// charge output tokens more). Micro-USD per 1,000 tokens.
type providerRate struct {
	promptMicroPer1K int64
	outputMicroPer1K int64
}

// rateTable is the per-provider price book. The Ollama/stub rates are NOMINAL
// internal prices (self-hosted compute is "free" but a zero price would make cost
// dashboards and budget-by-cost meaningless), set to mimic a small open model. The
// cloud rates are illustrative placeholders for when those providers are wired.
//
// Keeping this a package var (not a const map — Go has no const maps) is fine: it
// is read-only after init and never mutated. A future iteration loads it from
// config; hard-coding here keeps the core dependency-free and simple.
var rateTable = map[ProviderKind]providerRate{
	// ~$0.10 / 1M prompt, ~$0.40 / 1M output — a plausible tiny-model internal price.
	ProviderKindOllama: {promptMicroPer1K: 100, outputMicroPer1K: 400},
	// The stub mirrors Ollama so deterministic tests assert a stable, non-zero cost.
	ProviderKindStub: {promptMicroPer1K: 100, outputMicroPer1K: 400},
	// Illustrative cloud prices (not wired in this core).
	ProviderKindOpenAI:    {promptMicroPer1K: 500, outputMicroPer1K: 1500},
	ProviderKindAnthropic: {promptMicroPer1K: 800, outputMicroPer1K: 2400},
}

// costMicroUSD prices a completion: prompt tokens at the provider's prompt rate +
// output tokens at its output rate, each per-1K. Integer math throughout (no
// floats). An unknown provider prices at zero (no fabricated cost) — the caller
// has already classified the provider, so this is a defensive default, not a path
// the happy case hits.
//
// ROUNDING: (tokens * ratePer1K) / 1000 truncates toward zero — a sub-1K-token
// completion is charged a fraction of the per-1K rate, rounded DOWN. Charging down
// (never up) is the customer-friendly, audit-defensible choice for a soft internal
// meter; Billing's authoritative ledger can apply its own rounding policy if it
// ever bills externally.
func costMicroUSD(kind ProviderKind, promptTokens, outputTokens int32) int64 {
	r, ok := rateTable[kind]
	if !ok {
		return 0
	}
	prompt := (int64(promptTokens) * r.promptMicroPer1K) / 1000
	output := (int64(outputTokens) * r.outputMicroPer1K) / 1000
	return prompt + output
}
