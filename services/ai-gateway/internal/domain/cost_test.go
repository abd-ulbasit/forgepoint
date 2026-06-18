// cost_test.go — UNIT tests for the per-provider cost computation.
package domain

import "testing"

func TestCostMicroUSD(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		kind     ProviderKind
		prompt   int32
		output   int32
		wantCost int64
	}{
		{
			// Ollama: prompt 1000 * 100/1000 = 100; output 1000 * 400/1000 = 400 → 500.
			name: "ollama round thousands", kind: ProviderKindOllama,
			prompt: 1000, output: 1000, wantCost: 500,
		},
		{
			// Sub-1K counts truncate toward zero (charge-down policy): 500*100/1000=50,
			// 300*400/1000=120 → 170.
			name: "stub partial thousands", kind: ProviderKindStub,
			prompt: 500, output: 300, wantCost: 170,
		},
		{
			// Tiny counts truncate to zero — the documented integer behavior.
			name: "tiny counts truncate to zero", kind: ProviderKindOllama,
			prompt: 5, output: 2, wantCost: 0,
		},
		{
			// An unknown provider prices at zero (defensive default).
			name: "unknown provider zero", kind: ProviderKindUnspecified,
			prompt: 1000, output: 1000, wantCost: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := costMicroUSD(tt.kind, tt.prompt, tt.output); got != tt.wantCost {
				t.Fatalf("costMicroUSD(%v, %d, %d) = %d, want %d",
					tt.kind, tt.prompt, tt.output, got, tt.wantCost)
			}
		})
	}
}
