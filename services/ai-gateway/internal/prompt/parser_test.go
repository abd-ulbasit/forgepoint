// parser_test.go — unit tests for the {{variable}} grammar: extraction + rendering.
//
// These are PURE-function tests (no I/O, no repository, no DB). They pin the grammar
// contract the rest of the registry depends on: what counts as a variable, how
// duplicates collapse, the first-appearance order, and the strict-vs-lenient
// missing-variable policy + the single-pass (injection-safe) substitution.
package prompt

import (
	"errors"
	"reflect"
	"testing"
)

func TestExtractVariables(t *testing.T) {
	tests := []struct {
		name     string
		template string
		want     []string
	}{
		{
			name:     "no placeholders yields empty (non-nil) slice",
			template: "a plain prompt with no variables",
			want:     []string{},
		},
		{
			name:     "distinct variables in first-appearance order",
			template: "Summarize {{document}} for a {{audience}} reader",
			want:     []string{"document", "audience"},
		},
		{
			name:     "duplicates collapse to one, keeping first-appearance order",
			template: "{{tone}}: rewrite {{text}} in {{tone}} tone, keep {{text}} meaning",
			want:     []string{"tone", "text"},
		},
		{
			name:     "inner whitespace is tolerated and trimmed",
			template: "Hello {{ name }}, welcome to {{   place}}",
			want:     []string{"name", "place"},
		},
		{
			name:     "underscores and digits are valid in names (not leading digit)",
			template: "{{user_id}} and {{item2}} but {{1bad}} is NOT a variable",
			want:     []string{"user_id", "item2"},
		},
		{
			name:     "single braces are not placeholders",
			template: "a {literal} brace and {{real}} placeholder",
			want:     []string{"real"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ExtractVariables(tc.template)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("ExtractVariables(%q) = %#v, want %#v", tc.template, got, tc.want)
			}
		})
	}
}

func TestExtractVariables_NeverNil(t *testing.T) {
	// The stored Variables slice must be non-nil even with no placeholders, so the
	// Postgres TEXT[] column gets a clean `{}` and callers can range without a guard.
	got := ExtractVariables("nothing here")
	if got == nil {
		t.Fatal("ExtractVariables returned nil; want a non-nil empty slice")
	}
	if len(got) != 0 {
		t.Fatalf("ExtractVariables returned %v; want empty", got)
	}
}

func TestRender_AllVariablesSupplied(t *testing.T) {
	template := "Summarize {{document}} for a {{audience}} reader in {{tone}} tone."
	vars := map[string]string{
		"document": "the Q3 report",
		"audience": "technical",
		"tone":     "concise",
	}
	got, err := Render(template, vars, MissingVarError)
	if err != nil {
		t.Fatalf("Render returned error: %v", err)
	}
	want := "Summarize the Q3 report for a technical reader in concise tone."
	if got != want {
		t.Fatalf("Render = %q, want %q", got, want)
	}
}

func TestRender_RepeatedVariableSubstitutedEverywhere(t *testing.T) {
	// One value for "x" fills BOTH occurrences.
	got, err := Render("{{x}}-{{x}}", map[string]string{"x": "ab"}, MissingVarError)
	if err != nil {
		t.Fatalf("Render error: %v", err)
	}
	if got != "ab-ab" {
		t.Fatalf("Render = %q, want %q", got, "ab-ab")
	}
}

func TestRender_MissingVar_StrictPolicyErrors(t *testing.T) {
	// STRICT policy: a declared variable with no value is an ErrValidation naming it.
	_, err := Render("Hello {{name}} from {{place}}", map[string]string{"name": "Sam"}, MissingVarError)
	if err == nil {
		t.Fatal("Render with a missing variable returned nil; want ErrValidation")
	}
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("Render error = %v; want errors.Is(ErrValidation)", err)
	}
	// The error should name the missing variable so the client can fix the call.
	if got := err.Error(); !contains(got, "place") {
		t.Fatalf("error %q does not name the missing variable %q", got, "place")
	}
}

func TestRender_MissingVar_LeaveEmptyPolicy(t *testing.T) {
	// LENIENT policy: a missing variable renders as the empty string, no error.
	got, err := Render("Hello {{name}} from {{place}}", map[string]string{"name": "Sam"}, MissingVarLeaveEmpty)
	if err != nil {
		t.Fatalf("Render(LeaveEmpty) error: %v", err)
	}
	if got != "Hello Sam from " {
		t.Fatalf("Render(LeaveEmpty) = %q, want %q", got, "Hello Sam from ")
	}
}

func TestRender_NoInjectionFromVariableValue(t *testing.T) {
	// A value that itself contains a "{{other}}" sequence must NOT be re-expanded — the
	// single-pass renderer only substitutes placeholders present in the ORIGINAL
	// template. This guards against template injection via user data.
	template := "User said: {{msg}}"
	vars := map[string]string{
		"msg":    "please use {{secret}} now",
		"secret": "TOP-SECRET", // must NOT leak in, because {{secret}} isn't in the template
	}
	got, err := Render(template, vars, MissingVarError)
	if err != nil {
		t.Fatalf("Render error: %v", err)
	}
	want := "User said: please use {{secret}} now"
	if got != want {
		t.Fatalf("Render = %q, want %q (the {{secret}} in the value must stay literal)", got, want)
	}
}

// contains is a tiny strings.Contains shim kept local so the test file's intent reads
// without importing strings just for one call site.
func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
