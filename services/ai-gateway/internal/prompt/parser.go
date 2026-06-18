// parser.go — the {{variable}} template grammar: extraction + rendering.
//
// ============================================================================
// THE PROMPT TEMPLATE GRAMMAR (a deliberately tiny, well-defined subset)
// ============================================================================
//
// A prompt template is plain text with {{variable}} placeholders, e.g.:
//
//	"Summarize {{document}} for a {{audience}} reader in {{tone}} tone."
//
// A placeholder is the literal "{{", then a variable NAME, then "}}". A valid
// variable name is [A-Za-z_][A-Za-z0-9_]* (an identifier — letters/digits/
// underscore, not starting with a digit). We use a regexp rather than a hand-rolled
// scanner because the grammar is regular and a single anchored pattern is both
// auditable and fast.
//
// WHY this is intentionally NOT a full template engine (Go text/template, Jinja):
// a prompt registry's value is a SIMPLE, predictable, auditable substitution — no
// conditionals, loops, function calls, or arbitrary expression evaluation. Those
// would (a) be a code-execution / injection surface on user-authored templates and
// (b) make "what variables does this prompt need?" undecidable. A flat {{name}}
// grammar keeps extraction total and rendering a pure string replace. This mirrors
// how MLflow / LangSmithHub model prompt variables: named slots, nothing more.
//
// These are PURE functions (no I/O, no state) — the most testable shape, and the
// reason the parser lives in the domain.
package prompt

import "regexp"

// variablePattern matches a single {{identifier}} placeholder and CAPTURES the
// identifier in group 1. Breakdown:
//
//	\{\{        literal "{{"
//	\s*         optional inner whitespace, so "{{ name }}" is accepted as "name"
//	([A-Za-z_][A-Za-z0-9_]*)   group 1: the variable name (an identifier)
//	\s*         optional inner whitespace
//	\}\}        literal "}}"
//
// Allowing surrounding whitespace inside the braces is a usability nicety (authors
// often write "{{ audience }}"); the captured name is the trimmed identifier, so
// the declared-variable set and the render lookup both key on the same clean name.
var variablePattern = regexp.MustCompile(`\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}`)

// ExtractVariables returns the DISTINCT variable names declared in a template, in
// FIRST-APPEARANCE order. Order is stable (not map-iteration random) so the stored
// Variables slice is deterministic — two creates of the same template yield the
// identical declared-variable list, which matters for equality, tests, and a stable
// API response.
//
// DE-DUPLICATION: "{{x}} ... {{x}}" declares the single variable "x". A caller
// satisfies it with ONE value for "x"; the renderer substitutes every occurrence.
//
// Returns an EMPTY (non-nil) slice for a template with no placeholders, so callers
// can range over it without a nil check and the stored value is a clean `{}` array
// rather than SQL NULL.
func ExtractVariables(template string) []string {
	matches := variablePattern.FindAllStringSubmatch(template, -1)
	// Pre-size for the common case; the seen set dedups so the final length may be
	// smaller. A non-nil empty slice is the no-placeholder result.
	out := make([]string, 0, len(matches))
	seen := make(map[string]struct{}, len(matches))
	for _, m := range matches {
		name := m[1] // group 1 — the trimmed identifier
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

// MissingVarPolicy selects how Render treats a placeholder whose variable has no
// value in the supplied map. We make the policy EXPLICIT (a typed choice, not a
// magic bool) so the decision is visible at the call site and documented in one place.
type MissingVarPolicy int

const (
	// MissingVarError — STRICT (the default the service uses): a placeholder with no
	// supplied value is an ERROR. RATIONALE: a prompt is fed to an LLM and BILLED; a
	// half-filled template ("Summarize  for a  reader") is almost always a caller bug
	// that would silently produce a low-quality, wrong, or cost-wasting completion.
	// Failing loudly with InvalidArgument forces the caller to pass every declared
	// variable — the same posture as a SQL prepared statement rejecting a missing
	// bind parameter. This is the policy RenderPrompt enforces.
	MissingVarError MissingVarPolicy = iota
	// MissingVarLeaveEmpty — LENIENT: a placeholder with no value renders as the
	// empty string. Offered as an alternative for callers that intentionally render
	// partial templates (e.g. progressive prompt assembly). NOT used by RenderPrompt;
	// provided so the policy is a real, documented choice rather than a hardcoded one.
	MissingVarLeaveEmpty
)

// Render substitutes {{variable}} placeholders in template with values from vars,
// honoring the MissingVarPolicy for any placeholder lacking a value.
//
// THE SUBSTITUTION (interview note): we do a SINGLE regexp pass with a replacer
// callback, NOT iterative strings.Replace per variable. WHY single-pass matters:
//
//  1. NO RE-SUBSTITUTION / INJECTION: if a variable's VALUE itself contains a
//     "{{other}}" sequence, a single pass leaves it untouched (we only replace the
//     placeholders found in the ORIGINAL template). Iterative replacement could
//     re-scan an already-substituted value and expand a placeholder the user's DATA
//     happened to contain — a template-injection bug. One pass over the original is
//     the safe construction.
//  2. CORRECT MISSING-VAR DETECTION: the callback sees every placeholder exactly
//     once and can apply the policy per-occurrence.
//
// On MissingVarError, the FIRST missing variable short-circuits with an
// ErrValidation naming it (collected via the closure's missing capture, checked
// after the replace). Returns the rendered text and nil on success.
func Render(template string, vars map[string]string, policy MissingVarPolicy) (string, error) {
	var missing string // the first unsatisfied variable name, "" if none
	rendered := variablePattern.ReplaceAllStringFunc(template, func(match string) string {
		// Re-extract the name from THIS match. FindStringSubmatch on the single match
		// is cheap and avoids threading capture groups through ReplaceAllStringFunc
		// (which only hands us the whole matched text, not the groups).
		name := variablePattern.FindStringSubmatch(match)[1]
		if val, ok := vars[name]; ok {
			return val
		}
		// No value supplied for this placeholder.
		if policy == MissingVarLeaveEmpty {
			return ""
		}
		// STRICT policy: record the first missing var (don't overwrite a prior one) and
		// leave the placeholder as-is; we'll turn the recorded name into an error below.
		if missing == "" {
			missing = name
		}
		return match
	})
	if missing != "" {
		return "", missingVarError(missing)
	}
	return rendered, nil
}
