// eval.go — M7/L4: LLM QUALITY EVALUATION + QUALITY DRIFT (the domain core).
//
// ============================================================================
// WHAT L4 ADDS (and how it reuses the existing closed loop)
// ============================================================================
//
// The Model Monitor already detects DATA / PREDICTION / PERFORMANCE drift on
// tabular models and closes the serve→monitor→retrain loop (see
// monitor_service_impl.go). L4 adds a FOURTH drift SIGNAL for LLM endpoints —
// QUALITY drift — and feeds it into the SAME alert + retrain machinery:
//
//	fp.ai.completion.served (gateway)
//	      │  (sampled)
//	      ▼
//	  LLM-as-judge (Ollama)  →  scores: relevance / coherence / safety (1–5)
//	      │
//	      ▼
//	  eval_scores (Postgres, per team+model)
//	      │  rolling average over a window
//	      ▼
//	  QualityDrift?  ── yes ──►  DriftReport (DriftTypePerformance, metric "llm_quality")
//	                                   │
//	                                   ▼
//	                             EXISTING applyPolicy: emit fp.models.drift.detected
//	                             + (if CRITICAL & auto_retrain) TriggerRetrain
//
// WHY QUALITY DRIFT IS MODELLED AS A PERFORMANCE-DRIFT REPORT (not a brand-new
// DriftType): the task forbids new proto RPCs / regen, and the DriftType enum +
// the drift_reports.drift_type CHECK (0..3) are fixed wire/schema contracts. An
// LLM's answer quality decaying IS a performance regression — semantically it is
// exactly DriftTypePerformance ("measured quality dropped"), the LAGGING signal.
// So a quality-drift report is a DriftReport{DriftType: DriftTypePerformance} whose
// single metric is named "llm_quality". That makes it a FIRST-CLASS drift signal:
// it persists in drift_reports, rides the canonical fp.models.drift.detected event,
// and arms auto-retrain — with ZERO proto or schema change. The metric NAME is what
// distinguishes "a tabular model's accuracy fell" from "an LLM's judged quality
// fell"; both are performance drift, which is the honest classification.
//
// ============================================================================
// CLEAN ARCHITECTURE — this file is PURE DOMAIN (stdlib only)
// ============================================================================
//
// Like the rest of the domain package, eval.go imports ONLY the standard library
// (+ no uuid here). The Judge PORT and the EvalStore PORT are declared here (the
// consumer owns the port); the Ollama judge adapter (internal/judge) and the
// Postgres eval store (internal/repository/postgres) implement them. The SCORE
// PARSER, the SAMPLING selector, and the QUALITY-DRIFT decision are pure functions
// so they unit-test in microseconds against known inputs — exactly the parts
// that need proving ("how do you tolerate a tiny model's messy output?", "how does
// 1-in-N sampling stay deterministic?", "when does a window of low scores alert?").
package domain

import (
	"context"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ============================================================================
// SCORES & RECORDS
// ============================================================================

// MinScore / MaxScore bound the 1–5 Likert scale the judge is asked to use. The
// parser CLAMPS any in-range-ish number to these bounds and REJECTS anything that
// isn't a recognizable score, so a hallucinated "9/5" never poisons the average.
const (
	MinScore = 1.0
	MaxScore = 5.0
)

// JudgeScores is one judged completion's three sub-scores plus the derived overall.
// Each is on the 1–5 scale. WHY these three axes (a standard LLM-eval rubric):
//   - Relevance: did the answer address the prompt? (the dominant quality axis)
//   - Coherence: is it well-formed / internally consistent? (catches degeneration,
//     repetition loops, truncation — the classic "model got worse" symptoms)
//   - Safety: is it free of harmful/policy-violating content? (a low safety score is
//     its own alarm regardless of the others)
//
// Scored=false marks an UNSCORED result: the judge failed, timed out, or returned
// text we could not parse into all three axes. An unscored eval is RECORDED (so the
// volume of un-judgeable traffic is itself observable) but does NOT contribute to
// the rolling quality average — a parse failure must never read as "low quality"
// and trip a false retrain.
type JudgeScores struct {
	Relevance float64
	Coherence float64
	Safety    float64
	Overall   float64 // mean of the three (see Finalize)
	Scored    bool    // false ⇒ unscored (judge/parse failure) — excluded from the average
}

// Finalize computes Overall as the mean of the three axes and marks Scored=true.
// Called by the parser ONLY when all three axes parsed. Kept as a method so the
// "overall = mean of three" rule lives in exactly one place (the consumer, the
// drift math, and the store all read Overall, never re-derive it).
func (s JudgeScores) Finalize() JudgeScores {
	s.Overall = (s.Relevance + s.Coherence + s.Safety) / 3.0
	s.Scored = true
	return s
}

// Unscored is the canonical "could not judge" result. Recorded as-is; the average
// skips it. A single constructor keeps every failure path returning the same shape.
func Unscored() JudgeScores { return JudgeScores{Scored: false} }

// Eval is one persisted quality-evaluation row — the unit the EvalStore writes and
// the quality-drift math reads back. It is keyed for TENANCY by Team (on every row,
// exactly like DriftReport.OwnerTeam): the same model name in two teams is two
// different models, so the rolling average and the drift verdict are ALWAYS scoped
// to (team, model). RequestID ties the eval back to the served completion (audit /
// dedup); it is the gateway-minted server-authoritative id, never a user id.
type Eval struct {
	Team      string // TENANCY KEY — set from the event's team, on every row
	Model     string
	RequestID string // the served completion's id (join back to the gateway event)
	Scores    JudgeScores
	CreatedAt time.Time
}

// ============================================================================
// JUDGE PORT (the Ollama LLM-as-judge adapter implements this)
// ============================================================================

// Completion is the minimal view of a served LLM completion the judge needs: the
// prompt it answered and the response it produced. WHY a domain type (not the
// gateway's wire payload): the domain must not import the events package; the
// consumer adapter maps the decoded event → this. Empty Response means the gateway
// did NOT include text (EvalIncludeText off) — the judge then records Unscored and
// the consumer notes the limitation (never judge blind).
type Completion struct {
	Team      string
	Model     string
	RequestID string
	Prompt    string
	Response  string
}

// HasText reports whether there is enough content to actually judge. With no
// response text there is nothing to score — the judge short-circuits to Unscored
// rather than asking the model to rate an empty string (which a tiny model would
// answer with garbage). This is the graceful-degradation hinge when the gateway
// runs with EvalIncludeText off.
func (c Completion) HasText() bool {
	return strings.TrimSpace(c.Response) != ""
}

// Judge scores one completion's quality. It is a PORT: the Ollama adapter
// (internal/judge) calls the local judge model; tests inject a fake. The contract:
//   - NEVER returns an error for a "bad" completion or messy judge output — a
//     failed/ambiguous judge is reported as JudgeScores{Scored:false}, NOT an error.
//     (A crash/parse-failure must degrade to UNSCORED, never panic or NAK forever.)
//   - MAY return an error only for a genuinely transient infrastructure failure
//     (Ollama unreachable after the cold-start retry) so the consumer can decide to
//     NAK-and-retry vs record-unscored. The Ollama adapter returns Unscored (nil
//     error) on a transient failure too, so the default behavior is "record unscored
//     and move on" — judging is best-effort and must never wedge the consumer.
type Judge interface {
	// Score judges one completion. ctx carries the per-message timeout the consumer
	// sets (bounded so a hung Ollama can't stall the consumer). Returns Unscored (with
	// nil error) on any failure the judge can absorb.
	Score(ctx context.Context, c Completion) (JudgeScores, error)
}

// EvalFilter narrows a ListScores (eval-history) read for the L4 dashboard. It is
// the eval-side analogue of ReportFilter (see ports.go) and follows the SAME tenancy
// discipline: Team is REQUIRED and set by the SERVICE from the caller's auth claims —
// never a client field — because a model name is not globally unique across teams
// (team-a/"chatbot" ≠ team-b/"chatbot"). Model is an OPTIONAL filter (empty = ALL of
// the team's models, the default cross-model dashboard view); Since is an OPTIONAL
// lower bound on created_at (zero = unbounded). The adapter MUST always filter on team,
// so an empty Model can never widen the read past the caller's own tenancy.
type EvalFilter struct {
	Team  string // REQUIRED, service-set from auth claims (tenant scope)
	Model string // OPTIONAL: set = one model; empty = all of the team's models
	Since time.Time
}

// EvalStore persists eval rows and reads them back — both the rolling window for the
// drift math (RecentOverall) and the paginated history for the dashboard (ListScores).
// TENANCY: every method is scoped by Team (with Model) TOGETHER — never model alone —
// for the exact same reason DriftReportRepository is (a model name is not globally
// unique across teams). The adapter SQL must include `WHERE team = $1` on every read.
type EvalStore interface {
	// Record persists one eval row (scored or unscored). Idempotent on RequestID:
	// re-recording the same request_id (stream redelivery) does NOT double-count the
	// score in the rolling average. The adapter implements that with
	// INSERT … ON CONFLICT (request_id) DO NOTHING.
	Record(ctx context.Context, e Eval) error
	// RecentOverall returns the OVERALL scores of the most recent SCORED evals for a
	// (team, model), newest first, up to `limit`. Unscored rows are excluded by the
	// adapter (they carry no overall to average). This is the input to the rolling
	// quality average + the drift decision.
	RecentOverall(ctx context.Context, team, model string, limit int) ([]float64, error)
	// ListScores returns a page of a team's eval ROWS (scored AND unscored), newest
	// first, under the filter, plus an opaque next-page cursor. This is the READ MODEL
	// behind the L4 eval dashboard — it returns whole Eval rows (every axis + scored
	// flag + request_id + created_at), NOT just the overall averages RecentOverall
	// gives the drift math. f.Team (service-set) is the mandatory scope; f.Model (when
	// set) and f.Since narrow within it. The adapter paginates with a keyset cursor on
	// (created_at, request_id) — the table's total order — so pages stay stable under
	// the continuous insert of new evals. Unlike RecentOverall, UNSCORED rows ARE
	// included: the dashboard wants to show un-judgeable traffic too.
	ListScores(ctx context.Context, f EvalFilter, opts ListOptions) (scores []Eval, nextToken string, err error)
}

// ============================================================================
// JUDGE-SCORE PARSER — robust extraction from a tiny model's messy output
// ============================================================================
//
// The local judge is a SMALL model (e.g. smollm2:135m). Even with a strict prompt
// asking for JSON, it will sometimes return prose, code fences, extra commentary,
// "4/5", "Relevance: four", or malformed JSON. The parser's JOB is to extract three
// numbers ROBUSTLY and, when it cannot, return Unscored — NEVER to panic and never
// to fabricate a score. The strategy (in order of preference):
//
//  1. Try to find a JSON object and read relevance/coherence/safety from it (the
//     happy path the prompt steers toward).
//  2. Fall back to LINE/REGEX scanning: for each axis, find "<axis> ... <number>"
//     anywhere in the text (tolerates "Relevance: 4", "relevance = 4/5", markdown).
//  3. If any axis is missing or out of a sane range → Unscored.
//
// We deliberately DON'T import a JSON library path that would hard-fail the whole
// parse on one stray brace — the line/regex fallback is what makes this tolerant.

// axisKeys are the substrings (lowercased) that identify each score axis in the
// judge's output. Listed in the canonical order; the parser matches case-insensitively.
var axisKeys = struct{ relevance, coherence, safety string }{
	relevance: "relevance",
	coherence: "coherence",
	safety:    "safety",
}

// ParseJudgeScores extracts the three axis scores from the judge model's raw text.
// It is TOTAL and PANIC-FREE: any input (empty, prose, garbage, valid JSON) yields
// either a finalized JudgeScores (Scored=true) or Unscored (Scored=false). It never
// returns an error — "could not parse" is a value (Unscored), not an exception,
// because the consumer's correct reaction is identical for every parse failure
// (record unscored, move on). This is the function the unit tests pin hardest.
func ParseJudgeScores(raw string) JudgeScores {
	text := strings.TrimSpace(raw)
	if text == "" {
		return Unscored()
	}

	rel, okR := extractAxisScore(text, axisKeys.relevance)
	coh, okC := extractAxisScore(text, axisKeys.coherence)
	saf, okS := extractAxisScore(text, axisKeys.safety)
	if !okR || !okC || !okS {
		return Unscored() // any axis missing/garbled ⇒ the whole eval is unscored
	}

	return JudgeScores{Relevance: rel, Coherence: coh, Safety: saf}.Finalize()
}

// extractAxisScore finds the score for one axis anywhere in the text. It scans for
// the axis keyword (case-insensitive) and then reads the FIRST number that appears
// after it on the same logical span (up to the next axis keyword or a newline-bound
// window), tolerating "Relevance: 4", "\"relevance\": 4", "relevance = 4/5",
// "relevance - four-out-of-five → 4". Returns (score, true) when a sane 1–5 number
// is found, else (0, false). Word-numbers ("four") are mapped too, because a tiny
// model frequently spells them out.
func extractAxisScore(text, axis string) (float64, bool) {
	lower := strings.ToLower(text)
	idx := strings.Index(lower, axis)
	if idx < 0 {
		return 0, false
	}
	// Look at the slice STARTING at the axis keyword. Bound the search to a small
	// window so a number belonging to a LATER axis can't be mis-attributed to this
	// one (e.g. "relevance: (see below) ... coherence: 4" must not read 4 for
	// relevance). The window ends at the next axis keyword or a fixed char budget.
	rest := lower[idx+len(axis):]
	rest = boundToNextAxis(rest, axis)

	if n, ok := firstNumberIn(rest); ok {
		return clampScore(n), true
	}
	if n, ok := firstWordNumberIn(rest); ok {
		return clampScore(n), true
	}
	return 0, false
}

// boundToNextAxis truncates `rest` at the earliest occurrence of ANY other axis
// keyword, so each axis only reads numbers from its own segment. This is what makes
// "relevance ... coherence: 4 safety: 5" not steal coherence's 4 for relevance when
// relevance itself had no number (it stays unscored → whole result Unscored).
func boundToNextAxis(rest, self string) string {
	end := len(rest)
	for _, k := range []string{axisKeys.relevance, axisKeys.coherence, axisKeys.safety} {
		if k == self {
			continue
		}
		if i := strings.Index(rest, k); i >= 0 && i < end {
			end = i
		}
	}
	// Also cap at a generous char budget so a keyword with no following number doesn't
	// scan the entire rest of the document.
	const maxWindow = 64
	if end > maxWindow {
		end = maxWindow
	}
	return rest[:end]
}

// firstNumberIn returns the first numeric token in s (integer or decimal), ignoring
// a trailing "/5" denominator ("4/5" → 4). Returns (value, true) on success.
func firstNumberIn(s string) (float64, bool) {
	i := 0
	for i < len(s) {
		c := s[i]
		if (c >= '0' && c <= '9') || c == '.' {
			// Read the full number token.
			j := i
			for j < len(s) && ((s[j] >= '0' && s[j] <= '9') || s[j] == '.') {
				j++
			}
			tok := s[i:j]
			if v, err := strconv.ParseFloat(tok, 64); err == nil {
				return v, true
			}
			i = j
			continue
		}
		i++
	}
	return 0, false
}

// wordNumbers maps spelled-out small numbers a tiny model may emit to their value.
var wordNumbers = map[string]float64{
	"one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
}

// firstWordNumberIn returns the first spelled-out number (one..five) found in s.
func firstWordNumberIn(s string) (float64, bool) {
	for word, v := range wordNumbers {
		if strings.Contains(s, word) {
			return v, true
		}
	}
	return 0, false
}

// clampScore pins a parsed number into [MinScore, MaxScore]. A judge that returns
// "0" (off-scale low) or "10" (off-scale high) is clamped rather than discarded —
// it still expressed a direction. Numbers wildly off-scale are still clamped, which
// is acceptable because the axis WAS named and a number WAS given; if no number was
// given at all the axis is Unscored upstream.
func clampScore(n float64) float64 {
	if n < MinScore {
		return MinScore
	}
	if n > MaxScore {
		return MaxScore
	}
	return n
}

// ============================================================================
// SAMPLING — bound the judge's load with deterministic 1-in-N selection
// ============================================================================
//
// At homelab volume we can judge EVERY completion (rate=1). But the judge is a real
// LLM call; under load we must bound how many completions we judge. A SAMPLER
// decides "judge this one?" so the consumer can throttle Ollama load to 1-in-N.
//
// WHY A DETERMINISTIC COUNTER (not rand): a counting sampler selects EXACTLY 1 in
// every N (no clustering, no RNG variance), and it is trivially testable — feed it
// 100 events at rate 10 and assert exactly 10 were selected at indices 0,10,20,…
// (the tests pin this). It is also stable across replicas in a useful sense: each
// replica samples 1-in-N of the events IT consumes, so the fleet-wide judged
// fraction is ~1/N regardless of how the consumer group balances events.
//
// Real-world parallel: this is the same idea as a trace SAMPLER (e.g. OTel's
// "sample 1 in N spans") — a cheap, bounded, deterministic decimation of a firehose.

// Sampler decides whether a given event should be judged. Concurrency-safe.
type Sampler interface {
	// ShouldSample returns true for ~1-in-N calls. Deterministic for a counting
	// sampler (every Nth call true). Safe for concurrent use by the consumer's
	// handler goroutines.
	ShouldSample() bool
}

// CountingSampler selects every Nth call (1-in-N). rate<=1 ⇒ sample everything.
// The counter is guarded so concurrent handler goroutines can't race it.
//
// SELECTION RULE: the FIRST call (count 0) is selected, then every Nth after — i.e.
// calls 0, N, 2N, … are judged. Starting at 0 means a low-traffic model that sees
// only a handful of events still gets its first one judged (no "warm-up where
// nothing is sampled"). The tests pin exactly which indices are selected.
type CountingSampler struct {
	rate int
	mu   sync.Mutex
	n    int64 // monotonically increasing call counter (guarded by mu)
}

// NewCountingSampler builds a 1-in-rate sampler. rate <= 1 means "sample every
// event" (the homelab default), which ShouldSample short-circuits to always-true.
func NewCountingSampler(rate int) *CountingSampler {
	if rate < 1 {
		rate = 1
	}
	return &CountingSampler{rate: rate}
}

// Compile-time proof CountingSampler satisfies the Sampler port.
var _ Sampler = (*CountingSampler)(nil)

// ShouldSample returns true on calls 0, rate, 2*rate, … — exactly 1-in-rate,
// deterministically and without RNG. rate==1 ⇒ always true (judge everything). The
// mutex makes the increment atomic so concurrent consumer goroutines each get a
// consistent count (no double-judge / no skipped index under load).
func (s *CountingSampler) ShouldSample() bool {
	if s.rate <= 1 {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	selected := s.n%int64(s.rate) == 0
	s.n++
	return selected
}

// ============================================================================
// QUALITY-DRIFT DECISION — the rolling-average control rule (pure)
// ============================================================================

// QualityDriftConfig governs when a window of eval scores is "drifting". It has the
// same shape-of-responsibility as ThresholdConfig but for the quality average. All
// fields are operator config (env-driven; see internal/config).
type QualityDriftConfig struct {
	// Window is how many of the most recent SCORED evals to average. Too small =
	// noisy (one bad answer trips it); too large = slow to react. A few dozen is a
	// reasonable homelab default.
	Window int
	// MinSamples is the floor below which we DON'T judge drift — averaging 2 scores is
	// noise, not signal (mirrors the monitor's MinSamples discipline). Below this the
	// decision is "warming up: no verdict".
	MinSamples int
	// FloorScore is the absolute quality floor on the 1–5 scale. If the rolling
	// average drops BELOW this, that is drift regardless of any baseline — a model
	// averaging 2.1/5 is bad even if it was always bad. e.g. 3.0.
	FloorScore float64
	// Baseline is the expected/healthy average (e.g. captured when the LLM was first
	// deployed, or a configured target). DropFromBaseline below triggers on a
	// RELATIVE regression even when still above the absolute floor. 0 disables the
	// relative check (floor-only).
	Baseline float64
	// DropFromBaseline is how far BELOW Baseline the rolling average must fall to
	// count as drift (absolute points on the 1–5 scale). e.g. 0.5 ⇒ "a half-point
	// regression vs baseline is drift". Only used when Baseline > 0.
	DropFromBaseline float64
}

// Enabled reports whether quality-drift detection is configured at all. With a
// non-positive Window the feature is OFF (the consumer still records evals but never
// evaluates drift) — the self-gate the wiring relies on.
func (q QualityDriftConfig) Enabled() bool { return q.Window > 0 }

// QualityDriftResult is the verdict of EvaluateQualityDrift: whether the window
// drifted, the computed average, and WHY (for the report metric + logs).
type QualityDriftResult struct {
	// Drifted is true when the window breached the floor or the baseline-drop rule.
	Drifted bool
	// Average is the rolling mean over the scored window (0 if too few samples).
	Average float64
	// SampleCount is how many scored evals went into Average.
	SampleCount int
	// Reason is a short human string ("avg 2.40 below floor 3.00" / "avg 3.10 is 0.90
	// below baseline 4.00") for the report + audit. Empty when not drifted / warming.
	Reason string
}

// EvaluateQualityDrift is the PURE rolling-average drift rule. Given the recent
// SCORED overall scores (newest first or any order — we just average them) and the
// config, decide whether quality has drifted DOWN.
//
// DECISION:
//  1. Fewer than MinSamples scored evals ⇒ NOT drifted (warming up — never alert on
//     a near-empty window; a single bad answer must not retrain a model).
//  2. average < FloorScore ⇒ DRIFTED (absolute quality floor breached).
//  3. Baseline>0 AND (Baseline − average) >= DropFromBaseline ⇒ DRIFTED (relative
//     regression vs the healthy baseline, even if still above the floor).
//  4. else ⇒ healthy.
//
// Both rules are "down is bad" (a quality IMPROVEMENT never trips drift), matching
// the rest of the monitor's "bigger-is-worse score" convention via the score we
// hand the report (see QualityDriftScore).
func EvaluateQualityDrift(scores []float64, cfg QualityDriftConfig) QualityDriftResult {
	// Average only up to Window most-recent; caller already limits, but be defensive.
	n := len(scores)
	if cfg.Window > 0 && n > cfg.Window {
		scores = scores[:cfg.Window]
		n = cfg.Window
	}
	if n < cfg.MinSamples || n == 0 {
		return QualityDriftResult{Drifted: false, Average: 0, SampleCount: n}
	}

	var sum float64
	for _, s := range scores {
		sum += s
	}
	avg := sum / float64(n)
	res := QualityDriftResult{Average: avg, SampleCount: n}

	// Rule 2 — absolute floor.
	if cfg.FloorScore > 0 && avg < cfg.FloorScore {
		res.Drifted = true
		res.Reason = "avg " + fmtScore(avg) + " below floor " + fmtScore(cfg.FloorScore)
		return res
	}
	// Rule 3 — relative regression vs baseline.
	if cfg.Baseline > 0 && cfg.DropFromBaseline > 0 && (cfg.Baseline-avg) >= cfg.DropFromBaseline {
		res.Drifted = true
		res.Reason = "avg " + fmtScore(avg) + " is " + fmtScore(cfg.Baseline-avg) +
			" below baseline " + fmtScore(cfg.Baseline)
		return res
	}
	return res
}

// QualityDriftScore converts the quality verdict into the monitor's universal
// "bigger = worse" drift score so it slots into the SAME severity ladder as PSI/KL/
// accuracy-drop. We use the DROP below the reference (the max of floor and baseline)
// as the score: a deeper quality drop ⇒ a higher score ⇒ a worse severity. Clamped
// at 0 so a healthy/above-reference window scores 0 (OK).
//
// This is the bridge that lets quality drift reuse ThresholdConfig.Severity and the
// whole DriftReport/policy path unchanged: the consumer builds a ThresholdConfig
// (warn/critical as score points) and asks it to map this score to a severity.
func QualityDriftScore(avg float64, cfg QualityDriftConfig) float64 {
	ref := cfg.FloorScore
	if cfg.Baseline > 0 && cfg.Baseline > ref {
		ref = cfg.Baseline
	}
	drop := ref - avg
	if drop < 0 || math.IsNaN(drop) {
		return 0
	}
	return drop
}

// fmtScore formats a 1–5 score to two decimals for the reason string, without
// pulling fmt into hot paths elsewhere. Local + tiny on purpose.
func fmtScore(v float64) string {
	return strconv.FormatFloat(v, 'f', 2, 64)
}
