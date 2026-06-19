package domain

// list_eval_scores_test.go — UNIT tests for MonitorService.ListEvalScores, the read
// model behind the L4 eval dashboard. These run against the in-memory fakeEvalStore
// (see quality_eval_service_test.go), NO testcontainers / NO database — the fake holds
// the EXACT invariants the Postgres adapter must (team-scoped read, optional model/since
// filters, newest-first keyset pagination), so the team-isolation / filter / cap behavior
// is real, not stubbed away.
//
// WHAT EACH TEST PINS (the task contract):
//   - the team's scores come back (happy path), newest-first;
//   - a DIFFERENT team sees NONE (tenant isolation — the load-bearing security property);
//   - the model_name filter narrows to one model;
//   - page_size is DEFAULTED (0) and CAPPED at 100 at the service boundary.

import (
	"context"
	"testing"
	"time"
)

// seedEval records one eval row into the fake store at a fixed time. scored=true with the
// given overall; the axes are set to overall too (the dashboard rounds them, and these
// tests assert on selection/ordering, not the axis values). Recording goes through the
// SAME Record path the consumer uses, so idempotency-on-request_id is exercised too.
func seedEval(t *testing.T, store *fakeEvalStore, team, model, reqID string, overall float64, at time.Time) {
	t.Helper()
	if err := store.Record(context.Background(), Eval{
		Team:      team,
		Model:     model,
		RequestID: reqID,
		Scores:    JudgeScores{Relevance: overall, Coherence: overall, Safety: overall, Overall: overall, Scored: true},
		CreatedAt: at,
	}); err != nil {
		t.Fatalf("seed eval %s: %v", reqID, err)
	}
}

// TestListEvalScores_ReturnsTeamScoresNewestFirst is the happy path: the caller's team
// scores come back, ordered newest-first, with the row fields intact.
func TestListEvalScores_ReturnsTeamScoresNewestFirst(t *testing.T) {
	h := newHarness(time.Hour, true)
	base := time.Unix(1_700_000_000, 0)

	// Three scored evals for team-a/chatbot at increasing times.
	seedEval(t, h.evals, "team-a", "chatbot", "req-1", 4, base)
	seedEval(t, h.evals, "team-a", "chatbot", "req-2", 5, base.Add(1*time.Minute))
	seedEval(t, h.evals, "team-a", "chatbot", "req-3", 3, base.Add(2*time.Minute))

	scores, next, err := h.svc.ListEvalScores(context.Background(), "team-a", EvalFilter{}, ListOptions{})
	if err != nil {
		t.Fatalf("ListEvalScores: %v", err)
	}
	if len(scores) != 3 {
		t.Fatalf("want 3 scores, got %d", len(scores))
	}
	// Newest-first: req-3 (latest), req-2, req-1.
	if scores[0].RequestID != "req-3" || scores[1].RequestID != "req-2" || scores[2].RequestID != "req-1" {
		t.Fatalf("wrong order (want req-3,req-2,req-1): %s,%s,%s", scores[0].RequestID, scores[1].RequestID, scores[2].RequestID)
	}
	// All rows are this team's, and the request_id/overall round-tripped.
	for _, s := range scores {
		if s.Team != "team-a" {
			t.Fatalf("leaked a non-team-a row: %+v", s)
		}
	}
	if next != "" {
		t.Fatalf("single page should have no next token, got %q", next)
	}
}

// TestListEvalScores_TeamIsolation is the SECURITY assertion: team-b's evals are NEVER
// returned to team-a, even for the same model name (model names are not unique across
// teams). This is the whole reason ownerTeam scopes the read and the filter's Team is
// service-set, never client-set.
func TestListEvalScores_TeamIsolation(t *testing.T) {
	h := newHarness(time.Hour, true)
	base := time.Unix(1_700_000_000, 0)

	// SAME model name "chatbot" in two teams — the cross-tenant trap.
	seedEval(t, h.evals, "team-a", "chatbot", "a-1", 5, base)
	seedEval(t, h.evals, "team-a", "chatbot", "a-2", 4, base.Add(time.Minute))
	seedEval(t, h.evals, "team-b", "chatbot", "b-1", 2, base.Add(2*time.Minute))
	seedEval(t, h.evals, "team-b", "chatbot", "b-2", 1, base.Add(3*time.Minute))

	// team-a sees ONLY its two rows.
	aScores, _, err := h.svc.ListEvalScores(context.Background(), "team-a", EvalFilter{Model: "chatbot"}, ListOptions{})
	if err != nil {
		t.Fatalf("team-a list: %v", err)
	}
	if len(aScores) != 2 {
		t.Fatalf("team-a should see exactly its 2 rows, got %d", len(aScores))
	}
	for _, s := range aScores {
		if s.RequestID == "b-1" || s.RequestID == "b-2" {
			t.Fatalf("TENANCY BREACH: team-a saw team-b's row %s", s.RequestID)
		}
	}

	// A DIFFERENT team (team-c) that has no evals at all sees NONE — proving the read is
	// keyed by team, not just "any team's chatbot".
	cScores, _, err := h.svc.ListEvalScores(context.Background(), "team-c", EvalFilter{Model: "chatbot"}, ListOptions{})
	if err != nil {
		t.Fatalf("team-c list: %v", err)
	}
	if len(cScores) != 0 {
		t.Fatalf("team-c has no evals; must see 0, got %d", len(cScores))
	}

	// Even if a malicious caller tried to set the filter's Team to team-b, the service
	// OVERWRITES it with the claim-derived ownerTeam — so team-a still sees only team-a.
	spoofed, _, err := h.svc.ListEvalScores(context.Background(), "team-a", EvalFilter{Team: "team-b", Model: "chatbot"}, ListOptions{})
	if err != nil {
		t.Fatalf("spoofed team list: %v", err)
	}
	if len(spoofed) != 2 {
		t.Fatalf("filter.Team must be ignored in favor of ownerTeam; want 2 team-a rows, got %d", len(spoofed))
	}
	for _, s := range spoofed {
		if s.Team != "team-a" {
			t.Fatalf("TENANCY BREACH via spoofed filter.Team: got %s", s.Team)
		}
	}
}

// TestListEvalScores_ModelFilter pins the optional model_name narrowing: empty lists ALL
// of the team's models; a set value narrows to one.
func TestListEvalScores_ModelFilter(t *testing.T) {
	h := newHarness(time.Hour, true)
	base := time.Unix(1_700_000_000, 0)

	seedEval(t, h.evals, "team-a", "chatbot", "c-1", 5, base)
	seedEval(t, h.evals, "team-a", "chatbot", "c-2", 4, base.Add(time.Minute))
	seedEval(t, h.evals, "team-a", "summarizer", "s-1", 3, base.Add(2*time.Minute))

	// EMPTY model_name = all of the team's models (cross-model dashboard view).
	all, _, err := h.svc.ListEvalScores(context.Background(), "team-a", EvalFilter{}, ListOptions{})
	if err != nil {
		t.Fatalf("list all models: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("empty model_name should span all team models (3), got %d", len(all))
	}

	// model_name="chatbot" narrows to just the two chatbot rows.
	chatbot, _, err := h.svc.ListEvalScores(context.Background(), "team-a", EvalFilter{Model: "chatbot"}, ListOptions{})
	if err != nil {
		t.Fatalf("list chatbot: %v", err)
	}
	if len(chatbot) != 2 {
		t.Fatalf("model_name=chatbot should return 2 rows, got %d", len(chatbot))
	}
	for _, s := range chatbot {
		if s.Model != "chatbot" {
			t.Fatalf("model filter leaked a %q row", s.Model)
		}
	}
}

// TestListEvalScores_SinceFilter pins the optional created_at lower bound.
func TestListEvalScores_SinceFilter(t *testing.T) {
	h := newHarness(time.Hour, true)
	base := time.Unix(1_700_000_000, 0)

	seedEval(t, h.evals, "team-a", "chatbot", "old", 4, base)
	seedEval(t, h.evals, "team-a", "chatbot", "new", 5, base.Add(10*time.Minute))

	// since = base+5m excludes the "old" row (at base) and keeps "new" (at base+10m).
	scores, _, err := h.svc.ListEvalScores(context.Background(), "team-a", EvalFilter{Since: base.Add(5 * time.Minute)}, ListOptions{})
	if err != nil {
		t.Fatalf("list since: %v", err)
	}
	if len(scores) != 1 || scores[0].RequestID != "new" {
		t.Fatalf("since filter should keep only the newer row, got %+v", scores)
	}
}

// TestListEvalScores_PageSizeDefaultedAndCapped pins the boundary normalization: 0 →
// DefaultListPageSize, and an over-cap request is CLAMPED to MaxListPageSize (100), not
// honored — the DoS / accidental-firehose guard. We seed MORE than the cap so a clamp is
// observable as a truncated first page plus a next-page cursor.
func TestListEvalScores_PageSizeDefaultedAndCapped(t *testing.T) {
	h := newHarness(time.Hour, true)
	base := time.Unix(1_700_000_000, 0)

	// Seed 150 scored rows so both the default (20) and the cap (100) leave a next page.
	for i := 0; i < 150; i++ {
		seedEval(t, h.evals, "team-a", "chatbot", paddedReqID(i), 4, base.Add(time.Duration(i)*time.Second))
	}

	// page_size 0 → DefaultListPageSize (20).
	def, defNext, err := h.svc.ListEvalScores(context.Background(), "team-a", EvalFilter{}, ListOptions{PageSize: 0})
	if err != nil {
		t.Fatalf("default page: %v", err)
	}
	if len(def) != DefaultListPageSize {
		t.Fatalf("page_size 0 should default to %d, got %d", DefaultListPageSize, len(def))
	}
	if defNext == "" {
		t.Fatalf("with 150 rows and a 20-row page, a next token is expected")
	}

	// page_size 5000 → clamped to MaxListPageSize (100), NOT 150.
	capped, capNext, err := h.svc.ListEvalScores(context.Background(), "team-a", EvalFilter{}, ListOptions{PageSize: 5000})
	if err != nil {
		t.Fatalf("over-cap page: %v", err)
	}
	if len(capped) != MaxListPageSize {
		t.Fatalf("over-cap page_size must clamp to %d, got %d", MaxListPageSize, len(capped))
	}
	if capNext == "" {
		t.Fatalf("100-row capped page over 150 rows must leave a next token")
	}

	// The capped page's cursor must page the remaining 50 rows (keyset forwarding works).
	rest, restNext, err := h.svc.ListEvalScores(context.Background(), "team-a", EvalFilter{}, ListOptions{PageSize: 5000, PageToken: capNext})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if len(rest) != 150-MaxListPageSize {
		t.Fatalf("second page should hold the remaining %d rows, got %d", 150-MaxListPageSize, len(rest))
	}
	if restNext != "" {
		t.Fatalf("the final page must have no next token, got %q", restNext)
	}
}

// paddedReqID makes a zero-padded request id so lexical and numeric order agree (keeps
// the keyset tiebreaker deterministic in the seed loop). Distinct from the single-rune
// reqID helper in quality_eval_service_test.go (which doesn't pad past 26 ids).
func paddedReqID(i int) string {
	const digits = "0123456789"
	return "req-" + string([]byte{digits[(i/100)%10], digits[(i/10)%10], digits[i%10]})
}
