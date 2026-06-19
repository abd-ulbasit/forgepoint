// eval_repository_test.go — INTEGRATION tests for the eval_scores Postgres adapter's
// READ model (ListScores), the query behind the L4 eval dashboard.
//
// These run against a REAL Postgres (via newTestStore → testutil.StartPostgres, which
// calls SkipIfNoDocker) so the actual SQL is exercised: the (team [, model] [, since])
// WHERE, the keyset cursor on (created_at, request_id), the newest-first ORDER BY, and
// the +1-sentinel next-page detection. Mocking the DB would hide exactly the tenancy +
// ordering behavior we must prove. The existing eval Record/RecentOverall paths are
// covered elsewhere; this file pins ListScores.
package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
)

// recordEval is a terse helper to insert one scored eval row at a fixed time through the
// production Record path (which is idempotent on request_id).
func recordEval(t *testing.T, r *EvalRepository, ctx context.Context, team, model, reqID string, overall float64, at time.Time) {
	t.Helper()
	if err := r.Record(ctx, domain.Eval{
		Team:      team,
		Model:     model,
		RequestID: reqID,
		Scores:    domain.JudgeScores{Relevance: overall, Coherence: overall, Safety: overall, Overall: overall, Scored: true},
		CreatedAt: at,
	}); err != nil {
		t.Fatalf("record eval %s: %v", reqID, err)
	}
}

// TestEval_ListScores_TenancyAndFilters proves the read is team-scoped, that the optional
// model + since filters narrow correctly, and that an unscoped model lists all of the
// team's models — all against real SQL.
func TestEval_ListScores_TenancyAndFilters(t *testing.T) {
	store, ctx := newTestStore(t)
	r := store.Evals()
	base := tnow()

	// team-a has two models; team-b has the SAME model name "chatbot" (the cross-tenant trap).
	recordEval(t, r, ctx, "team-a", "chatbot", "a-c-1", 4, base)
	recordEval(t, r, ctx, "team-a", "chatbot", "a-c-2", 5, base.Add(1*time.Minute))
	recordEval(t, r, ctx, "team-a", "summarizer", "a-s-1", 3, base.Add(2*time.Minute))
	recordEval(t, r, ctx, "team-b", "chatbot", "b-c-1", 1, base.Add(3*time.Minute))

	// team-a, no model filter: all 3 of team-a's rows, newest-first, ZERO team-b rows.
	all, _, err := r.ListScores(ctx, domain.EvalFilter{Team: "team-a"}, domain.ListOptions{PageSize: 50})
	if err != nil {
		t.Fatalf("list all team-a: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("team-a should have 3 rows, got %d", len(all))
	}
	for _, e := range all {
		if e.Team != "team-a" {
			t.Fatalf("TENANCY BREACH: got a %q row in team-a's list", e.Team)
		}
	}
	// Newest-first ordering by created_at.
	if all[0].RequestID != "a-s-1" || all[2].RequestID != "a-c-1" {
		t.Fatalf("not newest-first: %s ... %s", all[0].RequestID, all[2].RequestID)
	}

	// team-a + model "chatbot": exactly the two chatbot rows, never team-b's chatbot.
	chatbot, _, err := r.ListScores(ctx, domain.EvalFilter{Team: "team-a", Model: "chatbot"}, domain.ListOptions{PageSize: 50})
	if err != nil {
		t.Fatalf("list team-a chatbot: %v", err)
	}
	if len(chatbot) != 2 {
		t.Fatalf("team-a chatbot should have 2 rows, got %d", len(chatbot))
	}
	for _, e := range chatbot {
		if e.RequestID == "b-c-1" {
			t.Fatalf("TENANCY BREACH: team-a saw team-b's chatbot row")
		}
	}

	// since filter: only rows at-or-after base+90s (excludes a-c-1 @base, a-c-2 @+60s).
	recent, _, err := r.ListScores(ctx, domain.EvalFilter{Team: "team-a", Since: base.Add(90 * time.Second)}, domain.ListOptions{PageSize: 50})
	if err != nil {
		t.Fatalf("list since: %v", err)
	}
	if len(recent) != 1 || recent[0].RequestID != "a-s-1" {
		t.Fatalf("since filter wrong: %+v", recent)
	}
}

// TestEval_ListScores_KeysetPagination proves the keyset cursor walks the full set with no
// gaps or duplicates and that the page-size sentinel mints a next token only when more
// rows remain — against real SQL ordering.
func TestEval_ListScores_KeysetPagination(t *testing.T) {
	store, ctx := newTestStore(t)
	r := store.Evals()
	base := tnow()

	const total = 25
	for i := 0; i < total; i++ {
		// Distinct created_at per row so ordering is unambiguous.
		recordEval(t, r, ctx, "team-a", "chatbot", reqIDPad(i), 4, base.Add(time.Duration(i)*time.Second))
	}

	// Page through in chunks of 10; collect every request_id and assert no dup/gap.
	seen := map[string]bool{}
	token := ""
	pages := 0
	for {
		page, next, err := r.ListScores(ctx, domain.EvalFilter{Team: "team-a"}, domain.ListOptions{PageSize: 10, PageToken: token})
		if err != nil {
			t.Fatalf("page %d: %v", pages, err)
		}
		pages++
		for _, e := range page {
			if seen[e.RequestID] {
				t.Fatalf("DUPLICATE row across pages: %s", e.RequestID)
			}
			seen[e.RequestID] = true
		}
		if next == "" {
			break
		}
		token = next
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != total {
		t.Fatalf("keyset pagination dropped rows: saw %d of %d", len(seen), total)
	}
	// 25 rows / 10 per page = 3 pages (10, 10, 5).
	if pages != 3 {
		t.Fatalf("want 3 pages for 25 rows at size 10, got %d", pages)
	}
}

// reqIDPad zero-pads so lexical order matches numeric order (keyset tiebreaker determinism).
func reqIDPad(i int) string {
	const digits = "0123456789"
	return "req-" + string([]byte{digits[(i/10)%10], digits[i%10]})
}
