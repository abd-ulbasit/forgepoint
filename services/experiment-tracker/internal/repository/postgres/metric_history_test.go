package postgres

import (
	"testing"

	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/domain"
)

// seedMetrics writes a known grid of points for one run: two keys, several steps
// each. Returns the run for follow-up queries.
func seedMetrics(t *testing.T, s *Store, team string) domain.Run {
	t.Helper()
	ctx := contextBG()
	_, run := seedExperimentAndRun(t, ctx, s, team)
	batch := []domain.MetricPoint{
		mp("loss", 1, 0.9), mp("loss", 2, 0.7), mp("loss", 3, 0.5), mp("loss", 4, 0.4),
		mp("accuracy", 1, 0.3), mp("accuracy", 2, 0.5), mp("accuracy", 3, 0.8),
	}
	if _, err := s.Runs().AppendMetrics(ctx, run.ID, batch); err != nil {
		t.Fatalf("seed metrics: %v", err)
	}
	return run
}

// TestMetricHistoryOrdering: results come back ordered (key, step, ts) — the
// stable order GroupIntoSeries/ComputeFinalMetrics rely on. All keys interleave
// by key first, then ascending step.
func TestMetricHistoryOrdering(t *testing.T) {
	s, ctx := newTestStore(t)
	run := seedMetrics(t, s, "team-a")

	pts := drainHistory(t, ctx, s.Runs(), domain.MetricHistoryQuery{RunID: run.ID})
	if len(pts) != 7 {
		t.Fatalf("expected 7 points, got %d", len(pts))
	}
	// First 3 are accuracy (a < l), ascending step; then loss ascending step.
	for i := 1; i < len(pts); i++ {
		prev, cur := pts[i-1], pts[i]
		if cur.Key < prev.Key || (cur.Key == prev.Key && cur.Step < prev.Step) {
			t.Fatalf("ordering violated at %d: %+v before %+v", i, prev, cur)
		}
	}
}

// TestMetricHistoryKeyFilter: Keys filter restricts to the requested metric(s) via
// a single bound ANY($n) array parameter (no per-key string concatenation).
func TestMetricHistoryKeyFilter(t *testing.T) {
	s, ctx := newTestStore(t)
	run := seedMetrics(t, s, "team-a")

	pts := drainHistory(t, ctx, s.Runs(), domain.MetricHistoryQuery{
		RunID: run.ID,
		Keys:  []string{"loss"},
	})
	if len(pts) != 4 {
		t.Fatalf("expected 4 loss points, got %d", len(pts))
	}
	for _, p := range pts {
		if p.Key != "loss" {
			t.Fatalf("key filter leaked %q", p.Key)
		}
	}
}

// TestMetricHistoryStepRange: MinStep/MaxStep bound the scan inclusively, and the
// Has* flags distinguish "step 0" from "no bound".
func TestMetricHistoryStepRange(t *testing.T) {
	s, ctx := newTestStore(t)
	run := seedMetrics(t, s, "team-a")

	// loss steps in [2,3] → 2 points.
	pts := drainHistory(t, ctx, s.Runs(), domain.MetricHistoryQuery{
		RunID:      run.ID,
		Keys:       []string{"loss"},
		MinStep:    2,
		MaxStep:    3,
		HasMinStep: true,
		HasMaxStep: true,
	})
	if len(pts) != 2 {
		t.Fatalf("expected 2 points in step [2,3], got %d (%+v)", len(pts), pts)
	}
	for _, p := range pts {
		if p.Step < 2 || p.Step > 3 {
			t.Fatalf("step range leaked step %d", p.Step)
		}
	}

	// MinStep only (>=3) across both keys: loss@3,4 + accuracy@3 = 3 points.
	pts2 := drainHistory(t, ctx, s.Runs(), domain.MetricHistoryQuery{
		RunID:      run.ID,
		MinStep:    3,
		HasMinStep: true,
	})
	if len(pts2) != 3 {
		t.Fatalf("expected 3 points with step>=3, got %d", len(pts2))
	}
}

// TestMetricHistoryPaginationExact: paging at size 2 walks the whole 7-point grid
// with no gaps or duplicates — the keyset (key,step,ts) cursor boundary is exact.
func TestMetricHistoryPaginationExact(t *testing.T) {
	s, ctx := newTestStore(t)
	run := seedMetrics(t, s, "team-a")

	seen := map[string]bool{}
	token := ""
	pages := 0
	for {
		pts, next, err := s.Runs().GetMetricHistory(ctx, domain.MetricHistoryQuery{
			RunID:      run.ID,
			Pagination: domain.ListOptions{PageSize: 2, PageToken: token},
		})
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		for _, p := range pts {
			k := p.Key + ":" + itoa(p.Step)
			if seen[k] {
				t.Fatalf("duplicate point %s across pages", k)
			}
			seen[k] = true
		}
		pages++
		if next == "" {
			break
		}
		token = next
		if pages > 20 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != 7 {
		t.Fatalf("paged %d distinct points, want 7", len(seen))
	}
}

// TestMetricHistoryEmptyRun: a run with no metrics returns an empty page and no
// token — not an error.
func TestMetricHistoryEmptyRun(t *testing.T) {
	s, ctx := newTestStore(t)
	_, run := seedExperimentAndRun(t, ctx, s, "team-a")
	pts, next, err := s.Runs().GetMetricHistory(ctx, domain.MetricHistoryQuery{RunID: run.ID})
	if err != nil {
		t.Fatalf("history on empty run: %v", err)
	}
	if len(pts) != 0 || next != "" {
		t.Fatalf("expected empty result, got %d points token=%q", len(pts), next)
	}
}
