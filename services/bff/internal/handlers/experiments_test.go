package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	experimentv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/experiment/v1"
)

// TestListRuns_ForwardsTokenAndShapesJSON verifies a second resource handler
// (experiments) also forwards the token and returns the expected JSON shape —
// proving the forwarding pattern is uniform, not special-cased to models.
func TestListRuns_ForwardsTokenAndShapesJSON(t *testing.T) {
	t.Parallel()

	var sawAuthz string
	exp := &mockExperiment{
		listRunsFn: func(ctx context.Context, _ *experimentv1.ListRunsRequest) (*experimentv1.ListRunsResponse, error) {
			sawAuthz = authzFromCtx(ctx)
			return &experimentv1.ListRunsResponse{
				Runs:       []*experimentv1.Run{{Id: "r-1"}},
				Pagination: &commonv1.PaginationResponse{TotalCount: 1},
			}, nil
		},
	}
	h := NewExperimentsHandler(exp, testLogger())
	r := requestWithToken(http.MethodGet, "/api/v1/runs?experiment_id=exp-7", "run-token", nil)
	w := serveThroughAuth(h.ListRuns, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if sawAuthz != "Bearer run-token" {
		t.Errorf("forwarded authz = %q, want %q", sawAuthz, "Bearer run-token")
	}
	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("response not JSON: %v", err)
	}
	if _, ok := got["runs"]; !ok {
		t.Errorf("expected 'runs' key, got %v", keys(got))
	}
}
