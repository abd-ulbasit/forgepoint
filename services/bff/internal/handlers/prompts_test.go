package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	aiv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/ai/v1"
	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	monitorv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/monitor/v1"
)

// ============================================================================
// PROMPT REGISTRY (L3) + LLM EVALS (L4) — BFF route tests.
//
// These prove the same three guarantees every other handler test asserts:
//   1. the caller's bearer token is FORWARDED as outgoing gRPC metadata,
//   2. the request is parsed into the right proto shape (path/query/body),
//   3. the proto response is relayed as the expected camelCase JSON shape.
// They run the handler behind the REAL RequireAuth middleware (serveThroughAuth)
// so the token lands in context exactly as in production — no special-casing.
// ============================================================================

// ---- ListPrompts ------------------------------------------------------------

// TestListPrompts_ForwardsTokenAndShapesJSON: GET /api/v1/prompts relays the
// prompt list, forwards the token, and passes pagination through.
func TestListPrompts_ForwardsTokenAndShapesJSON(t *testing.T) {
	t.Parallel()

	var sawAuthz string
	var sawPageSize int32
	ai := &mockAIGateway{
		listPromptsFn: func(ctx context.Context, in *aiv1.ListPromptsRequest) (*aiv1.ListPromptsResponse, error) {
			sawAuthz = authzFromCtx(ctx)
			sawPageSize = in.GetPagination().GetPageSize()
			return &aiv1.ListPromptsResponse{
				Prompts: []*aiv1.Prompt{
					{
						Id:        "p-1",
						Name:      "summarize",
						Version:   2,
						Stage:     aiv1.PromptStage_PROMPT_STAGE_PRODUCTION,
						Template:  "Summarize {{text}}",
						Variables: []string{"text"},
						Team:      "platform",
						CreatedAt: timestamppb.Now(),
					},
				},
				Pagination: &commonv1.PaginationResponse{TotalCount: 1},
			}, nil
		},
	}
	h := NewPromptsHandler(ai, testLogger())

	r := requestWithToken(http.MethodGet, "/api/v1/prompts?page_size=25", "prompt-tok", nil)
	w := serveThroughAuth(h.List, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if sawAuthz != "Bearer prompt-tok" {
		t.Errorf("list did not forward token; saw %q", sawAuthz)
	}
	if sawPageSize != 25 {
		t.Errorf("page_size not parsed into pagination; saw %d", sawPageSize)
	}

	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("response not JSON: %v; body=%s", err, w.Body.String())
	}
	if _, ok := got["prompts"]; !ok {
		t.Errorf("expected 'prompts' key, got %v", keys(got))
	}
	// Enum must serialize as its SCREAMING_SNAKE name (protojson UseProtoNames:false
	// keeps the field name camelCase but the enum VALUE stays as its full name).
	if !strings.Contains(w.Body.String(), "PROMPT_STAGE_PRODUCTION") {
		t.Errorf("expected the stage enum name in the body; got %s", w.Body.String())
	}
}

// ---- CreatePrompt -----------------------------------------------------------

// TestCreatePrompt_ParsesBodyAndReturns201: POST /api/v1/prompts parses the
// proto-JSON body, forwards the token, and returns 201 with the created prompt.
func TestCreatePrompt_ParsesBodyAndReturns201(t *testing.T) {
	t.Parallel()

	var sawAuthz, sawName, sawTemplate, sawIdem string
	ai := &mockAIGateway{
		createPromptFn: func(ctx context.Context, in *aiv1.CreatePromptRequest) (*aiv1.CreatePromptResponse, error) {
			sawAuthz = authzFromCtx(ctx)
			sawName = in.GetName()
			sawTemplate = in.GetTemplate()
			sawIdem = in.GetIdempotencyKey()
			return &aiv1.CreatePromptResponse{
				Prompt: &aiv1.Prompt{
					Id:        "p-9",
					Name:      in.GetName(),
					Version:   1,
					Stage:     aiv1.PromptStage_PROMPT_STAGE_DEV,
					Template:  in.GetTemplate(),
					Variables: []string{"name"},
				},
			}, nil
		},
	}
	h := NewPromptsHandler(ai, testLogger())

	body := `{"name":"greeting","template":"Hello {{name}}","description":"a hello","idempotencyKey":"idem-123"}`
	r := requestWithToken(http.MethodPost, "/api/v1/prompts", "create-tok", strings.NewReader(body))
	w := serveThroughAuth(h.Create, r)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body.String())
	}
	if sawAuthz != "Bearer create-tok" {
		t.Errorf("create did not forward token; saw %q", sawAuthz)
	}
	if sawName != "greeting" || sawTemplate != "Hello {{name}}" {
		t.Errorf("body not parsed; name=%q template=%q", sawName, sawTemplate)
	}
	if sawIdem != "idem-123" {
		t.Errorf("idempotencyKey not parsed; saw %q", sawIdem)
	}
	if !strings.Contains(w.Body.String(), "greeting") {
		t.Errorf("expected the created prompt in the body; got %s", w.Body.String())
	}
}

// TestCreatePrompt_BadJSON_400: a malformed body is a cheap 400, no upstream call.
func TestCreatePrompt_BadJSON_400(t *testing.T) {
	t.Parallel()

	ai := &mockAIGateway{
		createPromptFn: func(context.Context, *aiv1.CreatePromptRequest) (*aiv1.CreatePromptResponse, error) {
			t.Fatal("upstream CreatePrompt should NOT be called on bad JSON")
			return nil, nil
		},
	}
	h := NewPromptsHandler(ai, testLogger())

	r := requestWithToken(http.MethodPost, "/api/v1/prompts", "tok", strings.NewReader("{not json"))
	w := serveThroughAuth(h.Create, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
}

// TestCreatePrompt_Conflict_Maps409: a duplicate-name AlreadyExists from the
// gateway maps to HTTP 409 (the standard gRPC->HTTP table).
func TestCreatePrompt_Conflict_Maps409(t *testing.T) {
	t.Parallel()

	ai := &mockAIGateway{
		createPromptFn: func(context.Context, *aiv1.CreatePromptRequest) (*aiv1.CreatePromptResponse, error) {
			return nil, status.Error(codes.AlreadyExists, "prompt exists")
		},
	}
	h := NewPromptsHandler(ai, testLogger())

	r := requestWithToken(http.MethodPost, "/api/v1/prompts", "tok", strings.NewReader(`{"name":"x","template":"y"}`))
	w := serveThroughAuth(h.Create, r)

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", w.Code, w.Body.String())
	}
}

// ---- GetPrompt + RenderPrompt ----------------------------------------------

// TestGetPrompt_ParsesNameAndVersion: the {name} path var and ?version= query
// land in the request; version is pinned only when a positive integer.
func TestGetPrompt_ParsesNameAndVersion(t *testing.T) {
	t.Parallel()

	var sawName string
	var sawVersion int32
	ai := &mockAIGateway{
		getPromptFn: func(_ context.Context, in *aiv1.GetPromptRequest) (*aiv1.GetPromptResponse, error) {
			sawName = in.GetName()
			sawVersion = in.GetVersion()
			return &aiv1.GetPromptResponse{Prompt: &aiv1.Prompt{Name: in.GetName(), Version: in.GetVersion()}}, nil
		},
	}
	h := NewPromptsHandler(ai, testLogger())

	// In production Go 1.22's mux fills PathValue("name") from the route pattern;
	// in a direct handler test we set it explicitly (the same convention the
	// pipelines/executions tests use via r.SetPathValue), then run through the
	// real RequireAuth so the token lands in context.
	r := requestWithToken(http.MethodGet, "/api/v1/prompts/summarize?version=3", "tok", nil)
	r.SetPathValue("name", "summarize")
	w := serveThroughAuth(h.Get, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if sawName != "summarize" {
		t.Errorf("name path var = %q, want summarize", sawName)
	}
	if sawVersion != 3 {
		t.Errorf("version query = %d, want 3", sawVersion)
	}
}

// TestRenderPrompt_BodyVariablesAndPathName: the variables map comes from the
// body, the name from the PATH (overriding any name in the body), and the
// rendered text is relayed back.
func TestRenderPrompt_BodyVariablesAndPathName(t *testing.T) {
	t.Parallel()

	var sawName string
	var sawVars map[string]string
	ai := &mockAIGateway{
		renderPromptFn: func(_ context.Context, in *aiv1.RenderPromptRequest) (*aiv1.RenderPromptResponse, error) {
			sawName = in.GetName()
			sawVars = in.GetVariables()
			return &aiv1.RenderPromptResponse{Rendered: "Hello Ada", Version: 4}, nil
		},
	}
	h := NewPromptsHandler(ai, testLogger())

	// The body carries a stray "name" that MUST be ignored in favor of the path.
	body := `{"name":"ignored","variables":{"name":"Ada"}}`
	r := requestWithToken(http.MethodPost, "/api/v1/prompts/greeting/render", "tok", strings.NewReader(body))
	r.SetPathValue("name", "greeting")
	w := serveThroughAuth(h.Render, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if sawName != "greeting" {
		t.Errorf("render name = %q, want greeting (path must win)", sawName)
	}
	if sawVars["name"] != "Ada" {
		t.Errorf("variables not parsed from body; saw %v", sawVars)
	}
	if !strings.Contains(w.Body.String(), "Hello Ada") {
		t.Errorf("expected the rendered text in the body; got %s", w.Body.String())
	}
}

// ---- Evals (L4) -------------------------------------------------------------

// TestEvals_ForwardsTokenFiltersAndShapesJSON: GET /api/v1/evals forwards the
// token, passes the optional model_name + since filters, and relays the scores.
func TestEvals_ForwardsTokenFiltersAndShapesJSON(t *testing.T) {
	t.Parallel()

	var sawAuthz, sawModel string
	var hadSince bool
	mon := &mockMonitor{
		listEvalsFn: func(ctx context.Context, in *monitorv1.ListEvalScoresRequest) (*monitorv1.ListEvalScoresResponse, error) {
			sawAuthz = authzFromCtx(ctx)
			sawModel = in.GetModelName()
			hadSince = in.GetSince() != nil
			return &monitorv1.ListEvalScoresResponse{
				Scores: []*monitorv1.EvalScore{
					{
						Model:     "smollm2:135m",
						Relevance: 4,
						Coherence: 5,
						Safety:    5,
						Overall:   5,
						Scored:    true,
						RequestId: "req-1",
						CreatedAt: timestamppb.Now(),
					},
				},
				Pagination: &commonv1.PaginationResponse{TotalCount: 1},
			}, nil
		},
	}
	h := NewMonitorsHandler(mon, testLogger())

	r := requestWithToken(http.MethodGet,
		"/api/v1/evals?model_name=smollm2:135m&since=2026-06-01T00:00:00Z", "eval-tok", nil)
	w := serveThroughAuth(h.Evals, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if sawAuthz != "Bearer eval-tok" {
		t.Errorf("evals did not forward token; saw %q", sawAuthz)
	}
	if sawModel != "smollm2:135m" {
		t.Errorf("model_name filter = %q, want smollm2:135m", sawModel)
	}
	if !hadSince {
		t.Error("expected the RFC3339 since filter to be parsed into a Timestamp")
	}

	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("response not JSON: %v; body=%s", err, w.Body.String())
	}
	if _, ok := got["scores"]; !ok {
		t.Errorf("expected 'scores' key, got %v", keys(got))
	}
}

// TestEvals_BadSinceIgnored: a malformed `since` is silently dropped (treated as
// no lower bound) rather than failing the request — a soft filter, like page_size.
func TestEvals_BadSinceIgnored(t *testing.T) {
	t.Parallel()

	var hadSince bool
	mon := &mockMonitor{
		listEvalsFn: func(_ context.Context, in *monitorv1.ListEvalScoresRequest) (*monitorv1.ListEvalScoresResponse, error) {
			hadSince = in.GetSince() != nil
			return &monitorv1.ListEvalScoresResponse{
				Pagination: &commonv1.PaginationResponse{},
			}, nil
		},
	}
	h := NewMonitorsHandler(mon, testLogger())

	r := requestWithToken(http.MethodGet, "/api/v1/evals?since=not-a-time", "tok", nil)
	w := serveThroughAuth(h.Evals, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (bad since must not fail); body=%s", w.Code, w.Body.String())
	}
	if hadSince {
		t.Error("a malformed since should be dropped, not parsed")
	}
}
