package handlers

import (
	"log/slog"
	"net/http"
	"strconv"

	aiv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/ai/v1"
	"github.com/abd-ulbasit/forgepoint/services/bff/internal/httpx"
)

// ============================================================================
// PromptsHandler — the BFF surface for the AI Gateway's PROMPT REGISTRY (L3).
// ============================================================================
//
// Four routes, the same logic-free shape every other resource handler uses
// (parse -> forward the caller's token -> relay proto<->JSON):
//
//	GET  /api/v1/prompts                 -> ListPrompts  (paginated, newest first)
//	POST /api/v1/prompts                 -> CreatePrompt (body = name/template/desc)
//	GET  /api/v1/prompts/{name}          -> GetPrompt    (optional ?version=N)
//	POST /api/v1/prompts/{name}/render   -> RenderPrompt (body = {variables: {...}})
//
// WHY A SEPARATE HANDLER from AIHandler (chat): the two share an upstream stub
// (the AI gateway) but nothing else — different request shapes, different routes,
// and the prompt routes are all PLAIN UNARY (no SSE). Folding them into AIHandler
// would bloat that type's dependency surface with four RPCs the chat playground
// never calls. A dedicated PromptsHandler keeps each handler's port honest (the
// AIGatewayClient interface lists everything; this handler just uses the prompt
// slice of it) and keeps the chat-streaming machinery isolated from the simple
// CRUD here.
//
// PROMPT REGISTRY, THE PATTERN: prompts are VERSIONED, immutable templates —
// CreatePrompt with an existing name mints a NEW version rather than mutating in
// place (think MLflow's model registry, but for prompts). The registry is the
// "source of truth" the gateway renders from at completion time; this BFF surface
// is the read/write console for it. The render endpoint expands {{variables}}
// SERVER-SIDE against the supplied map so the preview matches the real engine.
type PromptsHandler struct {
	ai     AIGatewayClient
	logger *slog.Logger
}

// NewPromptsHandler wires the (segmented) AI gateway client. It is given the same
// AIGatewayClient interface the chat handler gets — Go interfaces are structural,
// so the concrete stub satisfies both, and this handler simply uses the prompt
// methods.
func NewPromptsHandler(ai AIGatewayClient, logger *slog.Logger) *PromptsHandler {
	return &PromptsHandler{ai: ai, logger: logger}
}

// List handles GET /api/v1/prompts — paginated prompt list. Pagination is the
// platform's standard cursor scheme (httpx.Pagination reads page_size/page_token).
func (h *PromptsHandler) List(w http.ResponseWriter, r *http.Request) {
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.ai.ListPrompts(ctx, &aiv1.ListPromptsRequest{
		Pagination: httpx.Pagination(r),
	})
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	httpx.WriteProto(w, http.StatusOK, resp)
}

// Create handles POST /api/v1/prompts. The body is the CreatePromptRequest in
// proto-JSON shape ({name, template, description, idempotencyKey}). The BFF does
// NO validation — the gateway owns template syntax + variable-extraction rules;
// a bad template surfaces as a downstream InvalidArgument -> 400 with the
// service's safe-to-show message. We return 201 Created on success because a new
// prompt (or version) is a created resource.
//
// IDEMPOTENCY: like RegisterModel, a client-supplied idempotency_key makes a
// retried submit safe (the gateway dedups on it). The SPA generates a UUID per
// submit, so a double-click or a network retry can't mint two versions.
func (h *PromptsHandler) Create(w http.ResponseWriter, r *http.Request) {
	req := &aiv1.CreatePromptRequest{}
	if err := unmarshalProto(r, req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid prompt JSON")
		return
	}
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.ai.CreatePrompt(ctx, req)
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	httpx.WriteProto(w, http.StatusCreated, resp)
}

// Get handles GET /api/v1/prompts/{name} with an OPTIONAL ?version=N query param.
// Omitting version asks the gateway for the latest version of that name; passing
// a positive integer pins a specific one. A non-numeric or non-positive version
// is treated as "unset" (latest) rather than a 400 — it's a soft constraint, and
// the gateway already defaults to latest when version==0.
func (h *PromptsHandler) Get(w http.ResponseWriter, r *http.Request) {
	req := &aiv1.GetPromptRequest{Name: r.PathValue("name")}
	if raw := r.URL.Query().Get("version"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			req.Version = int32(n)
		}
	}
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.ai.GetPrompt(ctx, req)
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	httpx.WriteProto(w, http.StatusOK, resp)
}

// Render handles POST /api/v1/prompts/{name}/render. The body is a RenderPrompt
// request in proto-JSON shape — `{"variables": {"k":"v", ...}}` (and optionally
// `"version": N`). The NAME comes from the PATH, so we set it AFTER unmarshalling
// the body to guarantee the path wins over any stray `name` in the body (the URL
// is the canonical identifier — letting the body override it would be a confusing
// mismatch). Rendering happens upstream against the gateway's templating engine;
// the BFF relays the rendered string as data, rendered by the SPA as plain TEXT.
func (h *PromptsHandler) Render(w http.ResponseWriter, r *http.Request) {
	req := &aiv1.RenderPromptRequest{}
	// An empty body is allowed (a template with no variables renders verbatim).
	if r.ContentLength != 0 {
		if err := unmarshalProto(r, req); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, "invalid render request JSON")
			return
		}
	}
	// Path is canonical — overwrite any name the body may have carried.
	req.Name = r.PathValue("name")

	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.ai.RenderPrompt(ctx, req)
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	httpx.WriteProto(w, http.StatusOK, resp)
}
