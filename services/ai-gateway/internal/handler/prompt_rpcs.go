// prompt_rpcs.go — the PROMPT REGISTRY RPCs (M7/L3): CreatePrompt, GetPrompt,
// ListPrompts, RenderPrompt. These REPLACE the embedded Unimplemented base for the
// four prompt methods when the prompt registry is wired (h.prompts != nil).
//
// ============================================================================
// THE HANDLER'S THREE JOBS, applied to the prompt registry
// ============================================================================
//
//  1. PROTO → DOMAIN: pull the CLIENT-owned fields (name/template/variables/...) from
//     the request, and derive the TEAM from the VERIFIED auth claims (teamFromContext
//     in rpcs.go) — NEVER from a request field. The proto requests have NO team field,
//     so a client structurally CANNOT pass a team; the team comes only from the JWT.
//     This is the tenancy boundary: every prompt write/read is scoped to the caller's
//     own team, so Team A can never create/get/list/render Team B's prompts.
//
//  2. CALL the prompt service (the handler holds the prompt.PromptService INTERFACE;
//     it has no idea the impl is Postgres-backed — tests inject a fake).
//
//  3. DOMAIN → PROTO + ERROR MAPPING: convert the result to a proto Response and map
//     the domain's business sentinels to gRPC codes (promptToStatus in mapping.go):
//     ErrValidation    → InvalidArgument
//     ErrNotFound      → NotFound       (team-scoped: another team's prompt is "not found")
//     ErrAlreadyExists → AlreadyExists
//     never leaking internal error text to the client.
//
// THE NIL GUARD: if h.prompts is nil (no database configured), we return
// codes.Unimplemented — the SAME contract the embedded base gives. The DB self-gating
// in main.go decides whether prompts is wired; the handler just honors it. We do NOT
// nil-deref: once we OVERRIDE these methods, the embedded base no longer shields them,
// so each method explicitly guards nil.
package handler

import (
	"context"

	aiv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/ai/v1"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/prompt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errPromptsNotWired is the FIXED message returned when a prompt RPC runs without a
// configured database. codes.Unimplemented (not Internal) is the honest signal: "this
// server is not serving the prompt registry" — identical to the embedded base.
const errPromptsNotWired = "prompt registry is not enabled (no database configured)"

// CreatePrompt cuts a new version of a named prompt for the caller's team. The
// template's {{variables}} are parsed server-side; the version is assigned server-
// side (1 for a new name, N+1 otherwise); the stage defaults to DEV. Idempotent on
// idempotency_key.
func (h *AIGatewayHandler) CreatePrompt(ctx context.Context, req *aiv1.CreatePromptRequest) (*aiv1.CreatePromptResponse, error) {
	if h.prompts == nil {
		return nil, status.Error(codes.Unimplemented, errPromptsNotWired)
	}
	team, err := teamFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// Cheap transport-level guard for the most common client mistake; the domain
	// re-validates authoritatively.
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if req.GetTemplate() == "" {
		return nil, status.Error(codes.InvalidArgument, "template is required")
	}

	created, err := h.prompts.CreatePrompt(ctx, team, prompt.CreatePromptInput{
		Name:           req.GetName(),
		Template:       req.GetTemplate(),
		Description:    req.GetDescription(),
		IdempotencyKey: req.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, promptToStatus(ctx, "CreatePrompt", err)
	}
	return &aiv1.CreatePromptResponse{Prompt: promptToProto(created)}, nil
}

// GetPrompt resolves a prompt by (team, name) + optional version. version==0 resolves
// to the latest PRODUCTION version, falling back to the latest version overall (see
// the service's documented rule). Team-scoped: another team's prompt is NotFound.
func (h *AIGatewayHandler) GetPrompt(ctx context.Context, req *aiv1.GetPromptRequest) (*aiv1.GetPromptResponse, error) {
	if h.prompts == nil {
		return nil, status.Error(codes.Unimplemented, errPromptsNotWired)
	}
	team, err := teamFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	p, err := h.prompts.GetPrompt(ctx, team, req.GetName(), int(req.GetVersion()))
	if err != nil {
		return nil, promptToStatus(ctx, "GetPrompt", err)
	}
	return &aiv1.GetPromptResponse{Prompt: promptToProto(p)}, nil
}

// ListPrompts returns a team-scoped, newest-first, keyset-paginated page of the
// caller team's prompts. The team comes from claims, so the list can only ever show
// the caller's own prompts — there is no team field to widen the query to another's.
func (h *AIGatewayHandler) ListPrompts(ctx context.Context, req *aiv1.ListPromptsRequest) (*aiv1.ListPromptsResponse, error) {
	if h.prompts == nil {
		return nil, status.Error(codes.Unimplemented, errPromptsNotWired)
	}
	team, err := teamFromContext(ctx)
	if err != nil {
		return nil, err
	}
	size, err := promptPageSizeFromProto(req.GetPagination())
	if err != nil {
		return nil, err
	}

	page, err := h.prompts.ListPrompts(ctx, team, prompt.ListPromptsInput{
		PageSize:  size,
		PageToken: req.GetPagination().GetPageToken(),
	})
	if err != nil {
		return nil, promptToStatus(ctx, "ListPrompts", err)
	}

	prompts := make([]*aiv1.Prompt, 0, len(page.Items))
	for _, p := range page.Items {
		prompts = append(prompts, promptToProto(p))
	}
	return &aiv1.ListPromptsResponse{
		Prompts:    prompts,
		Pagination: promptPaginationResponse(page.NextToken),
	}, nil
}

// RenderPrompt loads the prompt (team-scoped, same version rule as GetPrompt) and
// substitutes the supplied variables into the template under the STRICT missing-var
// policy (a declared variable with no value → InvalidArgument). Returns the rendered
// text + the version that was actually rendered.
func (h *AIGatewayHandler) RenderPrompt(ctx context.Context, req *aiv1.RenderPromptRequest) (*aiv1.RenderPromptResponse, error) {
	if h.prompts == nil {
		return nil, status.Error(codes.Unimplemented, errPromptsNotWired)
	}
	team, err := teamFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	result, err := h.prompts.RenderPrompt(ctx, team, prompt.RenderPromptInput{
		Name:      req.GetName(),
		Version:   int(req.GetVersion()),
		Variables: req.GetVariables(),
	})
	if err != nil {
		return nil, promptToStatus(ctx, "RenderPrompt", err)
	}
	return &aiv1.RenderPromptResponse{
		Rendered: result.Rendered,
		//nolint:gosec // version is a small bounded counter, safe int→int32
		Version: int32(result.Version),
	}, nil
}
