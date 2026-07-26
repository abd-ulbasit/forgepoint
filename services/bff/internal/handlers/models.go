package handlers

import (
	"encoding/json"
	"log/slog"
	"net/http"

	registryv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/registry/v1"
	"github.com/abd-ulbasit/forgepoint/services/bff/internal/httpx"
)

// ModelsHandler maps the /api/v1/models/* HTTP surface onto the registry gRPC
// service. Every method forwards the caller's token via httpx.ContextWithToken.
type ModelsHandler struct {
	registry RegistryClient
	logger   *slog.Logger
}

func NewModelsHandler(registry RegistryClient, logger *slog.Logger) *ModelsHandler {
	return &ModelsHandler{registry: registry, logger: logger}
}

// List handles GET /api/v1/models. Pagination + optional filters from query.
func (h *ModelsHandler) List(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	// ContextWithToken copies the bearer token into outgoing gRPC metadata so the
	// registry authenticates THIS user. This single line is the token-forwarding
	// contract, repeated on every downstream call.
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.registry.ListModels(ctx, &registryv1.ListModelsRequest{
		TaskTypeFilter:  q.Get("task_type"),
		FrameworkFilter: q.Get("framework"),
		IncludeArchived: q.Get("include_archived") == "true",
		Pagination:      httpx.Pagination(r),
	})
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	// protojson serializes the whole response (models[] + pagination cursor) with
	// correct enum-name / timestamp handling.
	httpx.WriteProto(w, http.StatusOK, resp)
}

// Get handles GET /api/v1/models/{id}. The registry's GetModel accepts id OR
// name; the path var is the id.
func (h *ModelsHandler) Get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.registry.GetModel(ctx, &registryv1.GetModelRequest{Id: id})
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	httpx.WriteProto(w, http.StatusOK, resp)
}

// registerModelRequest is the SPA-facing JSON body for model registration.
type registerModelRequest struct {
	Name           string            `json:"name"`
	Description    string            `json:"description"`
	Framework      string            `json:"framework"`
	TaskType       string            `json:"taskType"`
	Tags           map[string]string `json:"tags"`
	IdempotencyKey string            `json:"idempotencyKey"`
}

// Register handles POST /api/v1/models. The BFF does NO validation beyond
// "is this JSON" — every business rule (name uniqueness, allowed frameworks)
// lives in the registry service (ADR guardrail). A bad value comes back as
// InvalidArgument/AlreadyExists -> 400/409.
func (h *ModelsHandler) Register(w http.ResponseWriter, r *http.Request) {
	var body registerModelRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.registry.RegisterModel(ctx, &registryv1.RegisterModelRequest{
		Name:           body.Name,
		Description:    body.Description,
		Framework:      body.Framework,
		TaskType:       body.TaskType,
		Tags:           body.Tags,
		IdempotencyKey: body.IdempotencyKey,
	})
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	// 201 Created — a new resource was registered.
	httpx.WriteProto(w, http.StatusCreated, resp)
}

// Versions handles GET /api/v1/models/{id}/versions.
func (h *ModelsHandler) Versions(w http.ResponseWriter, r *http.Request) {
	modelID := r.PathValue("id")
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.registry.ListVersions(ctx, &registryv1.ListVersionsRequest{
		ModelId:    modelID,
		Pagination: httpx.Pagination(r),
	})
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	httpx.WriteProto(w, http.StatusOK, resp)
}
