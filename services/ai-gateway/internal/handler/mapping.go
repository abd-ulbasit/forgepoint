// mapping.go — the proto↔domain anti-corruption mapping for the AI Gateway handler.
//
// This is the ONLY place aiv1 (generated proto) enum/message values meet the domain
// vocabulary. Centralizing the (total, compile-time-visible) mappings here means a
// proto regeneration ripples into one file, not across the RPC logic, and the
// switches guard any unmapped value with a safe default.
package handler

import (
	"context"
	"errors"
	"log/slog"

	aiv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/ai/v1"
	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/prompt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// chatRequestToDomain maps the proto request to the domain form, stamping the
// server-minted request_id. Note what it does NOT read: there is no team field on
// the proto request (the team is claims-only), so this mapping structurally cannot
// pull a spoofed team from the body.
func chatRequestToDomain(req *aiv1.ChatCompletionRequest, requestID string) domain.ChatRequest {
	msgs := make([]domain.Message, 0, len(req.GetMessages()))
	for _, m := range req.GetMessages() {
		msgs = append(msgs, domain.Message{
			Role:    chatRoleToDomain(m.GetRole()),
			Content: m.GetContent(),
		})
	}
	return domain.ChatRequest{
		Model:       req.GetModel(),
		Messages:    msgs,
		Temperature: req.GetTemperature(),
		MaxTokens:   req.GetMaxTokens(),
		Provider:    providerKindToDomain(req.GetProvider()),
		RequestID:   requestID,
	}
}

// terminalFrame builds the final streamed frame from the domain Completion: done +
// finish_reason + usage + served_by + request_id. usage is nil-safe (a failover-
// exhausted completion has zero usage, which renders as a present-but-empty usage —
// callers read finish_reason=ERROR as the signal).
func terminalFrame(c domain.Completion, requestID string) *aiv1.ChatCompletionResponse {
	return &aiv1.ChatCompletionResponse{
		Done:         true,
		FinishReason: finishReasonToProto(c.FinishReason),
		Usage: &aiv1.TokenUsage{
			PromptTokens:     c.Usage.PromptTokens,
			CompletionTokens: c.Usage.CompletionTokens,
			TotalTokens:      c.Usage.TotalTokens,
			CostMicroUsd:     c.Usage.CostMicroUSD,
		},
		ServedBy:  providerKindToProto(c.ServedBy),
		CacheHit:  c.CacheHit,
		RequestId: requestID,
	}
}

// chatRoleToDomain maps proto ChatRole → domain Role.
func chatRoleToDomain(r aiv1.ChatRole) domain.Role {
	switch r {
	case aiv1.ChatRole_CHAT_ROLE_SYSTEM:
		return domain.RoleSystem
	case aiv1.ChatRole_CHAT_ROLE_USER:
		return domain.RoleUser
	case aiv1.ChatRole_CHAT_ROLE_ASSISTANT:
		return domain.RoleAssistant
	default:
		return domain.RoleUnspecified
	}
}

// providerKindToDomain maps proto ProviderKind → domain ProviderKind.
func providerKindToDomain(k aiv1.ProviderKind) domain.ProviderKind {
	switch k {
	case aiv1.ProviderKind_PROVIDER_KIND_OLLAMA:
		return domain.ProviderKindOllama
	case aiv1.ProviderKind_PROVIDER_KIND_STUB:
		return domain.ProviderKindStub
	case aiv1.ProviderKind_PROVIDER_KIND_OPENAI:
		return domain.ProviderKindOpenAI
	case aiv1.ProviderKind_PROVIDER_KIND_ANTHROPIC:
		return domain.ProviderKindAnthropic
	default:
		return domain.ProviderKindUnspecified
	}
}

// providerKindToProto maps domain ProviderKind → proto ProviderKind.
func providerKindToProto(k domain.ProviderKind) aiv1.ProviderKind {
	switch k {
	case domain.ProviderKindOllama:
		return aiv1.ProviderKind_PROVIDER_KIND_OLLAMA
	case domain.ProviderKindStub:
		return aiv1.ProviderKind_PROVIDER_KIND_STUB
	case domain.ProviderKindOpenAI:
		return aiv1.ProviderKind_PROVIDER_KIND_OPENAI
	case domain.ProviderKindAnthropic:
		return aiv1.ProviderKind_PROVIDER_KIND_ANTHROPIC
	default:
		return aiv1.ProviderKind_PROVIDER_KIND_UNSPECIFIED
	}
}

// finishReasonToProto maps domain FinishReason → proto FinishReason.
func finishReasonToProto(r domain.FinishReason) aiv1.FinishReason {
	switch r {
	case domain.FinishReasonStop:
		return aiv1.FinishReason_FINISH_REASON_STOP
	case domain.FinishReasonLength:
		return aiv1.FinishReason_FINISH_REASON_LENGTH
	case domain.FinishReasonContentFilter:
		return aiv1.FinishReason_FINISH_REASON_CONTENT_FILTER
	case domain.FinishReasonError:
		return aiv1.FinishReason_FINISH_REASON_ERROR
	default:
		return aiv1.FinishReason_FINISH_REASON_UNSPECIFIED
	}
}

// isErr is a thin errors.Is wrapper kept here so the RPC file reads cleanly.
func isErr(err, target error) bool { return errors.Is(err, target) }

// ============================================================================
// PROMPT REGISTRY MAPPING (L3) — proto↔domain for the four prompt RPCs
// ============================================================================

// promptToProto maps a domain prompt.Prompt to its wire form. The stage enum is
// mapped EXPLICITLY (a switch, not an int cast) so the wire enum can evolve
// independently of the domain enum. CreatedAt → a protobuf Timestamp only when set
// (a zero time → nil, clean wire). Variables are copied as-is (a nil slice marshals
// to an empty repeated field, the intended "no declared variables" shape).
func promptToProto(p prompt.Prompt) *aiv1.Prompt {
	pp := &aiv1.Prompt{
		Id:   p.ID,
		Name: p.Name,
		//nolint:gosec // version is a small bounded counter, safe int→int32
		Version:     int32(p.Version),
		Stage:       promptStageToProto(p.Stage),
		Template:    p.Template,
		Variables:   p.Variables,
		Description: p.Description,
		Team:        p.Team,
	}
	if !p.CreatedAt.IsZero() {
		pp.CreatedAt = timestamppb.New(p.CreatedAt)
	}
	return pp
}

// promptStageToProto maps a domain prompt.Stage → the wire PromptStage. The numeric
// values coincide, but the explicit switch keeps the two enums decoupled (a reorder
// of either side fails to compile a NEW case rather than silently mis-mapping).
func promptStageToProto(s prompt.Stage) aiv1.PromptStage {
	switch s {
	case prompt.StageDev:
		return aiv1.PromptStage_PROMPT_STAGE_DEV
	case prompt.StageProduction:
		return aiv1.PromptStage_PROMPT_STAGE_PRODUCTION
	case prompt.StageArchived:
		return aiv1.PromptStage_PROMPT_STAGE_ARCHIVED
	default:
		return aiv1.PromptStage_PROMPT_STAGE_UNSPECIFIED
	}
}

// promptToStatus converts a prompt-registry domain error into a gRPC status with a
// SANITIZED, client-safe message — the single translation point handler→wire for the
// prompt RPCs. For mapped sentinels it returns the matching code; for an unmapped
// error it LOGS the real error (operator visibility) and returns a fixed "internal
// error" so nothing internal reaches the wire. op is the RPC name, used only in the
// server-side log line.
//
//	ErrValidation    → InvalidArgument   (malformed input / missing render variable;
//	                                      the wrapped message describes the client's
//	                                      own bad input, so it is safe to surface)
//	ErrNotFound      → NotFound          (absent OR another team's prompt — the domain
//	                                      does not distinguish, so a cross-tenant read
//	                                      is indistinguishable from "doesn't exist")
//	ErrAlreadyExists → AlreadyExists     (version-assignment conflict not absorbed by
//	                                      the retry loop)
//	(anything else)  → Internal          (SANITIZED; the real error is logged)
func promptToStatus(ctx context.Context, op string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, prompt.ErrValidation):
		// Client-fault: the wrapped message names the bad field / missing variable.
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, prompt.ErrNotFound):
		return status.Error(codes.NotFound, "prompt not found")
	case errors.Is(err, prompt.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, "prompt version already exists")
	default:
		slog.ErrorContext(ctx, "ai-gateway prompt handler: unmapped internal error",
			slog.String("rpc", op), slog.Any("error", err))
		return status.Error(codes.Internal, "internal error")
	}
}

// promptPageSizeFromProto reads + clamps the requested page size, mirroring the
// Registry's handler. 0/unset → 0 (the service applies DefaultPageSize); an over-cap
// or negative value is REJECTED as InvalidArgument rather than silently clamped, so
// the client's pagination math can't be quietly wrong.
func promptPageSizeFromProto(p *commonv1.PaginationRequest) (int, error) {
	size := int(p.GetPageSize())
	if size < 0 {
		return 0, status.Error(codes.InvalidArgument, "page_size must not be negative")
	}
	if size > prompt.MaxPageSize {
		return 0, status.Errorf(codes.InvalidArgument, "page_size must not exceed %d", prompt.MaxPageSize)
	}
	return size, nil
}

// promptPaginationResponse builds the wire PaginationResponse from the next-page
// cursor. TotalCount is left 0 (the proto's "total unknown"): no prompt-registry
// consumer needs an exact total yet, and computing one would be a second full COUNT
// query per list. The cursor alone drives "is there a next page?".
func promptPaginationResponse(nextToken string) *commonv1.PaginationResponse {
	return &commonv1.PaginationResponse{NextPageToken: nextToken}
}
