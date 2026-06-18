// mapping.go — the proto↔domain anti-corruption mapping for the AI Gateway handler.
//
// This is the ONLY place aiv1 (generated proto) enum/message values meet the domain
// vocabulary. Centralizing the (total, compile-time-visible) mappings here means a
// proto regeneration ripples into one file, not across the RPC logic, and the
// switches guard any unmapped value with a safe default.
package handler

import (
	"errors"

	aiv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/ai/v1"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
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
