// rpcs.go — the implemented AI Gateway RPCs: ChatCompletion (server-streaming),
// ListProviders, GetUsage.
package handler

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	aiv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/ai/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
)

// ============================================================================
// ChatCompletion — SERVER-STREAMING
// ============================================================================
//
// THE STREAM-AUTH MECHANIC: a server-streaming RPC's handler
// gets a grpc.ServerStreamingServer whose Context() is the per-RPC context the auth
// STREAM interceptor populated with claims. So grpcutil.ClaimsFromContext on
// stream.Context() yields the verified principal — exactly as a unary handler reads
// ctx. The interceptor wraps the ServerStream so its Context() carries the claims;
// without that wrapper, stream RPCs couldn't see the auth context. We read the TEAM
// from there, never from the request body.
//
// THE STREAMING FLOW:
//  1. Read claims → team (Unauthenticated if absent; the interceptor already
//     rejected, this is defense-in-depth).
//  2. Map the proto request → domain.ChatRequest, mint a server-side request_id.
//  3. Call svc.ChatCompletion with a SINK that maps each domain.Delta → a proto
//     frame and stream.Send()s it. The domain drives failover/budget/cost; the sink
//     only translates+sends.
//  4. On the terminal Completion, send the FINAL frame (done + finish_reason +
//     usage + served_by + request_id).
//
// ERROR HANDLING: a PRE-STREAM domain error (invalid input, budget exceeded, no
// provider) maps to a gRPC status the RPC returns (no frames sent). A FAILOVER-
// EXHAUSTED error (ErrAllProvidersFailed) is delivered to the client AS a terminal
// error FRAME by the domain (the sink already streamed nothing useful), so the RPC
// returns nil — the client reads finish_reason=ERROR, not a transport error. This
// matches the proto's design: the terminal frame carries the finish reason.
func (h *AIGatewayHandler) ChatCompletion(req *aiv1.ChatCompletionRequest, stream aiv1.AIGatewayService_ChatCompletionServer) error {
	if h.svc == nil {
		return status.Error(codes.Internal, "ai gateway service not wired")
	}
	ctx := stream.Context()

	// 1. TEAM from verified claims (never the request body).
	team, err := teamFromContext(ctx)
	if err != nil {
		return err
	}

	// 2. proto → domain + server-minted request_id (correlation/audit handle).
	requestID := uuid.NewString()
	domReq := chatRequestToDomain(req, requestID)

	// 3. The sink: translate each domain delta → a proto frame and send it.
	sink := func(d domain.Delta) error {
		return stream.Send(&aiv1.ChatCompletionResponse{
			Delta:     d.Text,
			Done:      false,
			RequestId: requestID,
		})
	}

	// Drive the domain. It pushes deltas via sink and returns the terminal summary.
	completion, svcErr := h.svc.ChatCompletion(ctx, team, domReq, sink)
	if svcErr != nil {
		// PRE-STREAM errors map to a status the RPC returns (the domain sent no useful
		// frames). FAILOVER-EXHAUSTED is special: the domain already sent a terminal
		// error frame, so we surface a clean terminal frame and return nil.
		return h.handleChatError(stream, svcErr, completion, requestID)
	}

	// 4. Terminal frame: done + finish_reason + usage + served_by.
	return stream.Send(terminalFrame(completion, requestID))
}

// handleChatError maps a domain error from ChatCompletion to the right wire outcome.
func (h *AIGatewayHandler) handleChatError(stream aiv1.AIGatewayService_ChatCompletionServer, err error, completion domain.Completion, requestID string) error {
	switch {
	case isErr(err, domain.ErrInvalidInput):
		return status.Error(codes.InvalidArgument, "request has no messages")
	case isErr(err, domain.ErrModelNotAllowed):
		// RUNTIME GOVERNANCE DENY: the requested model/provider is not on the allow-list.
		// PermissionDenied (not InvalidArgument) is the honest code — the request is
		// well-formed; POLICY forbids it. The audit interceptor records this code as a
		// DENY, so the attempt to call a disallowed/shadow model is in the audit trail.
		// The message names neither the model nor any payload (no PII / no config leak).
		return status.Error(codes.PermissionDenied, "requested model or provider is not permitted")
	case isErr(err, domain.ErrBudgetExceeded):
		return status.Error(codes.ResourceExhausted, "team token budget exceeded")
	case isErr(err, domain.ErrNoProvider):
		return status.Error(codes.FailedPrecondition, "no eligible provider for the request")
	case isErr(err, domain.ErrAllProvidersFailed):
		// The domain already streamed a terminal error frame to the client. We send a
		// final, well-formed terminal frame (finish_reason=ERROR) and return nil so the
		// client reads the failure in-band, exactly as the proto contract specifies.
		return stream.Send(terminalFrame(domain.Completion{
			ServedBy:     completion.ServedBy,
			FinishReason: domain.FinishReasonError,
		}, requestID))
	default:
		// errSinkFailed (client gone) or an unexpected error: nothing more to send; the
		// transport already failed. Return a sanitized Internal (we never echo raw text).
		return status.Error(codes.Internal, "completion stream aborted")
	}
}

// ============================================================================
// ListProviders — report configured backends + live circuit state.
// ============================================================================
func (h *AIGatewayHandler) ListProviders(_ context.Context, _ *aiv1.ListProvidersRequest) (*aiv1.ListProvidersResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Internal, "ai gateway service not wired")
	}
	snaps := h.svc.Providers()
	out := make([]*aiv1.Provider, 0, len(snaps))
	for _, s := range snaps {
		out = append(out, &aiv1.Provider{
			Kind:         providerKindToProto(s.Kind),
			Name:         s.Name,
			Enabled:      s.Enabled,
			CircuitState: s.CircuitState.String(),
			Models:       s.Models,
		})
	}
	return &aiv1.ListProvidersResponse{Providers: out}, nil
}

// ============================================================================
// GetUsage — the caller team's token/cost usage + remaining budget.
// ============================================================================
//
// The team comes from claims (a caller can only see ITS OWN usage — no team field in
// the request to spoof another team's numbers). An admin cross-team view is L3 work.
func (h *AIGatewayHandler) GetUsage(ctx context.Context, _ *aiv1.GetUsageRequest) (*aiv1.GetUsageResponse, error) {
	if h.svc == nil {
		return nil, status.Error(codes.Internal, "ai gateway service not wired")
	}
	team, err := teamFromContext(ctx)
	if err != nil {
		return nil, err
	}
	summary, err := h.svc.Usage(ctx, team)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to read team usage")
	}
	// Populate the FULL breakdown — prompt + completion + total — not just the total.
	// This is the GetUsage breakdown fix: the prompt/completion split comes from the
	// domain's UsageStore accumulator (via UsageSummary), so the response no longer
	// reports prompt=0/completion=0 the way it did when only the budget total existed.
	return &aiv1.GetUsageResponse{
		Team: team,
		Total: &aiv1.TokenUsage{
			PromptTokens:     int32(summary.PromptTokens),     //nolint:gosec // bounded token count
			CompletionTokens: int32(summary.CompletionTokens), //nolint:gosec // bounded token count
			TotalTokens:      int32(summary.TotalTokens),      //nolint:gosec // bounded token count
		},
		BudgetTokens:    summary.BudgetTokens,
		RemainingTokens: summary.RemainingTokens,
	}, nil
}

// teamFromContext extracts the verified team from the auth claims. Missing claims or
// an empty team → Unauthenticated (defense-in-depth: the interceptor already rejects
// unauthenticated RPCs, but we refuse to bill/serve an anonymous or team-less
// principal rather than charge a zero-value team).
func teamFromContext(ctx context.Context) (string, error) {
	claims, ok := grpcutil.ClaimsFromContext(ctx)
	if !ok || claims == nil || claims.Team == "" {
		return "", status.Error(codes.Unauthenticated, "missing team in authentication claims")
	}
	return claims.Team, nil
}
