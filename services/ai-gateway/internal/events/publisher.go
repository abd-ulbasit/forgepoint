// publisher.go — the events-layer adapter that implements domain.EventPublisher.
//
// It maps the domain payloads (CompletionServed, WarmSignal) to the wire form and
// hands them to natsutil.Publisher, which wraps each in the standard EventEnvelope
// (id, type, source, timestamp, correlation_id, data), sets the dedup Msg-Id,
// injects the W3C trace context, and publishes to the canonical subject.
//
// ============================================================================
// PAYLOAD ENCODING — why a PLAIN STRUCT here (and it's still protojson-symmetric)
// ============================================================================
//
// There is no events.v1 PROTO message for the AI completion/warm events in the
// generated tree (the events proto predates M7), and the HARD RULE forbids
// regenerating protos. natsutil.Publisher handles exactly this case: a payload that
// is NOT a proto.Message is marshalled with encoding/json (the documented path for
// the audit Record and pre-marshalled json.RawMessage producers). So these events
// travel as JSON structs inside the SAME EventEnvelope every other event uses —
// consumers decode the envelope identically and read .data as the JSON struct. When
// the events proto gains AI messages, this adapter swaps the struct for the proto
// (and natsutil routes it to protojson) with no change to the domain or the wiring.
//
// The struct field tags use lowerCamelCase to match the proto3-JSON convention the
// rest of the platform's events use, so an L2 consumer (or a future proto message)
// sees the same field names.
//
// BEST-EFFORT, OFF THE RESPONSE PATH: a publish error is RETURNED (not swallowed)
// so the use-case can log-and-continue; it never fails a completion the client
// already received.
package events

import (
	"context"
	"fmt"

	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
)

// natsPublisher is the minimal slice of *natsutil.Publisher this adapter needs.
// Depending on the interface (not the concrete type) lets unit tests inject a fake
// that records subject+payload without a real broker, while main.go passes a real
// *natsutil.Publisher built over JetStream.
type natsPublisher interface {
	Publish(ctx context.Context, subject string, payload any) error
}

// Publisher satisfies domain.EventPublisher.
type Publisher struct {
	pub natsPublisher
}

// NewPublisher builds the adapter over a natsutil.Publisher (constructed with
// source = Source so EventEnvelope.source is stamped "ai-gateway").
func NewPublisher(pub natsPublisher) *Publisher {
	return &Publisher{pub: pub}
}

// Compile-time proof we satisfy the port.
var _ domain.EventPublisher = (*Publisher)(nil)

// completionServedPayload is the wire form of the cost/audit event. Field tags are
// lowerCamelCase (proto3-JSON convention) so this is forward-compatible with a
// future events.v1.AiCompletionServed proto message. NO message content travels
// (PII discipline): only IDs, counts, cost, and timing.
type completionServedPayload struct {
	RequestID    string `json:"requestId"`
	Team         string `json:"team"`
	Model        string `json:"model"`
	Provider     string `json:"provider"`
	PromptTokens int32  `json:"promptTokens"`
	OutputTokens int32  `json:"outputTokens"`
	TotalTokens  int32  `json:"totalTokens"`
	CostMicroUSD int64  `json:"costMicroUsd"`
	CacheHit     bool   `json:"cacheHit"`
	LatencyMs    int64  `json:"latencyMs"`
}

// warmSignalPayload is the minimal wire form of the warm request. The KEDA scaler
// only counts pending messages, so this payload exists mostly for observability /
// audit of WHO triggered a scale-up.
type warmSignalPayload struct {
	RequestID string `json:"requestId"`
	Team      string `json:"team"`
	Model     string `json:"model"`
	Provider  string `json:"provider"`
}

// PublishCompletionServed maps the domain payload and publishes to fp.ai.completion.served.
func (p *Publisher) PublishCompletionServed(ctx context.Context, ev domain.CompletionServed) error {
	payload := completionServedPayload{
		RequestID:    ev.RequestID,
		Team:         ev.Team,
		Model:        ev.Model,
		Provider:     ev.Provider.String(),
		PromptTokens: ev.PromptTokens,
		OutputTokens: ev.OutputTokens,
		TotalTokens:  ev.TotalTokens,
		CostMicroUSD: ev.CostMicroUSD,
		CacheHit:     ev.CacheHit,
		LatencyMs:    ev.LatencyMs,
	}
	if err := p.pub.Publish(ctx, SubjectCompletionServed, payload); err != nil {
		return fmt.Errorf("events: publish CompletionServed (request_id=%s): %w", ev.RequestID, err)
	}
	return nil
}

// PublishWarmSignal publishes the warm request to fp.ai.warm.requested (on the
// AI_REQUESTS stream) so KEDA scales Ollama 0->1.
func (p *Publisher) PublishWarmSignal(ctx context.Context, ev domain.WarmSignal) error {
	payload := warmSignalPayload{
		RequestID: ev.RequestID,
		Team:      ev.Team,
		Model:     ev.Model,
		Provider:  ev.Provider.String(),
	}
	if err := p.pub.Publish(ctx, SubjectWarmRequest, payload); err != nil {
		return fmt.Errorf("events: publish WarmSignal (request_id=%s): %w", ev.RequestID, err)
	}
	return nil
}
