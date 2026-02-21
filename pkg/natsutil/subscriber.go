package natsutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"
)

// ============================================================================
// NATS SUBSCRIBER
// ============================================================================
//
// WHY a Subscriber struct:
//   1. Automatic envelope deserialization (handlers receive EventEnvelope)
//   2. Consumer group support (queue subscription for multi-replica services)
//   3. DLQ handling (after max retries, route to dead letter subject)
//   4. Retry tracking via NATS delivery count metadata
//
// CONSUMER MODEL:
//   JetStream supports two consumer models:
//   - Push-based: NATS pushes messages to the consumer (simpler, lower latency)
//   - Pull-based: Consumer polls for messages (more control, back-pressure)
//   We use pull-based consumers for better control over processing rate and
//   to implement DLQ logic (inspect delivery count before processing).
//
// CONSUMER GROUPS (JetStream "durable consumers"):
//   When multiple replicas subscribe with the same consumer name, JetStream
//   distributes messages across them (similar to Kafka consumer groups).
//   Each message is delivered to exactly one consumer in the group.
// ============================================================================

// EventHandler processes a single event. Return nil to ACK, return error to NAK.
type EventHandler func(ctx context.Context, envelope EventEnvelope) error

// ErrProcessingFailed is returned by handlers when message processing fails
// and the message should be retried (or sent to DLQ after max retries).
var ErrProcessingFailed = errors.New("natsutil: processing failed")

// SubOption configures a Subscriber.
type SubOption func(*subConfig)

type subConfig struct {
	consumerGroup string
	maxRetries    int
	dlqSubject    string
}

// WithConsumerGroup sets the consumer group name for load balancing across
// replicas. All subscribers with the same group name share messages.
func WithConsumerGroup(name string) SubOption {
	return func(cfg *subConfig) {
		cfg.consumerGroup = name
	}
}

// WithMaxRetries sets the maximum number of delivery attempts before routing
// to the DLQ. Default: 0 (no DLQ, retry forever via NATS redelivery).
func WithMaxRetries(n int) SubOption {
	return func(cfg *subConfig) {
		cfg.maxRetries = n
	}
}

// WithDLQSubject sets the NATS subject for dead letter messages.
// Messages exceeding MaxRetries are published here.
func WithDLQSubject(subject string) SubOption {
	return func(cfg *subConfig) {
		cfg.dlqSubject = subject
	}
}

// Subscriber wraps JetStream consumption with automatic envelope
// deserialization, consumer groups, and DLQ support.
type Subscriber struct {
	js  jetstream.JetStream
	cfg subConfig
}

// NewSubscriber creates a new Subscriber with the given options.
func NewSubscriber(js jetstream.JetStream, opts ...SubOption) *Subscriber {
	cfg := subConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Subscriber{js: js, cfg: cfg}
}

// Subscribe starts consuming messages from the given stream/subject filter
// and calls the handler for each message.
//
// Parameters:
//   - stream: the JetStream stream name (e.g., "MODELS")
//   - filterSubject: subject filter within the stream (e.g., "goml.models.>")
//   - handler: function called for each event
//
// The consumer is durable (survives restarts) when a consumer group is set.
// Messages are ACKed on handler success, NAKed on failure.
func (s *Subscriber) Subscribe(
	ctx context.Context,
	stream string,
	filterSubject string,
	handler EventHandler,
) error {
	// Build consumer config.
	consumerCfg := jetstream.ConsumerConfig{
		FilterSubject: filterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	}

	// If consumer group is set, make the consumer durable with a name.
	// All subscribers with the same durable name share messages.
	if s.cfg.consumerGroup != "" {
		consumerCfg.Durable = s.cfg.consumerGroup
	}

	if s.cfg.maxRetries > 0 {
		consumerCfg.MaxDeliver = s.cfg.maxRetries + 1 // +1 because first delivery isn't a "retry"
	}

	// Create or update the consumer on the stream.
	consumer, err := s.js.CreateOrUpdateConsumer(ctx, stream, consumerCfg)
	if err != nil {
		return fmt.Errorf("natsutil: create consumer on stream %s: %w", stream, err)
	}

	// Start consuming messages asynchronously.
	// Consume() spawns a goroutine that pulls messages and calls our callback.
	_, err = consumer.Consume(func(msg jetstream.Msg) {
		s.handleMessage(ctx, msg, handler)
	})
	if err != nil {
		return fmt.Errorf("natsutil: consume from stream %s: %w", stream, err)
	}

	return nil
}

// handleMessage deserializes the envelope, calls the handler, and manages
// ACK/NAK/DLQ based on the result.
func (s *Subscriber) handleMessage(ctx context.Context, msg jetstream.Msg, handler EventHandler) {
	// Deserialize the event envelope.
	var envelope EventEnvelope
	if err := json.Unmarshal(msg.Data(), &envelope); err != nil {
		slog.ErrorContext(ctx, "failed to unmarshal event envelope",
			slog.String("subject", msg.Subject()),
			slog.String("error", err.Error()),
		)
		// Can't process if we can't deserialize — terminate (don't redeliver).
		if err := msg.Term(); err != nil {
			slog.ErrorContext(ctx, "failed to term message", slog.String("error", err.Error()))
		}
		return
	}

	// Call the handler.
	if err := handler(ctx, envelope); err != nil {
		slog.WarnContext(ctx, "handler failed",
			slog.String("event.id", envelope.ID),
			slog.String("event.type", envelope.Type),
			slog.String("subject", msg.Subject()),
			slog.String("error", err.Error()),
		)

		// Check if we should route to DLQ.
		meta, metaErr := msg.Metadata()
		if metaErr == nil && s.cfg.maxRetries > 0 && int(meta.NumDelivered) >= s.cfg.maxRetries {
			s.routeToDLQ(ctx, msg, envelope)
			return
		}

		// NAK with default backoff — NATS will redeliver after its backoff.
		if nakErr := msg.Nak(); nakErr != nil {
			slog.ErrorContext(ctx, "failed to NAK message", slog.String("error", nakErr.Error()))
		}
		return
	}

	// Success — ACK the message.
	if err := msg.Ack(); err != nil {
		slog.ErrorContext(ctx, "failed to ACK message", slog.String("error", err.Error()))
	}
}

// routeToDLQ publishes the failed message to the dead letter subject and
// ACKs the original (so it's removed from the main consumer's redelivery queue).
func (s *Subscriber) routeToDLQ(ctx context.Context, msg jetstream.Msg, envelope EventEnvelope) {
	if s.cfg.dlqSubject == "" {
		// No DLQ configured — just term the message (stop redelivery).
		if err := msg.Term(); err != nil {
			slog.ErrorContext(ctx, "failed to term message", slog.String("error", err.Error()))
		}
		return
	}

	slog.WarnContext(ctx, "routing message to DLQ",
		slog.String("event.id", envelope.ID),
		slog.String("event.type", envelope.Type),
		slog.String("dlq.subject", s.cfg.dlqSubject),
	)

	// Re-publish the original message to the DLQ subject.
	_, err := s.js.Publish(ctx, s.cfg.dlqSubject, msg.Data())
	if err != nil {
		slog.ErrorContext(ctx, "failed to publish to DLQ",
			slog.String("dlq.subject", s.cfg.dlqSubject),
			slog.String("error", err.Error()),
		)
		// NAK so NATS retries (we failed to DLQ it).
		if nakErr := msg.Nak(); nakErr != nil {
			slog.ErrorContext(ctx, "failed to NAK message", slog.String("error", nakErr.Error()))
		}
		return
	}

	// ACK the original message — it's now in the DLQ.
	if err := msg.Term(); err != nil {
		slog.ErrorContext(ctx, "failed to term message after DLQ", slog.String("error", err.Error()))
	}
}
