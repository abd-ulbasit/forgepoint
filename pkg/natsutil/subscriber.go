package natsutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

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
//   5. Idempotent consumption via an optional ProcessedStore
//   6. Trace continuity: re-attaches the producer's trace context per message
//   7. Graceful shutdown: Close()/ctx cancellation stops the consume loop
//
// CONSUMER MODEL:
//   JetStream supports push- and pull-based consumers. consumer.Consume (used
//   here) is a managed pull consumer: the library maintains a background pull
//   loop and invokes our callback per message, while giving us a handle
//   (ConsumeContext) to stop it. Keeping that handle is essential — discarding
//   it leaks the loop for the process lifetime and prevents graceful drain.
//
// CONSUMER GROUPS (JetStream "durable consumers"):
//   When multiple replicas subscribe with the same durable name, JetStream
//   distributes messages across them (like Kafka consumer groups). Each message
//   is delivered to exactly one consumer in the group.
// ============================================================================

// defaultAckWait is how long JetStream waits for an ACK before redelivering.
// It must be comfortably larger than the per-message handler timeout, otherwise
// a slow-but-fine handler gets its message redelivered underneath it (duplicate
// work). 30s is a safe default for our workloads.
const defaultAckWait = 30 * time.Second

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
	store         ProcessedStore
	msgTimeout    time.Duration
	ackWait       time.Duration
}

// WithConsumerGroup sets the consumer group name for load balancing across
// replicas. All subscribers with the same group name share messages.
func WithConsumerGroup(name string) SubOption {
	return func(cfg *subConfig) {
		cfg.consumerGroup = name
	}
}

// WithMaxRetries sets the maximum number of RETRIES (redeliveries) before a
// message is routed to the DLQ. Total delivery attempts = maxRetries + 1 (the
// initial delivery plus the retries). Default: 0 (no DLQ; NATS redelivers
// according to its own policy).
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

// WithIdempotencyStore enables consumer-side deduplication. Before invoking the
// handler, the subscriber checks the store for the event ID; after a successful
// handle it records the ID. See ProcessedStore for the exactly-once caveat.
func WithIdempotencyStore(store ProcessedStore) SubOption {
	return func(cfg *subConfig) {
		cfg.store = store
	}
}

// WithMessageTimeout bounds how long a single handler invocation may run. The
// per-message context is cancelled after d. Keep d < AckWait so a handler that
// hits its timeout still NAKs before JetStream redelivers. Default: 0 (no
// timeout — the handler is bounded only by the subscribe context).
func WithMessageTimeout(d time.Duration) SubOption {
	return func(cfg *subConfig) {
		cfg.msgTimeout = d
	}
}

// WithAckWait overrides the JetStream AckWait (redelivery timeout). Default 30s.
func WithAckWait(d time.Duration) SubOption {
	return func(cfg *subConfig) {
		cfg.ackWait = d
	}
}

// Subscriber wraps JetStream consumption with automatic envelope
// deserialization, consumer groups, DLQ, idempotency, and trace propagation.
type Subscriber struct {
	js  jetstream.JetStream
	cfg subConfig

	mu       sync.Mutex
	consumes []jetstream.ConsumeContext // active consume loops, stopped on Close
}

// NewSubscriber creates a new Subscriber with the given options.
func NewSubscriber(js jetstream.JetStream, opts ...SubOption) *Subscriber {
	cfg := subConfig{ackWait: defaultAckWait}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Subscriber{js: js, cfg: cfg}
}

// Subscribe starts consuming messages from the given stream/subject filter and
// calls the handler for each message.
//
// Parameters:
//   - stream: the JetStream stream name (e.g., "MODELS")
//   - filterSubject: subject filter within the stream (e.g., "fp.models.>")
//   - handler: function called for each event
//
// The consumer is durable (survives restarts) when a consumer group is set.
// Messages are ACKed on handler success, NAKed on failure, and DLQ'd after
// MaxRetries. The consume loop is stopped when ctx is cancelled or Close() is
// called — whichever comes first — so the subscription drains cleanly on
// shutdown instead of leaking.
func (s *Subscriber) Subscribe(
	ctx context.Context,
	stream string,
	filterSubject string,
	handler EventHandler,
) error {
	consumerCfg := jetstream.ConsumerConfig{
		FilterSubject: filterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       s.cfg.ackWait,
	}

	// A durable name makes the consumer survive restarts and lets replicas with
	// the same name form a consumer group (shared work).
	if s.cfg.consumerGroup != "" {
		consumerCfg.Durable = s.cfg.consumerGroup
	}

	// MaxDeliver = maxRetries + 1: the initial delivery plus N retries. After
	// that JetStream stops redelivering on its own; our DLQ logic fires on the
	// final allowed delivery (see handleMessage).
	if s.cfg.maxRetries > 0 {
		consumerCfg.MaxDeliver = s.cfg.maxRetries + 1
	}

	consumer, err := s.js.CreateOrUpdateConsumer(ctx, stream, consumerCfg)
	if err != nil {
		return fmt.Errorf("natsutil: create consumer on stream %s: %w", stream, err)
	}

	// Consume returns a handle to the managed pull loop. We MUST keep it: it is
	// how the loop is stopped. Discarding it leaks a goroutine + subscription
	// for the process lifetime and makes graceful shutdown impossible.
	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		s.handleMessage(ctx, msg, handler)
	})
	if err != nil {
		return fmt.Errorf("natsutil: consume from stream %s: %w", stream, err)
	}

	s.mu.Lock()
	s.consumes = append(s.consumes, cc)
	s.mu.Unlock()

	// Stop the loop when the subscribe context is cancelled (e.g., SIGTERM),
	// so in-flight pulls drain rather than being abandoned.
	go func() {
		<-ctx.Done()
		cc.Stop()
	}()

	return nil
}

// Close stops all active consume loops. Idempotent and safe to call alongside
// context cancellation (Stop is safe to call more than once).
func (s *Subscriber) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cc := range s.consumes {
		cc.Stop()
	}
	s.consumes = nil
}

// handleMessage deserializes the envelope, builds a per-message context (trace +
// correlation + timeout), applies idempotency, calls the handler, and manages
// ACK/NAK/DLQ based on the result.
func (s *Subscriber) handleMessage(parent context.Context, msg jetstream.Msg, handler EventHandler) {
	var envelope EventEnvelope
	if err := json.Unmarshal(msg.Data(), &envelope); err != nil {
		slog.ErrorContext(parent, "failed to unmarshal event envelope",
			slog.String("subject", msg.Subject()),
			slog.String("error", err.Error()),
		)
		// A message we can't even parse is a poison message — redelivery will
		// never help, so Term it (stop redelivery) instead of NAK-looping.
		s.term(parent, msg)
		return
	}

	// Build the per-message context:
	//   - re-attach the producer's trace context so consumer spans link to the
	//     producer's trace (distributed tracing across the async hop),
	//   - carry the correlation ID for logs/onward events,
	//   - bound the handler with a timeout (if configured).
	ctx := extractTraceContext(parent, envelope)
	ctx = ContextWithCorrelationID(ctx, envelope.CorrelationID)
	if s.cfg.msgTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.msgTimeout)
		defer cancel()
	}

	// Idempotency: skip events we've already successfully processed. This is
	// the consumer-side dedup that makes "every consumer handles duplicates
	// safely" actually true, beyond JetStream's publish-window dedup.
	if s.cfg.store != nil {
		processed, err := s.cfg.store.IsProcessed(ctx, envelope.ID)
		if err != nil {
			slog.ErrorContext(ctx, "idempotency check failed",
				slog.String("event.id", envelope.ID), slog.String("error", err.Error()))
			s.nak(ctx, msg) // can't be sure — retry later
			return
		}
		if processed {
			// Duplicate: ACK so JetStream stops redelivering, but do NOT re-run
			// the handler's side effects.
			s.ack(ctx, msg)
			return
		}
	}

	if err := handler(ctx, envelope); err != nil {
		slog.WarnContext(ctx, "handler failed",
			slog.String("event.id", envelope.ID),
			slog.String("event.type", envelope.Type),
			slog.String("subject", msg.Subject()),
			slog.String("error", err.Error()),
		)
		s.handleFailure(ctx, msg, envelope)
		return
	}

	// Handler succeeded. Record the ID BEFORE acking so a duplicate redelivery
	// is recognized. If recording fails, NAK to retry — at-least-once. For true
	// exactly-once, a service should mark processed in the SAME DB transaction
	// as its writes; this store is the library-level convenience layer.
	if s.cfg.store != nil {
		if err := s.cfg.store.MarkProcessed(ctx, envelope.ID); err != nil {
			slog.ErrorContext(ctx, "failed to mark event processed",
				slog.String("event.id", envelope.ID), slog.String("error", err.Error()))
			s.nak(ctx, msg)
			return
		}
	}

	s.ack(ctx, msg)
}

// handleFailure decides between DLQ and NAK after a handler error.
func (s *Subscriber) handleFailure(ctx context.Context, msg jetstream.Msg, envelope EventEnvelope) {
	meta, metaErr := msg.Metadata()

	// DLQ on the FINAL allowed delivery. With MaxDeliver = maxRetries + 1, the
	// last delivery has NumDelivered == maxRetries + 1; after it JetStream will
	// not redeliver, so this is our last chance to preserve the message.
	if metaErr == nil && s.cfg.maxRetries > 0 && int(meta.NumDelivered) >= s.cfg.maxRetries+1 {
		s.routeToDLQ(ctx, msg, envelope)
		return
	}

	// Otherwise NAK — JetStream redelivers after AckWait/its backoff.
	s.nak(ctx, msg)
}

// routeToDLQ publishes the failed message to the dead letter subject and Terms
// the original (removing it from redelivery). If no DLQ subject is configured,
// the message is Term'd (dropped) to stop an infinite redelivery loop.
func (s *Subscriber) routeToDLQ(ctx context.Context, msg jetstream.Msg, envelope EventEnvelope) {
	if s.cfg.dlqSubject == "" {
		s.term(ctx, msg)
		return
	}

	slog.WarnContext(ctx, "routing message to DLQ",
		slog.String("event.id", envelope.ID),
		slog.String("event.type", envelope.Type),
		slog.String("dlq.subject", s.cfg.dlqSubject),
	)

	// Preserve the envelope ID as the DLQ message's Nats-Msg-Id so DLQ
	// consumers get the same dedup protection and the original identity.
	_, err := s.js.Publish(ctx, s.cfg.dlqSubject, msg.Data(), jetstream.WithMsgID(envelope.ID))
	if err != nil {
		slog.ErrorContext(ctx, "failed to publish to DLQ",
			slog.String("dlq.subject", s.cfg.dlqSubject),
			slog.String("error", err.Error()),
		)
		// We failed to DLQ it — NAK so it isn't lost; we'll try again.
		s.nak(ctx, msg)
		return
	}

	s.term(ctx, msg)
}

// ack/nak/term centralize acknowledgement + error logging so the call sites
// above stay readable.

func (s *Subscriber) ack(ctx context.Context, msg jetstream.Msg) {
	if err := msg.Ack(); err != nil {
		slog.ErrorContext(ctx, "failed to ACK message", slog.String("error", err.Error()))
	}
}

func (s *Subscriber) nak(ctx context.Context, msg jetstream.Msg) {
	if err := msg.Nak(); err != nil {
		slog.ErrorContext(ctx, "failed to NAK message", slog.String("error", err.Error()))
	}
}

func (s *Subscriber) term(ctx context.Context, msg jetstream.Msg) {
	if err := msg.Term(); err != nil {
		slog.ErrorContext(ctx, "failed to term message", slog.String("error", err.Error()))
	}
}
