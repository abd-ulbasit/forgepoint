package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	pkgaudit "github.com/abd-ulbasit/forgepoint/pkg/audit"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// ============================================================================
// AUDIT CONSUMER — the choreography reactor that PERSISTS audit events
// ============================================================================
//
// This is the INBOUND adapter that drains fp.audit.recorded and appends each
// record to the hash-chained, append-only table via the Repository. It is the
// PERSIST half of the package's split, and it is the SAME choreography shape the
// notification service uses: one durable consumer, idempotent, with a DLQ for
// poison messages.
//
//	fp.audit.recorded ──► Consumer.handle (THIS) ──► Repository.Append ──► Postgres
//	    (every service                                  (hash chain +
//	     publishes here)                                 append-only)
//
// IDEMPOTENCY: NATS is at-least-once. The Repository's ON CONFLICT (event_id)
// makes a duplicate a no-op at the DATABASE — the authoritative guard, because it
// survives a process crash between handling and ack (unlike an in-memory
// ProcessedStore). We therefore rely on the DB for correctness and do NOT wire a
// separate ProcessedStore here: the durable, transactional dedup IS the right
// layer for an audit log (see pkg/natsutil idempotency.go's exactly-once caveat —
// the transactional version is the gold standard, and that is exactly what the
// repository's ON CONFLICT gives us).
// ============================================================================

// Consumer subscribes to fp.audit.recorded and appends each record to the sink.
type Consumer struct {
	sub    *natsutil.Subscriber
	repo   *Repository
	logger *slog.Logger
}

// NewConsumer builds the audit consumer over the JetStream context and the
// repository. It configures a durable consumer group (so replicas share work), a
// retry cap, and a DLQ — mirroring notification's reactor resilience knobs.
func NewConsumer(js jetstream.JetStream, repo *Repository, logger *slog.Logger) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	sub := natsutil.NewSubscriber(js,
		natsutil.WithConsumerGroup(ConsumerGroup),
		// A small retry budget: a transient DB blip is retried; a persistent fault
		// (e.g. a malformed record that can't be decoded) is Term'd by natsutil at
		// the envelope layer or DLQ'd after the cap. 4 retries = 5 total attempts.
		natsutil.WithMaxRetries(4),
		natsutil.WithDLQSubject(DLQSubject),
	)
	return &Consumer{sub: sub, repo: repo, logger: logger}
}

// Start binds the durable consumer on the AUDIT stream / fp.audit.recorded subject
// and begins draining. ctx governs the consume loop's lifetime alongside Close().
func (c *Consumer) Start(ctx context.Context) error {
	if err := c.sub.Subscribe(ctx, StreamName, SubjectRecorded, c.handle); err != nil {
		return fmt.Errorf("auth/audit: subscribe %s/%s: %w", StreamName, SubjectRecorded, err)
	}
	c.logger.InfoContext(ctx, "audit consumer started",
		slog.String("stream", StreamName),
		slog.String("subject", SubjectRecorded),
		slog.String("durable", ConsumerGroup),
	)
	return nil
}

// Close stops the consume loop. Idempotent (natsutil Subscriber.Close is).
func (c *Consumer) Close() { c.sub.Close() }

// handle decodes the envelope's data into a pkgaudit.Record and appends it. The
// envelope's ID is the IDEMPOTENCY KEY passed to Append (event_id) — a redelivery
// of the same envelope carries the same id and the repository skips it.
//
// ERROR CLASSIFICATION (transient vs poison):
//   - A decode failure is a POISON message (it will never decode) → wrap
//     ErrProcessingFailed so natsutil heads it to the DLQ after the cap rather
//     than NAK-looping forever.
//   - An Append failure is TRANSIENT (DB blip) → return a plain error so natsutil
//     NAKs and retries; the ON CONFLICT makes the retry safe.
func (c *Consumer) handle(ctx context.Context, env natsutil.EventEnvelope) error {
	var rec pkgaudit.Record
	if err := json.Unmarshal(env.Data, &rec); err != nil {
		// Poison: a record we can't decode can never be persisted. Wrap the
		// permanent-fault sentinel so it is DLQ'd, not retried indefinitely.
		c.logger.WarnContext(ctx, "undecodable audit record (routing toward DLQ)",
			slog.String("event.id", env.ID),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("%w: decode audit record: %v", natsutil.ErrProcessingFailed, err)
	}

	// The envelope id is the dedup key. The repository computes the hash chain and
	// appends (or skips on duplicate) under its single-writer lock.
	res, err := c.repo.Append(ctx, env.ID, rec)
	if err != nil {
		// Transient (DB) — return a plain error so natsutil retries.
		return fmt.Errorf("auth/audit: append record: %w", err)
	}

	if res.Appended {
		c.logger.InfoContext(ctx, "audit record appended",
			slog.String("event.id", env.ID),
			slog.Int64("seq", res.Seq),
			slog.String("action", rec.Action),
			slog.String("decision", string(rec.Decision)),
		)
	} else {
		c.logger.DebugContext(ctx, "duplicate audit record skipped (idempotent)",
			slog.String("event.id", env.ID),
			slog.Int64("seq", res.Seq),
		)
	}
	return nil // ACK
}
