package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/domain"
)

// ============================================================================
// SUBSCRIBER — the inbound adapter that CLOSES THE LOOP
// ============================================================================
//
// serve → monitor → RETRAIN. Model Monitor publishes ModelDriftDetected when a
// model's live distribution breaches a threshold. THIS consumer is the last edge
// of the closed loop: on a CRITICAL drift with auto_retrain armed, it triggers
// the model's retrain pipeline (a DEPLOYMENT_SAGA / TRAINING_DAG) — automated
// self-healing with no human in the path. (notification and experiment-tracker
// also consume this event, for alerting and history; each is an INDEPENDENT
// durable consumer on the same subject — JetStream fan-out — so our processing
// never starves theirs.)
//
// THREE GUARANTEES THIS ADAPTER UPHOLDS (all from pkg/natsutil, configured here):
//
//   1. DURABLE consumer — WithConsumerGroup gives the consumer a durable name so
//      it survives restarts AND so multiple orchestrator replicas form a consumer
//      group (each drift event triggers a retrain on exactly ONE replica, never
//      three). This is the Kafka-consumer-group / SQS-visibility model.
//
//   2. IDEMPOTENT handling — WithIdempotencyStore dedupes on EventEnvelope.id so a
//      redelivered drift event (at-least-once: ACK lost, consumer crash, …) does
//      NOT start a second retrain saga. TWO layers of dedup, belt-and-suspenders:
//        - transport: the envelope id (handled by natsutil before our handler),
//        - business:  we pass drift.report_id as the saga's IdempotencyKey, so even
//          a DIFFERENT envelope carrying the SAME report (a monitor re-publish)
//          returns the SAME Execution instead of retraining twice.
//      At-least-once delivery + idempotent effect = exactly-once IN EFFECT.
//
//   3. DLQ for poison messages — WithMaxRetries + WithDLQSubject: a drift event we
//      can never act on (e.g. references a pipeline that no longer exists) is
//      retried a bounded number of times and then parked on the DLQ for an
//      operator, instead of NAK-looping forever and blocking the consumer.
//
// WHAT IS A "POISON" RETRAIN HERE vs a TRANSIENT failure:
//   - TRANSIENT (DB blip, NATS hiccup): the handler returns an error → NAK →
//     JetStream redelivers → it eventually succeeds. We do NOT DLQ these early.
//   - POISON (the drift names a retrain_pipeline_id that is archived/missing, or
//     malformed payload): the handler keeps failing; after MaxRetries it goes to
//     the DLQ. The domain's NotFound/Archived errors are the signal it is poison.
// ============================================================================

// driftStream is the JetStream stream the drift subject lives in. main.go ensures
// the stream exists (CreateOrUpdateStream over fp.models.>) before subscribing;
// the constant keeps the wiring and the tests referencing one name.
const driftStream = "MODELS"

// retrainGroup is the durable consumer name for the auto-retrain consumer. Stable
// across restarts/replicas so it forms ONE consumer group.
const retrainGroup = "pipeline-orchestrator-retrain"

// DriftRetrainSubscriber consumes ModelDriftDetected and triggers the auto-retrain
// saga. It depends ONLY on the domain's primary port (PipelineService) — it knows
// nothing about Postgres or the saga engine internals, exactly the dependency
// direction Clean Architecture prescribes (the adapter drives the domain inward).
type DriftRetrainSubscriber struct {
	svc    domain.PipelineService
	sub    *natsutil.Subscriber
	logger *slog.Logger
}

// NewDriftRetrainSubscriber builds the consumer. The natsutil.Subscriber is
// constructed by main.go with the consumer-group/DLQ/idempotency options (so the
// composition root owns those policy choices and tests can pass their own); this
// adapter only owns the decode + dispatch logic.
func NewDriftRetrainSubscriber(svc domain.PipelineService, sub *natsutil.Subscriber, logger *slog.Logger) *DriftRetrainSubscriber {
	if logger == nil {
		logger = slog.Default()
	}
	return &DriftRetrainSubscriber{svc: svc, sub: sub, logger: logger}
}

// Start begins consuming fp.models.drift.detected. It returns once the consume
// loop is running (the loop itself is managed by natsutil and stops on ctx cancel
// or Subscriber.Close).
func (d *DriftRetrainSubscriber) Start(ctx context.Context) error {
	return d.sub.Subscribe(ctx, driftStream, SubjectModelDriftDetected, d.handle)
}

// handle decodes one ModelDriftDetected and, if it warrants retraining, triggers
// the saga. The return value drives natsutil's ACK/NAK/DLQ:
//   - nil   → ACK (handled, or deliberately ignored — see the no-op cases)
//   - error → NAK (retry); after MaxRetries natsutil routes it to the DLQ.
func (d *DriftRetrainSubscriber) handle(ctx context.Context, env natsutil.EventEnvelope) error {
	var drift eventsv1.ModelDriftDetected
	if err := unmarshalCanonical(env.Data, &drift); err != nil {
		// A payload we cannot even decode is poison — redelivery will never help.
		// Returning an error here NAKs it; with MaxRetries+DLQ configured it lands
		// on the DLQ (and natsutil Terms the original) rather than looping forever.
		d.logger.ErrorContext(ctx, "undecodable drift event",
			slog.String("event.id", env.ID), slog.String("error", err.Error()))
		return fmt.Errorf("%w: decode drift event", natsutil.ErrProcessingFailed)
	}

	// POLICY GATE — only CRITICAL drift with auto_retrain armed closes the loop.
	// WARNING/OK drift, or a model whose owner has not opted into self-healing, is
	// observed by notification/experiment-tracker but must NOT auto-retrain. These
	// are SUCCESSFUL no-ops (ACK): the event was handled correctly by deciding not
	// to act. Returning nil (not an error) is important — a no-op must not NAK and
	// burn retry budget toward the DLQ.
	if !drift.GetAutoRetrain() || drift.GetSeverity() != eventsv1.DriftSeverity_DRIFT_SEVERITY_CRITICAL {
		d.logger.InfoContext(ctx, "drift does not warrant auto-retrain; ignoring",
			slog.String("model", drift.GetModelName()),
			slog.String("severity", drift.GetSeverity().String()),
			slog.Bool("auto_retrain", drift.GetAutoRetrain()),
		)
		return nil
	}

	// A CRITICAL+armed drift with NO retrain pipeline configured is a
	// MISCONFIGURATION, not a transient error: retrying can never make a pipeline
	// id appear. ACK it (don't DLQ-loop) and log loudly so an operator fixes the
	// monitor policy. (We could DLQ; we choose to log+ACK because the producer, not
	// this consumer, owns the fix, and re-delivering the same misconfigured event
	// would be pointless churn.)
	if drift.GetRetrainPipelineId() == "" {
		d.logger.WarnContext(ctx, "critical drift armed for retrain but no retrain_pipeline_id set",
			slog.String("model", drift.GetModelName()),
			slog.String("report.id", drift.GetReportId()),
		)
		return nil
	}

	// The principal for the run is the SERVICE identity "model-monitor" (recorded
	// as TriggeredBy) so the audit trail and Notification know this was automated,
	// not a human — and the loop is attributable. Team is carried from the drift's
	// model owner if present; here we scope by the model name's owning context. The
	// domain stamps TriggeredBy from this Actor.
	actor := domain.Actor{Subject: ServiceIdentity, Team: retrainTeam(&drift)}

	input := domain.TriggerInput{
		PipelineID: drift.GetRetrainPipelineId(),
		Input:      retrainInput(&drift),
		// BUSINESS idempotency: report_id is idempotent on the drift report's window
		// (one report → one event). Using it as the trigger key means a re-published
		// or redelivered report returns the SAME Execution — no double retrain — even
		// across different envelope ids. This is the domain-level half of
		// exactly-once-in-effect that complements the envelope-id transport dedup.
		IdempotencyKey: drift.GetReportId(),
	}

	_, err := d.svc.TriggerExecution(ctx, actor, input)
	if err != nil {
		return d.classify(ctx, &drift, err)
	}

	d.logger.InfoContext(ctx, "auto-retrain triggered from drift",
		slog.String("model", drift.GetModelName()),
		slog.String("pipeline.id", drift.GetRetrainPipelineId()),
		slog.String("report.id", drift.GetReportId()),
	)
	return nil
}

// classify decides whether a TriggerExecution error is POISON (will never succeed
// → let it reach the DLQ) or TRANSIENT (retry). This is the crux of "don't
// NAK-loop a message that can never be processed."
//
//   - ErrPipelineNotFound / ErrPipelineArchived: the configured retrain pipeline
//     does not exist or was archived. No redelivery fixes this — it is poison.
//     We still RETURN an error (so natsutil counts the delivery and eventually
//     DLQs it for an operator) but log it as poison so the alert is unambiguous.
//   - anything else (DB down, context deadline, infra): TRANSIENT — return the
//     error so JetStream redelivers and a later attempt can succeed.
//
// Both branches return an error (→ NAK). The difference is purely the LOG signal;
// the DLQ threshold (MaxRetries) bounds the poison case regardless. We keep them
// distinct so an operator triaging the DLQ sees "poison: archived pipeline" vs a
// transient blip that exhausted retries during an outage.
func (d *DriftRetrainSubscriber) classify(ctx context.Context, drift *eventsv1.ModelDriftDetected, err error) error {
	if errors.Is(err, domain.ErrPipelineNotFound) || errors.Is(err, domain.ErrPipelineArchived) {
		d.logger.ErrorContext(ctx, "auto-retrain target pipeline missing/archived (poison — will DLQ)",
			slog.String("pipeline.id", drift.GetRetrainPipelineId()),
			slog.String("report.id", drift.GetReportId()),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("%w: retrain target unusable", natsutil.ErrProcessingFailed)
	}
	d.logger.WarnContext(ctx, "auto-retrain trigger failed (transient — will retry)",
		slog.String("pipeline.id", drift.GetRetrainPipelineId()),
		slog.String("report.id", drift.GetReportId()),
		slog.String("error", err.Error()),
	)
	return err
}

// retrainInput builds the run input handed to the retrain pipeline from the
// drift's free-form retrain_context PLUS a few stable, self-describing keys so the
// retrain DAG knows WHICH model/version drifted and WHY it was triggered — without
// a callback to Model Monitor. retrain_context is a google.protobuf.Struct, so
// AsMap() yields a plain map[string]any the domain TriggerInput expects (the
// domain is proto-free).
func retrainInput(drift *eventsv1.ModelDriftDetected) map[string]any {
	in := map[string]any{}
	if rc := drift.GetRetrainContext(); rc != nil {
		for k, v := range rc.AsMap() {
			in[k] = v
		}
	}
	// Stable provenance keys (do not let retrain_context overwrite these — they are
	// the authoritative trigger facts, set AFTER the merge above).
	in["triggered_by"] = "drift"
	in["model_name"] = drift.GetModelName()
	in["model_version"] = drift.GetModelVersion()
	in["drift_report_id"] = drift.GetReportId()
	in["drift_severity"] = drift.GetSeverity().String()
	return in
}

// retrainTeam derives the owning team for the retrain run. The drift event does
// not carry a team field directly (it is about a model, identified by name); in
// the absence of an explicit owner on the event we leave Team empty and let the
// domain/repository resolve tenancy from the target pipeline. Kept as a function
// (not inlined) so when the event contract adds an owner_team to drift, there is
// exactly one place to wire it.
func retrainTeam(_ *eventsv1.ModelDriftDetected) string {
	return ""
}
