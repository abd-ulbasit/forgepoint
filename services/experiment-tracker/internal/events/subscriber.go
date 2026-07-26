package events

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
)

// ============================================================================
// SUBSCRIBER ADAPTERS — the WIDE event-driven sink
// ============================================================================
//
// Experiment Tracker CONSUMES a broad slice of the platform's lifecycle events
// and records each as a lineage row (see lineage.go for the WHY). This file is
// the inbound adapter: for every consumed subject it DECODES the canonical
// events.v1 payload (protojson) and dispatches to the LineageRecorder. The
// per-subject durable consumers are wired in SubscribeAll, each with:
//
//   - a DURABLE consumer name (consumer group) → survives restarts, and lets
//     replicas share the work (JetStream's Kafka-consumer-group equivalent),
//   - an IDEMPOTENCY store (natsutil ProcessedStore) → a redelivered envelope id
//     is ACKed without re-recording (consumer-side dedup on top of the lineage
//     table's UNIQUE(event_id) — belt and suspenders),
//   - a DLQ → a poison message (one that always fails to decode/record) is routed
//     to a dead-letter subject after MaxRetries instead of NAK-looping forever.
//
// CONSUMED SET (grounded in docs/design/event-contract.md, the experiment-tracker
// migration note + the subject registry):
//
//   fp.models.registered          ModelRegistered        → MODEL_REGISTERED
//   fp.models.version.created      ModelVersionCreated    → MODEL_VERSION_CREATED
//   fp.models.promoted             ModelPromoted          → MODEL_PROMOTED
//   fp.pipelines.model.deployed    ModelDeployed          → MODEL_DEPLOYED
//   fp.pipelines.step.completed    StepCompleted          → PIPELINE_STEP_COMPLETED
//   fp.inference.completed         InferenceCompleted     → INFERENCE_COMPLETED
//   fp.features.written            FeaturesWritten        → FEATURES_WRITTEN
//   fp.features.view.defined       FeatureViewDefined     → FEATURE_VIEW_DEFINED
//   fp.billing.usage.recorded      UsageRecorded          → USAGE_RECORDED
//   fp.notifications.delivered     NotificationDelivered  → NOTIFICATION_DELIVERED
//   fp.notifications.failed        NotificationFailed     → NOTIFICATION_FAILED
//   fp.models.drift.detected       ModelDriftDetected     → MODEL_DRIFT_DETECTED
//   fp.pipelines.started           PipelineStarted        → PIPELINE_STARTED
//   fp.pipelines.completed         PipelineCompleted      → PIPELINE_COMPLETED
//
// The last three are the cross-service provenance facts the contract assigns this
// service (event-contract.md subject registry + the experiment-tracker migration
// note): drift on a model's history, and a pipeline run's start/finish boundaries.
// Note ModelDriftDetected lives under fp.models.* but is PRODUCED by model-monitor
// (EventEnvelope.source = "model-monitor"); we record it for the model's lineage —
// the serve→monitor→retrain loop itself is closed by pipeline-orchestrator
// consuming the same event, not by this service.
//
// WHY one consumer PER subject (not one fp.> firehose): each consumed event needs
// its OWN decoder and its own DLQ accounting, and the streams the events live in
// differ (MODELS, PIPELINES, INFERENCE, FEATURES, BILLING, NOTIFICATIONS). A
// per-subject durable consumer also lets us scale/replay one event type
// independently — the operational granularity you want in production.

// Consumed subjects (the canonical subjects this service binds to).
const (
	SubjectModelRegistered       = "fp.models.registered"
	SubjectModelVersionCreated   = "fp.models.version.created"
	SubjectModelPromoted         = "fp.models.promoted"
	SubjectModelDeployed         = "fp.pipelines.model.deployed"
	SubjectStepCompleted         = "fp.pipelines.step.completed"
	SubjectInferenceCompleted    = "fp.inference.completed"
	SubjectFeaturesWritten       = "fp.features.written"
	SubjectFeatureViewDefined    = "fp.features.view.defined"
	SubjectUsageRecorded         = "fp.billing.usage.recorded"
	SubjectNotificationDelivered = "fp.notifications.delivered"
	SubjectNotificationFailed    = "fp.notifications.failed"
	SubjectModelDriftDetected    = "fp.models.drift.detected"
	SubjectPipelineStarted       = "fp.pipelines.started"
	SubjectPipelineCompleted     = "fp.pipelines.completed"
)

// SubscriberConfig is the RESILIENCE POLICY the wiring layer (main.go) hands the
// adapter: the JetStream handle, the durable-name base, the idempotency backend,
// and the DLQ/retry caps. The adapter owns the per-subject consumer NAMING (it
// must — see WHY below); the wiring layer owns the policy. This split keeps the
// adapter free of "what's our retry budget / which store" decisions while still
// letting it create correctly-named consumers.
type SubscriberConfig struct {
	JS jetstream.JetStream

	// ConsumerBase is the durable-name PREFIX. The adapter derives a UNIQUE durable
	// per subject (ConsumerBase + "-" + sanitized-subject).
	//
	// WHY one durable PER SUBJECT (not one shared group across all subjects) — the
	// bug this prevents: a JetStream durable consumer has ONE FilterSubject. Binding
	// three subjects in the same stream (e.g. fp.models.registered / .version.created
	// / .promoted) under ONE durable name makes each CreateOrUpdateConsumer OVERWRITE
	// the prior one's filter — only the last subject survives, and messages get
	// delivered to a handler expecting a different payload type (decode failures).
	// A distinct durable per subject gives each its own server-side cursor, filter,
	// redelivery accounting, and DLQ budget — the operational isolation you want.
	// (Within a durable, multiple REPLICAS still share the work — the consumer-group
	// semantics are preserved per subject.)
	ConsumerBase string

	// Store is the consumer-side idempotency backend (Postgres/Redis in prod, the
	// memory store in tests). Shared across all per-subject consumers — dedup is by
	// global EventEnvelope.id, so one store is correct.
	Store natsutil.ProcessedStore

	// MaxRetries / DLQSubject / AckWait are the DLQ policy, applied to every
	// per-subject consumer identically.
	MaxRetries int
	DLQSubject string
	AckWait    time.Duration
}

// Subscriber is the inbound adapter. It dispatches decoded events to the
// LineageRecorder and owns the per-subject natsutil.Subscriber instances it
// builds from the injected SubscriberConfig (one per consumed subject, each with
// its own durable name — see SubscriberConfig.ConsumerBase for WHY).
type Subscriber struct {
	cfg      SubscriberConfig
	recorder LineageRecorder

	mu   sync.Mutex
	subs []*natsutil.Subscriber // per-subject subscribers, stopped on Close
}

// NewSubscriber builds the inbound adapter from the resilience policy (config)
// and the LineageRecorder decoded events are written to.
func NewSubscriber(cfg SubscriberConfig, recorder LineageRecorder) *Subscriber {
	return &Subscriber{cfg: cfg, recorder: recorder}
}

// optsForSubject builds the natsutil.SubOptions for ONE subject's consumer: a
// UNIQUE durable name, the shared idempotency store, and the DLQ/retry/ackwait
// policy. Centralizing this guarantees every per-subject consumer is configured
// identically except for its (unique) durable name.
func (s *Subscriber) optsForSubject(subject string) []natsutil.SubOption {
	opts := []natsutil.SubOption{
		natsutil.WithConsumerGroup(durableName(s.cfg.ConsumerBase, subject)),
	}
	if s.cfg.Store != nil {
		opts = append(opts, natsutil.WithIdempotencyStore(s.cfg.Store))
	}
	if s.cfg.MaxRetries > 0 {
		opts = append(opts, natsutil.WithMaxRetries(s.cfg.MaxRetries))
	}
	if s.cfg.DLQSubject != "" {
		opts = append(opts, natsutil.WithDLQSubject(s.cfg.DLQSubject))
	}
	if s.cfg.AckWait > 0 {
		opts = append(opts, natsutil.WithAckWait(s.cfg.AckWait))
	}
	return opts
}

// Close stops every per-subject consume loop. Called on graceful shutdown
// alongside cancelling the SubscribeAll context. Safe to call multiple times
// (each natsutil.Subscriber.Close is idempotent).
func (s *Subscriber) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sub := range s.subs {
		sub.Close()
	}
	s.subs = nil
}

// durableName derives a JetStream-safe durable name for a subject by replacing
// the '.' separators (illegal in a durable name) with '_'. fp.models.registered
// under base "exp-tracker" → "exp-tracker-fp_models_registered" — unique per
// subject, stable across restarts (so the consumer resumes its cursor).
func durableName(base, subject string) string {
	return base + "-" + strings.ReplaceAll(subject, ".", "_")
}

// subscription pairs a subject with the JetStream stream it lives in and the
// handler that decodes+records it. SubscribeAll iterates these; a test can also
// drive a single handler directly (HandlerFor) without a live stream.
type subscription struct {
	stream  string
	subject string
	handler natsutil.EventHandler
}

// subscriptions returns the full consumed set with each event's decode→record
// handler bound. Defining them as data (not eleven copy-pasted Subscribe calls)
// keeps the consumer set auditable in one place and impossible to drift between
// "what we decode" and "what we subscribe to".
//
// Stream names follow the platform convention: the uppercased first domain
// segment of the subject (fp.MODELS.* → "MODELS"). The orchestrator that creates
// the streams (M3 infra) owns their config; the consumer only needs the name to
// bind a durable consumer.
func (s *Subscriber) subscriptions() []subscription {
	return []subscription{
		{"MODELS", SubjectModelRegistered, s.handleModelRegistered},
		{"MODELS", SubjectModelVersionCreated, s.handleModelVersionCreated},
		{"MODELS", SubjectModelPromoted, s.handleModelPromoted},
		{"PIPELINES", SubjectModelDeployed, s.handleModelDeployed},
		{"PIPELINES", SubjectStepCompleted, s.handleStepCompleted},
		{"INFERENCE", SubjectInferenceCompleted, s.handleInferenceCompleted},
		{"FEATURES", SubjectFeaturesWritten, s.handleFeaturesWritten},
		{"FEATURES", SubjectFeatureViewDefined, s.handleFeatureViewDefined},
		{"BILLING", SubjectUsageRecorded, s.handleUsageRecorded},
		{"NOTIFICATIONS", SubjectNotificationDelivered, s.handleNotificationDelivered},
		{"NOTIFICATIONS", SubjectNotificationFailed, s.handleNotificationFailed},
		// Drift lives on fp.models.* (stream MODELS) though model-monitor produces it
		// — the subject domain is the RESOURCE (a model), not the producer.
		{"MODELS", SubjectModelDriftDetected, s.handleModelDriftDetected},
		{"PIPELINES", SubjectPipelineStarted, s.handlePipelineStarted},
		{"PIPELINES", SubjectPipelineCompleted, s.handlePipelineCompleted},
	}
}

// SubscribeAll binds a dedicated durable consumer for every consumed subject. It
// is called once at startup (main.go, next stage). Each subject gets its OWN
// natsutil.Subscriber (its own durable name — see SubscriberConfig.ConsumerBase)
// so consumers don't clobber each other's filter. The ctx governs the lifetime of
// all the consume loops; cancel it (or call Close) to drain cleanly. A failure to
// bind ANY subject aborts startup (returns the error) so we never run silently
// missing half our inputs.
func (s *Subscriber) SubscribeAll(ctx context.Context) error {
	for _, sb := range s.subscriptions() {
		natsSub := natsutil.NewSubscriber(s.cfg.JS, s.optsForSubject(sb.subject)...)
		if err := natsSub.Subscribe(ctx, sb.stream, sb.subject, sb.handler); err != nil {
			natsSub.Close()
			return fmt.Errorf("events: subscribe %s on stream %s: %w", sb.subject, sb.stream, err)
		}
		s.mu.Lock()
		s.subs = append(s.subs, natsSub)
		s.mu.Unlock()
	}
	return nil
}

// HandlerFor exposes a single subject's handler for testing (drive a decoded
// envelope through the exact production decode→record path without a live
// JetStream consumer). Returns nil for an unknown subject so a test typo fails
// loudly rather than silently testing nothing.
func (s *Subscriber) HandlerFor(subject string) natsutil.EventHandler {
	for _, sb := range s.subscriptions() {
		if sb.subject == subject {
			return sb.handler
		}
	}
	return nil
}

// ----------------------------------------------------------------------------
// Per-event handlers: decode events.v1 payload → LineageEvent → Record.
// ----------------------------------------------------------------------------
//
// Each handler is the same shape: unmarshal the typed payload, project it to a
// flat LineageEvent (carrying only the correlation handles + a PII-free summary),
// and record it. record() centralizes the EventID/Source/Record plumbing so each
// handler is just the per-event PROJECTION — the part that actually differs.

func (s *Subscriber) handleModelRegistered(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.ModelRegistered
	if err := decode(env, &ev); err != nil {
		return err
	}
	return s.record(ctx, env, LineageEvent{
		Kind:       LineageModelRegistered,
		ModelID:    ev.GetModelId(),
		Summary:    fmt.Sprintf("model %q registered by team %s", ev.GetModelName(), ev.GetTeam()),
		OccurredAt: tsToTime(ev.GetRegisteredAt()),
	})
}

func (s *Subscriber) handleModelVersionCreated(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.ModelVersionCreated
	if err := decode(env, &ev); err != nil {
		return err
	}
	return s.record(ctx, env, LineageEvent{
		Kind:         LineageModelVersionCreated,
		ModelID:      ev.GetModelId(),
		ModelVersion: ev.GetVersion(),
		Summary:      fmt.Sprintf("version %s created for model %q", ev.GetVersion(), ev.GetModelName()),
		OccurredAt:   tsToTime(ev.GetCreatedAt()),
	})
}

func (s *Subscriber) handleModelPromoted(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.ModelPromoted
	if err := decode(env, &ev); err != nil {
		return err
	}
	return s.record(ctx, env, LineageEvent{
		Kind:         LineageModelPromoted,
		ModelID:      ev.GetModelId(),
		ModelVersion: ev.GetVersion(),
		Summary:      fmt.Sprintf("promoted %q %s %s→%s", ev.GetModelName(), ev.GetVersion(), ev.GetFromStage(), ev.GetToStage()),
		OccurredAt:   tsToTime(ev.GetPromotedAt()),
	})
}

func (s *Subscriber) handleModelDeployed(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.ModelDeployed
	if err := decode(env, &ev); err != nil {
		return err
	}
	return s.record(ctx, env, LineageEvent{
		Kind:         LineageModelDeployed,
		ModelID:      ev.GetModelId(),
		ModelVersion: ev.GetVersion(),
		ExecutionID:  ev.GetExecutionId(),
		Summary:      fmt.Sprintf("deployed %q %s at %d bps", ev.GetModelName(), ev.GetVersion(), ev.GetWeightBps()),
		OccurredAt:   tsToTime(ev.GetDeployedAt()),
	})
}

func (s *Subscriber) handleStepCompleted(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.StepCompleted
	if err := decode(env, &ev); err != nil {
		return err
	}
	return s.record(ctx, env, LineageEvent{
		Kind:        LineagePipelineStepDone,
		ExecutionID: ev.GetExecutionId(),
		Summary:     fmt.Sprintf("pipeline step %s (%s) completed", ev.GetStepId(), ev.GetStepType()),
		OccurredAt:  tsToTime(ev.GetCompletedAt()),
	})
}

func (s *Subscriber) handleInferenceCompleted(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.InferenceCompleted
	if err := decode(env, &ev); err != nil {
		return err
	}
	return s.record(ctx, env, LineageEvent{
		Kind:         LineageInferenceCompleted,
		ModelID:      ev.GetModelId(),
		ModelVersion: ev.GetVersion(),
		RequestID:    ev.GetRequestId(),
		Summary:      fmt.Sprintf("inference on %q %s in %dms", ev.GetModelName(), ev.GetVersion(), ev.GetLatencyMs()),
		OccurredAt:   tsToTime(ev.GetCompletedAt()),
	})
}

func (s *Subscriber) handleFeaturesWritten(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.FeaturesWritten
	if err := decode(env, &ev); err != nil {
		return err
	}
	return s.record(ctx, env, LineageEvent{
		Kind:       LineageFeaturesWritten,
		Summary:    fmt.Sprintf("%d features written to view %q (through v%d)", ev.GetWrittenCount(), ev.GetFeatureViewName(), ev.GetWrittenThroughVersion()),
		OccurredAt: tsToTime(ev.GetWrittenAt()),
	})
}

func (s *Subscriber) handleFeatureViewDefined(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.FeatureViewDefined
	if err := decode(env, &ev); err != nil {
		return err
	}
	return s.record(ctx, env, LineageEvent{
		Kind:       LineageFeatureViewDefined,
		Summary:    fmt.Sprintf("feature view %q defined (schema v%d)", ev.GetFeatureViewName(), ev.GetSchemaVersion()),
		OccurredAt: tsToTime(ev.GetDefinedAt()),
	})
}

func (s *Subscriber) handleUsageRecorded(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.UsageRecorded
	if err := decode(env, &ev); err != nil {
		return err
	}
	return s.record(ctx, env, LineageEvent{
		Kind:         LineageUsageRecorded,
		ModelID:      ev.GetModelId(),
		ModelVersion: ev.GetModelVersion(),
		RequestID:    ev.GetSourceRequestId(),
		Summary:      fmt.Sprintf("usage: %d %s for team %s (%d micros)", ev.GetQuantity(), ev.GetMeterType(), ev.GetTeam(), ev.GetCostMicros()),
		OccurredAt:   tsToTime(ev.GetOccurredAt()),
	})
}

func (s *Subscriber) handleNotificationDelivered(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.NotificationDelivered
	if err := decode(env, &ev); err != nil {
		return err
	}
	return s.record(ctx, env, LineageEvent{
		Kind:       LineageNotificationDelivered,
		Summary:    fmt.Sprintf("notification %s delivered via %s (re: %s)", ev.GetNotificationId(), ev.GetChannel(), ev.GetEventType()),
		OccurredAt: tsToTime(ev.GetDeliveredAt()),
	})
}

func (s *Subscriber) handleNotificationFailed(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.NotificationFailed
	if err := decode(env, &ev); err != nil {
		return err
	}
	return s.record(ctx, env, LineageEvent{
		Kind:       LineageNotificationFailed,
		Summary:    fmt.Sprintf("notification %s FAILED via %s after %d attempts: %s", ev.GetNotificationId(), ev.GetChannel(), ev.GetAttempts(), ev.GetErrorMessage()),
		OccurredAt: tsToTime(ev.GetFailedAt()),
	})
}

func (s *Subscriber) handleModelDriftDetected(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.ModelDriftDetected
	if err := decode(env, &ev); err != nil {
		return err
	}
	// Project the headline scalars only (model/version/type/severity/report_id) —
	// the PII-free, deep-linkable summary the contract calls "fat-but-flat". We do
	// NOT copy the per-feature DriftMetric breakdown into lineage: lineage is an
	// INDEX/timeline, not a second copy of the report (report_id is the deep-link).
	// ModelVersion comes from the drift payload's model_version (the exact serving
	// version that drifted — vital under canary traffic splitting).
	return s.record(ctx, env, LineageEvent{
		Kind:         LineageModelDriftDetected,
		ModelVersion: ev.GetModelVersion(),
		Summary: fmt.Sprintf("drift %s (%s) on %q %s, report %s",
			ev.GetDriftType(), ev.GetSeverity(), ev.GetModelName(), ev.GetModelVersion(), ev.GetReportId()),
		OccurredAt: tsToTime(ev.GetDetectedAt()),
	})
}

func (s *Subscriber) handlePipelineStarted(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.PipelineStarted
	if err := decode(env, &ev); err != nil {
		return err
	}
	// ExecutionID is the run-correlation handle a "run Y context" query joins on;
	// triggered_by distinguishes a human run from an automated one (e.g.
	// "model-monitor" for an auto-retrain), which the timeline surfaces.
	return s.record(ctx, env, LineageEvent{
		Kind:        LineagePipelineStarted,
		ExecutionID: ev.GetExecutionId(),
		Summary: fmt.Sprintf("pipeline %s (%s) started by %s",
			ev.GetPipelineId(), ev.GetPipelineType(), ev.GetTriggeredBy()),
		OccurredAt: tsToTime(ev.GetStartedAt()),
	})
}

func (s *Subscriber) handlePipelineCompleted(ctx context.Context, env natsutil.EventEnvelope) error {
	var ev eventsv1.PipelineCompleted
	if err := decode(env, &ev); err != nil {
		return err
	}
	// Closes the run boundary opened by PipelineStarted on the timeline; the
	// duration is the run's total wall-clock (a handy SLO scalar in the summary).
	return s.record(ctx, env, LineageEvent{
		Kind:        LineagePipelineCompleted,
		ExecutionID: ev.GetExecutionId(),
		Summary: fmt.Sprintf("pipeline %s (%s) completed in %s",
			ev.GetPipelineId(), ev.GetPipelineType(), ev.GetDuration().AsDuration()),
		OccurredAt: tsToTime(ev.GetCompletedAt()),
	})
}

// ----------------------------------------------------------------------------
// Shared decode + record plumbing.
// ----------------------------------------------------------------------------

// decode unmarshals the envelope's protojson Data into the typed payload. A
// failure is a POISON message — the bytes will never parse, so retrying is
// pointless. We surface it as natsutil.ErrProcessingFailed so the subscriber's
// failure path (after MaxRetries) routes it to the DLQ rather than NAK-looping.
// (natsutil also Terms a message whose ENVELOPE itself won't parse, before a
// handler ever runs; this handles the case where the envelope is fine but its
// payload is corrupt for THIS decoder.)
func decode(env natsutil.EventEnvelope, msg proto.Message) error {
	if err := protojson.Unmarshal(env.Data, msg); err != nil {
		// Wrap the sentinel so handleFailure DLQs after the retry budget, but log
		// the underlying cause for debugging. We do NOT echo env.Data (possible
		// payload content) into the error — the type/sentinel is enough.
		return fmt.Errorf("events: decode %T payload: %w", msg, natsutil.ErrProcessingFailed)
	}
	return nil
}

// record stamps the envelope-derived idempotency/audit fields onto the projected
// LineageEvent and writes it. EventID = envelope.ID is the dedupe key (the
// lineage table's UNIQUE(event_id) makes a redelivery a no-op); Source =
// envelope.Source records the actual producer. Centralizing this here means a
// handler can't forget to set the idempotency key.
func (s *Subscriber) record(ctx context.Context, env natsutil.EventEnvelope, le LineageEvent) error {
	le.EventID = env.ID
	le.Source = env.Source
	if err := s.recorder.Record(ctx, le); err != nil {
		// A record failure (e.g. transient DB error) is RETRYABLE — return the
		// error so the subscriber NAKs and JetStream redelivers. On redelivery the
		// idempotency store (or the UNIQUE index) prevents a double write. Only a
		// permanently-failing record exhausts the retry budget into the DLQ.
		return fmt.Errorf("events: record %s lineage: %w", le.Kind, err)
	}
	return nil
}

// tsToTime converts a proto timestamp to time.Time, tolerating nil (a producer
// that omitted the field) by returning the zero time rather than panicking on a
// nil pointer. The lineage row stores the zero time as "unknown event time".
func tsToTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}
