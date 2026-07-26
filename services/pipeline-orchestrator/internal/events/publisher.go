package events

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/domain"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ============================================================================
// PUBLISHER — the outbound adapter implementing domain.EventPublisher
// ============================================================================
//
// The saga engine emits framework-free domain.StepEvent values; this adapter
// turns each into (subject, forgepoint.events.v1 payload), encodes the payload
// as canonical proto-JSON, and publishes it through pkg/natsutil.Publisher (which
// wraps it in the EventEnvelope, sets the dedup Msg-Id, and propagates the
// correlation id + W3C trace context across the async hop).
//
// BEST-EFFORT, NON-BLOCKING (the EventPublisher port's contract): a NATS hiccup
// MUST NOT fail a successful deployment. Publish therefore LOGS a publish failure
// and returns it for the adapter's own bookkeeping, but the engine's emit()
// ignores the return value — the saga's outcome is decided by durable Postgres
// writes, not by whether a lifecycle event reached the bus. (Guaranteed delivery,
// where it matters, is the Outbox pattern's job — Billing — not this feed.)
//
// Compile-time proof this satisfies the port: if a signature drifts, the build
// breaks HERE, at the adapter, not at some far-away wiring site.
var _ domain.EventPublisher = (*Publisher)(nil)

// Publisher adapts pkg/natsutil.Publisher to the domain's EventPublisher port.
type Publisher struct {
	pub    *natsutil.Publisher
	logger *slog.Logger
}

// NewPublisher builds the adapter over a natsutil.Publisher already configured
// with source = Source ("pipeline-orchestrator"). main.go constructs the
// natsutil.Publisher (it owns the JetStream handle) and hands it in — this
// package never touches the connection, keeping the adapter testable with any
// natsutil.Publisher pointed at a test NATS.
func NewPublisher(pub *natsutil.Publisher, logger *slog.Logger) *Publisher {
	if logger == nil {
		logger = slog.Default()
	}
	return &Publisher{pub: pub, logger: logger}
}

// Publish maps one domain.StepEvent to its canonical subject + events.v1 payload
// and publishes it. It is the single translation point between the engine's
// internal event vocabulary (EventType) and the wire contract.
//
// THE MAPPING IS DELIBERATELY THIN (events doc: "thin-event / fat-read"): the
// lifecycle events carry IDs + the few fields a consumer needs to route/template;
// a consumer wanting the full step timeline calls GetExecution. So fields the
// domain StepEvent does not carry (e.g. PipelineCompleted.duration,
// PipelineFailed.failed_step_id, CompensationTriggered.compensating_step_ids)
// are left unset rather than fabricated — they are recoverable via the read API.
// The model-deploy events are the documented exception: they carry the resolved
// endpoint + the model identity (sourced from the step Output) so the gateway can
// act with no callback.
func (p *Publisher) Publish(ctx context.Context, ev domain.StepEvent) error {
	subject, payload, err := p.buildEvent(ev)
	if err != nil {
		// An UNKNOWN event type is a wiring bug, not a transport blip — log it and
		// return so it surfaces, but (per the best-effort contract) the engine still
		// won't abort the saga on it.
		p.logger.ErrorContext(ctx, "skip publishing unmappable lifecycle event",
			slog.String("error", err.Error()))
		return err
	}

	raw, err := marshalCanonical(payload)
	if err != nil {
		p.logger.ErrorContext(ctx, "failed to marshal lifecycle event payload",
			slog.String("subject", subject), slog.String("error", err.Error()))
		return err
	}

	// natsutil.Publisher.Publish json.Marshals the payload; a json.RawMessage is a
	// passthrough, so the canonical proto-JSON reaches the envelope's data verbatim.
	if err := p.pub.Publish(ctx, subject, raw); err != nil {
		// Logged (not swallowed silently) so a sustained publish failure is visible
		// in Grafana, but returned without escalation — the engine's emit() ignores
		// it. This is the best-effort lifecycle feed, not the durable record.
		p.logger.WarnContext(ctx, "failed to publish lifecycle event (best-effort, saga unaffected)",
			slog.String("subject", subject), slog.String("error", err.Error()))
		return err
	}
	return nil
}

// buildEvent is the pure EventType → (subject, payload) mapping. Split out from
// Publish so it is unit-testable without a NATS connection and so the exhaustive
// switch (every domain.EventType has a case) reads in one place.
func (p *Publisher) buildEvent(ev domain.StepEvent) (string, proto.Message, error) {
	ts := timestamppb.New(ev.OccurredAt)

	switch ev.Type {
	case domain.EventPipelineStarted:
		return SubjectPipelineStarted, &eventsv1.PipelineStarted{
			ExecutionId:  ev.ExecutionID,
			PipelineId:   ev.PipelineID,
			PipelineType: mapPipelineType(ev.PipelineType),
			TriggeredBy:  ev.TriggeredBy,
			StartedAt:    ts,
		}, nil

	case domain.EventStepCompleted:
		return SubjectStepCompleted, &eventsv1.StepCompleted{
			ExecutionId: ev.ExecutionID,
			PipelineId:  ev.PipelineID,
			StepId:      ev.StepID,
			StepType:    mapStepType(ev.StepType),
			Output:      toStruct(ev.Output),
			CompletedAt: ts,
		}, nil

	case domain.EventStepFailed:
		return SubjectStepFailed, &eventsv1.StepFailed{
			ExecutionId: ev.ExecutionID,
			PipelineId:  ev.PipelineID,
			StepId:      ev.StepID,
			StepType:    mapStepType(ev.StepType),
			Error:       ev.Error,
			Attempts:    int32(ev.Attempts),
			FailedAt:    ts,
		}, nil

	case domain.EventCompensationTriggered:
		// failed_step_id / compensating_step_ids are part of the wire contract but
		// not carried on the domain StepEvent (thin-event): a consumer that needs the
		// rollback plan reads GetExecution. We send the ids we DO have.
		return SubjectCompensationTriggered, &eventsv1.CompensationTriggered{
			ExecutionId:  ev.ExecutionID,
			PipelineId:   ev.PipelineID,
			FailedStepId: ev.StepID,
			TriggeredAt:  ts,
		}, nil

	case domain.EventPipelineCompleted:
		return SubjectPipelineCompleted, &eventsv1.PipelineCompleted{
			ExecutionId:  ev.ExecutionID,
			PipelineId:   ev.PipelineID,
			PipelineType: mapPipelineType(ev.PipelineType),
			CompletedAt:  ts,
		}, nil

	case domain.EventPipelineFailed:
		// ROUTING-CRITICAL SIGNAL (Finding 3): compensation_failed distinguishes a
		// CLEAN rollback (the saga undid its side effects — Notification logs, no
		// page) from a STUCK saga (COMPENSATION_FAILED: a side effect could NOT be
		// undone — orphaned serving pods possible — Notification PAGES a human). This
		// is the exact distinction the wire MUST carry; it is not denormalized
		// convenience, so the thin-event "recover it via GetExecution" justification
		// does not apply. The domain settles a stuck saga to FAILED with the cause
		// WRAPPED in domain.ErrCompensationFailed (see compensate()), and that wrapped
		// error reaches us as ev.Error. We detect it via errors-style sentinel
		// matching (compareing against domain.ErrCompensationFailed's message rather
		// than a hard-coded literal, so a reword of the sentinel can't silently break
		// the routing signal). failed_step_id is carried from ev.StepID when the
		// engine populates it for the terminal event.
		return SubjectPipelineFailed, &eventsv1.PipelineFailed{
			ExecutionId:        ev.ExecutionID,
			PipelineId:         ev.PipelineID,
			PipelineType:       mapPipelineType(ev.PipelineType),
			FailedStepId:       ev.StepID,
			Error:              ev.Error,
			CompensationFailed: isCompensationFailed(ev.Error),
			FailedAt:           ts,
		}, nil

	case domain.EventModelDeployed:
		// FAT event: the gateway/serving act on it with NO callback, so it carries
		// the resolved serving endpoint (SSRF-safe — resolved by the executor, never
		// client config) plus the model identity. The identity lives in the step
		// Output the executor returned (model_id/model_name/version_id/version,
		// weight_bps), so we read it from there.
		md := &eventsv1.ModelDeployed{
			ExecutionId: ev.ExecutionID,
			Endpoint:    ev.Endpoint,
			DeployedAt:  ts,
		}
		applyModelIdentity(md, ev.Output)
		md.WeightBps = outputInt32(ev.Output, "weight_bps")
		return SubjectModelDeployed, md, nil

	case domain.EventModelUndeployed:
		mu := &eventsv1.ModelUndeployed{
			ExecutionId:  ev.ExecutionID,
			Reason:       outputString(ev.Output, "reason"),
			UndeployedAt: ts,
		}
		applyModelUndeployIdentity(mu, ev.Output)
		return SubjectModelUndeployed, mu, nil

	default:
		return "", nil, fmt.Errorf("events: unknown lifecycle event type %d", int(ev.Type))
	}
}

// isCompensationFailed reports whether a PipelineFailed event describes a STUCK
// saga (one or more compensations failed → a side effect could not be undone, e.g.
// an orphaned serving pod) rather than a clean rollback.
//
// WHY DETECT IT FROM THE ERROR STRING: the domain settles a stuck saga to FAILED
// with the run cause WRAPPED in domain.ErrCompensationFailed (see the engine's
// compensate()); that wrapped error is sanitized onto the terminal event's Error
// field, so the sentinel's text travels with the event. We compare against
// domain.ErrCompensationFailed.Error() — the sentinel's OWN message — rather than a
// hard-coded literal, so if the wording is ever changed in one place the detection
// follows it and the routing signal can't silently rot. Notification keys its
// page/no-page decision on this flag (a stuck saga PAGES; a clean rollback does
// not), so getting it right is correctness, not cosmetics.
//
// NOTE on the tradeoff: the structurally cleaner source would be a typed
// CompensationFailed bool carried on domain.StepEvent set in settle() from the
// execution's terminal step states — string-sniffing an error is a code smell in
// isolation. We use the sentinel-message match here because it is unambiguous (the
// sentinel is a single exported value) and keeps the fix inside the adapter; the
// typed-field version is the follow-up that makes the domain emit the bool
// directly (and also populate failed_step_id on the terminal event).
func isCompensationFailed(errMsg string) bool {
	if errMsg == "" {
		return false
	}
	return strings.Contains(errMsg, domain.ErrCompensationFailed.Error())
}

// ============================================================================
// ENUM MAPPING — domain enum → events.v1 enum at the boundary
// ============================================================================
//
// The events contract DELIBERATELY re-declares its own enums (it must not import
// any service's API). The integer values are kept aligned with the domain's, so
// the mapping is a trivial, auditable switch. We use an explicit switch rather
// than a raw int cast so a reader (and the compiler, if a value is ever
// added) sees the conversion is intentional, and an unknown value maps to the
// safe UNSPECIFIED zero rather than smuggling a bogus integer onto the wire.

func mapPipelineType(t domain.PipelineType) eventsv1.PipelineType {
	switch t {
	case domain.PipelineTypeDeploymentSaga:
		return eventsv1.PipelineType_PIPELINE_TYPE_DEPLOYMENT_SAGA
	case domain.PipelineTypeTrainingDAG:
		return eventsv1.PipelineType_PIPELINE_TYPE_TRAINING_DAG
	case domain.PipelineTypeBatchInference:
		return eventsv1.PipelineType_PIPELINE_TYPE_BATCH_INFERENCE
	default:
		return eventsv1.PipelineType_PIPELINE_TYPE_UNSPECIFIED
	}
}

func mapStepType(t domain.StepType) eventsv1.StepType {
	switch t {
	case domain.StepTypeValidate:
		return eventsv1.StepType_STEP_TYPE_VALIDATE
	case domain.StepTypeBuild:
		return eventsv1.StepType_STEP_TYPE_BUILD
	case domain.StepTypeDeploy:
		return eventsv1.StepType_STEP_TYPE_DEPLOY
	case domain.StepTypeCanary:
		return eventsv1.StepType_STEP_TYPE_CANARY
	case domain.StepTypePromote:
		return eventsv1.StepType_STEP_TYPE_PROMOTE
	case domain.StepTypeTrain:
		return eventsv1.StepType_STEP_TYPE_TRAIN
	case domain.StepTypeEvaluate:
		return eventsv1.StepType_STEP_TYPE_EVALUATE
	case domain.StepTypeRegister:
		return eventsv1.StepType_STEP_TYPE_REGISTER
	case domain.StepTypeCustom:
		return eventsv1.StepType_STEP_TYPE_CUSTOM
	default:
		return eventsv1.StepType_STEP_TYPE_UNSPECIFIED
	}
}

// ============================================================================
// STEP-OUTPUT EXTRACTION — pulling model identity out of the step's Output
// ============================================================================
//
// The domain StepEvent carries the step's Output (map[string]any) but not typed
// model fields. For DEPLOY/PROMOTE the executor puts the model identity into that
// Output; we read the well-known keys here. Missing/typo'd keys yield zero values
// (the deploy event still publishes — a consumer keys primarily on endpoint +
// execution_id, and the names are denormalized convenience).

func applyModelIdentity(md *eventsv1.ModelDeployed, out map[string]any) {
	md.ModelId = outputString(out, "model_id")
	md.ModelName = outputString(out, "model_name")
	md.VersionId = outputString(out, "version_id")
	md.Version = outputString(out, "version")
}

func applyModelUndeployIdentity(mu *eventsv1.ModelUndeployed, out map[string]any) {
	mu.ModelId = outputString(out, "model_id")
	mu.ModelName = outputString(out, "model_name")
	mu.VersionId = outputString(out, "version_id")
	mu.Version = outputString(out, "version")
}

// outputString reads a string-valued key from a step Output map, tolerating a
// nil map and a non-string value (returns "").
func outputString(out map[string]any, key string) string {
	if out == nil {
		return ""
	}
	if v, ok := out[key].(string); ok {
		return v
	}
	return ""
}

// outputInt32 reads a numeric key from a step Output map. JSON-decoded numbers
// arrive as float64, but a value constructed in-process may be an int — handle
// both so the weight survives whichever path produced the Output.
func outputInt32(out map[string]any, key string) int32 {
	if out == nil {
		return 0
	}
	switch v := out[key].(type) {
	case float64:
		return int32(v)
	case int:
		return int32(v)
	case int32:
		return v
	case int64:
		return int32(v)
	default:
		return 0
	}
}

// toStruct converts a domain step Output (map[string]any) to a google.protobuf
// .Struct for the StepCompleted.output field. A nil/empty map yields nil (the
// field is omitted from the canonical JSON). An unconvertible value (a type
// structpb can't represent) is dropped rather than failing the publish — the
// lifecycle feed is best-effort and the full output is available via GetExecution.
func toStruct(out map[string]any) *structpb.Struct {
	if len(out) == 0 {
		return nil
	}
	s, err := structpb.NewStruct(out)
	if err != nil {
		return nil
	}
	return s
}
