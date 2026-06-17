package events

// ============================================================================
// SUBJECTS, STREAMS & SOURCE (the canonical strings — single source of truth)
// ============================================================================
//
// These are the exact subject/stream/source strings from the canonical event
// contract (docs/design/event-contract.md). They are constants — not derived —
// so a typo is a compile-stable, grep-able fact and the tests pin the precise
// strings producers/consumers agree on. Every value here matches the same
// constant in the OWNING service's events package (e.g. SubjectModelPromoted ==
// inference-gateway/registry's "fp.models.promoted"); they are duplicated rather
// than shared because each service module is independent (no shared "subjects"
// package on the platform), and the contract doc — not a Go import — is the
// reconciling source of truth.

const (
	// SourceName is stamped into EventEnvelope.Source on events THIS service
	// produces, so consumers/operators know the PRODUCER. Per the contract, the
	// subject names the RESOURCE domain (fp.models.* for drift) while Source names
	// the producer — here they DIVERGE (drift is about a model but produced by the
	// monitor), which is exactly the ownership-by-resource rule.
	SourceName = "model-monitor"

	// ---- PRODUCED -----------------------------------------------------------

	// SubjectModelDriftDetected is the one subject this service publishes to. It
	// lives under fp.models.* (the drift is ABOUT a model) though the monitor
	// produces it (event-contract.md, OWNERSHIP NOTE + conflict #5).
	SubjectModelDriftDetected = "fp.models.drift.detected"

	// ---- CONSUMED -----------------------------------------------------------

	// Inference stream (producer: inference-gateway). The fat InferenceCompleted
	// feeds data/prediction/performance windows; InferenceFailed feeds the
	// error-rate signal (contract: fp.inference.failed → model-monitor).
	SubjectInferenceCompleted = "fp.inference.completed"
	SubjectInferenceFailed    = "fp.inference.failed"

	// Model stream (producer: registry). ModelPromoted re-pins the drift baseline
	// to the newly promoted PRODUCTION version (without it, every new deploy reads
	// as drift vs a stale baseline — see ResetBaselineFromPromotion).
	SubjectModelPromoted = "fp.models.promoted"

	// Feature stream (producer: feature-store). FeaturesWritten scopes drift
	// checks / cache invalidation to the changed entities (thin event).
	SubjectFeaturesWritten = "fp.features.written"
)

// ============================================================================
// STREAMS
// ============================================================================
//
// In JetStream a CONSUMER attaches to a STREAM (the durable log), and a stream
// captures a SUBJECT TREE. The four subjects we touch live in four top-level
// trees, hence four streams. We name them to match every other service's naming
// of the same trees (MODELS, INFERENCE, FEATURES) so a single cluster has ONE
// stream per tree regardless of which service provisions it first
// (CreateOrUpdateStream is convergent/idempotent — see streams.go).

const (
	// StreamModels owns fp.models.> — both the drift events we PRODUCE and the
	// ModelPromoted events we CONSUME live here (same tree, one stream).
	StreamModels = "MODELS"
	// StreamInference owns fp.inference.> — the inference completed/failed stream.
	StreamInference = "INFERENCE"
	// StreamFeatures owns fp.features.> — the feature-write stream.
	StreamFeatures = "FEATURES"

	// The capture filters: a stream stores EVERYTHING under its tree even though
	// our consumers filter to specific subjects. This means an event published
	// before a monitor consumer exists is still persisted and replayable.
	subjectModelsAll    = "fp.models.>"
	subjectInferenceAll = "fp.inference.>"
	subjectFeaturesAll  = "fp.features.>"
)

// consumerGroupPrefix is the durable-consumer name PREFIX for this service. The
// full durable name is "<prefix>-<subject>" (one per consumed subject, built in
// Start). WHY per-subject durables: a JetStream durable consumer binds to exactly
// ONE filter subject, so a single shared durable cannot span the subjects we
// consume; and per-subject durables let each event type's retry/DLQ budget be
// reasoned about independently. All REPLICAS share the prefix, so for a given
// subject they form one consumer group and JetStream load-balances that subject's
// events across them (Kafka-consumer-group semantics) — each event handled by
// exactly one replica; idempotency makes a crash-redelivery to a DIFFERENT
// replica safe too.
const consumerGroupPrefix = "model-monitor"
