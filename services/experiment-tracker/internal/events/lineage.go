package events

import (
	"context"
	"time"
)

// ============================================================================
// LINEAGE — the experiment-tracker's role as the platform's WIDE event sink
// ============================================================================
//
// Experiment Tracker is the event-contract's broadest CONSUMER. Beyond its own
// run lifecycle it subscribes to a wide set of cross-service lifecycle facts —
// a model registered, a version created, a model promoted/deployed, an inference
// completed, a pipeline step completed, features written / a view defined, usage
// recorded, a notification delivered/failed. Each is recorded as a LINEAGE event:
// a timeline of "what happened to / around the models and runs this service
// tracks", so the UI can show a model's full provenance and a run's surrounding
// platform activity without calling back into nine other services.
//
// WHY a SEPARATE adapter-owned port (not a domain.ExperimentService method):
//
//   domain/ports.go is explicit that the INBOUND consume path is an ADAPTER, not
//   a domain port — "the domain doesn't model a subscription; it models the
//   business operations a subscription ultimately invokes." The run-ingestion
//   events (e.g. a future ModelVersionCreated → open a run) call the EXISTING
//   ExperimentService methods. But the wide lineage sink writes append-only
//   provenance rows that the current ExperimentService surface doesn't express.
//   Rather than bloat the domain interface, the events adapter owns this small
//   port (Hexagonal "the consumer defines the interface it needs"); the Postgres
//   lineage table implements it in a later phase, and tests inject a fake. This
//   keeps the domain interface focused on runs/experiments and keeps the sink's
//   storage concern at the adapter edge where it belongs.
//
// IDEMPOTENCY: every LineageEvent carries the originating EventEnvelope.id as its
// EventID. The subscriber already dedupes via natsutil's ProcessedStore, but the
// EventID is ALSO the natural business dedupe key for the lineage table's UNIQUE
// index — the transactional half of exactly-once (write the row + mark processed
// in one tx) the Postgres adapter will use. Carrying it here means the contract
// for "record this once" is visible at the port, not buried in the SQL.

// LineageEventKind classifies a recorded lineage row. It mirrors the consumed
// event's identity as a stable, queryable string (NOT the NATS subject, which is
// a transport detail that could be reorganized). The UI filters a model's
// timeline on these ("show only deploys", "show inferences").
type LineageEventKind string

const (
	LineageModelRegistered       LineageEventKind = "MODEL_REGISTERED"
	LineageModelVersionCreated   LineageEventKind = "MODEL_VERSION_CREATED"
	LineageModelPromoted         LineageEventKind = "MODEL_PROMOTED"
	LineageModelDeployed         LineageEventKind = "MODEL_DEPLOYED"
	LineageInferenceCompleted    LineageEventKind = "INFERENCE_COMPLETED"
	LineagePipelineStepDone      LineageEventKind = "PIPELINE_STEP_COMPLETED"
	LineageFeaturesWritten       LineageEventKind = "FEATURES_WRITTEN"
	LineageFeatureViewDefined    LineageEventKind = "FEATURE_VIEW_DEFINED"
	LineageUsageRecorded         LineageEventKind = "USAGE_RECORDED"
	LineageNotificationDelivered LineageEventKind = "NOTIFICATION_DELIVERED"
	LineageNotificationFailed    LineageEventKind = "NOTIFICATION_FAILED"

	// Contract-mandated lineage kinds (event-contract.md subject registry: the
	// drift + pipeline-boundary facts this service records for cross-service
	// provenance). ModelDriftDetected is produced by model-monitor (the loop
	// closer is pipeline-orchestrator, not us — we record drift for the model's
	// HISTORY); PipelineStarted/Completed bound a run's lifetime on the timeline.
	LineageModelDriftDetected LineageEventKind = "MODEL_DRIFT_DETECTED"
	LineagePipelineStarted    LineageEventKind = "PIPELINE_STARTED"
	LineagePipelineCompleted  LineageEventKind = "PIPELINE_COMPLETED"
)

// LineageEvent is the normalized, storage-shaped record the subscriber derives
// from a decoded platform event. It is deliberately FLAT and small: the few
// correlation handles a provenance timeline needs (which model, which version,
// which run/execution, a human summary, when), plus the source event id for
// dedupe. It does NOT store the full event payload — lineage is an index/timeline,
// not a second copy of the bus (the same "events are not a source of truth"
// discipline the contract applies to thin events).
type LineageEvent struct {
	// EventID is the originating EventEnvelope.id — the idempotency key. Two
	// deliveries of the same envelope produce the same EventID, so the lineage
	// table's UNIQUE(event_id) index makes the insert a no-op on a duplicate.
	EventID string

	// Kind is the classified event type (queryable timeline filter).
	Kind LineageEventKind

	// Source is the producing service (EventEnvelope.source) — useful when the
	// subject domain differs from the producer (e.g. drift is fp.models.* but
	// produced by model-monitor). Audit/debugging.
	Source string

	// Correlation handles, any of which may be empty depending on the event.
	// These are the JOIN keys a "model X timeline" or "run Y context" query uses.
	ModelID      string
	ModelVersion string // human version label or version id, per the source event
	RunID        string // set when the event maps to a tracked run
	ExecutionID  string // pipeline execution id, for pipeline-originated events
	RequestID    string // inference request id (the ground-truth join key)

	// Summary is a short, PII-free human description for the timeline UI
	// ("promoted iris v3 to PRODUCTION", "inference 12ms on iris v3"). Derived
	// from the event's headline scalars, never raw inputs/outputs.
	Summary string

	// OccurredAt is the PRODUCER clock from the event payload (event time), not
	// the consume time — so the timeline reflects when things actually happened.
	OccurredAt time.Time
}

// LineageRecorder is the adapter-owned persistence port for lineage rows. The
// Postgres adapter implements it (INSERT ... ON CONFLICT (event_id) DO NOTHING —
// the idempotent, append-only write); events tests inject a fake that records in
// memory so the dispatch logic is verifiable without a database.
//
// Record MUST be idempotent on LineageEvent.EventID: a redelivered envelope (or a
// retried Record after a crash between the DB write and the ProcessedStore mark)
// must not create a second row. The ON CONFLICT DO NOTHING guarantee lives in the
// SQL; this port documents the requirement so any implementation honors it.
type LineageRecorder interface {
	Record(ctx context.Context, ev LineageEvent) error
}
