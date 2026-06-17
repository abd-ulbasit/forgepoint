// Package events is the inference-gateway's NATS adapter layer — the OUTER ring
// that bridges the pure domain (internal/domain) to the async event bus. It
// implements the domain's EventPublisher port (producing the canonical inference
// events) and hosts the SUBSCRIBERS that turn consumed platform events into
// domain method calls (the routing table + quota cache are pure reactors on the
// control bus).
//
// ============================================================================
// WHERE THIS SITS IN CLEAN ARCHITECTURE
// ============================================================================
//
//	internal/domain     defines EventPublisher + the Apply* methods       (pure)
//	     ▲                                                                  │ calls
//	     │ implements / calls                                              ▼
//	internal/events     Publisher (impl) · subscribers (dispatch)   ← THIS PACKAGE
//	     │ uses
//	     ▼
//	pkg/natsutil        envelopes · idempotency · DLQ · trace propagation
//	gen/go/.../events   the canonical wire payloads (events.v1.*)
//
// This package is the ONLY place in the gateway where the generated events.v1
// proto types meet the domain types — the async equivalent of the handler's
// proto↔domain anti-corruption layer. The domain never imports gen/go or NATS;
// this adapter owns both and maps between them at the boundary.
//
// ============================================================================
// THE GATEWAY'S EVENT ROLE (grounded in docs/design/event-contract.md)
// ============================================================================
//
//	PRODUCES:
//	  fp.inference.completed   (InferenceCompleted) — every successful predict
//	  fp.inference.failed      (InferenceFailed)    — every failed predict
//
//	CONSUMES (updates the in-memory route table):
//	  fp.pipelines.model.deployed    (ModelDeployed)   → ApplyModelDeployed
//	  fp.pipelines.model.undeployed  (ModelUndeployed) → ApplyModelUndeployed
//	  fp.models.promoted             (ModelPromoted)   → ApplyModelPromoted
//	  fp.models.archived             (ModelArchived)   → ApplyModelArchived
//
//	CONSUMES (flips the per-team quota cache):
//	  fp.billing.quota.exceeded      (QuotaExceeded)   → QuotaCacheWriter.Block
//
// NOTE ON SUBJECT OWNERSHIP: a subject's domain is the RESOURCE, not the
// producer. The deploy/undeploy events live under fp.pipelines.* (owned by the
// orchestrator saga) even though they mutate OUR route table; promoted/archived
// live under fp.models.* (owned by the registry). EventEnvelope.source records
// the actual producer. We subscribe by subject regardless of who produced it.
package events

// ----------------------------------------------------------------------------
// CANONICAL SUBJECTS — the single source of truth for this service's wire names.
// ----------------------------------------------------------------------------
//
// These mirror the authoritative registry in proto/forgepoint/events/v1/events.proto
// and docs/design/event-contract.md EXACTLY. Defining them as constants (rather
// than scattering string literals across the publisher/subscriber/main wiring)
// means a subject typo is a compile-time-greppable single edit, and the tests
// assert against these same constants so a contract drift fails a test, not prod.
const (
	// Produced by the gateway.
	SubjectInferenceCompleted = "fp.inference.completed"
	SubjectInferenceFailed    = "fp.inference.failed"

	// Consumed by the gateway to drive the route table.
	SubjectModelDeployed   = "fp.pipelines.model.deployed"
	SubjectModelUndeployed = "fp.pipelines.model.undeployed"
	SubjectModelPromoted   = "fp.models.promoted"
	SubjectModelArchived   = "fp.models.archived"

	// Consumed by the gateway to flip the per-team quota cache.
	SubjectQuotaExceeded = "fp.billing.quota.exceeded"
)

// Source is the value stamped into EventEnvelope.source for every event this
// service PUBLISHES. The subject says WHICH RESOURCE; source says WHO produced
// it (see the ownership note above). Consumers and audit trails key off this.
const Source = "inference-gateway"

// ----------------------------------------------------------------------------
// JETSTREAM STREAMS this service interacts with.
// ----------------------------------------------------------------------------
//
// In JetStream a SUBJECT is bound to a STREAM (the durable log). A subscriber
// creates a consumer ON a stream filtered to a subject, so the stream must exist
// before Subscribe is called. In production these streams are provisioned once at
// platform bootstrap (a stream-bootstrap job / the infra Helm chart), NOT by each
// service — multiple services share the INFERENCE/MODELS/PIPELINES/BILLING
// streams, so letting any one service own their config invites conflicting
// definitions. We name them here as constants so main.go (next stage) and the
// tests provision exactly these.
//
// One stream per top-level domain (the convention the platform's design doc
// uses): all fp.models.> live in MODELS, all fp.pipelines.> in PIPELINES, etc.
// A single consumer can only filter WITHIN one stream, which is why the gateway
// needs three subscriber subscriptions across three streams (PIPELINES, MODELS,
// BILLING) plus it publishes into INFERENCE.
const (
	StreamInference = "INFERENCE" // owns fp.inference.>
	StreamPipelines = "PIPELINES" // owns fp.pipelines.>
	StreamModels    = "MODELS"    // owns fp.models.>
	StreamBilling   = "BILLING"   // owns fp.billing.>
)
