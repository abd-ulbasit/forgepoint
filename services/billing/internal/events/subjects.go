// Package events is the billing service's NATS adapter layer — the OUTER ring
// that bridges the pure domain (internal/domain) and the Postgres outbox table to
// the async event bus. It hosts TWO halves of the billing service's event role:
//
//   - the OUTBOX RELAY (relay.go): the PRODUCER side. Billing never publishes an
//     event directly from its write path; instead RecordUsage / GenerateInvoice
//     commit a row to the `outbox` table in the SAME Postgres tx as the business
//     write (the transactional-outbox pattern, see usage_store.go). This relay is
//     the loop that COMPLETES the pattern: it reads unpublished outbox rows,
//     publishes them to NATS, and stamps published_at. That is the DB→NATS half
//     of exactly-once-in-effect.
//   - the INFERENCE CONSUMER (subscriber.go): the CONSUMER side. Billing meters
//     usage by REACTING to fp.inference.completed events the gateway emits —
//     decoding each into a RecordUsage call (which itself writes the outbox,
//     closing the loop: consume → meter → emit UsageRecorded).
//
// ============================================================================
// WHERE THIS SITS IN CLEAN ARCHITECTURE
// ============================================================================
//
//	internal/domain      BillingService (RecordUsage) + OutboxEvent/payload types (pure)
//	     ▲ calls                                   ▲ builds the rows the relay reads
//	     │                                          │
//	internal/events      InferenceConsumer (dispatch) · OutboxRelay (publish)  ← THIS PACKAGE
//	     │ uses                                      │ reads the outbox table via OutboxReader
//	     ▼                                           ▼
//	pkg/natsutil         envelopes · idempotency · DLQ · trace propagation
//	gen/go/.../events    the canonical wire payloads (events.v1.*)
//
// This package is the ONLY place in billing where the generated events.v1 proto
// types meet the domain — the async equivalent of the handler's proto↔domain
// anti-corruption layer. The domain never imports gen/go or NATS; this adapter
// owns both and maps between them at the boundary.
//
// ============================================================================
// THE BILLING EVENT ROLE (grounded in docs/design/event-contract.md)
// ============================================================================
//
//	PRODUCES (via the OUTBOX relay — never published from the write path directly):
//	  fp.billing.usage.recorded    (UsageRecorded)    — every metered, priced fact
//	  fp.billing.quota.exceeded    (QuotaExceeded)    — when a write crosses quota
//	  fp.billing.invoice.generated (InvoiceGenerated) — when a period invoice finalizes
//
//	CONSUMES (the event contract lists billing as a consumer of BOTH — see
//	docs/design/event-contract.md, the subject registry and the per-service
//	"design" section: "billing — Consume events.InferenceCompleted,
//	events.ModelVersionReady (storage metering)"):
//	  fp.inference.completed        (InferenceCompleted) → RecordUsage INFERENCE_REQUEST
//	                                                       (+ INFERENCE_TOKENS when tokens>0)
//	  fp.models.version.ready       (ModelVersionReady)  → RecordUsage STORAGE_BYTES
//	                                                       (meter the artifact's size_bytes)
//
// WHY a SECOND consumed subject: a billing service has more than one money axis.
// Inference is the per-CALL revenue (request + token fees); storage is the
// per-ARTIFACT revenue — every model version that becomes READY parks bytes in
// object storage that the owning team pays to keep. Without consuming
// fp.models.version.ready, STORAGE_BYTES is never metered from any event and the
// platform silently never bills artifact storage (a revenue gap that no test or
// alert would surface — the meter simply stays empty). The registry server-
// MEASURES size_bytes on the verified upload (never client-asserted), so this is
// the authoritative storage fact to meter on.
//
// NOTE ON SUBJECT OWNERSHIP: a subject's domain is the RESOURCE, not the
// producer. fp.inference.completed is owned by the inference-gateway (it alone
// knows latency, the served version, the billed api_key_id) and
// fp.models.version.ready is owned by the registry (it measures the artifact);
// billing merely SUBSCRIBES to meter both. EventEnvelope.source records the
// actual producer; we subscribe by subject regardless of who produced it.
package events

// ----------------------------------------------------------------------------
// CANONICAL SUBJECTS — the single source of truth for this service's wire names.
// ----------------------------------------------------------------------------
//
// These mirror the authoritative registry in proto/forgepoint/events/v1/events.proto
// and docs/design/event-contract.md EXACTLY, and they equal the domain's
// EventType* consts (domain.EventTypeUsageRecorded, …) which the outbox rows carry
// in their event_type column. Defining them as constants (rather than scattering
// string literals) means a subject typo is a single compile-time-greppable edit,
// and the tests assert against these same constants so a contract drift fails a
// test, not prod.
const (
	// PRODUCED by billing through the outbox relay. These string values are
	// IDENTICAL to the domain.EventType* consts the outbox rows store in event_type
	// — the relay reads event_type straight off the row and publishes to it, so the
	// outbox column IS the subject. We re-state them here as the events-layer's
	// public names (and the tests assert relay output lands on exactly these).
	SubjectUsageRecorded    = "fp.billing.usage.recorded"
	SubjectQuotaExceeded    = "fp.billing.quota.exceeded"
	SubjectInvoiceGenerated = "fp.billing.invoice.generated"

	// CONSUMED by billing to meter usage.
	//
	// fp.inference.completed → the per-call inference fee (request + tokens). Owned
	// by the inference-gateway; billing subscribes to meter it.
	SubjectInferenceCompleted = "fp.inference.completed"
	// fp.models.version.ready → storage metering. Owned by the registry, which
	// stamps the server-measured size_bytes when an artifact upload is verified and
	// the version flips PENDING_UPLOAD → READY. Billing meters STORAGE_BYTES on
	// size_bytes. This is the SECOND axis the event contract requires billing to
	// consume; omitting it leaves STORAGE_BYTES un-metered (silent revenue gap).
	SubjectModelVersionReady = "fp.models.version.ready"

	// fp.ai.completion.served → the THIRD money axis (M7/L2): AI token metering. Owned
	// by the AI Gateway (services/ai-gateway), which emits one event per served LLM
	// completion carrying the gateway-resolved team and the prompt/completion/total
	// token counts. Billing's AIConsumer meters those tokens through INFERENCE_TOKENS.
	// This string is IDENTICAL to the producer's events.SubjectCompletionServed in
	// services/ai-gateway/internal/events/subjects.go — the two must match byte-for-byte
	// (a subject is the wire contract); the AIConsumer test asserts against this const.
	SubjectAICompletionServed = "fp.ai.completion.served"
)

// Source is the value stamped into EventEnvelope.source for every event billing
// PUBLISHES (through the relay's natsutil.Publisher). The subject says WHICH
// RESOURCE; source says WHO produced it. Consumers and audit trails key off this.
const Source = "billing"

// ----------------------------------------------------------------------------
// JETSTREAM STREAMS this service interacts with.
// ----------------------------------------------------------------------------
//
// In JetStream a SUBJECT is bound to a STREAM (the durable log). A subscriber
// creates a consumer ON a stream filtered to a subject, so the stream must exist
// before Subscribe is called; the relay publishes into a stream that must exist
// before Publish. In production these streams are provisioned once at platform
// bootstrap (a stream-bootstrap job / the infra Helm chart), NOT by each service —
// multiple services share the BILLING/INFERENCE streams, so letting any one
// service own their config invites conflicting definitions. We name them here as
// constants so main.go (next stage) and the tests provision exactly these.
//
// One stream per top-level domain (the platform convention): all fp.billing.>
// live in BILLING (where the relay publishes), all fp.inference.> in INFERENCE
// (where the consumer reads). The DLQ subject for billing's consumer lives under
// fp.dlq.> (a shared DLQ stream provisioned at bootstrap).
const (
	StreamBilling   = "BILLING"   // owns fp.billing.>   (the relay publishes here)
	StreamInference = "INFERENCE" // owns fp.inference.> (the inference consumer reads here)
	// StreamModels owns fp.models.> — the registry's model-lifecycle log. Billing's
	// storage-metering consumer reads fp.models.version.ready from it. Named exactly
	// as model-serving / experiment-tracker name it ("MODELS") so all consumers of
	// the model-lifecycle tree bind the SAME shared stream rather than each creating
	// a conflicting one. Provisioned at bootstrap (and reconciled in ensureStreams).
	StreamModels = "MODELS" // owns fp.models.>   (the storage-metering consumer reads here)
	// StreamAI owns fp.ai.> — the AI Gateway's cost/audit log (it OWNS and produces
	// into this stream; see services/ai-gateway/internal/events/subjects.go). Billing's
	// AIConsumer reads fp.ai.completion.served from it. Named exactly as the producer
	// names it ("AI") so the consumer binds the SAME stream the gateway publishes to.
	// In production the gateway's bootstrap provisions it; billing also reconciles it in
	// ensureStreams so a billing-first boot doesn't fail subscribing to a missing stream
	// (graceful degrade — the consumer can attach even if the gateway hasn't booted yet).
	StreamAI = "AI" // owns fp.ai.>   (the AI-token-metering consumer reads here)
)
