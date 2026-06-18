// Package events is the NATS adapter ring for the Model Monitor service — the
// thin translation layer between the async event bus (pkg/natsutil over NATS
// JetStream) and the monitor DOMAIN (internal/domain.MonitorService + the
// domain.DriftPublisher port).
//
// ============================================================================
// THIS SERVICE'S EVENT ROLE (grounded in docs/design/event-contract.md)
// ============================================================================
//
//	PRODUCES:  ModelDriftDetected → fp.models.drift.detected
//	CONSUMES:  InferenceCompleted → fp.inference.completed   (fold → data/pred/perf windows)
//	           InferenceFailed    → fp.inference.failed      (fold → error-rate signal)
//	           ModelPromoted      → fp.models.promoted       (re-baseline to new prod version)
//	           FeaturesWritten    → fp.features.written       (scope drift checks to changed entities)
//
// Model Monitor is the LOOP-CLOSER on the platform: it watches the live inference
// stream, scores windows against the training-time baseline, and on a CRITICAL
// breach emits ModelDriftDetected (consumed by notification, experiment-tracker,
// and — the loop closer — pipeline-orchestrator, which runs the retrain saga).
// (event-contract.md, conflict #5; the per-service migration note for
// "model-monitor": publish ModelDriftDetected; consume InferenceCompleted,
// FeaturesWritten, ModelPromoted.) We ALSO consume InferenceFailed: a spiking
// failure rate is its own degradation signal (contract: fp.inference.failed →
// model-monitor error-rate).
//
// ============================================================================
// THE TWO HALVES OF THIS ADAPTER (publisher + subscriber)
// ============================================================================
//
//	PUBLISHER (publisher.go): implements the domain's DriftPublisher PORT. The
//	  domain decides WHAT/WHEN to publish (it hands over a transport-neutral
//	  domain.DriftEvent in applyPolicy); the adapter owns the envelope, the
//	  canonical subject (fp.models.drift.detected), the events.v1 payload mapping,
//	  the enum conversions, and trace/correlation propagation. source="model-monitor"
//	  (the PRODUCER) even though the subject's domain is "models" (the RESOURCE) —
//	  the contract's ownership-by-resource rule (a drift fact is ABOUT a model but
//	  PRODUCED by the monitor; EventEnvelope.source records the producer).
//
//	SUBSCRIBERS (subscriber.go): one durable consumer per consumed subject. Each
//	  decodes the events.v1 payload, RESOLVES the model → its monitor binding
//	  (id + owner_team) via a MonitorResolver port (the authorized-binding
//	  boundary — see resolver.go), and dispatches to the right domain method:
//	    InferenceCompleted/Failed → MonitorService.ObserveInference (fold a window)
//	    ModelPromoted             → MonitorService.ResetBaselineFromPromotion (re-pin)
//	    FeaturesWritten           → a scoping log (thin event; no domain state change)
//
// ============================================================================
// THE AUTHORIZED-BINDING BOUNDARY (why the adapter resolves tenancy, not the domain)
// ============================================================================
//
// An InferenceCompleted / ModelPromoted event carries NO tenancy and NO monitor
// reference (events are decoupled from the monitor's data model — see the
// InferenceObservation doc in window.go). The domain must NOT fabricate a tenant
// from untrusted event data (a cross-tenant footgun): it receives an ALREADY
// AUTHORIZED binding. So the adapter resolves model → (monitor id, owner_team)
// BEFORE handing an observation to the domain, via the MonitorResolver port. A
// model with no monitor resolves to "no binding" and the event is ACKed and
// dropped (folding an unmonitored model's traffic is meaningless and would leak
// memory; the domain's ObserveInference also short-circuits on an empty MonitorID
// as defense-in-depth).
//
// ============================================================================
// IDEMPOTENCY + DLQ (the at-least-once contract, both layers)
// ============================================================================
//
// NATS JetStream is at-LEAST-once: a message can be redelivered (ACK lost,
// consumer crash). Every consumer here is safe under duplicate redelivery on TWO
// layers:
//
//  1. TRANSPORT dedupe — a natsutil ProcessedStore keyed on EventEnvelope.id
//     (WithIdempotencyStore) recognizes a duplicate envelope BEFORE the handler
//     runs and ACKs it without re-running the side effect.
//
//  2. BUSINESS dedupe — the domain is independently idempotent: Window.Add dedupes
//     on request_id WITHIN a window, DriftReport persistence is idempotent on
//     window_id (one report → one emitted event), and ResetBaselineFromPromotion
//     re-pinning to the same version is a harmless re-resolve. So even a
//     redelivery that slips past layer 1 (e.g. a different replica with a separate
//     store) produces the SAME effect.
//
//     at-least-once delivery + idempotent reaction + envelope-id dedupe
//     = exactly-once IN EFFECT.
//
// DLQ (WithMaxRetries + WithDLQSubject): a poison message — one whose handler
// keeps failing (a structurally-corrupt payload, or a transient dependency that
// never recovers) — is routed to a dead-letter subject after the retry budget
// instead of looping forever (the natsutil subscriber owns this machinery; the
// handlers only choose ACK-vs-NAK by returning nil vs an error).
//
// ============================================================================
// WIRE-FORMAT NOTE — THE PLATFORM IS UNIFORM ON PROTOJSON; THE DECODER IS
// DUAL-FORMAT ONLY AS DEFENSIVE COMPAT TOLERANCE
// ============================================================================
//
// The platform is now UNIFORM on canonical proto-JSON for proto events. The
// natsutil.Publisher has a single, consistent serialization dialect: it
// protojson.Marshal's anything that is a proto.Message, and reserves
// encoding/json for plain (non-proto) Go structs. So every producer that hands a
// generated events.v1 message to Publish emits the SAME canonical shape on the
// wire — camelCase names, RFC-3339 timestamp STRINGS, correct Struct/Any
// well-known encodings.
//
// Concretely, every event Model Monitor consumes is canonical proto-JSON today:
//
//   - InferenceCompleted (from inference-gateway) — raw *eventsv1.InferenceCompleted
//     to Publish → protojson.
//   - ModelPromoted (from registry) — raw *eventsv1.ModelPromoted → protojson.
//   - FeaturesWritten (from feature-store) — protojson.
//
// Before the natsutil dialect fix, registry & inference-gateway handed the raw
// proto to a Publisher that json.Marshal'd EVERYTHING, so they used to emit the
// encoding/json struct shape (snake_case tags, a Timestamp as {seconds,nanos})
// while feature-store et al. emitted protojson — producers genuinely disagreed.
// That divergence is GONE; describing it as current behavior would now be a lie.
//
// So why keep a dual-format decoder (codec.go) instead of protojson-only? Not to
// describe today's producers — protojson-first ALWAYS succeeds for them — but as
// DEFENSIVE forward/backward-compat tolerance:
//
//   - a rolling deploy where an OLDER producer build (pre-dialect-fix, still
//     json.Marshal-ing raw proto) is briefly live alongside the new one, and
//   - a hypothetical future producer that publishes a PLAIN Go struct (encoding/json
//     by design) for some payload this consumer must still read.
//
// The encoding/json branch is therefore a cheap safety net, not the expected path.
// protojson-first is the path that fires for every real producer today; the
// fallback costs one extra unmarshal attempt ONLY when protojson fails — negligible
// against the broker round-trip, and worth it for graceful behavior across a mixed
// fleet during a deploy. (Interview framing: "the platform converged on protojson
// — why didn't you delete the fallback?" → it's compat tolerance for rolling
// deploys / a future plain-JSON producer, not a claim that producers still
// disagree; the canonical path is what actually runs.)
//
// ============================================================================
// CLEAN ARCHITECTURE PLACEMENT
// ============================================================================
//
//	cmd/server/main.go              (next stage: wires NATS conn + these adapters)
//	   ↓
//	internal/events  (THIS PACKAGE) ─ depends on → domain.MonitorService (driving port)
//	   ↓ uses                                       domain.DriftPublisher (driven port)
//	pkg/natsutil (Publisher/Subscriber: envelopes,  eventsv1 (canonical wire payloads)
//	              idempotency, DLQ, trace propagation)
//
// The adapter contains NO business logic: the drift math, the windowing, the
// closed-loop policy all live in the domain (unit-tested there). This ring only
// translates wire ↔ domain and owns the delivery mechanics (dedupe, DLQ, ACK/NAK,
// the authorized-binding resolution).
package events
