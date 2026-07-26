# ADR 0004: Decoupled event schema (`forgepoint.events.v1`)

**Status:** Accepted
**Date:** 2026-06-17
**Deciders:** Abdul Basit Sajid
**Context phase:** async backbone / per-service event wiring (M2)

## Context

The platform's async nervous system is NATS JetStream: every cross-service fact travels
as an event wrapped in `common.v1.EventEnvelope` (`id`, `type`, `source`, `timestamp`,
`correlation_id`, and a `google.protobuf.Any data`). The question this ADR settles is
**where the event *payload* messages live and what they are allowed to depend on**.

The 9 service protos were authored in parallel, and each defined its **own inline event
messages** inside its service proto. An adversarial review found the async layer was
incoherent:

- **Two producers for one fact** — `inference.InferenceCompletedEvent`
  (`fp.inference.completed`) *and* `serving.PredictionCompletedEvent`
  (`fp.serving.prediction_completed`) both claimed "an inference happened, bill it".
- **Missing payloads** — the gateway and serving *consume* model-deploy / model-undeploy
  events, but **no proto ever defined them** (only code comments referenced the subjects).
- **Name drift** — the feature-write event was `FeaturesWritten` on the producer and
  `FeaturesIngested` on the consumer; the subscription would never have matched.

Beyond the conflicts, there was a deeper structural question: a consumer needs *some* of a
producer's data to react. Should the event **embed the producer's domain/API message**, or
carry a **self-contained flat copy** of the fields consumers need? The first is tempting
(no duplication), but it makes the consumer compile-depend on the producer's proto just to
read an event — across 9 services that quietly fuses "independent microservices" into one
proto blob.

We need one event contract, owned in one place, with a clear dependency rule.

## Options Considered

### Option A — Producer-owns-payload (events embed domain/API protos)

Each service keeps its event messages in its own proto; events embed the producer's domain
types (e.g. `FeaturesWritten { featurestore.FeatureView view = 1; }`).

- **Pro:** zero field duplication; the event always matches the producer's current model.
- **Con:** **import cycles / hard coupling.** A consumer (Experiment Tracker) must
  compile-depend on the Feature Store's API proto to read one event; multiply across 9
  services and every consumer transitively depends on every producer — the dependency graph
  is a mesh, not a star.
- **Con:** the event has **no independent lifecycle** — renaming an internal RPC field
  silently rewrites the wire contract every subscriber decodes. `buf breaking` on the
  service proto now polices the event bus too, conflating two contracts.
- **Con:** invites the original incoherence (each service names its own event → drift,
  duplicate producers).

### Option B — Centralized, self-contained event schema (chosen)

One package, `forgepoint.events.v1` (`proto/forgepoint/events/v1/events.proto`,
~29 payload messages), that imports **only** `forgepoint/common/v1` and well-known types —
**never any service's API proto**. Every payload carries the IDs plus the specific fields
its consumers need as **flat scalars and enums**. Enums that mirror a service enum
(`ModelStage`, `MeterType`, `DriftSeverity`) are **re-declared** here; each service maps its
domain enum ↔ the event enum at the publish/consume boundary.

- **Pro:** **no import cycles** — every consumer depends only on `events` + `common`. The
  dependency graph is a star around one shared contract, not a mesh.
- **Pro:** **one source of truth** — a single registry of subjects, payloads, producers, and
  consumers (`docs/design/event-contract.md`). Duplicate producers and name drift become
  impossible because there is exactly one definition of each event.
- **Pro:** **independent evolution** — the event bus is a *published contract* with its own
  `buf breaking` guarantee, decoupled from any service's RPC types. This is precisely what a
  schema registry (Confluent/Avro, AWS Glue) provides: events are a first-class versioned
  artifact, separate from application object models.
- **Pro:** **self-describing** — a consumer acts on an event without a synchronous callback
  into the producer (which would re-couple services, add hot-path latency, and risk reading
  state the producer's own read-projection hasn't caught up to).
- **Con:** **field duplication** — a field copied from producer state can go stale (mitigated:
  events are immutable facts about a *past moment*, so "stale" is the wrong frame — the value
  was true when emitted).
- **Con:** the boundary needs an explicit **enum-mapping step** in each handler.

### Option C — Raw JSON payloads (schemaless)

Publish `map`-shaped JSON in `EventEnvelope.data`; no proto for payloads.

- **Pro:** no codegen; trivially flexible.
- **Con:** **no contract** — typos and shape drift are runtime failures in a *different
  service*, the hardest class to debug. No `buf breaking`, no compile-time consumer safety,
  no generated types. This is exactly the incoherence we are escaping, with the compiler
  switched off.

## Decision

Adopt **Option B**: event payloads are a **self-contained schema package**
(`forgepoint.events.v1`), **decoupled from API/domain proto types**. Producers marshal one of
these messages into `EventEnvelope.data`; consumers unmarshal it by its `Any` type URL. One
contract, both ends of every pipe.

**Subject convention:** `fp.<domain>.<event...>` — **plural** resource domain
(`fp.models.registered`, not `fp.model.*`), **past-tense** event name. Plural matches the
authoritative platform-design NATS hierarchy (`fp.models.>`, `fp.pipelines.>`) and reads as a
stream-of-a-set (the Kafka-topic / EventBridge convention). The lone exception is
`fp.inference.*` — an *activity* stream, not a countable resource.

**Ownership by resource, not by service.** The subject's domain names the **resource the
event is about**; the **producer** may be a different service, recorded in
`EventEnvelope.source`. So a model's own lifecycle facts (`registered`, `version.*`,
`promoted`, `archived`, `drift.detected`) live under `fp.models.*` — even though
`drift.detected` is produced by **Model Monitor** (`source = "model-monitor"`), not the
Registry — while the *deployment workflow's* outcomes live under `fp.pipelines.model.*`,
produced by the Pipeline Orchestrator saga. This is what resolves the prior `fp.models.*` vs
`fp.pipelines.*` split: the resource owns the subject; `source` records the producer.

**Fat-vs-thin is a per-event choice.** *Fat-but-flat* events (`InferenceCompleted`,
`ModelDriftDetected`, `UsageRecorded`) carry enough denormalized data that the consumer needs
no callback — chosen where a reaction must be immediate and self-contained. *Thin* events
(`FeaturesWritten` carries entity ids + a version range; a consumer that wants values calls
`GetOnlineFeatures`) are "something changed, pull if you care" — chosen where the full object
is large or rarely needed.

The deciding factor: only Option B keeps "decoupled microservices" *true* at the dependency
level (a star, not a mesh) while giving the event bus its own versioned, lintable contract.
Option A trades that for avoiding duplication we are happy to pay; Option C throws away the
compiler.

## Consequences

- **Positive:** no proto import cycles; one registry of every subject/payload/producer/
  consumer; events evolve independently of RPC types (own `buf breaking` line); consumers
  decode by `Any` type URL with generated types and no producer callback.
- **Negative:** flat payloads duplicate some producer fields (can go stale — accepted, since
  an event is an immutable past fact); each handler maps its domain enum ↔ the event enum at
  the boundary.
- **Idempotency:** `EventEnvelope.id` is the **transport** dedupe key (JetStream is
  at-least-once); each payload also carries a **business** idempotency handle
  (`InferenceCompleted.request_id`, `ModelDriftDetected.report_id`,
  `UsageRecorded.source_request_id`) so consumers dedupe at the domain level too. At-least-once
  delivery + idempotent consumers = **exactly-once in effect**.
- **Follow-ups:** the domain layer must NOT import the generated `eventsv1.*` types — the
  event **adapter** (`internal/events/`) maps the service's neutral domain event to the wire
  payload (see the registry's `ProjectionEvent` seam in `domain/ports.go`), preserving Clean
  Architecture. `buf` was deliberately not run and nothing committed as part of defining this
  contract.
