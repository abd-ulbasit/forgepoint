# Forgepoint Canonical Event Contract

**Date:** 2026-06-17
**Status:** Approved
**Owner:** Event-Contract Architect
**Source of truth (machine):** `proto/forgepoint/events/v1/events.proto`
**Authoritative design:** `docs/plans/forgepoint-platform-design.md` (Event Flow Map / NATS subject hierarchy)

## Why this document exists

The 9 service protos were authored in parallel. Each defined its own ad-hoc event
payload messages inside its service proto, and an adversarial review found they
**disagreed** — two services both claimed the "an inference happened" event, the
model-deploy lifecycle events that the gateway and serving consume were never
defined anywhere, and the feature-write event had two different names on the
producer and consumer sides. The platform's async nervous system was incoherent.

This document is the human-readable catalog of the **single canonical event
contract**, now defined once in `forgepoint.events.v1`. Every producer marshals
one of these messages into `common.v1.EventEnvelope.data` (a
`google.protobuf.Any`) and publishes it; every consumer unmarshals the same
message by its `Any` type URL. One contract, both ends of every pipe.

---

## Naming convention

### Grammar

```
fp.<domain>.<event...>
```

- `fp.` — platform namespace; every Forgepoint subject starts with it.
- `<domain>` — the **resource** the event is about, **plural** (see below).
- `<event...>` — a dot-segmented, **past-tense** event name. Deeper sub-paths
  group related events (e.g. `fp.models.version.created`).

### Decision: plural domain segment

We use the **plural** resource noun: `fp.models.registered`, not
`fp.model.registered`.

**Justification:**

1. The authoritative platform-design doc's NATS hierarchy already uses plurals
   throughout (`fp.models.>`, `fp.pipelines.>`, `fp.features.>`, `fp.billing.>`,
   `fp.notifications.>`). Plural matches the approved design with zero churn.
2. A subject is a **stream** of all events about a **set** of resources, so the
   plural reads correctly: `fp.models.>` = "every event about models". This is
   the Kafka-topic / AWS-EventBridge convention (topics named for the plural
   resource).
3. Wildcard subscriptions read naturally — Notification subscribes to `fp.>`;
   the Registry projection to `fp.models.>`.

**Consistency rule for the platform:** *countable resources are plural; activity
streams keep their natural noun.* The only non-pluralized domain is
`fp.inference.*` — "the inference stream" is an activity, not a countable
resource.

### Past tense

Events name something that **already happened** (`registered`, `completed`,
`deployed`, `detected`). Never imperative — `fp.models.register` would be a
command, not an event.

### A domain segment is not a service name

The subject's domain is the **resource**; the **producer** may be a different
service. Two important cases:

- **Drift events** live under `fp.models.drift.detected` (they're about a
  *model*) but are produced by **Model Monitor**, not the Registry.
- **Model-deploy lifecycle** lives under `fp.pipelines.model.*` (the act of
  deploying is a *workflow outcome*) and is produced by the **Pipeline
  Orchestrator** saga.

`EventEnvelope.source` records the actual producing service; the subject records
the resource domain. This is how the prior `fp.models.*` vs `fp.pipelines.*`
inconsistency is resolved: **ownership by resource** — a model's own lifecycle
facts (`registered`, `version.*`, `promoted`, `archived`, `drift.detected`) live
under `fp.models.*`, while the deployment workflow's outcomes live under
`fp.pipelines.*`.

---

## Subject registry (authoritative)

| Subject | Payload message | Producer | Consumers | Key fields |
|---|---|---|---|---|
| `fp.models.registered` | `ModelRegistered` | registry | pipeline-orchestrator, experiment-tracker | model_id, model_name, owner_id, team |
| `fp.models.version.created` | `ModelVersionCreated` | registry | experiment-tracker | model_id, version_id, version (PENDING_UPLOAD) |
| `fp.models.version.ready` | `ModelVersionReady` | registry | serving, billing, pipeline-orchestrator | version_id, artifact_path, artifact_digest, size_bytes |
| `fp.models.promoted` | `ModelPromoted` | registry | inference-gateway, serving, model-monitor, billing | version_id, from_stage, to_stage, demoted_version_id |
| `fp.models.archived` | `ModelArchived` | registry | inference-gateway, serving, billing | model_id, model_name, archived_by |
| `fp.models.drift.detected` | `ModelDriftDetected` | model-monitor | notification, experiment-tracker, pipeline-orchestrator | model_name, model_version, drift_type, severity, report_id, auto_retrain, retrain_pipeline_id |
| `fp.pipelines.started` | `PipelineStarted` | pipeline-orchestrator | notification, experiment-tracker | execution_id, pipeline_id, pipeline_type, triggered_by |
| `fp.pipelines.step.completed` | `StepCompleted` | pipeline-orchestrator | experiment-tracker | execution_id, step_id, step_type, output |
| `fp.pipelines.step.failed` | `StepFailed` | pipeline-orchestrator | notification | execution_id, step_id, error, attempts |
| `fp.pipelines.completed` | `PipelineCompleted` | pipeline-orchestrator | experiment-tracker, notification | execution_id, pipeline_type, duration |
| `fp.pipelines.failed` | `PipelineFailed` | pipeline-orchestrator | notification | execution_id, failed_step_id, error, compensation_failed |
| `fp.pipelines.compensation.triggered` | `CompensationTriggered` | pipeline-orchestrator | notification | execution_id, failed_step_id, compensating_step_ids |
| `fp.pipelines.model.deployed` | `ModelDeployed` | pipeline-orchestrator | inference-gateway, serving, experiment-tracker | model_name, version, endpoint, weight_bps, execution_id |
| `fp.pipelines.model.undeployed` | `ModelUndeployed` | pipeline-orchestrator | inference-gateway, serving | model_name, version, reason, execution_id |
| `fp.inference.completed` | `InferenceCompleted` | inference-gateway | billing, experiment-tracker, model-monitor | request_id, model_id, version, api_key_id, latency, token_count, prediction_summary, feature_summary |
| `fp.inference.failed` | `InferenceFailed` | inference-gateway | model-monitor, notification | request_id, model_name, version, reason, error |
| `fp.features.view.defined` | `FeatureViewDefined` | feature-store | experiment-tracker | feature_view_id, schema_version, owner_team |
| `fp.features.written` | `FeaturesWritten` | feature-store | experiment-tracker, model-monitor | feature_view_id, entity_ids, written_count, written_through_version |
| `fp.billing.usage.recorded` | `UsageRecorded` | billing | experiment-tracker | record_id, team, meter_type, quantity, cost_micros, source_request_id |
| `fp.billing.quota.exceeded` | `QuotaExceeded` | billing | notification, inference-gateway | team, meter_type, quota_limit, current_usage |
| `fp.billing.invoice.generated` | `InvoiceGenerated` | billing | notification | invoice_id, invoice_number, team, total_micros |
| `fp.experiments.run.created` | `RunCreated` | experiment-tracker | notification | run_id, experiment_id, model_version_id |
| `fp.experiments.run.finished` | `RunFinished` | experiment-tracker | notification, experiment-tracker | run_id, status, final_metrics |
| `fp.notifications.delivered` | `NotificationDelivered` | notification | experiment-tracker | notification_id, recipient_user_id, channel, event_type |
| `fp.notifications.failed` | `NotificationFailed` | notification | experiment-tracker, on-call escalation | notification_id, channel, attempts, error_message |

---

## Design: event schema decoupled from API schema

The event payloads **do not import any service's domain/API messages**. Every
event carries the IDs + the specific fields its consumers need as **flat scalars
and enums**. Enums that mirror a service's enum (e.g. `ModelStage`, `MeterType`,
`DriftSeverity`) are **re-declared** in the events package; the service handler
maps its own domain enum to/from the event enum at the publish/consume boundary.

**Why:**

1. **No import cycles / no coupling.** If `FeaturesWritten` embedded
   `featurestore.FeatureView`, then the Experiment Tracker would compile-depend on
   the Feature Store's API proto just to read an event. Across 9 services that
   collapses "decoupled microservices" into one proto blob. Flat events keep each
   consumer depending only on `events` + `common`.
2. **Independent evolution.** A service can refactor its RPC types without
   breaking the event bus, and vice-versa. The event is a published contract with
   its own lifecycle and its own `buf breaking` guarantee — exactly what a schema
   registry (Confluent/Avro, AWS Glue) gives you.
3. **Self-describing & stable.** A consumer acts on an event without a callback
   into the producer (which would re-couple the services, add hot-path latency,
   and risk reading state the producer's own read-projection hasn't caught up to).

### The tradeoff: fat-but-flat events vs thin events + callback

- **Fat-but-flat** (e.g. `InferenceCompleted`, `ModelDriftDetected`,
  `UsageRecorded`): carry enough denormalized data that the consumer needs no
  callback. Cost: larger messages; a field duplicated from producer state can go
  stale (acceptable — events are immutable facts about a past moment).
- **Thin event + callback** (e.g. `FeaturesWritten` carries entity ids + a
  version range; a consumer that wants the actual values calls
  `GetOnlineFeatures`): small messages, but the consumer must call back,
  re-coupling it and adding a synchronous read dependency.

We choose per-event based on what consumers need to **act**: fat where a reaction
must be immediate and self-contained, thin where the event is just a "something
changed, pull if you care" notification.

### Idempotency

`EventEnvelope.id` is the transport-level dedupe key (NATS is at-least-once). Each
payload also carries a **business** idempotency handle so consumers dedupe at the
domain level too:

- `InferenceCompleted.request_id` — Billing won't double-meter a request it
  already recorded; Model Monitor uses it as the **join key** for delayed ground
  truth (`SubmitGroundTruth`).
- `ModelDriftDetected.report_id` — the report is itself idempotent on its window
  id; one report → one event.
- `UsageRecorded.source_request_id` — two ledger records must never share one.

At-least-once delivery + idempotent consumers = **exactly-once in effect**.

---

## Conflict resolutions

### 1. One canonical inference event

**Conflict:** `inference.InferenceCompletedEvent` (`fp.inference.completed`) and
`serving.PredictionCompletedEvent` (`fp.serving.prediction_completed`) both
metered/billed an inference. Two producers for one fact.

**Resolution:** The **Inference Gateway owns** `InferenceCompleted`
(`fp.inference.completed`). It is the entry point and the only component that
knows end-to-end latency, the version that actually served (post traffic-split),
the billed principal (`api_key_id`), the canary flag, and the `request_id` it
minted. **Model Serving must not publish a competing metering event** —
`serving.PredictionCompletedEvent` is deleted. Billing **and** Model Monitor both
consume the gateway's single event. The event carries `request_id` as a stable
join key so Model Monitor can later match delayed ground truth; `token_count` was
added so Billing can meter `METER_TYPE_INFERENCE_TOKENS` off this one event.

### 2. Model-deploy lifecycle (was missing)

**Conflict:** The Inference Gateway and Serving consume model-deploy /
model-undeploy events, but **no proto defined the payloads**. The gateway's code
comments referenced `fp.pipelines.model.deployed` / `.undeployed`; the design doc
listed `ModelDeployed` only as a table entry.

**Resolution:** The **Pipeline Orchestrator owns** `ModelDeployed` /
`ModelUndeployed` (`fp.pipelines.model.deployed` / `.undeployed`) — the
deployment saga is what performs the deploy. Payloads added with `endpoint` and
`weight_bps` so the gateway can build a route target (endpoint resolved by the
saga, never client-supplied — SSRF guard) and the initial canary weight.
Serving's `ModelLoadedEvent` / `ModelUnloadedEvent` are deleted; the
authoritative deploy lifecycle is the saga's, not a serving-pod side note.

### 3. Registry `ModelVersionReady` added

**Conflict:** Registry emitted `ModelVersionCreated` at row creation
(`status=PENDING_UPLOAD`), but serving/billing consumers need the **READY**
transition (artifact uploaded + checksum-verified), which no event signaled.

**Resolution:** Added `ModelVersionReady` (`fp.models.version.ready`).
`ModelVersionCreated` signals intent; `ModelVersionReady` signals the version is
physically servable, carrying `artifact_path`, `artifact_digest`, `size_bytes`.
Consumers: serving (loadable now), billing (meter storage), pipeline-orchestrator
(a deploy/promote saga blocked on readiness can proceed).

### 4. One feature-write event name

**Conflict:** Design doc + Experiment Tracker consumer used `FeaturesIngested` /
`fp.features.ingested`; the Feature Store proto renamed it to `FeaturesWritten` /
`fp.features.written`. The Experiment Tracker subscribed to the old subject —
**mismatch, events would never be received.**

**Resolution:** One name — **`FeaturesWritten` on `fp.features.written`**. Fix
agents **must repoint the Experiment Tracker subscription** from
`fp.features.ingested` to `fp.features.written`.

### 5. Drift owned by Model Monitor

**Resolution:** Model Monitor owns `ModelDriftDetected`
(`fp.models.drift.detected`), closing `serve → monitor → retrain`. Consumers:
notification (alert), experiment-tracker (record), pipeline-orchestrator (on
CRITICAL + `auto_retrain`, run the retrain pipeline). Lives under `fp.models.*`
because it's about a model; `EventEnvelope.source = "model-monitor"` records the
producer.

### 6. Billing subjects

**Resolution:** Kept the design-doc-sanctioned `fp.billing.quota.exceeded` and
`fp.billing.invoice.generated`. `UsageRecorded` was implied by the outbox example
but had no sanctioned subject; we add **`fp.billing.usage.recorded`** under the
existing `fp.billing.>` tree. **Doc addition:** the platform-design NATS
hierarchy should add `fp.billing.usage.recorded` to its `fp.billing.>` listing.

### 7. One subject convention

**Resolution:** `fp.<domain>.<event...>`, **plural** domain (justified above).
The prior `fp.models.*` vs `fp.pipelines.*` split is resolved by **ownership by
resource**: model lifecycle facts under `fp.models.*`; the deployment workflow's
outcomes under `fp.pipelines.model.*`.

---

## Migration notes for the per-service fix agents

Each service keeps its RPC request/response types and **deletes its inline event
payload messages**, importing `forgepoint.events.v1` instead.

- **registry** — remove `ModelRegistered`, `ModelVersionCreated`, `ModelPromoted`,
  `ModelArchived` from `registry.proto`. Publish `events.ModelRegistered`,
  `events.ModelVersionCreated`, **`events.ModelVersionReady`** (new — emit on the
  PENDING_UPLOAD → READY transition), `events.ModelPromoted`,
  `events.ModelArchived`.
- **inference-gateway** — remove `InferenceCompletedEvent` / `InferenceFailedEvent`.
  Publish `events.InferenceCompleted` / `events.InferenceFailed`. Consume
  `events.ModelDeployed` / `events.ModelUndeployed` (subjects
  `fp.pipelines.model.deployed` / `.undeployed`), `events.QuotaExceeded`,
  `events.ModelPromoted`, `events.ModelArchived`.
- **serving** — **delete** `ModelLoadedEvent`, `PredictionCompletedEvent`,
  `ModelUnloadedEvent` (it publishes **no** lifecycle/billing events now). Consume
  `events.ModelVersionReady`, `events.ModelDeployed`, `events.ModelUndeployed`,
  `events.ModelPromoted`, `events.ModelArchived`.
- **pipeline-orchestrator** — remove `PipelineStartedEvent`, `StepCompletedEvent`,
  `StepFailedEvent`, `CompensationTriggeredEvent`, `PipelineCompletedEvent`,
  `PipelineFailedEvent`. Publish the `events.*` equivalents **plus new**
  `events.ModelDeployed` / `events.ModelUndeployed`. Consume
  `events.ModelDriftDetected` (auto-retrain trigger).
- **feature-store** — remove `FeatureViewDefined`, `FeaturesWritten`. Publish
  `events.FeatureViewDefined`, `events.FeaturesWritten`.
- **experiment-tracker** — remove `RunCreatedEvent`, `RunFinishedEvent`. Publish
  `events.RunCreated`, `events.RunFinished`. **Repoint** consumer subscriptions:
  `fp.inference.completed`, `fp.models.version.created`,
  `fp.pipelines.step.completed`, **`fp.features.written`** (was
  `fp.features.ingested`), `fp.billing.usage.recorded`, `fp.models.drift.detected`,
  `fp.pipelines.model.deployed`, `fp.notifications.{delivered,failed}`.
- **billing** — remove `UsageRecorded`, `InvoiceGenerated`, `QuotaExceeded`.
  Publish `events.UsageRecorded` (`fp.billing.usage.recorded`),
  `events.QuotaExceeded`, `events.InvoiceGenerated` via the outbox. Consume
  `events.InferenceCompleted`, `events.ModelVersionReady` (storage metering).
- **notification** — remove `NotificationDeliveredEvent`, `NotificationFailedEvent`.
  Publish `events.NotificationDelivered`, `events.NotificationFailed`. Continues
  consuming `fp.>` opaquely (matches on `EventEnvelope.type`).
- **model-monitor** — remove `ModelDriftDetectedEvent`. Publish
  `events.ModelDriftDetected` (`fp.models.drift.detected`). Consume
  `events.InferenceCompleted`, `events.FeaturesWritten`, `events.ModelPromoted`
  (re-baseline).

`buf` is **not** run and nothing is committed as part of this contract definition,
per the task brief.
