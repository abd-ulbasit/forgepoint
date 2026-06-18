# Forgepoint API Reference

This directory holds the **generated** reference documentation for every
Forgepoint gRPC API. The source of truth is the protobuf definitions in
[`proto/`](../../proto); these docs are rendered from them, so they never drift.

## Contents

| Document | What it covers |
|---|---|
| [`proto-reference.md`](proto-reference.md) | Full reference for **all 12** proto packages — every message, field, enum, service, and RPC, with the explanatory comments from the `.proto` files. |

The reference covers these services (one proto package each):

| Package | Service | Purpose |
|---|---|---|
| `forgepoint.auth.v1` | `AuthService` | Login, users, API keys, RBAC, token validation |
| `forgepoint.registry.v1` | `RegistryService` | Model registry (CQRS): register/version/promote, list/get |
| `forgepoint.pipeline.v1` | `PipelineOrchestratorService` | Saga/DAG workflow engine: trigger, get, **watch** (streaming) |
| `forgepoint.monitor.v1` | `MonitorService` | Drift detection, drift reports, model health, ground-truth |
| `forgepoint.inference.v1` | inference gateway | Routing, traffic splitting, predictions |
| `forgepoint.serving.v1` | model serving | Load/serve model versions |
| `forgepoint.featurestore.v1` | feature store | Event-sourced features |
| `forgepoint.experiment.v1` | experiment tracker | Runs, metrics, params |
| `forgepoint.billing.v1` | billing | Usage metering (outbox) |
| `forgepoint.notification.v1` | notification | Event-reactor notifications |
| `forgepoint.events.v1` | — | Canonical NATS event payloads (single source of truth) |
| `forgepoint.common.v1` | — | Shared types: `EventEnvelope`, pagination, `ErrorDetail` |

## Python SDK

For a typed, ergonomic Python client over these APIs — with secure-by-default
channels, transparent JWT auth, and a streaming pipeline watcher — see the
**[Forgepoint Python SDK](../../sdk/python/README.md)**. The SDK's generated
stubs come from these same protos via an isolated buf template.

## Regenerating these docs

The reference is produced by **buf** with the `pseudomuto-doc` remote plugin,
using an isolated template that writes **only** into this directory (it never
touches `gen/go`):

```bash
# From the repo root:
cd proto && buf generate --template ../sdk/python/buf.gen.docs.yaml
```

> Do not edit `proto-reference.md` by hand it is regenerated. To change the
> docs, edit the comments in the `.proto` files and regenerate.
