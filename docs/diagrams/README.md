# Forgepoint Diagrams

Architecture diagrams for Forgepoint, authored as **Mermaid** inside Markdown so they render
directly on GitHub and in most editors — diagrams-as-code, versioned and reviewable alongside
the system they describe.

## Contents

| File | What it covers |
|---|---|
| [`c4-architecture.md`](./c4-architecture.md) | The **C4 model** of the platform, plus the closed-loop sequence. |

### [`c4-architecture.md`](./c4-architecture.md) — the C4 model

A [C4](https://c4model.com/) walk from the outside in, each diagram with a teaching paragraph:

- **Level 1 — System Context:** the actors (ML Engineer, API Client, Operator), the Forgepoint
  boundary, and the external world it touches (notification channels, infrastructure). No cloud
  specifics — architecture, not deployment.
- **Level 2 — Containers:** the **10 services** + API Gateway/BFF + data infra
  (Postgres-per-service, Redis, NATS JetStream, MinIO) + observability, drawn with **sync gRPC**
  vs **async NATS** edges. The `fp.*` event arrows are taken **verbatim** from the
  [canonical event contract](../design/event-contract.md) (there is a cross-check table under
  the diagram).
- **Level 3 — Components:** the inside of the three signature services —
  **(a)** Pipeline Orchestrator (saga: DAG executor + reverse-order compensation + state machine
  + the `StepExecutor` port), **(b)** Model Monitor (the closed loop: windows → drift math →
  threshold → `ModelDriftDetected` → orchestrator trigger), and **(c)** Model Registry (CQRS:
  write-Postgres command side + read-Redis projection fed by a `ProjectionEmitter`).
- **The Closed Loop:** a sequence diagram of `serve → InferenceCompleted → monitor windows →
  drift detected → orchestrator retrains → registry new version → canary promote → serve`.

## Sources of truth

These diagrams are grounded in, and should stay consistent with:

- [`docs/plans/forgepoint-platform-design.md`](../plans/forgepoint-platform-design.md) — services, communication, infrastructure.
- [`docs/design/event-contract.md`](../design/event-contract.md) — the authoritative async event flows (every `fp.*` edge).
- [`docs/design/service-architecture.md`](../design/service-architecture.md) — the per-service Clean/Hexagonal layout the Level-3 diagrams reflect.
- Each service's `internal/domain/ports.go` — the ports the component diagrams name.

## Rendering

Mermaid renders natively on GitHub. Locally, use the Mermaid VS Code extension, the
[live editor](https://mermaid.live/), or `mmdc` (mermaid-cli). The C4 diagrams use Mermaid's
`C4Context` / `C4Container` syntax; the component and sequence diagrams use `flowchart` /
`sequenceDiagram` where that reads more clearly than C4's `Component` grammar.

## Maintenance

When the event contract or a service's ports change, update the affected diagram **and** the
cross-check table under the Container diagram in the same PR — a diagram that drifts from the
contract is worse than no diagram.
