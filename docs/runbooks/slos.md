# Forgepoint SLO Definitions

**Phase:** 15 — Operability & SRE
**Owner:** Platform Engineering
**Last reviewed:** 2026-06-18
**Prometheus rules:** `deploy/sre/slo-recording-rules.yaml`, `deploy/sre/slo-alerts.yaml`

---

## What is an SLO and why does it matter here?

An **SLI** (Service Level Indicator) is a metric that measures one dimension of
service behavior that users care about. An **SLO** (Service Level Objective) is
the target value for that SLI. The gap between 100% and the SLO target is the
**error budget** — the amount of unreliability the team is *permitted* to consume
before the SLO is violated.

Error budgets turn reliability into a **product decision**, not an engineering
instinct:

- While the budget is healthy, the team can ship features and accept the
  risk of incidents.
- When the budget is nearly exhausted, reliability work takes priority over
  features — automatically, without a management directive.

The Google SRE Workbook (Chapter 2) formalizes this. Forgepoint follows the same
model. The SLO window is **30 rolling days** for all services.

---

## Metric source

All SLI metrics flow through the OTel collector path:

```
service (OTel SDK) → OTLP push → otel-collector:4317
  → prometheus exporter :8889
    → Prometheus scrape (job: otel-collector-app-metrics)
      → recording rules (fp:<service>:*) → alert evaluation
```

The `service_name` label on every metric is the OTel `service.name` resource
attribute, set per-service in `pkg/observability`. It matches the service name
in the table below.

---

## SLO table

| Service | SLI | Metric family | Target | Error budget (30d) | Burn-rate alert tiers |
|---------|-----|---------------|--------|---------------------|----------------------|
| inference-gateway | HTTP availability (non-5xx / total) | `http_server_duration_milliseconds_count` | **99.9%** | 43.8 min / 30d | fast: >14.4× (1h+5m), slow: >6× (6h+30m) |
| inference-gateway | p99 latency | `http_server_duration_milliseconds_bucket` | **< 500ms** | n/a (threshold) | single window: >500ms for 5m |
| registry | gRPC availability (OK / total) | `rpc_server_duration_milliseconds_count` | **99.9%** | 43.8 min / 30d | fast: >14.4× (1h+5m), slow: >6× (6h+30m) |
| auth | gRPC availability (OK / total) | `rpc_server_duration_milliseconds_count` | **99.9%** | 43.8 min / 30d | fast: >14.4× (1h+5m), slow: >6× (6h+30m) |
| pipeline-orchestrator | gRPC availability (OK / total) | `rpc_server_duration_milliseconds_count` | **99.5%** | 3.6 h / 30d | fast: >14.4× (1h+5m), slow: >6× (6h+30m) |

---

## Per-service SLO rationale

### inference-gateway — 99.9% availability + 500ms p99 latency

**Why 99.9%?** This is the external prediction API — the surface every user and
every integrated application touches. A 0.1% error rate means roughly 1 in 1000
prediction requests fails, which at scale (10k req/min) is 10 errors/min. That
is the threshold where user-facing applications start showing degraded behavior.

**Why 500ms p99?** Synchronous prediction is latency-sensitive. Downstream clients
typically have 1–2s timeout budgets; 500ms for the gateway allows the model-serving
backend ~200–300ms for inference plus network overhead. If the p99 exceeds 500ms
sustainably, the backend model serving instances are the first place to look.

**Error budget math:**
```
30 days = 43,200 minutes
0.1% budget = 43.2 minutes of total outage equivalent
(or proportionally more time at partial error rates)
```

What exhausting the budget means: freeze all non-reliability feature work on
inference-gateway for the remainder of the 30-day window. Conduct a post-mortem.
Re-evaluate model serving capacity and circuit breaker thresholds.

---

### registry — 99.9% availability

**Why 99.9%?** The registry sits on both the control plane (model registration,
versioning) and the data plane (inference-gateway looks up model metadata before
routing). A registry outage degrades gateway routing and blocks all pipeline
deployments. Same tier as the gateway for this reason.

**SLI note on NOT_FOUND:** gRPC status code 5 (NOT_FOUND) is counted as an error
in the current SLI definition. The reasoning: a GetModel call returning NOT_FOUND
means either the caller has a stale reference (upstream bug) or the Redis
projection is missing the model (registry bug). Both are worth surfacing. If
operational data shows NOT_FOUND is predominantly caller-side error noise, add
`rpc_grpc_status_code!~"0|5"` to exclude it from the SLI — document that change
in an ADR.

---

### auth — 99.9% availability

**Why 99.9%?** Auth is in the hot path of EVERY gRPC request across ALL ten
services (the `AuthUnaryInterceptor` in `pkg/grpcutil` calls `ValidateToken` for
every non-exempt RPC). An auth outage is a **platform-wide outage**. The SLO
target matches the gateway's because auth is the dependency that gates the
gateway's own SLO.

**Why not 99.99%?** The homelab k3s environment runs on a single node; 99.99%
(4.3 min/30d budget) is unachievable without HA Postgres and a multi-replica auth
service. Set 99.99% as the target for the AWS EKS environment (Phase 12) once RDS
Multi-AZ is in place.

---

### pipeline-orchestrator — 99.5% availability

**Why 99.5% (lower than the others)?** Pipeline execution is an inherently
infrastructure-heavy, long-running operation. Sagas span minutes (validate → deploy
→ canary → promote), invoke K8s Jobs, wait on model-serving pod readiness, and call
external services. A transient infrastructure hiccup that fails a single saga step
counts as a gRPC error from the caller's perspective (the triggering client gets an
error code) even though the saga may successfully compensate and be retried.

Holding the orchestrator to 99.9% would mean every legitimate saga failure (a model
that genuinely fails canary metrics, a training job that times out) burns the error
budget — misaligning the SLO with the intended signal.

**Error budget math:**
```
30 days = 43,200 minutes
0.5% budget = 216 minutes = 3.6 hours of total outage equivalent
```

What exhausting the budget means: freeze pipeline feature work. Investigate
whether the budget is being burned by orchestrator bugs (should be fixed) vs.
upstream model quality (should be handled by better validation steps, not an
orchestrator SLO).

---

## Error budget policy

| Budget remaining | Action |
|-----------------|--------|
| > 50% | Normal operations. Ship features. |
| 25%–50% | Monitor closely. Prioritize reliability fixes in the current sprint. |
| 10%–25% | Feature freeze on the affected service. Incident review required. |
| < 10% | Escalate to engineering lead. All hands on reliability. Post-mortem required even if no single incident triggered it. |
| Exhausted (0%) | SLO violation. Mandatory post-mortem. Monthly review with stakeholders. |

---

## Burn-rate alert tiers

The alerts in `deploy/sre/slo-alerts.yaml` implement the Google SRE Workbook's
multi-window, multi-burn-rate pattern. Here is the interpretation guide for the
on-call engineer:

| Alert suffix | Tier | Burn rate | Implication | Response |
|---|---|---|---|---|
| `FastBurn` | Critical (page) | > 14.4× | Budget gone in < 2 hours | Wake up, escalate immediately |
| `SlowBurn` | Warning (ticket) | > 6× | Budget gone in < 5 days | Create ticket, fix this sprint |
| No SLO alert | Healthy | < 6× | Will not miss SLO at this rate | Normal ops |

**Why two windows per tier (e.g., 1h AND 5m for fast burn)?**

The short window (5m) detects the event quickly. The long window (1h) confirms it
is not a 30-second transient spike. Both must be true simultaneously before the
alert fires. This dual-window approach eliminates the "3am false-positive" problem
that plagues single-window threshold alerts.

---

## Non-SLO infrastructure alerts (from `slo-alerts.yaml`)

These alerts are operational hygiene, not SLO-driven:

| Alert | Threshold | Severity | Notes |
|-------|-----------|----------|-------|
| `ForgepointServiceDown` | 0 ready replicas for 2m | critical | Requires kube-state-metrics |
| `HighContainerRestartRate` | >5 restarts in 15m | warning | Crash-loop signal |
| `PodOOMKilled` | Last terminated = OOMKilled | warning | Memory limit or leak |
| `NATSConsumerLagHigh` | >10k pending messages for 5m | warning | No data loss; delayed processing |
| `PostgresDown` | `fp_db_up == 0` for 1m | critical | Requires custom gauge from service |
| `ModelDriftStorm` | >5 drift detections in 15m | warning | Auto-retrain flood risk |
| `CircuitBreakerOpen` | `fp_gateway_circuit_breaker_state{state="open"}` for 2m | warning | Model backend failing |

---

## Extending SLOs to new services

When adding SLOs for additional services (billing, feature-store, etc.):

1. Add recording rules to `deploy/sre/slo-recording-rules.yaml` following the
   established pattern (`fp:<service>:<ratio>:<window>` naming).
2. Add alert rules to `deploy/sre/slo-alerts.yaml`.
3. Add a row to the SLO table above and a rationale section.
4. Add a corresponding entry to `docs/runbooks/` with the service-specific
   diagnosis steps.
5. Record the SLO target decision in an ADR (`docs/adr/`) — the *why* behind
   99.9% vs 99.5% is as important as the number itself.
