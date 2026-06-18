# Forgepoint Runbooks

**Phase:** 15 — Operability & SRE + 24 — Backup & DR
**Audience:** On-call platform engineers
**Prometheus alert rules:** `deploy/sre/slo-recording-rules.yaml`, `deploy/sre/slo-alerts.yaml`

---

## Overview

This directory contains the operational runbooks for the Forgepoint platform.
Each runbook maps to one or more Prometheus alert rules in `deploy/sre/` and
answers the question: **"It's 3am and this alert is firing — what do I do?"**

Every alert's `annotations.runbook_url` points here.

---

## Runbook index

| Alert(s) | Runbook | Severity | SLO impact |
|----------|---------|----------|------------|
| `ForgepointServiceDown`, `HighContainerRestartRate`, `PodOOMKilled`, `AuthFastBurn`, `PipelineOrchestratorFastBurn`, `SagaStuck` | [service-down.md](./service-down.md) | Critical / Warning | Yes |
| `InferenceGatewayFastBurn`, `InferenceGatewaySlowBurn`, `RegistryFastBurn`, `RegistrySlowBurn`, `AuthSlowBurn`, `PipelineOrchestratorSlowBurn`, `CircuitBreakerOpen` | [high-error-rate.md](./high-error-rate.md) | Critical / Warning | Yes |
| `InferenceGatewayHighLatency` | [high-latency.md](./high-latency.md) | Warning | Indirect |
| `NATSConsumerLagHigh` | [nats-consumer-lag-high.md](./nats-consumer-lag-high.md) | Warning | Indirect |
| `PostgresDown` | [postgres-down.md](./postgres-down.md) | Critical | Yes |
| `ModelDriftStorm` | [model-drift-storm.md](./model-drift-storm.md) | Warning | Indirect |
| All DR scenarios (data loss, cluster loss) | [dr.md](./dr.md) | P0 | Yes |

---

## SLO reference

Full SLO definitions, error budget calculations, and the error budget policy are
in [slos.md](./slos.md).

---

## General on-call orientation

### Access the observability stack

```bash
# Prometheus query UI:
kubectl port-forward -n fp-infra svc/prometheus 9090:9090
# Open http://localhost:9090

# Grafana dashboards:
kubectl port-forward -n fp-infra svc/grafana 3000:3000
# Open http://localhost:3000 (admin / admin by default in homelab)

# Tempo traces:
# Available in Grafana → Explore → Tempo datasource
```

### Read structured service logs

```bash
# All services log structured JSON to stdout.
# Filter by level=error across all fp-system pods:
kubectl logs -n fp-system --selector='app.kubernetes.io/part-of=forgepoint' \
  --since=30m | grep '"level":"error"'

# Or use Grafana → Explore → Loki:
# {namespace="fp-system"} | json | level = "error"
```

### Check cluster health quickly

```bash
# All pods — are any not Running/Completed?
kubectl get pods -A | grep -v 'Running\|Completed\|Pending'

# Recent events — warnings and errors:
kubectl get events -A --sort-by='.lastTimestamp' | tail -30 | grep -v Normal
```

### Architecture reminder

```
External Clients
    │
    ▼
inference-gateway (HTTP)
    │
    ├── auth (gRPC — ValidateToken on every request)
    ├── registry (gRPC — model metadata lookup)
    └── model-serving (gRPC — ONNX inference)
          │
          ├── pipeline-orchestrator (saga: deploy new models)
          ├── feature-store (offline/online features)
          ├── experiment-tracker (async metrics via NATS)
          ├── billing (usage metering via NATS)
          ├── notification (event reactor via NATS)
          └── model-monitor (drift detection → auto-retrain)

Infrastructure (fp-infra namespace):
  Postgres (one DB per service) | NATS JetStream | Redis | MinIO | OTel Collector | Prometheus | Grafana | Tempo | Loki
```

---

## Contributing to runbooks

When adding a new alert to `deploy/sre/slo-alerts.yaml`:
1. Create a corresponding runbook in this directory.
2. Set `annotations.runbook_url` in the alert to the GitHub URL of the new file.
3. Add a row to the index table above.
4. Include: symptom, impact table, diagnosis steps with exact commands for THIS
   platform, mitigation, and escalation.

The test for a good runbook: could a competent engineer who has never seen this
codebase follow it at 3am without asking anyone for help?
