# deploy/rollouts — Phase 21: Progressive Delivery

**Phase:** 21 (M6 — Platform security, GitOps & autoscaling)
**Pattern:** Argo Rollouts canary strategy with Prometheus AnalysisTemplate gates
**Depends on:** Phase 10 (Prometheus observability), Phase 16 (model-monitor drift metrics), Phase 20 (ArgoCD GitOps)

---

## What is here

| File | Type | Service | Purpose |
|---|---|---|---|
| `rollout-inference-gateway.yaml` | Rollout | fp-inference-gateway | Canary: 20% → pause → 50% → pause → 100% |
| `rollout-model-serving.yaml` | Rollout | fp-model-serving | Canary: 20% → pause → 50% → pause → 100% (+ sidecar template) |
| `services-inference-gateway.yaml` | Service x2 | fp-inference-gateway | stable + canary Service pair |
| `services-model-serving.yaml` | Service x2 | fp-model-serving | stable + canary Service pair |
| `analysis-template-inference-gateway.yaml` | AnalysisTemplate x1 | fp-inference-gateway | success-rate + p99-latency gates |
| `analysis-template-model-serving.yaml` | AnalysisTemplate x3 | fp-model-serving | success-rate + p99-latency + drift-score gates |
| `kustomization.yaml` | Kustomize | all | Bundles all resources for `kubectl apply -k` |

---

## Why progressive delivery

A Kubernetes Deployment's rolling update ships the new version to 100% of pods
without any traffic or metric gate. Argo Rollouts replaces the Deployment with a
Rollout CRD that adds:

1. **Canary traffic splitting** — route a fraction of production traffic to new
   pods while keeping the rest on the known-good (stable) version.
2. **Prometheus analysis gates** — auto-rollback if error rate, p99 latency, or
   (for model-serving) drift score breaches thresholds.
3. **Manual pause steps** — CI/CD-friendly promotion checkpoints.

### Canary vs blue-green

| | Canary | Blue-Green |
|---|---|---|
| Traffic cut | Gradual (20% → 50% → 100%) | Instant (0% → 100%) |
| Blast radius at first exposure | 20% | 100% |
| Resource cost during rollout | Low (1–2 extra pods) | High (full second environment) |
| Rollback speed | Seconds (weight → 0%) | Instant (selector flip) |
| Metric signal | Strong (real production traffic) | Only from pre-prod testing |

We use canary for both services because the gradual ramp gives real production
signal at bounded blast radius, and the resource cost of an extra pod is
trivial vs. running a second full environment.

---

## Prerequisites

### 1. Install Argo Rollouts controller

```bash
kubectl create namespace argo-rollouts

kubectl apply -n argo-rollouts \
  -f https://github.com/argoproj/argo-rollouts/releases/download/v1.7.2/install.yaml

# Verify:
kubectl get pods -n argo-rollouts

# Install kubectl plugin (macOS):
brew install argoproj/tap/kubectl-argo-rollouts
kubectl argo rollouts version
```

### 2. Disable Helm chart Deployments

The Rollout replaces the Helm chart's `Deployment`. Before applying these
manifests, disable the chart's Deployment template:

```bash
# For inference-gateway:
helm upgrade fp-inference-gateway deploy/helm/fp-inference-gateway \
  --namespace fp-system \
  --set deployment.enabled=false

# For model-serving (NOTE: fp-models, NOT fp-system — serving's authoritative
# namespace; see deploy/rollouts/rollout-model-serving.yaml):
helm upgrade fp-model-serving deploy/helm/fp-model-serving \
  --namespace fp-models \
  --set deployment.enabled=false
```

> **Namespaces:** `fp-inference-gateway` lives in `fp-system`; `fp-model-serving` lives
> in `fp-models`. The `kubectl argo rollouts …` examples below use `-n fp-system` because
> they target the gateway — swap to `-n fp-models` for the `fp-model-serving` rollout
> (e.g. `kubectl argo rollouts status fp-model-serving -n fp-models`).

All other chart resources (ConfigMap, Secret, Service, ServiceAccount, NetworkPolicy,
PDB, ServiceMonitor) remain managed by Helm. The Rollout owns only the pod template.

### 3. Verify Prometheus is reachable

The AnalysisTemplates query:
```
http://prometheus-operated.fp-infra.svc.cluster.local:9090
```

This is the kube-prometheus-stack (or Prometheus Operator) address. Adjust if
your Prometheus service has a different name or namespace.

---

## Apply

```bash
# Dry-run (requires Argo Rollouts CRDs present):
kubectl apply -k deploy/rollouts/ --dry-run=server

# Apply:
kubectl apply -k deploy/rollouts/

# Verify Rollouts are healthy:
kubectl argo rollouts list rollouts -n fp-system
kubectl argo rollouts status fp-inference-gateway -n fp-system
```

---

## Triggering a canary

```bash
# Update the image to a new version (this starts the canary):
kubectl argo rollouts set image fp-inference-gateway \
  inference-gateway=ghcr.io/abd-ulbasit/forgepoint/inference-gateway:<new-sha> \
  -n fp-system

# Watch the canary progress:
kubectl argo rollouts get rollout fp-inference-gateway -n fp-system --watch

# Promote past a manual pause (after human review):
kubectl argo rollouts promote fp-inference-gateway -n fp-system

# Abort a canary (manual rollback):
kubectl argo rollouts abort fp-inference-gateway -n fp-system

# Retry after abort (re-start the canary from step 1):
kubectl argo rollouts retry rollout fp-inference-gateway -n fp-system
```

---

## Auto-rollback demo

To demonstrate auto-rollback with a bad version:

1. Deploy a new image that returns HTTP 500s (or has high latency).
2. The AnalysisRun at step 2 (20% gate) will observe `success-rate > 1%`.
3. After `failureLimit: 0` consecutive failures, the AnalysisRun reports Failed.
4. The Rollout controller resets canary weight to 0% and scales the canary
   ReplicaSet to 0. Stable remains at 100%.
5. `kubectl argo rollouts get rollout fp-inference-gateway -n fp-system`
   shows status: `Degraded`.

---

## Updating Prometheus metric names (required after Phase 10)

The AnalysisTemplate queries use PLACEHOLDER metric names. After Phase 10
instruments OpenTelemetry metrics:

1. Find the actual metric names:
   ```bash
   kubectl port-forward svc/prometheus-operated 9090:9090 -n fp-infra
   # Open http://localhost:9090 and search for "forgepoint_" or "grpc_server_"
   ```

2. Update the `query:` fields in:
   - `analysis-template-inference-gateway.yaml` (2 metrics)
   - `analysis-template-model-serving.yaml` (3 metrics, including drift-score)

3. For the drift-score metric, verify model-monitor (Phase 16) is exporting
   a Prometheus gauge with the expected label set (`service`, `track`, `severity`).

---

## ArgoCD integration (Phase 20)

Add `deploy/argocd/apps/rollouts.yaml` Application pointing at `deploy/rollouts/`
after the Argo Rollouts controller is installed. See `kustomization.yaml` for the
example Application fragment.

---

## Closed loop: Phase 16 + Phase 21

The `fp-model-serving-drift-score` AnalysisTemplate ties Phase 16 (model-monitor)
to Phase 21 (progressive delivery):

```
inference-gateway → model-serving → model-monitor (Phase 16)
                                          │
                                    drift score metric
                                          │
                                    AnalysisTemplate gate (Phase 21)
                                          │
                              auto-rollback if drift score > 0.3
```

A regression in the model-serving binary that distorts feature preprocessing
will cause drift scores to rise on canary pods → the Phase 21 gate fires
auto-rollback → the bad version never reaches stable. No human required.
