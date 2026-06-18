# deploy/keda — Phase 22: Event-Driven Autoscaling

**Phase:** 22 (M6 — Platform security, GitOps & autoscaling)
**Pattern:** KEDA ScaledObjects targeting NATS JetStream consumer lag
**Depends on:** Phase 10 (Prometheus), Phase 20 (ArgoCD), NATS JetStream running in fp-infra

---

## What is here

Four `ScaledObject` manifests — one per event-consuming service — plus a
`kustomization.yaml` that bundles them for `kubectl apply -k` or ArgoCD sync.

| File | Service | Stream | Consumer | min→max | lagThreshold | Scale-to-zero |
|---|---|---|---|---|---|---|
| `scaled-object-billing.yaml` | fp-billing | INFERENCE | billing | 1→5 | 30 | No (outbox relay) |
| `scaled-object-model-monitor.yaml` | fp-model-monitor | INFERENCE | model-monitor | 0→6 | 50 | Yes |
| `scaled-object-experiment-tracker.yaml` | fp-experiment-tracker | PLATFORM_EVENTS | experiment-tracker | 0→6 | 100 | Yes |
| `scaled-object-notification.yaml` | fp-notification | PLATFORM_EVENTS | notification | 0→4 | 20 | Yes |

---

## Why KEDA instead of HPA

HPA scales on CPU or memory — a lagging symptom of backlog, not the backlog
itself. A message consumer sitting at 2 % CPU waiting on a Postgres write has
zero pending events *or* ten thousand; HPA can't distinguish them. KEDA reads
the NATS monitoring endpoint (`/jsz`) for the exact consumer lag count and drives
scaling from that number directly.

KEDA also adds **scale-to-zero**: an HPA cannot drop below 1 replica; KEDA can
drop to 0 when the stream is fully drained, and wake a pod the moment a message
arrives. Three of the four services here (model-monitor, experiment-tracker,
notification) scale to zero when idle. Billing does not — the Outbox relay
goroutine must run continuously.

---

## Prerequisites

### 1. Install KEDA

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update

helm install keda kedacore/keda \
  --namespace keda \
  --create-namespace \
  --version 2.16.0 \
  --set watchNamespace=fp-system
```

Verify:

```bash
kubectl get pods -n keda
kubectl get crd scaledobjects.keda.sh
```

### 2. Verify NATS JetStream streams exist

The ScaledObjects reference two streams: `INFERENCE` and `PLATFORM_EVENTS`.
These must be created by the producing services on startup (or via a stream
provisioning Job). Check with:

```bash
kubectl exec -n fp-infra deploy/nats -- \
  nats stream ls --server nats://localhost:4222
```

Streams that don't exist will cause the ScaledObject to report an error in
`kubectl describe scaledobject fp-model-monitor`.

### 3. Verify durable consumers exist

Each service registers its durable consumer on startup. If the consumer doesn't
exist yet (service has never run), KEDA will fail to read the lag. Services use
`js.PullSubscribe` / `js.Subscribe` with `nats.Durable(name)` — the consumer is
created on first connect.

---

## Apply

```bash
# Dry-run (verify YAML + server-side validation; requires KEDA CRDs present):
kubectl apply -k deploy/keda/ --dry-run=server

# Apply:
kubectl apply -k deploy/keda/

# Verify ScaledObjects are READY:
kubectl get scaledobjects -n fp-system
# Expected: READY=True, ACTIVE=True/False, TRIGGERS=nats-jetstream
```

---

## Observe scaling in action

```bash
# Watch HPA owned by KEDA:
kubectl get hpa -n fp-system -w

# Watch replica count live:
kubectl get deploy -n fp-system -w

# Inspect KEDA's view of a ScaledObject:
kubectl describe scaledobject fp-model-monitor -n fp-system
```

To trigger a scale-up: publish a batch of inference events to NATS and watch
the consumer lag grow, then watch KEDA add replicas.

---

## Tuning reference

| Parameter | Default | When to change |
|---|---|---|
| `lagThreshold` | per-service | Increase if replicas are added/removed too aggressively; lower if you want faster scale-out |
| `activationLagThreshold` | 1–5 | Raise to absorb small bursts without cold-starting pods (trading latency for cost) |
| `pollingInterval` | 20–30 s | Lower for faster reaction (more NATS /jsz queries); raise to reduce monitoring load |
| `cooldownPeriod` | 120–300 s | Lower for faster scale-in; raise if your traffic is bursty and you see repeated scale up/down cycles |
| `minReplicaCount` | 0 or 1 | Set to 1 for services with background duties (outbox relay, cron polling) |
| `maxReplicaCount` | 4–6 | Cap at DB connection pool size / downstream rate limit |

---

## ArgoCD integration (Phase 20)

Add a `deploy/argocd/apps/keda-scaled-objects.yaml` Application pointing at
`deploy/keda/` after KEDA is installed in the cluster. See `kustomization.yaml`
for the example fragment.
