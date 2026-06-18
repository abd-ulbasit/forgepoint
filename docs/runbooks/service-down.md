# Runbook: ServiceDown / SagaStuck

**Alerts:** `ForgepointServiceDown`, `HighContainerRestartRate`, `PodOOMKilled`,
`AuthFastBurn`, `PipelineOrchestratorFastBurn`, `SagaStuck`
**Severity:** Critical (ServiceDown, AuthFastBurn) / Warning (others)
**SLO impact:** Yes — all fast-burn alerts burn the error budget at 14.4×+
**Last reviewed:** 2026-06-18

---

## Symptom

- Prometheus fires `ForgepointServiceDown`: a Forgepoint service has 0 ready
  replicas in `fp-system`, `fp-models`, or `fp-jobs`.
- OR: `HighContainerRestartRate`: a pod has restarted 5+ times in 15 minutes.
- OR: `SagaStuck`: a pipeline execution has been in RUNNING state for >10 minutes
  without a step completing.
- OR: `AuthFastBurn`: all services returning Unauthenticated because auth is down.

---

## Impact

| Service | Impact when down |
|---------|-----------------|
| auth | Platform-wide: ALL gRPC requests fail with Unauthenticated (interceptor rejects them) |
| inference-gateway | All model predictions fail; external clients get 503 |
| registry | Model lookups fail; gateway routing degrades; pipeline deployments blocked |
| pipeline-orchestrator | No new deployments or training runs; stuck sagas may leave partial K8s resources |
| model-serving | Predictions fail for affected model versions; circuit breaker opens in gateway |
| billing | Usage under-metering (NATS events lag). No immediate user impact. |
| feature-store / experiment-tracker / notification | Degraded analytics; delayed notifications. No prediction impact. |

---

## Diagnosis steps

### Step 1 — Identify the failing service

```bash
# List all pods across service namespaces — look for CrashLoopBackOff, OOMKilled, Pending
kubectl get pods -n fp-system
kubectl get pods -n fp-models
kubectl get pods -n fp-jobs

# If the alert label has a specific deployment:
kubectl get pods -n fp-system -l app.kubernetes.io/name=<service-name>
```

### Step 2 — Read the pod events and logs

```bash
# Events on the pod — tells you WHY it failed to start (image pull, OOM, etc.)
kubectl describe pod <pod-name> -n <namespace>

# Current logs (if the container is running but unhealthy):
kubectl logs <pod-name> -n <namespace> -c <container-name>

# Previous container logs (the logs from before the last restart — the crash logs):
kubectl logs <pod-name> -n <namespace> -c <container-name> --previous
```

### Step 3 — Check readiness and liveness

```bash
# Is the pod failing its own health checks?
kubectl describe pod <pod-name> -n <namespace> | grep -A5 "Liveness\|Readiness"

# Port-forward to the pod directly to test health endpoints manually:
kubectl port-forward <pod-name> 8081:8081 -n <namespace>
curl http://localhost:8081/healthz   # liveness
curl http://localhost:8081/readyz    # readiness (checks DB, NATS)
```

A `/readyz` failure means one of the service's dependencies is failing.
The JSON body tells you which check failed:

```json
{"status": "not ready", "checks": {"postgres": "dial tcp: connection refused", "nats": "ok"}}
```

### Step 4 — Check the dependency the readiness check failed

If `/readyz` reports Postgres failure:
```bash
# Is the Postgres pod running in fp-infra?
kubectl get pods -n fp-infra -l app.kubernetes.io/name=postgres

# Connect to Postgres from a debug pod:
kubectl run -it --rm psql-debug --image=postgres:17 --restart=Never -n fp-infra -- \
  psql "postgresql://fp:<password>@postgres.fp-infra.svc.cluster.local:5432/fp_<service>"

# Check if the target database exists and is accepting connections:
\l
SELECT 1;
```

If `/readyz` reports NATS failure:
```bash
# Is NATS running?
kubectl get pods -n fp-infra -l app.kubernetes.io/name=nats
kubectl logs -n fp-infra -l app.kubernetes.io/name=nats --tail=50

# NATS health endpoint:
kubectl port-forward -n fp-infra svc/nats 8222:8222
curl http://localhost:8222/healthz
```

### Step 5 — SagaStuck diagnosis

```bash
# Get the execution ID from the alert label.
# Connect to the pipeline-orchestrator's Postgres DB:
kubectl run -it --rm psql-debug --image=postgres:17 --restart=Never -n fp-infra -- \
  psql "postgresql://fp:<password>@postgres.fp-infra.svc.cluster.local:5432/fp_pipeline"

# Find the stuck execution and its last completed step:
SELECT e.id, e.status, e.current_step, e.started_at,
       EXTRACT(EPOCH FROM (NOW() - e.started_at)) AS age_seconds
FROM executions e
WHERE e.status = 'RUNNING'
ORDER BY e.started_at;

# See the step execution history for the stuck saga:
SELECT se.step_id, se.status, se.started_at, se.completed_at, se.error
FROM step_executions se
WHERE se.execution_id = '<execution-id>'
ORDER BY se.started_at;
```

Check if the K8s Job the saga created (for train/deploy steps) is still running:
```bash
kubectl get jobs -n fp-jobs
kubectl describe job <job-name> -n fp-jobs
kubectl logs -n fp-jobs -l job-name=<job-name>
```

### Step 6 — Check Prometheus and Grafana for context

```bash
# Port-forward Prometheus UI:
kubectl port-forward -n fp-infra svc/prometheus 9090:9090
# Query: rate of errors over the last 30m for the failing service
# PromQL: rate(rpc_server_duration_milliseconds_count{service_name="auth", rpc_grpc_status_code!="0"}[5m])
```

---

## Mitigation

### Service crash-loop (CrashLoopBackOff)

```bash
# 1. Read the crash logs (--previous) to understand the error.
# 2. If it's a configuration issue (wrong env var, missing secret):
kubectl describe configmap <service>-config -n fp-system
kubectl describe secret <service>-secret -n fp-system

# 3. If it's an OOM kill, temporarily increase the memory limit:
kubectl set resources deployment <service> -n fp-system \
  --limits=memory=512Mi --requests=memory=256Mi

# 4. If a recent deployment caused the regression, roll back:
kubectl rollout undo deployment/<service> -n fp-system
kubectl rollout status deployment/<service> -n fp-system
```

### Service stuck in Pending (no nodes with capacity)

```bash
# Check node resources:
kubectl describe nodes | grep -A5 "Allocated resources"
# If the homelab node is full, delete low-priority pods (e.g., grafana, loki)
# to free capacity while investigating.
```

### SagaStuck — manual intervention

```bash
# Option 1: Restart the orchestrator pod — the crash-recovery logic in
# pipeline-orchestrator/internal/domain/saga.go will reload state from
# Postgres and resume from the last completed step.
kubectl rollout restart deployment/pipeline-orchestrator -n fp-system

# Option 2: If the execution is unrecoverable (e.g., the K8s Job was
# manually deleted), mark it as FAILED in Postgres to stop the saga from
# looping:
# psql fp_pipeline:
UPDATE executions SET status = 'FAILED', completed_at = NOW()
WHERE id = '<execution-id>';

# Then manually clean up any partial K8s resources the saga created:
kubectl delete deployment model-<model>-v<version> -n fp-models
kubectl delete service model-<model>-v<version> -n fp-models
```

---

## Escalation

| Condition | Action |
|-----------|--------|
| Auth service down > 5 minutes | P0 — page engineering lead; all services degraded |
| Inference gateway down > 10 minutes | P1 — external users impacted; notify stakeholders |
| No root cause found after 30 minutes | Escalate to senior platform engineer |
| Data loss suspected | Escalate immediately + open DR runbook (`docs/runbooks/dr.md`) |

---

## Post-incident checklist

- [ ] Root cause documented in a post-mortem
- [ ] How long was the SLO alert firing before it was noticed? (Response time gap?)
- [ ] Did the `/readyz` endpoint correctly reflect the failure? (If not, fix it.)
- [ ] Was crash-recovery logic tested? (SagaStuck — should have auto-recovered)
- [ ] Is there a pre-mortem fix? (Resource limit increase, retry config tuning)
