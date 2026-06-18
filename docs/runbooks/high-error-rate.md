# Runbook: HighErrorRate (SLO Burn-Rate Alerts)

**Alerts:** `InferenceGatewayFastBurn`, `InferenceGatewaySlowBurn`,
`RegistryFastBurn`, `RegistrySlowBurn`, `AuthSlowBurn`,
`PipelineOrchestratorFastBurn`, `PipelineOrchestratorSlowBurn`,
`CircuitBreakerOpen`
**Severity:** Critical (FastBurn) / Warning (SlowBurn, CircuitBreakerOpen)
**SLO impact:** Directly — these alerts fire when the error budget is burning
**Last reviewed:** 2026-06-18

---

## Symptom

A service's error rate is elevated enough that the 99.9% (or 99.5%) monthly
error budget will be exhausted within 2 hours (fast burn) or 5 days (slow burn).

The burn-rate metric (`fp:<service>:burn_rate:<window>`) exceeded the threshold:
- Fast burn: > 14.4× over both the 1h and 5m windows
- Slow burn: > 6× over both the 6h and 30m windows

Burn rate 1.0× = using exactly the budget; 14.4× = 720× the allowed rate.

---

## Impact

| Service | What fails at high error rate |
|---------|-------------------------------|
| inference-gateway | Prediction requests return 5xx to clients |
| registry | gRPC callers (gateway, orchestrator) cannot read/write model data |
| auth | ValidateToken failures propagate as Unauthenticated to ALL service callers |
| pipeline-orchestrator | Saga trigger / status calls fail; deployments blocked |

---

## Diagnosis steps

### Step 1 — Identify the error type

```bash
# Port-forward Prometheus
kubectl port-forward -n fp-infra svc/prometheus 9090:9090

# For inference-gateway (HTTP errors) — which status codes are failing?
# PromQL:
sum by (http_response_status_code) (
  rate(http_server_duration_milliseconds_count{
    service_name="inference-gateway"
  }[5m])
)

# For gRPC services — which status codes?
sum by (rpc_grpc_status_code, rpc_method) (
  rate(rpc_server_duration_milliseconds_count{
    service_name="registry"
  }[5m])
)
```

Map the status code to a cause:
- `gRPC 2` (Unknown) → unhandled panic (check recovery interceptor logs)
- `gRPC 4` (DeadlineExceeded) → timeout — check DB or downstream service latency
- `gRPC 5` (NotFound) → caller has stale reference OR read projection is stale
- `gRPC 13` (Internal) → bug in service logic (check logs for stack trace)
- `gRPC 14` (Unavailable) → downstream dependency is down (DB, NATS, upstream service)
- `HTTP 502` (Bad Gateway) → backend model-serving instance is down
- `HTTP 503` (Service Unavailable) → circuit breaker open OR bulkhead saturated
- `HTTP 429` (Too Many Requests) → rate limiter (NOT an error in the SLI sense — exclude if needed)

### Step 2 — Read the service logs

```bash
# Get structured logs for the failing service (last 200 lines):
kubectl logs -n fp-system -l app.kubernetes.io/name=<service-name> --tail=200 | \
  grep -E '"level":"error"|"level":"warn"'

# Or stream live:
kubectl logs -n fp-system -l app.kubernetes.io/name=<service-name> -f | \
  grep -E '"level":"error"'
```

The logs are structured JSON (slog). Key fields to scan:
- `"error"` — the actual error message
- `"trace_id"` — use this in Tempo to get the full distributed trace
- `"rpc_method"` — which RPC is failing
- `"status_code"` — the gRPC/HTTP status returned

### Step 3 — Trace a failing request in Tempo

```bash
# Port-forward Grafana:
kubectl port-forward -n fp-infra svc/grafana 3000:3000
# Navigate to: Explore → Tempo
# Search by service.name = "inference-gateway" and status = error
# Or paste a trace_id from the logs into Tempo's search
```

The distributed trace shows the full chain:
`gateway → auth (ValidateToken) → registry (GetModel) → model-serving (Predict)`

If the error originates in auth or registry rather than in the gateway itself,
the fast-burn on the gateway is a *symptom* of a dependency failure — go to
the dependency's runbook.

### Step 4 — Check for circuit breaker state

```bash
# PromQL — any circuit breakers open right now?
fp_gateway_circuit_breaker_state{state="open"}

# If a breaker is open, check the model-serving backend it's protecting:
kubectl get pods -n fp-models -l model=<model-name>,version=<version>
kubectl logs -n fp-models <model-serving-pod>
```

### Step 5 — Check for dependency health

For `gRPC 14 (Unavailable)` errors, the service cannot reach a dependency:

```bash
# Is the service's database reachable?
# (Replace with the specific service's DB name: fp_auth, fp_registry, etc.)
kubectl port-forward -n fp-infra svc/postgres 5432:5432 &
psql "postgresql://fp:<password>@localhost:5432/fp_registry" -c "SELECT 1"

# Is NATS reachable?
kubectl port-forward -n fp-infra svc/nats 4222:4222 &
# Use nats CLI if available:
nats sub "fp.>" --server=nats://localhost:4222 &
nats pub "fp.test.ping" "hello" --server=nats://localhost:4222

# Is Redis reachable (for registry CQRS read path)?
kubectl port-forward -n fp-infra svc/redis 6379:6379 &
redis-cli -p 6379 PING
```

### Step 6 — Test the service directly with grpcurl

```bash
# Port-forward the failing service:
kubectl port-forward -n fp-system svc/<service-name> 8080:8080

# List available RPCs:
grpcurl -plaintext localhost:8080 list

# Call a specific RPC to see the raw error:
# Auth — ValidateToken:
grpcurl -plaintext -d '{"token":"<a-valid-token>"}' \
  localhost:8080 forgepoint.auth.v1.AuthService/ValidateToken

# Registry — GetModel:
grpcurl -plaintext \
  -H "authorization: Bearer <token>" \
  -d '{"id":"<model-id>"}' \
  localhost:8080 forgepoint.registry.v1.RegistryService/GetModel

# Inference gateway — HTTP predict (it exposes HTTP, not gRPC directly):
kubectl port-forward -n fp-system svc/inference-gateway 8080:8080
curl -X POST http://localhost:8080/v1/models/<model-name>/predict \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"inputs": {"feature1": [1.0, 2.0]}}'
```

---

## Mitigation

### Transient spike (error rate returns to normal quickly)

If the Prometheus graph shows a brief spike (< 5 minutes) that has already
resolved, and the 5m burn rate has dropped back below threshold:
1. Confirm both windows (1h + 5m for fast burn) are now below the threshold.
2. Acknowledge the alert. No further action unless the spike repeats.
3. Create a ticket to investigate root cause during business hours.

### Backend model-serving failure (gateway CircuitBreakerOpen)

```bash
# 1. Check if the model-serving pod is unhealthy:
kubectl get pods -n fp-models -l model=<model-name>
kubectl describe pod <pod-name> -n fp-models

# 2. If the pod is crash-looping, restart it:
kubectl rollout restart deployment/model-<name>-<version> -n fp-models

# 3. The circuit breaker will transition to HALF-OPEN after its configured
#    timeout and probe the backend. Once the probe succeeds it closes automatically.
#    You can force a probe by waiting for the timeout (default: 30s) or by
#    restarting the gateway to reset breaker state:
kubectl rollout restart deployment/inference-gateway -n fp-system
```

### Registry read path stale (CQRS projection lag)

If GetModel is returning NotFound for models that exist in Postgres:

```bash
# Check if the NATS projection consumer is running (it subscribes to fp.models.>):
kubectl logs -n fp-system -l app.kubernetes.io/name=registry | \
  grep -E "projection|subscriber"

# Check Redis directly — does the key exist?
kubectl port-forward -n fp-infra svc/redis 6379:6379 &
redis-cli -p 6379 GET "model:<model-id>"

# Force rebuild of the Redis projection by restarting the registry
# (the projection subscriber re-consumes events from the NATS stream on start):
kubectl rollout restart deployment/registry -n fp-system
```

### Auth errors (JWT key or DB issue)

```bash
# Verify the JWT secret is correctly mounted:
kubectl exec -n fp-system <auth-pod> -- env | grep JWT_SECRET

# Check if the auth DB is reachable from within the pod:
kubectl exec -n fp-system <auth-pod> -- \
  sh -c 'nc -z postgres.fp-infra.svc.cluster.local 5432 && echo ok || echo fail'

# If the JWT signing key was rotated, old tokens become invalid.
# Verify the currently mounted secret matches the one used to issue active tokens.
kubectl get secret auth-secret -n fp-system -o jsonpath='{.data.jwt-secret}' | base64 -d
```

---

## Error budget status query

```bash
# How much budget remains for this month?
# PromQL — fraction of budget consumed (1 = fully consumed):
(
  sum_over_time(fp:inference_gateway:http_error_ratio:5m[30d])
  / count_over_time(fp:inference_gateway:http_error_ratio:5m[30d])
) / 0.001
```

---

## Escalation

| Condition | Action |
|-----------|--------|
| Fast-burn alert firing > 15 minutes | Escalate to senior engineer; page stakeholders if external API affected |
| Root cause not identified in 30 min | Bring in second engineer; consider rollback |
| Error budget < 10% remaining | Engineering lead notification; mandatory reliability sprint |
| Auth errors causing platform-wide outage | P0; use service-down.md escalation path |
