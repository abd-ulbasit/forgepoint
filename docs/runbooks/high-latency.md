# Runbook: HighLatency

**Alert:** `InferenceGatewayHighLatency`
**Threshold:** p99 latency > 500ms for 5 minutes
**Severity:** Warning
**SLO impact:** Indirect — latency SLO is threshold-based (no error budget yet)
**Last reviewed:** 2026-06-18

---

## Symptom

The inference gateway p99 request latency has exceeded 500ms for at least 5
minutes. The recording rule `fp:inference_gateway:latency_p99:5m` is above 500.

Note: 500ms is the informal target; the *page-level* threshold for the current
homelab setup. The prediction hot path budget is:
- Gateway overhead: ~50ms (auth check, routing, serialization)
- Model serving (ONNX inference): ~100–200ms typical
- Total budget: 300ms target, 500ms alert threshold

If p99 > 500ms, something in the chain is adding unexpected latency.

---

## Impact

- End-users and API clients experience slow predictions.
- Downstream clients with < 500ms timeout budgets will start seeing timeouts
  (which count as errors and will trigger the error rate alerts).
- The BFF dashboard (Phase 14) will render slowly on the prediction pane.
- k6 SLAs (Phase 13) will flag SLO regression in CI.

---

## Diagnosis steps

### Step 1 — Identify where the latency is

The distributed trace in Tempo is the most efficient tool here. Each span has its
own duration, so you can see immediately whether the gateway itself is slow or
it's waiting on a backend.

```bash
# Port-forward Grafana:
kubectl port-forward -n fp-infra svc/grafana 3000:3000
# Navigate to: Explore → Tempo → Search
# Filter: service.name = "inference-gateway", duration > 500ms
# Open the slowest trace and expand all spans.
```

Expected trace spans and their normal durations:
```
gateway.predict           total: ~150ms
  ├── auth.ValidateToken       ~10ms
  ├── gateway.route-lookup     ~1ms
  ├── gateway.circuit-check    ~1ms
  ├── gateway.rate-limit       ~2ms (Redis lookup)
  └── serving.Predict          ~120ms (ONNX inference)
```

If any span far exceeds its normal duration, that is the bottleneck.

### Step 2 — Query the histogram breakdown in Prometheus

```bash
kubectl port-forward -n fp-infra svc/prometheus 9090:9090

# p50/p95/p99 for the gateway:
histogram_quantile(0.99,
  sum by (le) (
    rate(http_server_duration_milliseconds_bucket{
      service_name="inference-gateway"
    }[5m])
  )
)

# Is latency isolated to specific model names? (needs model_name label on the metric)
histogram_quantile(0.99,
  sum by (le, model_name) (
    rate(http_server_duration_milliseconds_bucket{
      service_name="inference-gateway"
    }[5m])
  )
)
```

### Step 3 — Check model-serving backend latency

```bash
# p99 latency at the model-serving gRPC layer:
histogram_quantile(0.99,
  sum by (le, service_name) (
    rate(rpc_server_duration_milliseconds_bucket{
      service_name=~"model-serving.*"
    }[5m])
  )
)

# Check if a model-serving pod is resource-constrained (CPU throttling):
kubectl top pods -n fp-models
kubectl describe pod <model-serving-pod> -n fp-models | grep -A5 "Requests\|Limits"
```

CPU throttling is the most common cause of ONNX inference latency spikes on the
homelab. If the pod is hitting its CPU limit, the kernel throttles the container
and inference takes longer.

### Step 4 — Check Redis rate-limiter latency (gateway path)

```bash
# If Traces show the rate-limiter span is slow, Redis may be overloaded:
kubectl top pods -n fp-infra -l app.kubernetes.io/name=redis
kubectl logs -n fp-infra -l app.kubernetes.io/name=redis --tail=50

# Direct Redis latency test:
kubectl port-forward -n fp-infra svc/redis 6379:6379 &
redis-cli -p 6379 --latency
```

### Step 5 — Check auth latency (ValidateToken path)

```bash
# p99 for ValidateToken specifically:
histogram_quantile(0.99,
  sum by (le) (
    rate(rpc_server_duration_milliseconds_bucket{
      service_name="auth",
      rpc_method="ValidateToken"
    }[5m])
  )
)
```

If auth is slow, check its Postgres connection pool:
```bash
kubectl logs -n fp-system -l app.kubernetes.io/name=auth | \
  grep -E "pool|timeout|slow"
```

### Step 6 — Check for resource pressure on the gateway pod itself

```bash
kubectl top pods -n fp-system -l app.kubernetes.io/name=inference-gateway
kubectl describe pod <gateway-pod> -n fp-system
# Look for: CPU throttling (the throttled_periods_total in cgroup)
# Look for: GC pause (Go runtime GC stops the world briefly)
# Look for: OOM pressure (check dmesg or pod events)
```

---

## Mitigation

### Model-serving CPU throttled

```bash
# Increase the CPU limit for the affected model-serving deployment:
kubectl patch deployment model-<name>-<version> -n fp-models \
  --patch '{"spec":{"template":{"spec":{"containers":[{"name":"model-serving","resources":{"limits":{"cpu":"2000m"},"requests":{"cpu":"500m"}}}]}}}}'
```

For the homelab, the node likely has only 4–8 CPUs shared across all pods. If
multiple models are running, you may need to reduce the number of serving instances
or add a dedicated node.

### Redis latency spike (rate limiter)

```bash
# Restart Redis (will clear all in-memory rate limit counters — brief counting gap):
kubectl rollout restart deployment/redis -n fp-infra

# Or: reduce the rate-limiter check frequency in the gateway config
# (FP_RATE_LIMITER_REDIS_TIMEOUT env var)
```

### Gateway pod has high GC pressure

```bash
# Check if the gateway is allocating excessively (common on high request volume):
kubectl port-forward -n fp-system <gateway-pod> 6060:6060 &
# Go pprof heap profile (requires the service to have pprof enabled):
curl http://localhost:6060/debug/pprof/heap > heap.prof
go tool pprof -http=:8888 heap.prof
```

### Restart the gateway to clear stuck connections

If no root cause is found and latency is sustained:
```bash
kubectl rollout restart deployment/inference-gateway -n fp-system
kubectl rollout status deployment/inference-gateway -n fp-system
```

This clears any stuck HTTP/gRPC connections and resets the circuit breaker state.
Monitor latency in Grafana immediately after the rollout.

---

## Escalation

| Condition | Action |
|-----------|--------|
| Latency spike is also causing 5xx (p99 > 1s timeouts) | Switch to high-error-rate.md runbook |
| Model-serving pod cannot be resource-increased (node full) | Page infrastructure owner |
| No improvement after gateway restart | Escalate to senior engineer |
| Latency is model-specific (ONNX model too large) | Work with ML team to optimize or quantize model |
