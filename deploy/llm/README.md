# LLM Serving (Ollama) — M7 / L0

In-cluster LLM serving for the Forgepoint AI Gateway. The model is deliberately
**tiny** — the subject is the AI-platform infrastructure (gateway, routing, cache,
budgets, eval), not the model — exactly as the classic-ML side uses thin ONNX models.

## What runs here

| Piece | File | Notes |
|---|---|---|
| Ollama Deployment + PVC + Service | `ollama.yaml` | ns `fp-ml`, model on a PVC (survives scale-to-zero), `keep_alive=0`, mem-capped |
| KEDA scale-to-zero | `ollama-scaledobject.yaml` | warm-on-demand via NATS JetStream; activated by the L1 gateway |

**Model:** `smollm2:135m` (~270 MB on disk, ~380 MB loaded). Fallbacks if RAM allows:
`smollm2:360m`, `qwen2.5:0.5b`. Pulled once into the PVC.

## The hard constraint (and why the config is what it is)

This homelab is a single **k3s node, ~7 GB RAM, no GPU**, shared with a ~4 GB TTS
workload. Measured headroom: ~0.5–1.3 GB free + 12 GB swap. So:

- **`OLLAMA_KEEP_ALIVE=0`** — the model unloads the instant a request finishes;
  resident only during inference. (Measured: free RAM recovered 110 MB → 635 MB
  immediately after a generation.)
- **cgroup `MemoryMax`/limits = 1800Mi** — a hard ceiling; under memory pressure
  the kernel OOM-kills **Ollama**, never the TTS or Postgres.
- **Low memory *request* (256Mi)** so it schedules on the tight node (k8s schedules
  on requests, not live free RAM); high *limit* as the inference ceiling.
- **KEDA scale-to-zero** — zero pods (and zero RAM) when there is no traffic.

**Measured cold start:** ~6 s (model load from PVC + generate, under swap). Slow but
fine for a demo. For *responsive* real traffic, the gateway can route to cloud
providers (`gpt-4o-mini`/Haiku); local Ollama is the self-hosted pattern.

## Bring-up (L0 runbook)

```bash
KC=~/.kube/forgepoint-thinkpad.yaml
# 1) KEDA core (once)
helm --kubeconfig $KC install keda kedacore/keda -n keda --create-namespace --wait
# 2) Serving
kubectl --kubeconfig $KC apply -f deploy/llm/ollama.yaml
kubectl --kubeconfig $KC -n fp-ml rollout status deploy/ollama   # image pull is ~1.7GB
# 3) Pull the model into the PVC (once; persists across scale-to-zero)
kubectl --kubeconfig $KC -n fp-ml exec deploy/ollama -- ollama pull smollm2:135m
# 4) Smoke test through the cluster
kubectl --kubeconfig $KC -n fp-ml exec deploy/ollama -- \
  ollama run smollm2:135m "one sentence: what is a circuit breaker?"
# 5) scale-to-zero (apply with the L1 gateway that feeds the AI_REQUESTS stream)
# kubectl --kubeconfig $KC apply -f deploy/llm/ollama-scaledobject.yaml
```

## Status

- L0 serving: **working** (model pulled, chat verified through the ClusterIP Service,
  TTS + cluster unharmed).
- Scale-to-zero `ScaledObject`: **authored**, applied together with the L1 AI Gateway
  (which creates the `AI_REQUESTS` stream + publishes the warm signal).
