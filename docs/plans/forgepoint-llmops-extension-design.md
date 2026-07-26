# Forgepoint LLMOps Extension — Design Doc (M7)

**Status:** Draft for sign-off · **Author:** AI-implemented, developer-directed · **Date:** 2026-06-18

This extends Forgepoint from a **classic MLOps** platform (train→register→serve→monitor→retrain)
into an **LLMOps / AI-platform** layer (gateway → serve → cache → eval → govern), reusing the exact
distributed-systems patterns already built. It is M7 in the roadmap.

---

## 1. Why this, why now

By 2026 the hiring/spend center of gravity for "Platform Engineer (Go/K8s)" moved from classic
MLOps (commoditized) to **AI/LLM infrastructure**: AI gateways, LLM serving, semantic caching,
token-cost governance, evals/quality monitoring. The decisive insight for Forgepoint:

> **The platform patterns are model-size-agnostic.** An AI gateway's circuit-breaker, token
> budget, semantic cache, and streaming code is byte-identical whether the backend is a 135 M
> local model or GPT-4. So we keep the *model* thin (smallest local models) and make the
> *infrastructure* real — the same "domain as a vehicle" principle that governs the ONNX side.

**Thesis:** the classic-MLOps patterns extend to LLM infrastructure unchanged — provider
failover via circuit breaker, per-tenant token budgets via distributed rate-limiting, semantic
cache, cost chargeback, and eval-gated quality monitoring.

---

## 2. Compute reality (the hard constraint) — smallest models, on the thinkpad

The homelab is a single **k3s node, ~7 GB RAM, no GPU**, already running 10 services + BFF + infra.
Per the directive: **run the LLMs locally on the thinkpad via Ollama — no Mac, no GPU, smallest
possible models, and do not let them sit resident.**

| Concern | Decision |
|---|---|
| Runtime | **Ollama** (CPU inference, GGUF, one binary, OpenAI-compatible `/api/chat` + `/api/embeddings`) |
| Chat model | **`qwen2.5:0.5b-instruct`** (~0.4 GB, genuinely usable instruct quality) — fallback `smollm2:360m` (~0.3 GB) or `smollm2:135m` (~0.14 GB) if RAM is tight |
| Embeddings | **`all-minilm`** via Ollama (~0.05 GB, 384-dim) — one tool for chat + embeddings |
| Residency | **KEDA scale-to-zero**: the Ollama Deployment scales 1→0 when idle and 0→1 on demand; plus Ollama `keep_alive=30s` so the model unloads from RAM between requests. The model is resident only while actually answering — and that *showcases* the autoscaling pattern rather than fighting the RAM limit. |
| Peak budget | A single 0.5 B CPU inference peaks ~1–1.5 GB, only while generating; idle footprint ≈ 0 (scaled to zero). Fits alongside the running cluster. |
| Providers | **Local Ollama + a deterministic Stub provider** are the built-in, free, self-contained set. Cloud providers (OpenAI `gpt-4o-mini`, Anthropic `claude-haiku`) are an **optional, key-gated** plug-in — the multi-provider *failover/routing* story is fully demonstrable with **two local Ollama models + the stub**, so no cloud account is required. |

**Testing tiers:** (1) **Stub provider** — deterministic streamed tokens, no model/network → all CI
+ gateway-logic unit/integration tests (routing, failover, budgets, cache, streaming). (2) **Local
Ollama tiny model** — real tokens/latency for component + live E2E. (3) **Cloud (optional)** — only
if keys are present, for the genuine cross-provider demo.

---

## 3. New components and how they map to what exists

```
                       ┌──────────── AI Gateway (new) ────────────┐
 Web Chat/Playground → │ routing · provider failover · streaming  │ → Provider:
 (BFF SSE)             │ token budgets · semantic cache · guardrail│    • Ollama (thinkpad, KEDA 0→1)
                       └───────────────┬──────────────────────────┘    • Stub (tests)
                                       │  emits usage events                • Cloud (optional)
                   ┌───────────────────┼─────────────────────┬───────────────┐
            Prompt Registry      Billing (outbox)        Model-Monitor    Eval store
            (versioned prompts)  token-cost metering   (LLM-as-judge evals,
            reuse Registry CQRS                          quality drift → closed loop)
```

| New / extended | Pattern reused | From |
|---|---|---|
| **AI Gateway** routing + provider failover | **Circuit breaker** | inference-gateway |
| Per-tenant **token budgets** | **Distributed rate-limiter** (Redis) | inference-gateway |
| Prompt/model **A-B + canary** | **Traffic splitting** | model-serving / Argo Rollouts |
| **Token-cost metering / chargeback** | **Outbox** reliable events | billing |
| **LLM serving** (Ollama) autoscale | **HPA/KEDA** scale-to-zero | model-serving / KEDA |
| **Prompt Registry** (versioned prompts) | **CQRS** (write PG / read Redis) | registry |
| **Eval / quality drift → retrain/alert** | **Streaming drift + closed loop** | model-monitor |
| **Token streaming** to browser | **SSE** | BFF |
| **Audit** of prompts/responses | hash-chained **audit log** | pkg/audit |
| Event envelope, idempotent consumers | **NATS protojson** | pkg/natsutil |

Net new code is mostly *domain glue + a provider abstraction* — the hard infra already exists.

---

## 4. Architecture decisions (need sign-off)

**D1 — New `ai-gateway` service vs. evolve `inference-gateway`.**
- *A. Evolve inference-gateway* — it already owns circuit-breaker/rate-limit/traffic-split; least new scaffolding. Risk: conflates classic-model routing with LLM concerns (prompts, providers, streaming, cache) in one domain.
- *B. New `ai-gateway` service* (recommended) — clean domain separation; reuses the same `pkg/` patterns; the classic gateway stays focused. More scaffolding (proto, module, chart, deploy).
- **Recommendation: B.** Cleaner story; the two gateways share `pkg/` not domain.

**D2 — LLM serving deployment.**
- *A. Ollama as a k3s Deployment + KEDA scale-to-zero* (recommended) — in-cluster, demonstrates autoscaling, network-policy-governed. Needs a KEDA HTTP/queue trigger + a cold-start budget (~2–5 s first token).
- *B. Ollama as a host systemd service on the thinkpad* — simplest, lowest overhead, but outside the k8s story (no autoscaling demo).
- **Recommendation: A**, model `qwen2.5:0.5b`, `keep_alive=30s`, memory limit ~2 Gi, with B as the fallback if cold-start hurts demos.

**D3 — Provider set.** Built-in **Ollama + Stub**; **cloud optional** (config + secret-gated). Recommendation: ship Ollama+Stub; add an `OpenAIProvider`/`AnthropicProvider` behind `AI_PROVIDERS` config so the cross-provider failover demo works with *two local models* by default and *cloud* if keys exist.

**D4 — Semantic cache.** Embed prompt via Ollama `all-minilm` → cosine-similarity lookup in Redis (manual cosine over a bounded recent set, or RediSearch vector if we add the module). Recommendation: manual cosine over a Redis-stored bounded window first (no new infra), RediSearch as a stretch.

**D5 — Prompt registry.** Reuse the registry CQRS pattern as a **focused `prompt-registry`** (versioned prompts, stages dev/prod, render-with-variables) — or fold into ai-gateway initially. Recommendation: start as a module inside ai-gateway, extract to its own service in a later phase (avoids premature service sprawl).

**D6 — Eval/quality.** Extend **model-monitor** with an `LLMEvaluator` (LLM-as-judge using the local model: relevance/toxicity/groundedness scores) + quality-drift detection → the existing alert/retrain closed loop. Recommendation: extend model-monitor (keep the closed-loop story in one place).

**D7 — Cost/budgets.** Token budgets enforced in the gateway via the Redis rate-limiter (tokens/min/team); usage emitted via **billing's outbox** for chargeback. No new pattern.

---

## 5. APIs / events (sketch)

- `proto/forgepoint/ai/v1/ai_gateway.proto` — `ChatCompletion(stream)`, `ListProviders`, `GetUsage`; messages carry model, messages[], stream flag, token/cost in the response trailer.
- `proto/forgepoint/prompt/v1` (later) — `CreatePrompt`, `GetPrompt(version|stage)`, `RenderPrompt`.
- Events (protojson, `fp.ai.*`): `fp.ai.completion.served` (tokens, cost, cache_hit, provider, latency), `fp.ai.budget.exceeded`, `fp.ai.eval.scored`, `fp.ai.quality.drift`.
- BFF: `POST /api/v1/chat` (SSE stream), `GET /api/v1/prompts`, `GET /api/v1/ai/usage`, `GET /api/v1/evals`.
- Web: a **Chat/Playground** page (streaming), a **Prompts** page, an **Evals/quality** dashboard.

---

## 6. Milestones (L-phases)

- **L0 — Foundation:** Ollama on thinkpad (k3s Deployment + KEDA scale-to-zero) with `qwen2.5:0.5b` + `all-minilm`; the `Provider` interface + `StubProvider` + `OllamaProvider`; smoke test (chat + embed). *Pattern: serving + autoscale.*
- **L1 — AI Gateway core:** `ai-gateway` service — chat routing, **streaming**, **circuit-breaker provider failover**, **per-tenant token budgets** (rate-limiter), basic guardrails (PII/length). Proto→domain→handler→events→BFF SSE→Web chat page. *Adversarially verified.*
- **L2 — Semantic cache + cost:** embedding-based cache (Ollama+Redis); token-cost metering via **billing outbox**; usage page.
- **L3 — Prompt registry:** versioned prompts (CQRS), render-with-vars, dev/prod stages, gateway uses them.
- **L4 — Eval + quality drift:** `LLMEvaluator` (LLM-as-judge) in **model-monitor**, quality-drift detection → closed-loop alert/retrain; evals dashboard.
- **L5 — Governance tie-in:** prompt/response **audit** (hash-chained), Kyverno policy gate on provider/model allow-lists; optional cloud provider enablement.

Each phase: proto-first, clean architecture, TDD + adversarial bug/security verification,
deployed to k3s — same bar as M0–M6.

---

## 7. Risks / mitigations

- **Cold start (KEDA 0→1 + model load):** first token ~2–5 s. Mitigate with a warm-pool of 1 during demos, or `keep_alive` tuning; document the tradeoff (latency vs. RAM).
- **RAM pressure:** cap Ollama at ~2 Gi, prefer `0.5b`/`360m`; if the cluster is tight, drop a non-essential service replica or run Ollama host-side (D2-B).
- **Determinism in tests:** the StubProvider (not the model) backs all CI/unit tests, so tests stay fast + hermetic; the live model is for component/E2E only.
- **No GPU:** every choice above is CPU-only by design.

---

## 8. Open decisions for sign-off

1. **D1**: new `ai-gateway` service (recommended) vs. evolve `inference-gateway`?
2. **D2**: Ollama as k3s Deployment + KEDA scale-to-zero (recommended) vs. host systemd?
3. **D3**: ship Ollama+Stub now, cloud providers optional/key-gated (recommended) — or include a cloud provider from L1?
4. **Model**: `qwen2.5:0.5b` (recommended) vs. `smollm2:360m`/`135m` (smaller, lower quality)?
5. **Scope of first cut**: stop after **L1+L2** (gateway + cache + cost — a complete, demoable AI gateway) and assess, or commit the full L0–L5 up front?

On sign-off I will build L0→L1 first (the smallest end-to-end slice: thinkpad Ollama → gateway →
streaming chat in the Web UI), then proceed phase-by-phase.
