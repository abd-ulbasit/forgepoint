# ADR 0008: Istio service mesh — STRICT mTLS, deny-by-default authz, mesh resilience

**Status:** Accepted
**Date:** 2026-06-18
**Deciders:** Abdul Basit Sajid
**Context phase:** M3 Production hardening — Phase 10 (Istio service mesh), per CLAUDE.md build priority

## Context

Forgepoint is 10 microservices that talk **east-west** over gRPC (BFF → 7 services;
model-monitor → orchestrator for closed-loop retrain; orchestrator → registry /
inference-gateway / model-serving via saga step executors; inference-gateway →
model-serving for predictions). Today the security and resilience of those hops lives
**in the application + library layer**:

- **Identity / authz:** the app validates a user JWT in-process (`pkg/auth`, shared
  HMAC secret) — it answers *"which USER may run this RPC?"* but says **nothing** about
  *which WORKLOAD* opened the connection. A compromised pod in `fp-system` can replay a
  stolen JWT to any service; there is no cryptographic check that the caller IS the BFF.
- **Transport:** in-cluster gRPC is **plaintext**. A NetworkPolicy (where the CNI
  enforces it — k3s' Flannel does **not**) restricts by IP/namespace, but nothing
  **encrypts** the traffic or **authenticates** the peer.
- **Resilience:** the inference-gateway has an excellent **application** circuit breaker
  (`internal/domain/circuit_breaker.go`, 3-state, per model+version) and the saga does
  **application** traffic splitting (`weight_bps`). But there is no **transport-level**
  ejection of a wedged pod, and no exact L7 traffic weighting independent of replica
  counts.

The forces: we want **zero-trust east-west** (encrypt + cryptographically identify
every service hop), **defense-in-depth authz** (workload identity *and* user identity),
**uniform L7 observability** (golden metrics/traces for every hop without per-service
code), and **mesh-native traffic management** (exact canary weighting, transport
circuit breaking) — **without** rewriting the services and **without** discarding the
app-layer patterns that are themselves the teaching artifacts (ADR 0007).

And one hard constraint: the homelab k3s node has ~7 GB RAM and cannot host istiod +
sidecars, so whatever we choose must be **authorable and correct now, installable
later** on a bigger cluster (M5 EKS).

## Options Considered

### Option A — Stay library-only (no mesh)

Keep doing identity/resilience in app code + libraries; rely on NetworkPolicy for
isolation.

- **Pro:** zero new infra; no sidecar memory/latency tax; simplest to run on the homelab.
- **Pro:** the app circuit breaker / saga split / JWT auth already exist and are the
  ADR-0007 teaching patterns.
- **Con:** **no transport encryption** and **no workload identity** — the zero-trust
  story is missing its foundation; a stolen JWT from any pod is fully usable.
- **Con:** NetworkPolicy is **IP/namespace**-grained and is a **no-op on Flannel** — not
  real enforcement, and never identity-based.
- **Con:** L7 observability (per-hop golden signals, distributed-trace spans for every
  call) must be hand-rolled per service.

### Option B — Adopt a service mesh: Istio, STRICT mTLS + deny-by-default authz (chosen)

A sidecar mesh provides mTLS, SPIFFE workload identity, identity-based authz, and
traffic management **transparently** — the services don't change.

- **Pro:** **STRICT mTLS** encrypts + mutually authenticates **every** east-west hop;
  every connection carries an unspoofable SPIFFE identity minted from the pod's
  ServiceAccount. This is the missing zero-trust foundation.
- **Pro:** **deny-by-default AuthorizationPolicy** on the SPIFFE principal is a second,
  orthogonal authz layer (workload identity) **on top of** the app JWT (user identity)
  — true defense in depth.
- **Pro:** **outlier detection** adds transport-level circuit breaking per endpoint, and
  **VirtualService weights** give exact L7 canary splitting — both **complement** the
  app-layer breaker/split rather than replacing them.
- **Pro:** free, uniform L7 telemetry + mTLS cert rotation; services untouched.
- **Con:** **operational complexity + cost** — istiod, a sidecar per pod (~50–100 MB +
  a small latency hop), a new CRD surface, and a real learning/operating burden.
- **Con:** footguns (probes under STRICT, stateful infra in the mesh, the
  empty-policy-means-deny-all semantic) must be understood or they cause outages.

### Option C — A lighter mesh (Linkerd) or ambient/sidecar-less

Linkerd (simpler, Rust micro-proxy, lower overhead) or Istio **ambient** mode (no
per-pod sidecar; a per-node ztunnel for L4 mTLS + optional waypoints for L7).

- **Pro:** lower memory/latency overhead; Linkerd is famously simpler to operate.
- **Pro:** Istio ambient removes the per-pod sidecar tax — attractive for a small cluster.
- **Con:** Linkerd's L7 authorization + traffic-management surface is **less rich** than
  Istio's (no equally expressive AuthorizationPolicy/VirtualService); weaker as a
  *teaching* instrument for the patterns we want to show.
- **Con:** Istio ambient's L7 (waypoint) story was still maturing at authoring time;
  the **sidecar** model is the canonical, best-documented one to *learn and explain*.

## Decision

Adopt **Option B**: an **Istio sidecar mesh** with **mesh-wide STRICT mTLS**, a
**deny-by-default AuthorizationPolicy** allow-list encoding the real call graph, and
mesh resilience (**outlier detection** + **weighted VirtualService canary**), all
**authored and validated now, installed on a larger cluster later** (homelab can't host
it). Manifests live in `deploy/istio/`.

Deciding factors:

1. **The mesh adds exactly the layers the app can't.** mTLS encryption + SPIFFE
   workload identity are *transport-layer* facts the application fundamentally cannot
   provide itself. The mesh fills the zero-trust gap **without touching service code**.
2. **It complements, never duplicates, the ADR-0007 patterns.** We keep the app circuit
   breaker and saga traffic split (they're the teaching patterns) and **add** the mesh's
   transport-level twins beside them — the layering itself is the senior-engineer story.
3. **Istio over Linkerd/ambient for teaching depth.** The richest, most-explainable
   AuthorizationPolicy/VirtualService/DestinationRule surface — the project optimizes for
   interview-explainable depth (CLAUDE.md), and Istio's sidecar model is the canonical one.

### STRICT mTLS (not PERMISSIVE)

`PeerAuthentication` is set **STRICT** mesh-wide (`security.istio.io/v1`, root namespace
`istio-system`). PERMISSIVE accepts plaintext *and* mTLS (the migration default); STRICT
accepts **only** mTLS. We ship STRICT because it is the actual zero-trust end-state:
every accepted byte is encrypted and from a cryptographically-identified peer. The
**migration ramp** (mesh everything under PERMISSIVE → verify all traffic is already
mTLS via auto-mTLS → *then* flip STRICT) is documented in the README; we author the end
state because the homelab is authored-not-applied.

**Probes survive STRICT** via Istio's default **probe rewrite** (kubelet → pilot-agent
:15020 → app loopback, bypassing the mTLS inbound listener), reinforced by Forgepoint's
existing **two-port split** (health 8080 ≠ gRPC 9090) — so the charts' `httpGet /healthz`
probes need **zero** changes. (Full mechanism in `deploy/istio/10-peerauthentication.yaml`.)

### Deny-by-default authz + SPIFFE identity vs. the app JWT (defense in depth)

Each namespace gets an **empty-spec `ALLOW` AuthorizationPolicy** — which in Istio's
additive-allow model means **deny-all** — plus explicit `ALLOW`s pinning each edge of
the **verified** call graph to the caller's SPIFFE principal
(`cluster.local/ns/<ns>/sa/fp-<svc>`) on the gRPC port. This is **orthogonal** to the app
JWT:

| Layer | Identity | Question answered | Spoofable? |
|---|---|---|---|
| **Mesh authz** (this ADR) | SPIFFE cert from the pod's ServiceAccount | *Which WORKLOAD may open this connection?* | No — minted by istiod, mTLS-verified |
| **App authz** (`pkg/auth`, unchanged) | the end-user's bearer JWT | *Which USER/role may run this RPC?* | Mitigated by HMAC signature |

A stolen user JWT replayed from a pod that **isn't** the BFF is blocked at the mesh
(wrong workload identity); a compromised BFF pod is allowed by the mesh but still needs a
valid user JWT + role at the app. **Neither layer alone suffices; together they are
zero-trust.** This mirrors SPIFFE/SPIRE's "identity is the new perimeter" — Istio ships
its own SPIFFE-compliant CA; SPIRE is the standalone equivalent.

### Mesh circuit breaking vs. the app circuit breaker (the layering)

Istio **outlier detection** (`DestinationRule`) ejects a misbehaving **endpoint (pod IP)**
from the load-balancing pool on consecutive transport errors — pure L4/L7 health, knows
nothing about models. The inference-gateway's **app breaker** trips per **model+version**
on *business* signals and serves a domain-aware **fallback**. We keep **both**: the mesh
breaker stops hammering a crashed pod even before the app notices; the app breaker
provides the business-correct degradation the mesh can't know about. Same defense-in-depth
shape as the authz layering. (Full contrast table in `deploy/istio/20-destinationrules.yaml`.)

### Cost / complexity tradeoff & why authored-not-applied on the homelab

A sidecar mesh costs real memory (~50–100 MB/pod × ~22 pods ≈ 1–2 GB of sidecars **plus**
istiod) and adds a per-hop latency tax and a CRD operational surface. The homelab k3s node
(~7 GB) cannot host it. So Phase 10 is **manifest authoring**: every object is real,
uses current API versions (`security.istio.io/v1`, `networking.istio.io/v1`), parses as a
multi-doc load, cross-checks every selector/SA against the actual Helm output, and is
`istioctl`-installable — but it is **not** applied to the homelab. It installs on a bigger
cluster (multi-node Kind / larger k3s / **EKS via M5 Terraform**).

## Consequences

- **Positive:** zero-trust east-west (every hop encrypted + identity-checked) with **no
  service code change**; a second, workload-identity authz layer beside the user-JWT
  layer; transport-level circuit breaking + exact L7 canary weighting that **complement**
  the app patterns; uniform mTLS rotation + L7 telemetry for free.
- **Negative:** real operational complexity and resource cost (istiod + sidecars), a new
  CRD surface, and well-known footguns (STRICT-vs-probes, stateful infra exclusion, the
  empty-policy-deny-all semantic) that must be understood — documented inline in each
  manifest and in the README so they're explainable, not magic.
- **Negative / honest:** **not running on the homelab.** The mesh is validated and
  installable, not live; the *enforced* security on the homelab remains the app JWT (+
  NetworkPolicy where a real CNI exists). We name this rather than imply a meshed cluster.
- **Interview framing:** *"You already have a circuit breaker and JWT auth and a traffic
  split — why a mesh?"* → because those are **application-layer** facts; the mesh adds the
  **transport-layer** facts they cannot provide (encryption + unspoofable workload
  identity) and **complements** the app patterns with their transport-level twins. The
  layering — app breaker **and** outlier detection, user JWT **and** SPIFFE authz, saga
  split **and** VirtualService weights — is deliberate defense in depth, not duplication.
- **Resolved — model-serving namespace = `fp-models` everywhere.** An earlier draft of
  this ADR framed the split as "Helm `fp-models` vs Rollouts `fp-system`" and called Helm
  authoritative. That framing was **wrong**: the Helm chart is *namespace-agnostic* (every
  template renders `metadata.namespace: {{ .Release.Namespace }}`), so it never pinned
  `fp-models`. The *install path* actually resolved serving to `fp-system` for BOTH Helm
  (the Makefile forced `--namespace fp-system` for all charts) and the Argo Rollouts
  manifests (hardcoded `fp-system`) — while the BFF config, the gateway/BFF egress
  allow-lists, the CLAUDE.md namespace map, and these mesh objects all expected `fp-models`.
  Net effect of the bug: the fp-models `DestinationRule` + `AuthorizationPolicy` selected
  **zero** pods while serving ran in `fp-system` under the deny-all baseline with no allow
  for inference-gateway → serving — the real predict edge was BLOCKED/misrouted and the
  policy meant to protect serving protected nothing. Fixed by making the *install target*
  match the platform's authoritative `fp-models`: the Makefile now derives `fp-models` for
  `SVC=model-serving`, and `deploy/rollouts/{rollout,services,analysis-template}-model-serving.yaml`
  pin `fp-models` (the rollouts `kustomization.yaml` dropped its global `namespace:` that
  was overriding them). The namespaced AnalysisTemplates moved with the Rollout (a Rollout
  resolves `templateName` in its own namespace).
- **Follow-ups:** wire the canary VirtualService to Argo Rollouts'
  `trafficRouting.istio` for exact analysis-gated promotion (README); tighten the assumed
  namespace-broad feature-store allow to the real caller SA once a feature-read client is
  wired; add a 443 HTTPS Gateway server with a cert-manager `credentialName` when M5
  provisions DNS + ACM.
```
