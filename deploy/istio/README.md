# Forgepoint Service Mesh (Istio) — Phase 10

> **Status: AUTHORED + validated, NOT applied to the homelab.**
> The homelab k3s node has ~7 GB RAM and no Istio installed; istiod + per-pod Envoy
> sidecars (≈50–100 MB/pod × ~22 pods) would not fit. These manifests are real,
> correct, and `istioctl`-installable on a larger cluster (a multi-node Kind, a
> bigger k3s, or EKS via the M5 Terraform stack). Every YAML here parses as a
> multi-doc load and uses current Istio API versions (`security.istio.io/v1`,
> `networking.istio.io/v1`).

This directory is the **mesh layer** of Forgepoint: mutual-TLS zero-trust transport,
identity-based authorization encoding the real service call graph, mesh-level circuit
breaking, canary traffic splitting, and the public ingress edge. It **complements** the
application layer (the inference-gateway's app circuit breaker, the saga's `weight_bps`
traffic split, the BFF's JWT auth) — it does not replace it. See
`docs/adr/0008-istio-service-mesh.md` for the full rationale and the layering.

---

## What's here

| File | Kind(s) | Purpose |
|------|---------|---------|
| `00-namespaces.yaml` | Namespace labels | Turn on sidecar injection for `fp-system` + `fp-models`; document why `fp-infra` is excluded. |
| `10-peerauthentication.yaml` | PeerAuthentication | Mesh-wide **STRICT mTLS** (zero trust). |
| `20-destinationrules.yaml` | DestinationRule ×3 | Mesh-wide mTLS client default + connection pools + **outlier detection** (mesh circuit breaking) for the hot hosts (model-serving, registry). The model-serving DR also declares the canary `stable`/`canary` **subsets** (one DR per host — resilience + subsets together). |
| `30-authorizationpolicy-fp-system.yaml` | AuthorizationPolicy ×11 | **Deny-all baseline** + explicit allows encoding the real call graph in `fp-system`. |
| `31-authorizationpolicy-fp-models.yaml` | AuthorizationPolicy ×2 | Deny-all + allow `inference-gateway` / `pipeline-orchestrator` → `model-serving`. |
| `40-gateway-bff.yaml` | Gateway + VirtualService | Public ingress edge → the **BFF** (the only public surface, ADR 0001). |
| `50-virtualservice-model-serving.yaml` | VirtualService | **90/10 weighted split** for model-serving across the `stable`/`canary` subsets (subsets declared on the host DR in `20-*.yaml`). The 90/10 is an **illustrative initial state**, not the runtime split — see "Canary endpoints & weights per deploy path" below. |

---

## The encoded call graph (derived from source, not guessed)

```
external ──▶ istio-ingressgateway ──▶ bff            (40-gateway-bff.yaml; ADR 0001 edge)
                                       │
   ┌──────┬──────────┬────────────────┼───────────┬──────────┬──────────────┐
   ▼      ▼          ▼                 ▼           ▼          ▼              ▼
 auth  registry  pipeline-orch    experiment   monitor    billing      notification
          ▲          ▲  ▲             (sink)              (sink)        (sink/reactor)
          │          │  └──────── monitor (closed-loop retrain, ADR 0003)
          │          └─────────── (BFF triggers pipelines)
 pipeline─┘ (saga DEPLOY: register/promote)
 pipeline ──▶ inference-gateway   (saga CANARY: set weight_bps)
 # no ingressgw ─▶ inference-gateway edge: external predict is NOT wired (BFF is the
 # only public surface, ADR 0001). Re-add it when a predict Gateway/VS is authored.

 inference-gateway ──▶ model-serving (fp-models)   (predict forward)   ┐ cross-ns,
 pipeline-orch ──────▶ model-serving (fp-models)   (deploy/health)     ┘ 31-*.yaml

 feature-store: no wired inbound gRPC client found → namespace-broad allow (assumed)
```

**Verified in code:**
- `services/bff/cmd/server/main.go` + `internal/clients/clients.go` → BFF dials **7**
  services (auth, registry, pipeline-orchestrator, experiment-tracker, model-monitor,
  billing, notification).
- `services/model-monitor/internal/config/config.go` (`OrchestratorEndpoint`) → monitor → pipeline-orchestrator.
- `registry / billing / notification / experiment-tracker / feature-store` have **zero**
  outbound gRPC client constructors → callers never, callees only.

**Architectural (port exists, adapter is a stub today — encoded so the mesh is ready):**
- `inference-gateway → model-serving` (`domain.ModelServerClient`).
- `pipeline-orchestrator → registry / inference-gateway / model-serving` (saga step executors).

---

## Prerequisites

```bash
# istioctl, matching the chart versions you'll install (example pins a recent line).
curl -L https://istio.io/downloadIstio | ISTIO_VERSION=1.24.2 sh -
export PATH="$PWD/istio-1.24.2/bin:$PATH"
istioctl version
```

## Install Istio (demo profile — fine for a non-prod cluster)

The **demo** profile installs istiod + an ingress gateway + an egress gateway with
generous defaults and full telemetry — the right profile for validating policy. (Use the
**default** profile for a leaner prod-ish install; `minimal` for control-plane-only.)

```bash
# 1. Pre-flight: will this cluster support the install?
istioctl x precheck

# 2. Install the control plane + ingress/egress gateways.
istioctl install --set profile=demo -y

# 3. (Recommended) install the Istio CNI so the sidecar's iptables setup needs no
#    NET_ADMIN on the app pod — keeps the "restricted" PSS posture the charts use.
#    istioctl install --set profile=demo --set components.cni.enabled=true -y
```

## Apply order (matters)

Apply by filename prefix — the numbering IS the order. Rationale: label namespaces
**before** workloads roll so they get a sidecar; establish mTLS + authz **before**
opening the ingress; routing last.

```bash
kubectl apply -f 00-namespaces.yaml                       # injection on (fp-system, fp-models)

# Roll existing workloads so they pick up a sidecar (they were deployed pre-mesh):
kubectl rollout restart deployment -n fp-system
kubectl rollout restart deployment -n fp-models

kubectl apply -f 10-peerauthentication.yaml               # STRICT mTLS (see migration note below)
kubectl apply -f 20-destinationrules.yaml                 # client mTLS + outlier detection
kubectl apply -f 30-authorizationpolicy-fp-system.yaml    # deny-all + the allow-list
kubectl apply -f 31-authorizationpolicy-fp-models.yaml
kubectl apply -f 40-gateway-bff.yaml                      # public edge → BFF
kubectl apply -f 50-virtualservice-model-serving.yaml     # canary 90/10
```

### Safe STRICT-mTLS migration (do NOT flip a live platform straight to STRICT)

On a cluster that already has traffic, ramp instead of applying `10-` immediately:

1. Apply `00-`, roll all workloads → every pod now has a sidecar. With **no**
   PeerAuthentication, the mesh default is **PERMISSIVE**, and Istio auto-mTLS already
   upgrades meshed↔meshed traffic to mTLS.
2. Verify everything is *already* mTLS (commands below). It will be.
3. **Then** apply `10-peerauthentication.yaml` (STRICT). Because traffic is already
   mTLS, this only closes the door on any straggler plaintext path — which step 2
   proved is none.

(Authored-not-applied here, so the files ship the **end state**, STRICT.)

---

## Verify mTLS + authz

```bash
# Is the workload's traffic mTLS? (per-pod TLS posture)
istioctl x describe pod -n fp-system <bff-pod>

# Client/server cert + listener config for a pod (look for ISTIO_MUTUAL, the SPIFFE
# URI SAN spiffe://cluster.local/ns/fp-system/sa/fp-bff):
istioctl proxy-config secret -n fp-system <bff-pod>
istioctl proxy-config listener -n fp-system <bff-pod> --port 9090 -o json | grep -i tls

# (Older istioctl) the classic check:
istioctl authn tls-check <bff-pod>.fp-system fp-registry.fp-system.svc.cluster.local

# Confirm authz: from a NON-allowed pod, a call to registry:9090 should be RBAC-denied.
# From the BFF pod it should succeed. Watch the registry sidecar's RBAC denials:
kubectl logs -n fp-system <registry-pod> -c istio-proxy | grep -i rbac

# Routing: confirm the 90/10 split is programmed into the gateway's sidecar.
istioctl proxy-config route -n fp-system <inference-gateway-pod> \
  --name fp-model-serving.fp-models.svc.cluster.local -o json
```

A clean run shows: every east-west connection `ISTIO_MUTUAL`; a non-listed caller
`RBAC: access denied`; the BFF reachable from the ingress on `forgepoint.local`; and
the model-serving route weighted 90/10 across the `stable`/`canary` subsets.

Reach the BFF through the ingress on the homelab (no public DNS):

```bash
GW=$(kubectl -n istio-system get svc istio-ingressgateway -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
curl --resolve forgepoint.local:80:$GW http://forgepoint.local/healthz
```

---

## Canary endpoints & weights per deploy path (read before trusting the 90/10)

The `forgepoint.io/track` label that the DR subsets select on is emitted by **two
different mechanisms** depending on how model-serving is deployed, so the `stable`/
`canary` subsets do **not** always both have endpoints, and the authored 90/10 is **not**
the live split:

| Deploy path | `track=stable` pods | `track=canary` pods | Who sets the VS weights | Effective runtime split |
|-------------|---------------------|---------------------|-------------------------|-------------------------|
| **Argo Rollouts** (`deploy/rollouts/rollout-model-serving.yaml`) | Stamped by Rollouts on the stable track | Stamped by Rollouts **only during an active rollout** | **Argo Rollouts rewrites** the `weight:` fields here via `trafficRouting.istio` (canary → 0 at rest) | 100/0 at rest; steps up (10→25→50→100) only mid-rollout |
| **Plain Helm** (`deploy/helm/fp-model-serving`) | **Defaulted** by the chart (`podLabels: { forgepoint.io/track: stable }`) so the subset is always populated | **None** — nothing stamps `track=canary` without Rollouts | Nobody — the static authored weights stand, but canary's 10% has no endpoints | Effectively **100% stable** (canary route has zero endpoints → no blackhole) |

So the static **90/10** authored in `50-virtualservice-model-serving.yaml` is an
**illustrative initial canary shape** — a demonstrable standalone artifact. At runtime
Argo Rollouts overwrites it, and under plain Helm the canary subset has no endpoints to
receive the 10%. The chart's default `track: stable` pod label (added to `values.yaml`
`podLabels`, NOT to the immutable Deployment selector) is what makes the plain-Helm path
**safe**: before it, both subsets matched zero pods and **all** predict traffic
blackholed.

## Wiring the canary to Argo Rollouts (optional upgrade)

`deploy/rollouts/rollout-model-serving.yaml` currently weights canary by **replica
ratio** (no `trafficRouting.istio` block). To get **exact** L7 weighting + analysis-
gated promotion, point the Rollout at `50-virtualservice-model-serving.yaml`:

```yaml
# in spec.strategy.canary:
trafficRouting:
  istio:
    virtualService:
      name: fp-model-serving
      routes: [canary-split]
    destinationRule:
      # The subsets live on the single host DR in 20-destinationrules.yaml
      # (named `fp-model-serving`), not a separate subsets-only DR — one DR per host.
      name: fp-model-serving
      stableSubsetName: stable
      canarySubsetName: canary
```

Argo Rollouts then **rewrites the weights** in this VirtualService automatically at
each canary step. (Authored, not wired, to keep the static 90/10 demonstrable.)

---

## Production TLS at the edge (homelab has none)

`40-gateway-bff.yaml` ships an HTTP (port 80) server so it installs without DNS/certs.
For a real domain (ties to **M5 Terraform**: ACM/Route53 on EKS), add an HTTPS server
and force redirect:

```yaml
servers:
  - port: { number: 443, name: https, protocol: HTTPS }
    hosts: ["forgepoint.example.com"]
    tls:
      mode: SIMPLE
      credentialName: fp-ingress-tls   # a kubernetes.io/tls Secret (cert-manager)
  - port: { number: 80, name: http, protocol: HTTP }
    hosts: ["forgepoint.example.com"]
    tls:
      httpsRedirect: true               # 301 every plaintext request to HTTPS
```

---

## Assumptions & notes

- **model-serving namespace = `fp-models`** (authoritative, now consistent everywhere).
  The namespace map (`deploy/istio/00-namespaces.yaml`), the BFF config (`FP_MODEL_SERVING_ADDR=
  fp-model-serving.fp-models…`), the inference-gateway + BFF NetworkPolicy egress
  allow-lists, and these mesh objects all target `fp-models`. The Helm chart itself is
  **namespace-agnostic** — every template renders `metadata.namespace:
  {{ .Release.Namespace }}`, so the chart does not pin a namespace; the *install target*
  does. Previously that target resolved to `fp-system` for serving (the Makefile forced
  `--namespace fp-system` for ALL charts, and the Argo Rollouts manifests hardcoded
  `fp-system`) — a **bug**: the fp-models DestinationRule + AuthorizationPolicy selected
  zero pods while serving actually ran in `fp-system`, leaving the predict edge
  unprotected/misrouted. Fixed: the Makefile now derives `fp-models` for `SVC=model-serving`
  (`make helm-install SVC=model-serving` → `--namespace fp-models`), and
  `deploy/rollouts/{rollout,services,analysis-template}-model-serving.yaml` pin
  `fp-models` (the rollouts `kustomization.yaml` dropped its global `namespace:` so it no
  longer stomps these). The mesh layer and every caller now agree.
- **feature-store inbound edge is assumed.** No wired gRPC client reaches it in the
  inspected code, and the BFF does not dial it. Per the task's fallback rule, it gets a
  **namespace-broad** allow (`from.namespaces: [fp-system]`) rather than a single-SA
  pin — labeled `forgepoint.io/authz-grain: namespace-broad-assumed-edge`. Tighten to
  the real caller SA once a feature-read client is wired.
- **Service-account principals** are `cluster.local/ns/<ns>/sa/fp-<svc>` — each chart
  creates an SA named after its release fullname (`fp-<svc>`, per the Makefile install
  + `_helpers.serviceAccountName`). If an SA name is overridden, update the matching
  principal.
- **Probes survive STRICT mTLS** via Istio's default probe rewrite (kubelet → pilot-
  agent :15020 → app loopback), reinforced by the platform's separate `health` port
  (8080) distinct from the gRPC port (9090). No chart changes needed. Full explanation
  in `10-peerauthentication.yaml`.
