# ADR 0001: Backend-for-Frontend (BFF) for the Web UI

**Status:** Accepted
**Date:** 2026-06-10
**Deciders:** Abdul Basit Sajid
**Context phase:** Implementation Plan — Phase 14

## Context

The platform's services expose **gRPC** APIs (Protobuf, HTTP/2). The web UI runs in a
**browser**, which cannot open raw gRPC connections — browsers lack control over HTTP/2
frames, so a gRPC stub cannot talk to a service directly. Something must sit between the
browser and the services to translate protocols.

Additionally, the UI is **screen-oriented**, not service-oriented. A single dashboard screen
needs data from Model Registry + Pipeline Orchestrator + Experiment Tracker + Billing. The
services are split by *domain ownership* (database-per-service), not by *what one screen
needs*. There is an impedance mismatch between how data is owned and how it is viewed.

Three other concerns are browser-specific and don't belong in the domain services:
1. **Auth shape:** services authenticate with bearer JWT; browsers want HttpOnly cookies
   (XSS-safe token storage), CSRF protection, and CORS.
2. **Real-time:** Pipeline Orchestrator streams execution status via gRPC server-streaming;
   the browser needs SSE or WebSocket.
3. **Chattiness:** without aggregation, one screen = N round trips from the browser over the
   public internet.

We need to choose the translation layer.

## Options Considered

### Option A — grpc-gateway (generated REST reverse proxy)

A code-generated reverse proxy that maps gRPC methods to REST/JSON 1:1 from proto
annotations.

- **Pro:** almost no hand-written code; REST/JSON for free; reuses existing protos.
- **Con:** strictly 1:1 — no cross-service aggregation, so the browser still makes N calls
  and stitches them client-side. No view tailoring.
- **Con:** leaks internal service API shape straight to the frontend → frontend is coupled
  to service boundaries; every service refactor risks breaking the UI.
- **Con:** browser auth (cookies/CSRF) and gRPC-stream → SSE bridging have no home.

### Option B — gRPC-Web + Envoy

Browser uses a generated gRPC-Web client; an Envoy proxy transcodes gRPC-Web ↔ gRPC.

- **Pro:** end-to-end typed clients generated from proto.
- **Con:** requires operating an Envoy proxy (extra infra/config).
- **Con:** still no server-side aggregation — browser calls services directly, chatty.
- **Con:** browser streaming support is awkward (server-streaming only, clunky); frontend
  tightly coupled to service-level protos; no natural home for cookie/CSRF/CORS.

### Option C — Backend-for-Frontend (chosen)

A dedicated Go service that speaks gRPC to the platform services and exposes a JSON/HTTP +
SSE API **shaped for the UI's screens**.

- **Pro:** server-side composition/fan-out → one call per screen → snappy UX.
- **Pro:** decouples frontend from internal service boundaries (the BFF is the absorbing
  seam).
- **Pro:** natural home for browser auth (cookie↔JWT bridge, CSRF, CORS) and for bridging
  gRPC server-streams → SSE.
- **Pro:** BFF is itself a recognized, interview-grade pattern (Netflix/SoundCloud lineage),
  and gives more idiomatic Go to write (errgroup fan-out, partial-failure degradation) —
  aligned with the project's learning goal.
- **Con:** more code to write and another service to deploy.
- **Con:** risk of accreting business logic and becoming a god-service (mitigated below).

## Decision

Adopt **Option C — a Backend-for-Frontend service** (`services/bff`) as the sole interface
between the web UI and the platform.

The deciding factors: the UI needs cross-service **composition** and **gRPC-stream → SSE**
bridging, neither of which the 1:1 edge-translation options provide; and the browser-auth
concerns need a home outside the domain services. Edge translation (Option A) would be the
right call only for a thin 1:1 admin panel over single-service CRUD with no real-time — which
this is not.

## Guardrail (binding)

**The BFF contains ZERO business logic.** Its only responsibilities are:
1. **Composition** — fan-out to services and aggregate into view-shaped payloads.
2. **Protocol/stream translation** — gRPC ↔ JSON, gRPC server-stream → SSE.
3. **Browser-session concerns** — cookie↔JWT bridge, CSRF, CORS.

Any rule, validation, or calculation belongs in a domain service. This guardrail is enforced
in code review. It is what keeps the BFF from degenerating into the god-service that is the
pattern's main failure mode.

## Consequences

- **Positive:** one HTTP/SSE call per screen; frontend insulated from service refactors;
  clean place for browser auth and real-time; a defensible architectural story.
- **Negative:** an extra service to build, test, and operate; discipline required to hold the
  zero-business-logic line.
- **Follow-ups:** SSE-vs-WebSocket chosen as SSE (one-directional status updates fit, no
  extra protocol) — revisit if the UI later needs client→server streaming. The BFF's own
  availability becomes part of the user-facing SLO (Phase 15).
