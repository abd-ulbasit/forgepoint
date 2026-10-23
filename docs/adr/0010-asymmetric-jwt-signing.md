# ADR 0010: Asymmetric JWT signing with a published key set (replace the shared HS256 key)

**Status:** Proposed
**Date:** 2026-10-24
**Deciders:** Abdul Basit Sajid
**Context phase:** Security audit follow-up (October 2026). Design note only; no code changes yet.

## Context

`services/auth` mints access tokens with **HS256** (`services/auth/internal/domain/jwt.go`),
and every other service verifies them locally with `pkg/auth/validator.go`, using the
**same** symmetric key. That key, `FP_JWT_SECRET`, is rendered into the Secret of 11 Helm
charts (all 11 `templates/secret.yaml` files reference it) and stored once in Secrets Manager
(`<prefix>/auth/jwt`, created by hand with the aws CLI).

The verifier side is careful: it pins the algorithm (identity check on
`jwt.SigningMethodHS256` plus `WithValidMethods`), rejects `alg=none` and enforces a
key of at least 32 bytes. The weakness is structural:

1. **Every verifier can mint.** With HMAC the verification key *is* the signing key.
   Compromising any one of the 11 services (billing, notification, the BFF's
   neighbours...) yields a key that forges a token for **any** identity and role,
   including admin, accepted by every other service.
2. **No key rotation without an outage.** The validator holds exactly one key and
   tokens carry no `kid`. Rotating means updating 11 Secrets and restarting 11
   services in lockstep; any service still on the old key rejects new tokens (or
   the reverse) until it restarts.
3. **The blast radius of a leaked key is the whole platform,** and the old dev
   placeholder value is already public in git history (see SECURITY.md), so any
   environment that ever used it must rotate. Today that rotation is the painful
   lockstep in point 2.

## Options Considered

### Option A — Keep HS256, add a `kid` and accept a key *set* in the validator
- **Pro:** smallest change; enables zero-downtime rotation (sign with the new key,
  verify with old+new, then drop the old one).
- **Con:** does not fix point 1. Every verifier still holds a minting key.

### Option B — Asymmetric signing (EdDSA/Ed25519 or ES256), JWKS published by auth (recommended)
- **Pro:** only `auth` holds a private key; the other services get **public** keys
  that cannot mint anything, so a compromised service can no longer forge
  identities. Key IDs (`kid`) and a key set give zero-downtime rotation. Public keys
  are not secrets, so they no longer need a Secret per chart.
- **Con:** a key-distribution path is needed (a JWKS endpoint on auth, or a ConfigMap /
  Secrets Manager entry with the public set); validators must cache and refresh it;
  the token format changes (`alg`, `kid`), so the migration has a dual-accept window.

### Option C — Opaque tokens with introspection at auth (or the mesh)
- **Pro:** no signing key outside auth at all; instant revocation.
- **Con:** a network call per request (or per cache miss) to auth, and a hard runtime
  dependency on auth for every RPC. It undoes the design choice of local verification.

## Decision

Proposed: **Option B, with Ed25519 (`EdDSA`)**. Small keys and fast verification,
and the algorithm admits no parameter choices that could go wrong. `ES256` is the
fallback if a client library lacks EdDSA.

- auth holds the private key(s) (Secrets Manager, read only by the External Secrets
  Operator into the `fp-auth` Secret). It signs with the active key, puts its `kid` in
  the header, and serves the **public** key set at an internal JWKS endpoint (and/or
  writes it to a ConfigMap that charts mount).
- `pkg/auth` validator: accept exactly `EdDSA`, look up the key by `kid` in a cached
  set (refresh on unknown `kid`, rate-limited), and keep the existing
  algorithm-pinning and claim checks. Add `iss` and `aud` checks while there.
- Charts: drop `FP_JWT_SECRET` from every chart except `fp-auth`.

## Migration

1. **Validator first (no behaviour change):** `pkg/auth` accepts a *set* of keys:
   the current HS256 secret **and** an Ed25519 public key set. HS256 tokens still
   verify. Ship to all services.
2. **auth signs EdDSA with `kid`:** generate the first Ed25519 key, publish its public
   half, and switch issuance. Old HS256 tokens keep verifying until they expire
   (access-token TTL).
3. **Remove HS256:** after one TTL plus a margin, drop HS256 acceptance from the
   validator and remove `FP_JWT_SECRET` from the 11 non-auth charts, the ExternalSecret
   and Secrets Manager. Keep the dev-placeholder denylist in the fp-auth chart until
   then.
4. **Rotation runbook:** add key N+1 to the published set, wait for every validator's
   cache refresh interval, switch signing to N+1, and remove N after one token TTL.
   No restarts are needed.

## Consequences

- **Positive:** a compromised non-auth service can no longer mint tokens; rotation is
  routine instead of a coordinated outage; fewer Secrets to manage.
- **Negative:** auth gains a JWKS endpoint and key-management code; validators gain a
  cache with refresh logic; a short dual-algorithm window.
- **Follow-ups:** decide JWKS-over-HTTP vs ConfigMap distribution (ConfigMap is simpler
  inside one cluster; HTTP is needed once anything outside the cluster verifies);
  until this lands, any environment that ever used the public dev placeholder as its
  HS256 key must rotate it now.
