# ADR 0009: Tamper-evident audit log (hash chain + append-only, capture-via-interceptor / persist-via-choreography)

**Status:** Accepted
**Date:** 2026-06-18
**Deciders:** Abdul Basit Sajid
**Context phase:** Implementation Plan — Phase 25 / platform security (M6)

## Context

The platform needs a durable, trustworthy record of **WHO did WHAT, WHEN, and whether it was
ALLOWED or DENIED** — the security/compliance log that incident response and auditors read.
This is distinct from application logs (debugging, in Loki) and from domain events (business
facts other services react to). Three forces shape the design:

1. **It must capture DENIED access, not just successful mutations.** A burst of denied admin
   RPCs from an anonymous caller is the single most valuable signal an audit log carries. A
   log that only kept successes would hide exactly the attacks it exists to catch.
2. **It must be trustworthy after a breach.** If an attacker who gains write access can
   silently rewrite history, the log is worthless precisely when it matters. The log needs
   to make tampering **detectable**.
3. **It must be cheap to adopt across every service** without coupling every service to the
   audit database (database-per-service forbids cross-service DB access) and without putting a
   synchronous Postgres write on every RPC's hot path.

Two structural questions follow: **how do we make the log tamper-resistant**, and **how do we
split "capture an action" (happens everywhere) from "persist it" (happens once)**.

## Options Considered

### Tamper-resistance

#### Option A — Plain append-only table (no chain)

Insert rows; never UPDATE/DELETE; rely on a DB trigger to block mutation.

- **Pro:** simple; the trigger stops the common "rewrite through the app role" path.
- **Con:** a privileged attacker (superuser, or anyone who can drop the trigger) can edit or
  delete rows leaving **no evidence** — there is nothing that *commits* row N to row N-1, so a
  surgical edit is undetectable.

#### Option B — Hash chain over an append-only table (chosen)

Each row stores `entry_hash = sha256(prev_hash ‖ canonicalJSON(record))`, where `prev_hash`
is the previous row's `entry_hash` (genesis chains onto `""`). Combined with the append-only
trigger.

- **Pro:** **tampering is detectable.** Editing any field changes that row's hash and — because
  every later row mixed in that hash — **every subsequent link** too. A verifier re-walks the
  chain and the first mismatch pinpoints where history was altered. Deletion and reordering are
  detected the same way (the next row's stored `prev_hash` no longer matches).
- **Pro:** this is the **certificate-transparency / blockchain** discipline applied to an audit
  log: an entry cryptographically commits to all prior entries.
- **Con:** it is tamper-**EVIDENT**, not tamper-**PROOF** — see Residual threat below.
- **Con:** appends must be **serialized** (single-writer) or concurrent appenders fork the chain.

#### Option C — External immutable store only (e.g. write to S3 Object-Lock / a SaaS audit vendor)

- **Pro:** strong immutability without running our own chain.
- **Con:** heavy external dependency for the in-cluster control plane; still want a queryable
  local copy for joins ("everything user X did") and for the platform to be self-contained in
  dev/test. (We adopt the *off-box head* idea from this option as the production hardening of B.)

### Capture / persist split

#### Option D — Each service writes audit rows directly to a shared audit DB

- **Con:** violates database-per-service; every service needs the audit DB's credentials; a
  synchronous Postgres write sits on every RPC.

#### Option E — Capture via a gRPC interceptor, persist via NATS choreography (chosen)

A `pkg/audit` **interceptor** in every service builds a `Record` from claims + method + status
and publishes it to `fp.audit.recorded`. The **auth service** hosts the single consumer that
appends to the hash-chained table. Same shape the notification service uses (many producers
publish, one reactor persists).

- **Pro:** adding audit to a service is a **one-line wiring change** (the interceptor); only auth
  knows the audit DB. Capture is decoupled from persistence.
- **Pro:** no synchronous DB write on the hot path — the interceptor publishes (fast, JetStream-
  durable) and the consumer appends at its own pace.
- **Con:** an async hop means a sink outage loses records for that window (bounded by JetStream
  durability once published; alert on publish failures).

## Decision

Adopt **Option B + Option E**: a **hash-chained, append-only audit log**, captured by an
**interceptor** in every service and persisted by a **single choreography consumer in the auth
service**.

### Where capture sits in the chain — and why DENY is *always* captured (two interceptors)

A single audit interceptor cannot capture both the authenticated actor on an ALLOW **and** the
denials the auth interceptor short-circuits. We therefore wire **two** audit interceptors around
auth:

```
recovery → logging → DENY-CAPTURE(outer) → auth → AUDIT(inner) → handler
```

- The **inner** interceptor (`grpcutil.WithUnaryInterceptors`) runs **after** the auth validator,
  so claims are populated — the `Record`'s `Actor` is the **authenticated identity**. Being inner
  of auth it also wraps the business handler, so it captures every **handler-level authZ DENY**
  (`requireAdmin` → `PermissionDenied`) as a `DENY` with the real actor, plus successful mutations
  (`ALLOW`) and non-security failures (`ERROR`).
- The **outer** interceptor (`grpcutil.WithPreAuthUnaryInterceptors`, `pkg/audit.DenyUnary-
  Interceptor`) **wraps** auth, so it observes auth's **own** short-circuit rejection — a
  missing/expired/forged token, or a validator `PermissionDenied`. The auth interceptor returns
  *before* its inner handler runs, so the inner audit interceptor never sees those denials; the
  outer one records them as a `DENY` with `Actor=anonymous` (auth rejected before claims existed).
  This is force #1 — **an anonymous probe of a privileged method is captured, not dropped onto the
  logging interceptor.**

A shared context marker makes the two interceptors record **exactly once**: the inner one flips the
marker when it emits, and the outer one stays silent when the marker is set (so an authenticated
handler-DENY is recorded once, by the inner interceptor, with the real actor). The decision table:

| resulting gRPC code            | Decision | audited?                          |
|--------------------------------|----------|-----------------------------------|
| `OK`                           | ALLOW    | only if the method is security-relevant (mutating/auth) |
| `Unauthenticated` / `PermissionDenied` | DENY | **always** (failed access is the signal) |
| any other error                | ERROR    | only if the method is security-relevant |

A **security-relevance predicate** keeps the log selective (mutating + auth RPCs are audited;
pure reads, health, and reflection are skipped on success) so a 10k-rps `ListModels` does not
drown the signal. DENY bypasses the predicate entirely.

Audit is **best-effort on the hot path**: if the sink errors, the interceptor logs and continues
— it **never** changes the RPC's outcome. The alternative (failing the RPC because audit failed)
would make audit a single point of failure for the whole platform.

### JSON, decoupled from the API proto (ADR 0004)

The `Record` is a **plain Go struct**, not a generated proto, JSON-serialized into the standard
`EventEnvelope.data`. Per ADR 0004 the audit trail must not be chained to any service's API proto
— an audit record describes a *security event* (actor/action/decision), a different and
slower-changing vocabulary. JSON-on-the-bus keeps records human-readable in the NATS CLI / DLQ,
consistent with every other Forgepoint event. **The struct has no field for the request payload**
— it records the gRPC *method* (the verb) and a best-effort target *resource id*, never the
arguments — so a password/token/API-key from a `CreateUser`/`CreateAPIKey` request can never reach
the log. `Err` carries only the sanitized gRPC status message, never `err.Error()` verbatim.

### Append-only enforced at the DB + single-writer chain integrity

Append-only is enforced in layers: the repository only ever `INSERT`s; a `BEFORE UPDATE OR DELETE`
**row** trigger and a `BEFORE TRUNCATE` **statement** trigger both `RAISE` (the TRUNCATE trigger is
mandatory because a row-level trigger never fires on `TRUNCATE` — without it, `TRUNCATE audit_log`
would erase the whole chain unseen); and a guarded migration block `REVOKE`s
`UPDATE/DELETE/TRUNCATE` from a dedicated non-owner app role (`fp_audit_app`), leaving it
`INSERT+SELECT` only.

**Honest limit:** a trigger is not a table ACL, so it fires
for the owner and a superuser too — but the **owner** (and a superuser) can `DISABLE`/`DROP` the
trigger and then mutate. So the trigger is only binding for a role that is **neither the owner nor a
superuser**. If the app connects as the table owner (the default single-role dev/test setup where
`POSTGRES_USER` owns everything), the guarantee is **weaker than it looks**. That is exactly why the
app should connect as the non-owner `fp_audit_app` role in production (provisioned by
`init-postgres.sh`); the migration's `REVOKE` then makes the trigger truly binding. The chain is computed
under a **single-writer lock**: `pg_advisory_xact_lock` (a transaction-scoped named mutex) held
across read-head → compute-hash → insert, so concurrent appenders **block** rather than both
chaining off the same `prev_hash` and **forking** the chain. We chose the advisory lock over
`SELECT … head … FOR UPDATE` because the **genesis** case (empty table) has no row to lock — the
advisory lock covers genesis and steady-state uniformly. Idempotency is at the DB: `event_id`
(the envelope id) is `UNIQUE` and the append is `ON CONFLICT (event_id) DO NOTHING`, so a NATS
**redelivery** is a no-op and never double-appends (which would break the chain).

### Residual threat: tamper-EVIDENT, not tamper-PROOF — off-box mitigation

The in-database chain detects edits/deletes/reorders, but an attacker with **write access who can
recompute the entire chain forward** from the edit point produces an internally-consistent forged
history. No pure in-DB chain defends against that. The standard mitigation — what **AWS CloudTrail
log-file integrity** and **certificate-transparency** logs do — is to periodically ship the **head
hash off-box** to an append-only store the app role cannot reach (a separate account's S3 with
Object-Lock, a notary, or a public ledger). A forger would then also have to rewrite every
externally-witnessed head, which they cannot. We document this off-box head shipment as the
**production hardening**; the in-DB chain + append-only trigger is the in-scope mechanism here.

The deciding factor: only B+E gives a log that is **trustworthy after a breach** (detectable
tampering) **and** cheap to roll out everywhere (one-line interceptor, one persistence owner)
without coupling services to the audit DB or adding a synchronous write to every RPC.

## Real-world comparisons

- **AWS CloudTrail** — management-event audit log with optional *log-file integrity validation*:
  hashed, signed digest files chained so deletion/modification is detectable. Same evident-not-proof
  posture; integrity rests on the chain + off-account delivery.
- **Linux `auditd`** — kernel-level capture of security-relevant syscalls (the OS analogue of our
  interceptor capturing security-relevant RPCs), shipped to an append-only store.
- **Stripe events** — immutable, append-only event objects with stable ids that consumers dedupe on
  (our `event_id` idempotency).
- **Certificate Transparency / Merkle logs** — the cryptographic-chaining lineage: each entry
  commits to all prior entries; auditors verify inclusion + consistency. Our linear SHA-256 chain is
  the single-tail simplification of that idea.

## Consequences

- **Positive:** tampering (edit/delete/reorder/truncate) is cryptographically detectable; UPDATE,
  DELETE, and TRUNCATE are blocked at the DB for any non-owner/non-superuser role (and the REVOKE
  strips even the attempt from the dedicated `fp_audit_app` role); capture is a one-line addition
  per service (two interceptors); the audit DB stays owned solely by auth; no synchronous audit
  write on the RPC hot path; **DENIED and anonymous access are captured at BOTH the auth-interceptor
  short-circuit (outer) and the handler authZ check (inner)** — the anonymous-probe signal is no
  longer lost.
- **Negative:** the DB triggers are bypassable by the table **owner**/a superuser via `DISABLE`/
  `DROP TRIGGER`, so they are only binding when the app connects as a **non-owner** role — until the
  service's DSN uses `fp_audit_app`, the single-role dev/test setup has the weaker guarantee;
  appends are serialized by the single-writer lock (fine at control-plane rates, ~1K/s; not a
  data-plane design); an async sink outage can lose records for a window (bounded by JetStream
  durability once published; alert on publish failures); tamper-evidence is not tamper-proof until
  off-box head shipment is added (a `TRUNCATE`-and-recompute-forward by a privileged DDL actor is
  only closed by the off-box head).
- **Idempotency:** `event_id` (envelope id) `UNIQUE` + `ON CONFLICT DO NOTHING` makes redelivery a
  no-op; the DB-level dedup survives a crash between handling and ack (the transactional gold
  standard from `pkg/natsutil`'s exactly-once note).
- **Follow-ups:** ship head hashes off-box (S3 Object-Lock) for tamper-PROOF (closes the
  TRUNCATE/recompute-forward residual a privileged DDL actor could exploit); point the auth
  service's `FP_DATABASE_URL` at the non-owner `fp_audit_app` role so the migration's REVOKE +
  triggers become fully binding (the role and grants now exist; switching the DSN is the remaining
  operational step); add an `/audit/verify` admin path that re-walks and reports the first broken
  link; roll the two interceptors out to the remaining services (snippet below).

### One-line rollout for the remaining services

Each remaining service adds **only the interceptors** (no repository, no consumer, no stream — auth
hosts all of those) by reusing its existing `natsutil.Publisher`. In the service's `main.go`, where
it builds the gRPC server:

```go
// Construct an audit sink over the service's existing NATS publisher.
auditSink := audit.NewNATSAuditSink(natsPublisher) // natsPublisher already exists for the service's own events
auditOpts := audit.Options{Source: "registry", Logger: logger} // Source = this service's name

srv := grpcutil.NewServer(
    grpcutil.WithLogger(logger),
    grpcutil.WithAuthValidator(validator, skipMethods...),
    // OUTER (wraps auth): captures auth's short-circuit Unauthenticated/PermissionDenied denials
    // — the anonymous-probe signal the inner interceptor never sees.
    grpcutil.WithPreAuthUnaryInterceptors(audit.DenyUnaryInterceptor(auditSink, auditOpts)),
    grpcutil.WithPreAuthStreamInterceptors(audit.DenyStreamInterceptor(auditSink, auditOpts)),
    // INNER (after the validator, so claims are populated when the record is built):
    grpcutil.WithUnaryInterceptors(audit.UnaryServerInterceptor(auditSink, auditOpts)),
    grpcutil.WithStreamInterceptors(audit.StreamServerInterceptor(auditSink, auditOpts)),
    grpcutil.WithReflection(),
)
```

The records all land on `fp.audit.recorded`; the auth-hosted consumer persists every service's
records into the one hash-chained log. No `go.mod` change beyond importing `pkg/audit` (already in
the workspace).
