# ADR 0011: Per-service database users instead of the shared master DSN

**Status:** Proposed
**Date:** 2026-10-24
**Deciders:** Abdul Basit Sajid
**Context phase:** Security audit follow-up (October 2026). Design note only; no code changes yet.

## Context

The architecture is database-per-service ("each service owns its Postgres database and
migrations; no cross-service DB access", per `deploy/k8s/infra/postgres/postgres.yaml`
and the `fp_<service>` databases in `scripts/init-postgres.sh`), but the *credentials*
are not per-service:

- **AWS:** both ExternalSecrets (`fp-auth`, `fp-infra-creds`) build `FP_DATABASE_URL` from
  the **RDS master user**, now RDS-managed (`manage_master_user_password`) but still the
  master. Every consumer of `fp-infra-creds` and the auth service connect as a role that
  owns every database, can create roles, and can read and alter any service's data.
- **Local / k3s:** every service connects as `fp`, the Postgres superuser created by the
  image entrypoint.

So the isolation boundary the architecture promises exists only by convention. One
compromised service, or one SQL injection, gets read/write on billing's ledger, auth's
users and the audit log, and could create a login role to persist. The repo already
shows the pattern it should follow in one place: `fp_audit_app`, a NOLOGIN least-privilege
role for the audit sink.

## Options Considered

### Option A — Status quo: one master DSN, rely on network policy and mesh authz
- **Pro:** nothing to build.
- **Con:** database-level isolation is zero; the master credential sits in many pods' env.

### Option B — One LOGIN role per service, owner of its own database only (recommended)
- **Pro:** a compromised service reaches only its own database; the credential has no
  `CREATEROLE`/`CREATEDB`/superuser; audit logs show *which* service did what.
- **Con:** role provisioning, password storage and rotation for ~8 roles; migrations
  must run as the service's owner role.

### Option C — Option B with IAM database authentication on RDS
- **Pro:** no stored passwords at all; pods get 15-minute tokens via IRSA
  (`rds-db:connect` scoped to one DB user); rotation is free.
- **Con:** RDS-only (the local stack still needs passwords); a client change to fetch
  tokens and reconnect before expiry; IAM auth has connection-rate limits to plan around.

## Decision

Proposed: **Option B now, Option C for AWS later.**

- Each service gets a role `fp_<service>_app` (LOGIN, NOINHERIT, no CREATEROLE/CREATEDB)
  that **owns only** `fp_<service>`. `REVOKE CONNECT ON DATABASE ... FROM PUBLIC` on
  every service database, so a role cannot even connect to another service's DB.
- The master/superuser credential is used **only** by a one-shot provisioning Job (or
  Terraform's `postgresql` provider in a separate, operator-run stack) and is never
  mounted into an application pod.
- Passwords: one Secrets Manager secret per service role (AWS), synced by its own
  ExternalSecret into that service's Secret only (`fp-<service>`), replacing the shared
  `fp-infra-creds` DSN.

## Migration

1. Provisioning: extend `scripts/init-postgres.sh` (local) and add an idempotent
   provisioning Job for RDS that creates the roles, transfers ownership of each
   `fp_<service>` database and its objects (`REASSIGN OWNED`), and revokes PUBLIC
   connect.
2. Secrets: create per-service secrets; add one ExternalSecret per service; point each
   chart's `secrets.existingSecret` at it (the charts already default to a Secret named
   after the chart, see `secrets.create: false`).
3. Cut over one service at a time; migrations (the `migrate` initContainer) already use
   the same DSN as the app, so they run as the owner role automatically.
4. Remove `FP_DATABASE_URL` from `fp-infra-creds`, and drop the master user secret ARN
   from every role except the provisioning path.
5. Later (Option C): enable IAM auth on RDS, grant `rds_iam` to the service roles, give
   each service's IRSA role `rds-db:connect` on its own user only, and switch clients to
   token auth.

## Consequences

- **Positive:** database isolation matches the architecture; credential blast radius is
  one service; the master credential leaves the application path entirely.
- **Negative:** ~8 more secrets to provision and rotate (until IAM auth); a provisioning
  step that must run before a new service's first deploy.
- **Follow-ups:** decide where role provisioning lives (Job vs operator-run Terraform
  stack); the RDS-managed master password rotation (every 7 days) then affects only
  that provisioning path, not every pod.
