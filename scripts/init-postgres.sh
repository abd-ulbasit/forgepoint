#!/bin/bash
# ============================================================================
# POSTGRES INITIALIZATION SCRIPT
# ============================================================================
#
# WHY per-service databases:
#   Each microservice owns its data — no cross-service DB access.
#   This enforces the "database per service" pattern:
#   - Services can't bypass APIs by querying each other's tables
#   - Schema changes in one service can't break another
#   - Each service can choose its own schema design independently
#   - In production, databases could be on separate RDS instances
#
# HOW IT WORKS:
#   Docker Compose mounts this script into /docker-entrypoint-initdb.d/
#   Postgres automatically executes scripts in that directory on first start.
#   Scripts run as the POSTGRES_USER (fp) with full privileges.
#
# NOTE: This only runs on FIRST container start (when the data volume is empty).
# To re-run: `docker compose down -v && docker compose up -d`
# ============================================================================

set -e

echo "Creating per-service databases..."

# Authoritative DB list (8 databases, one per service — matches the K8s ConfigMap
# in deploy/k8s/infra/postgres/postgres.yaml and the per-service database list above).
# Drift history: fp_feature → fp_featurestore (correct name), fp_monitor added.
# Local docker-compose and cluster must be in sync so connection strings are identical.
for db in fp_auth fp_registry fp_pipeline fp_featurestore fp_experiment fp_billing fp_notification fp_monitor; do
    echo "  Creating database: $db"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
        CREATE DATABASE $db;
        GRANT ALL PRIVILEGES ON DATABASE $db TO $POSTGRES_USER;
EOSQL
done

# ============================================================================
# DEDICATED NON-OWNER AUDIT APP ROLE (fp_audit_app) — least-privilege append
# ============================================================================
# WHY: the auth service's audit_log table is append-only, enforced by triggers
# (002_audit_log.up.sql). But a trigger can be DISABLEd/DROPped by the table's
# OWNER (and by a superuser) — so if the application connects as the SAME role that
# owns the table, the append-only guarantee is bypassable by that very role. The
# fix is to run the audit-writing path under a role that does NOT own audit_log and
# holds ONLY INSERT+SELECT on it. We provision that role here.
#
# The 002 migration's guarded DO block detects this role and REVOKEs UPDATE/DELETE/
# TRUNCATE from it, leaving INSERT+SELECT — so the trigger becomes truly binding
# (the app role can neither mutate nor drop the trigger). If this role is absent
# (e.g. a minimal local run), the migration no-ops the hardening and the triggers
# alone apply — see the migration's comments and ADR 0009.
#
# NON-LOGIN by default: created with NOLOGIN here so it cannot be used as a
# connection identity until an operator sets a password (ALTER ROLE fp_audit_app
# WITH LOGIN PASSWORD '...') and points the auth service's FP_DATABASE_URL at it.
# That keeps local dev working as POSTGRES_USER (fp) by default while the
# production-hardening role exists and is pre-granted the right privileges.
echo "Provisioning least-privilege audit app role (fp_audit_app)..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-'EOSQL'
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fp_audit_app') THEN
            -- NOLOGIN: a privilege holder, not yet a connection identity. An operator
            -- grants LOGIN + a password (or maps it via IAM auth) in production.
            CREATE ROLE fp_audit_app NOLOGIN;
        END IF;
    END
    $$;
EOSQL

# Grant the audit app role CONNECT on fp_auth and USAGE on its schema so that, once
# given LOGIN, it can reach audit_log. Table-level INSERT/SELECT and the REVOKE of
# mutation rights are applied by the 002 migration (which owns the table) — keeping
# table privileges with the schema that creates the table, and connection/role
# provisioning here at the cluster-bootstrap layer.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-'EOSQL'
    GRANT CONNECT ON DATABASE fp_auth TO fp_audit_app;
EOSQL
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname fp_auth <<-'EOSQL'
    GRANT USAGE ON SCHEMA public TO fp_audit_app;
EOSQL

echo "All databases created successfully."
