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

for db in fp_auth fp_registry fp_pipeline fp_feature fp_experiment fp_billing fp_notification; do
    echo "  Creating database: $db"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
        CREATE DATABASE $db;
        GRANT ALL PRIVILEGES ON DATABASE $db TO $POSTGRES_USER;
EOSQL
done

echo "All databases created successfully."
