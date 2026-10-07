#!/usr/bin/env bash
# Read-only Postgres login for Grafana: SELECT on the observability schema and nothing else.
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
create role grafana_reader login password '${GRAFANA_DB_PASSWORD}';
grant usage on schema observability to grafana_reader;
grant select on all tables in schema observability to grafana_reader;
alter default privileges for role ${POSTGRES_USER} in schema observability
    grant select on tables to grafana_reader;
SQL
