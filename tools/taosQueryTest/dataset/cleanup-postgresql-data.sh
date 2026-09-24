#!/bin/sh
set -eu

: "${FQ_PG_HOST:?FQ_PG_HOST is required}"
: "${FQ_PG_PORT:?FQ_PG_PORT is required}"
: "${FQ_PG_USER:?FQ_PG_USER is required}"
: "${FQ_PG_PASSWORD:?FQ_PG_PASSWORD is required}"

psql_bin="$(command -v psql 2>/dev/null || true)"
: "${psql_bin:?psql client is required in PATH}"

PGPASSWORD="$FQ_PG_PASSWORD" \
  "$psql_bin" --host="$FQ_PG_HOST" --port="$FQ_PG_PORT" \
  --username="$FQ_PG_USER" --dbname=postgres --set=ON_ERROR_STOP=1 <<'SQL'
DROP DATABASE IF EXISTS fq_default;
SQL
