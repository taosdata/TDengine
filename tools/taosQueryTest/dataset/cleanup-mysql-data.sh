#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
. "$script_dir/find-client.sh"

: "${FQ_MYSQL_HOST:?FQ_MYSQL_HOST is required}"
: "${FQ_MYSQL_PORT:?FQ_MYSQL_PORT is required}"
: "${FQ_MYSQL_USER:?FQ_MYSQL_USER is required}"
: "${FQ_MYSQL_PASSWORD:?FQ_MYSQL_PASSWORD is required}"

mysql_bin="$(qst_find_client QST_MYSQL_CLIENT mysql '/opt/taostest/fq/mysql/*/bin/mysql')" ||
  { printf '%s\n' 'mysql client is required (set QST_MYSQL_CLIENT or install it)' >&2; exit 1; }

MYSQL_PWD="$FQ_MYSQL_PASSWORD" \
  "$mysql_bin" --protocol=TCP --host="$FQ_MYSQL_HOST" --port="$FQ_MYSQL_PORT" \
  --user="$FQ_MYSQL_USER" --default-character-set=utf8mb4 <<'SQL'
DROP DATABASE IF EXISTS fq_alt;
DROP DATABASE IF EXISTS fq_default;
SQL
