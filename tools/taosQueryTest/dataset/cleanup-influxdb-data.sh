#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
. "$script_dir/find-client.sh"

: "${FQ_INFLUX_HOST:?FQ_INFLUX_HOST is required}"
: "${FQ_INFLUX_PORT:?FQ_INFLUX_PORT is required}"
: "${FQ_INFLUX_TOKEN:?FQ_INFLUX_TOKEN is required}"

influxdb3_bin="$(qst_find_client QST_INFLUXDB3_CLIENT influxdb3 '/opt/taostest/fq/influxdb/*/bin/influxdb3' QST_INFLUX_ADMIN_CLIENT)" ||
  { printf '%s\n' 'influxdb3 client is required (set QST_INFLUXDB3_CLIENT or install it)' >&2; exit 1; }

base_url="http://${FQ_INFLUX_HOST}:${FQ_INFLUX_PORT}"
delete_owned_database() {
  database_name=$1
  databases=$(INFLUXDB3_AUTH_TOKEN="$FQ_INFLUX_TOKEN" \
    "$influxdb3_bin" show databases --host "$base_url" --format csv)
  if printf '%s\n' "$databases" | grep -qx "$database_name"; then
    printf 'yes\n' | INFLUXDB3_AUTH_TOKEN="$FQ_INFLUX_TOKEN" \
      "$influxdb3_bin" delete database --host "$base_url" "$database_name"
  fi
}

delete_owned_database fq_alt
delete_owned_database fq_default
