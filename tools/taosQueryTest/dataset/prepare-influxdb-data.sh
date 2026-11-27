#!/bin/sh

# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
. "$script_dir/find-client.sh"

: "${FQ_INFLUX_HOST:?FQ_INFLUX_HOST is required}"
: "${FQ_INFLUX_PORT:?FQ_INFLUX_PORT is required}"
: "${FQ_INFLUX_TOKEN:?FQ_INFLUX_TOKEN is required}"

curl_bin="$(command -v curl 2>/dev/null || true)"
: "${curl_bin:?curl client is required in PATH}"

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

umask 077
default_data=$(mktemp "${TMPDIR:-/tmp}/taosQueryTest-influx-default.XXXXXX")
alt_data=$(mktemp "${TMPDIR:-/tmp}/taosQueryTest-influx-alt.XXXXXX")
trap 'rm -f "$default_data" "$alt_data"' EXIT HUP INT TERM

delete_owned_database fq_alt
delete_owned_database fq_default
INFLUXDB3_AUTH_TOKEN="$FQ_INFLUX_TOKEN" \
  "$influxdb3_bin" create database --host "$base_url" fq_default
INFLUXDB3_AUTH_TOKEN="$FQ_INFLUX_TOKEN" \
  "$influxdb3_bin" create database --host "$base_url" fq_alt

cat > "$default_data" <<'LINE_PROTOCOL'
fq_series,region=north,site=s1 device_id=1001i,reading=10.5,active=true,note="alpha",jdoc="{\"k\":\"v\",\"n\":1}" 1767225600123000000
fq_series,region=north,site=s1 device_id=1001i,reading=11.0,active=false,jdoc="{\"k\":\"w\",\"n\":2}" 1767225601000000000
fq_series,region=south,site=s2 device_id=1002i,reading=20.25,active=true,note="中文",jdoc="{\"k\":\"中\",\"n\":3}" 1767225602000000000
fq_series,region=east,site=s3 device_id=1003i,reading=-1.5,active=true,note="%_\\'",jdoc="{\"k\":\"x\",\"n\":4}" 1767225603000000000
fq_device,region=north,site=s1 device_id=1001i,device_name="meter-a",tier="gold" 1767225600123000000
fq_device,region=north,site=s1 device_id=1001i,device_name="meter-a",tier="gold" 1767225601000000000
fq_device,region=south,site=s2 device_id=1002i,device_name="meter-b",tier="silver" 1767225602000000000
fq_device,region=east,site=s3 device_id=1003i,device_name="meter-c",tier="bronze" 1767225603000000000
fq_precision,site=s1 value="ns" 1767225610123456789
fq_precision,site=s1 value="zero" 1767225610000000001
LINE_PROTOCOL

cat > "$alt_data" <<'LINE_PROTOCOL'
fq_series_alt,region=north,site=s1 device_id=1001i,reading=10.5,active=true,note="alpha",jdoc="{\"k\":\"v\",\"n\":1}" 1767225600123000000
LINE_PROTOCOL

INFLUXDB3_AUTH_TOKEN="$FQ_INFLUX_TOKEN" \
  "$influxdb3_bin" write --host "$base_url" --database fq_default \
  --precision ns --file "$default_data"
INFLUXDB3_AUTH_TOKEN="$FQ_INFLUX_TOKEN" \
  "$influxdb3_bin" write --host "$base_url" --database fq_alt \
  --precision ns --file "$alt_data"
