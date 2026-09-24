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
