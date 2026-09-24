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
