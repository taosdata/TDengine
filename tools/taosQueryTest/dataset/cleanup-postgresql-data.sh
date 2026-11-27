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
