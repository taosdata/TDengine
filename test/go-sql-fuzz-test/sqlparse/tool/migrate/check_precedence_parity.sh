#!/usr/bin/env bash

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

set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <path-to-lemon-sql.y> <path-to-td_sql.y>" >&2
  exit 1
fi

lemon_sql="$1"
go_sql="$2"

if [[ ! -f "$lemon_sql" || ! -f "$go_sql" ]]; then
  echo "input files must exist" >&2
  exit 1
fi

tmp_lemon="$(mktemp)"
tmp_go="$(mktemp)"
trap 'rm -f "$tmp_lemon" "$tmp_go"' EXIT

grep -E '^%(left|right|nonassoc)[[:space:]]' "$lemon_sql" \
  | sed -E 's/[.]$//' \
  | sed -E 's/^%(left|right|nonassoc)[[:space:]]+/%\1 /' > "$tmp_lemon"

grep -E '^%(left|right|nonassoc)[[:space:]]+<token>[[:space:]]' "$go_sql" \
  | sed -E 's/^%(left|right|nonassoc)[[:space:]]+<token>[[:space:]]+/%\1 /' > "$tmp_go"

echo "== Lemon precedence =="
cat "$tmp_lemon"
echo
echo "== goyacc precedence =="
cat "$tmp_go"
echo
echo "== Unified diff (Lemon -> goyacc) =="
diff -u "$tmp_lemon" "$tmp_go" || true
