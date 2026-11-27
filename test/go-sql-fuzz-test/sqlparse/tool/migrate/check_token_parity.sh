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

base_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

tmp_lemon="$(mktemp)"
tmp_go="$(mktemp)"
tmp_go_token="$(mktemp)"
tmp_go_prec="$(mktemp)"
trap 'rm -f "$tmp_lemon" "$tmp_go" "$tmp_go_token" "$tmp_go_prec"' EXIT

bash "$base_dir/extract_lemon_tokens.sh" "$lemon_sql" > "$tmp_lemon"
grep -E '^%token <token> ' "$go_sql" \
  | sed -E 's/^%token <token> ([A-Z0-9_]+).*/\1/' \
  | sort -u > "$tmp_go_token"
grep -E '^%(left|right|nonassoc)[[:space:]]+<token>[[:space:]]' "$go_sql" \
  | sed -E 's/^%(left|right|nonassoc)[[:space:]]+<token>[[:space:]]+//' \
  | tr ' ' '\n' \
  | rg '^[A-Z][A-Z0-9_]+$' \
  | sort -u > "$tmp_go_prec"
cat "$tmp_go_token" "$tmp_go_prec" | sort -u > "$tmp_go"

echo "lemon token count: $(wc -l < "$tmp_lemon")"
echo "goyacc token count: $(wc -l < "$tmp_go")"

echo
echo "== Missing In goyacc (tokens + precedence symbols) =="
comm -23 "$tmp_lemon" "$tmp_go" || true

echo
echo "== Extra In goyacc (tokens + precedence symbols) =="
comm -13 "$tmp_lemon" "$tmp_go" || true
