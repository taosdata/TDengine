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

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <path-to-lemon-sql.y>" >&2
  exit 1
fi

src="$1"
if [[ ! -f "$src" ]]; then
  echo "file not found: $src" >&2
  exit 1
fi

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

# 1) precedence declarations
grep -E '^%(left|right|nonassoc)[[:space:]]' "$src" \
  | sed -E 's/^%(left|right|nonassoc)[[:space:]]+//' \
  | tr ' .' '\n\n' \
  | rg '^[A-Z][A-Z0-9_]*$' > "$tmp" || true

# 2) grammar productions (rhs terminals)
grep -E '::=' "$src" \
  | sed -E 's/\{.*$//' \
  | sed -E 's/\([A-Za-z_][A-Za-z0-9_]*\)//g' \
  | tr -c 'A-Za-z0-9_\n' '\n' \
  | rg '^[A-Z][A-Z0-9_]{1,}$' >> "$tmp" || true

sort -u "$tmp"
