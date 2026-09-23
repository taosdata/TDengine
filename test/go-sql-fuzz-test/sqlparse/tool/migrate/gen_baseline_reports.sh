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

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
lemon_sql="$repo_root/lemon/sql.y"
go_sql="$repo_root/td_sql.y"
out_dir="$repo_root/reports/baseline"

mkdir -p "$out_dir"

bash "$repo_root/tool/migrate/check_token_parity.sh" "$lemon_sql" "$go_sql" > "$out_dir/token_diff.md"
bash "$repo_root/tool/migrate/check_precedence_parity.sh" "$lemon_sql" "$go_sql" > "$out_dir/precedence_diff.md"

{
  echo "# Rule Inventory"
  echo
  echo "## Lemon grammar production count"
  grep -Ec '::=' "$lemon_sql"
  echo
  echo "## goyacc grammar production count"
  grep -Ec '^[a-z_][a-z0-9_]*:' "$go_sql"
} > "$out_dir/rule_inventory.md"

echo "baseline reports generated under: $out_dir"
