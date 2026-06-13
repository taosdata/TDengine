#!/usr/bin/env bash
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
