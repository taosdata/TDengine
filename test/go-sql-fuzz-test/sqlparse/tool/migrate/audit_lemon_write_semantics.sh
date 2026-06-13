#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
lemon_sql="$repo_root/lemon/sql.y"
lemon_c="$repo_root/lemon/parAstCreater.c"
out_dir="$repo_root/reports/final"
out_file="$out_dir/lemon_write_semantics.md"

mkdir -p "$out_dir"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

defs="$tmp_dir/defs.tsv"
root_calls="$tmp_dir/root_calls.txt"

write_stmt_re='create(Create|Alter|Drop|Delete|Insert|Grant|Revoke|Kill|Balance|Merge|Split|Sync|Trim|Compact|Rollup|Scan|Flush|Use)[A-Za-z0-9_]*Stmt'

rg -n "^SNode\\*\\s+${write_stmt_re}\\s*\\(" "$lemon_c" \
  | sed -E 's/^([0-9]+):SNode\*\s+([A-Za-z0-9_]+).*/\2\t\1/' \
  | sort -u > "$defs"

rg -n -P '^[[:space:]]*(?!//).*pCxt->pRootNode\s*=\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(' "$lemon_sql" \
  | sed -E 's/^([0-9]+):.*pRootNode\s*=\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(.*/\2\t\1/' \
  | sort -u > "$root_calls"

{
  echo "# Lemon Write Semantics Audit"
  echo
  echo "Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo
  echo "Source files:"
  echo "- \`lemon/sql.y\`"
  echo "- \`lemon/parAstCreater.c\`"
  echo
  echo "Scope:"
  echo "- Functions matching: \`${write_stmt_re}\`"
  echo "- Active call: non-comment line containing function invocation in \`sql.y\`"
  echo "- Commented call: comment line containing function invocation in \`sql.y\`"
  echo
  echo "## Function Matrix"
  echo
  echo "| Function | parAstCreater.c line | Active in sql.y | Commented in sql.y | Status |"
  echo "|---|---:|---:|---:|---|"
  while IFS=$'\t' read -r fn line; do
    active_cnt="$( { rg -n -P "^[[:space:]]*(?!//).*\\b${fn}\\s*\\(" "$lemon_sql" || true; } | wc -l)"
    commented_cnt="$( { rg -n -P "^[[:space:]]*//.*\\b${fn}\\s*\\(" "$lemon_sql" || true; } | wc -l)"
    status="active"
    if [[ "$active_cnt" -eq 0 && "$commented_cnt" -gt 0 ]]; then
      status="commented-only"
    elif [[ "$active_cnt" -eq 0 && "$commented_cnt" -eq 0 ]]; then
      status="c-only-unreachable"
    fi
    printf "| %s | %s | %s | %s | %s |\n" "$fn" "$line" "$active_cnt" "$commented_cnt" "$status"
  done < "$defs"
  echo
  echo "## Active sql.y Root Functions Missing in parAstCreater.c"
  echo
  missing=0
  while IFS=$'\t' read -r fn _line; do
    if ! awk -F '\t' -v f="$fn" '$1==f{found=1} END{exit !found}' "$defs"; then
      if [[ "$fn" =~ ^create(Create|Alter|Drop|Delete|Insert|Grant|Revoke|Kill|Balance|Merge|Split|Sync|Trim|Compact|Rollup|Scan|Flush|Use)[A-Za-z0-9_]*Stmt$ ]]; then
        echo "- $fn"
        missing=1
      fi
    fi
  done < "$root_calls"
  if [[ "$missing" -eq 0 ]]; then
    echo "- none"
  fi
} > "$out_file"

echo "$out_file"
